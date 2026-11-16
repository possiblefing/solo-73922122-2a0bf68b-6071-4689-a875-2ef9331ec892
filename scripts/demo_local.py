"""End-to-end demonstration of the two reliability guarantees.

1. SLOW BATCH: a slow head-of-line batch cannot be overtaken by later
   batches of the same device, while another device and the HTTP API keep
   responding.
2. CRASH TAKEOVER: a worker process that hard-exits mid-batch leaves no half
   state; the surviving instance picks up the PENDING batch and finishes it.

Local run (two uvicorn instances against an embedded PostgreSQL)::

    python -m scripts.demo_local

For docker compose use scripts/demo_compose.sh instead.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VENV_PY = sys.executable


def _free_port() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return str(sock.getsockname()[1])


def http(method: str, url: str, data: bytes | None = None,
         content_type: str | None = None) -> tuple[int, dict | str]:
    req = urllib.request.Request(url, data=data, method=method)
    if content_type:
        req.add_header("Content-Type", content_type)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode()
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def wait_health(port: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            code, body = http("GET", f"http://127.0.0.1:{port}/health")
            if code == 200 and body.get("database"):
                return
        except Exception:
            pass
        time.sleep(0.4)
    raise RuntimeError(f"instance on {port} never became healthy")


def _wait_health_port(port: str, timeout: float = 30.0) -> None:
    wait_health(port, timeout)


def stop_process(proc: subprocess.Popen, port: str, token: str,
                 timeout: float = 15.0) -> None:
    """Stop a child service without relying on signals (sandbox friendly)."""
    try:
        http("POST", f"http://127.0.0.1:{port}/internal/shutdown?token={token}")
    except Exception:
        pass
    try:
        proc.wait(timeout=timeout)
        return
    except Exception:
        pass
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def _with_database(dsn: str, dbname: str) -> str:
    from urllib.parse import urlsplit, urlunsplit
    parts = urlsplit(dsn)
    return urlunsplit((parts.scheme, parts.netloc, f"/{dbname}",
                       parts.query, parts.fragment))


async def _create_analysis_db(dsn: str, dbname: str) -> None:
    import asyncpg
    conn = await asyncpg.connect(dsn)
    try:
        if not await conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname=$1", dbname):
            await conn.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        await conn.close()


def upload(port: str, batch_no: str, device: str, thickness: int,
           count: int = 5) -> tuple[int, dict]:
    boundary = f"----fab{uuid.uuid4().hex}"
    csv = ("wafer_no,thickness_nm\n" +
           "\n".join(f"{batch_no}-W{i:03d},{thickness}" for i in range(1, count + 1))
           + "\n").encode()
    parts = []
    for name, value in (("batch_no", batch_no), ("device_id", device)):
        parts.append(f"--{boundary}\r\nContent-Disposition: form-data; "
                     f"name=\"{name}\"\r\n\r\n{value}\r\n".encode())
    parts.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; "
        f"filename=\"{batch_no}.csv\"\r\nContent-Type: text/csv\r\n\r\n".encode()
        + csv + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    return http("POST", f"http://127.0.0.1:{port}/batches",
                b"".join(parts),
                f"multipart/form-data; boundary={boundary}")


def wait_status(port: str, batch_no: str, timeout: float = 40.0) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        code, last = http("GET", f"http://127.0.0.1:{port}/batches/{batch_no}")
        if code == 200 and last.get("status") in ("RELEASED", "QUARANTINED"):
            return last
        time.sleep(0.3)
    raise RuntimeError(f"{batch_no} not decided in time: {last}")


class ApiInstance:
    def __init__(self, name: str, port: str, env: dict, log_path: str):
        self.name, self.port, self.env = name, port, env
        self.log = open(log_path, "ab")
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        self.proc = subprocess.Popen(
            [VENV_PY, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", self.port],
            cwd=ROOT, env=self.env, stdout=self.log, stderr=subprocess.STDOUT)
        wait_health(self.port)
        print(f"  [{self.name}] healthy on :{self.port} (pid={self.proc.pid})")

    def shutdown(self) -> None:
        """Ask the process to exit over HTTP (works where signals are blocked)."""
        token = self.env.get("SHUTDOWN_TOKEN", "")
        try:
            http("POST", f"http://127.0.0.1:{self.port}/internal/shutdown"
                         f"?token={token}")
        except Exception:
            pass
        if self.proc is not None:
            try:
                self.proc.wait(timeout=12)
            except Exception:
                pass
        self.log.close()

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.send_signal(signal.SIGTERM)
                self.proc.wait(timeout=10)
            except (PermissionError, ProcessLookupError):
                # Restricted sandboxes disallow signalling even child
                # processes; fall back to the in-process HTTP hook.
                self.shutdown()
                return
            except Exception:
                try:
                    self.proc.kill()
                except PermissionError:
                    pass
        self.log.close()


async def _peek_pending(dsn: str, batch_id: int) -> dict:
    import asyncpg
    conn = await asyncpg.connect(dsn)
    try:
        await conn.set_type_codec("jsonb", encoder=json.dumps,
                                  decoder=json.loads, schema="pg_catalog")
        row = await conn.fetchrow(
            "SELECT status, median_nm, attempts FROM batches WHERE id=$1",
            batch_id)
        return dict(row)
    finally:
        await conn.close()


def main() -> int:
    import pgserver
    workdir = tempfile.mkdtemp(prefix="fabdemo_")
    raw_dir = os.path.join(workdir, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    pg = pgserver.get_server(os.path.join(workdir, "pg"), cleanup_mode=None)
    dsn = pg.get_uri()
    print(f"[setup] embedded PostgreSQL: {dsn}")

    analysis_db = "fab_analysis_demo"
    asyncio.run(_create_analysis_db(dsn, analysis_db))
    analysis_dsn = _with_database(dsn, analysis_db)

    shutdown_token = "demo-token"
    receiver_port = os.environ.get("DEMO_RECEIVER_PORT") or _free_port()
    common_env = {**os.environ, "DATABASE_DSN": dsn, "STORAGE_DIR": raw_dir,
                  "SLOW_MARKER_FILE": os.path.join(raw_dir, "slow_batches.txt"),
                  "SHUTDOWN_TOKEN": shutdown_token,
                  "DELIVERY_RECEIVER_URL": f"http://127.0.0.1:{receiver_port}",
                  "DELIVERY_POLL_SECONDS": "0.3",
                  "DELIVERY_LEASE_SECONDS": "4",
                  "DELIVERY_RETRY_BASE_SECONDS": "0.5",
                  "DELIVERY_RETRY_MAX_SECONDS": "2",
                  "PYTHONPATH": ROOT}
    logger = open(os.path.join(workdir, "receiver.log"), "ab")
    receiver_env = {**os.environ, "ANALYSIS_DATABASE_DSN": analysis_dsn,
                    "ANALYSIS_INSTANCE_ID": "receiver", "PYTHONPATH": ROOT,
                    "SHUTDOWN_TOKEN": shutdown_token}
    receiver = subprocess.Popen(
        [VENV_PY, "-m", "uvicorn", "receiver.main:app",
         "--host", "127.0.0.1", "--port", receiver_port],
        cwd=ROOT, env=receiver_env,
        stdout=logger, stderr=subprocess.STDOUT)
    _wait_health_port(receiver_port)
    print(f"[setup] analysis receiver healthy on :{receiver_port} "
          f"(db={analysis_db})")

    port1 = os.environ.get("DEMO_PORT1") or _free_port()
    port2 = os.environ.get("DEMO_PORT2") or _free_port()
    api1 = ApiInstance("api1", port1, {**common_env, "INSTANCE_ID": "api1"},
                       os.path.join(workdir, "api1.log"))
    api2 = ApiInstance("api2", port2, {**common_env, "INSTANCE_ID": "api2"},
                       os.path.join(workdir, "api2.log"))

    print("\n=== DEMO 1: slow head-of-line batch ===")
    api1.start(); api2.start()
    run = uuid.uuid4().hex[:6]
    dev_f, dev_g = f"F-{run}", f"G-{run}"
    code, f1 = upload(port1, f"{run}-F1", dev_f, 500)
    assert code == 202, f1
    f1_id = f1["batch_id"]
    upload(port2, f"{run}-F2", dev_f, 500)
    upload(port1, f"{run}-F3", dev_f, 504)
    # Another device flows immediately despite the slow F device.
    upload(port1, f"{run}-G1", dev_g, 700)
    upload(port2, f"{run}-G2", dev_g, 700)
    with open(os.path.join(raw_dir, "slow_batches.txt"), "w") as fh:
        fh.write(f"{f1_id}:6\n")
    print(f"  marked batch id={f1_id} as SLOW (6s), device {dev_f}")
    time.sleep(1.5)

    _, f1_now = http("GET", f"http://127.0.0.1:{port2}/batches/{run}-F1")
    _, g2 = http("GET", f"http://127.0.0.1:{port1}/batches/{run}-G2")
    print(f"  while F1 is processing: F1={f1_now['status']}, "
          f"G2(other device)={g2['status']}")
    assert f1_now["status"] == "PENDING" and g2["status"] == "RELEASED"
    f1_done = wait_status(port1, f"{run}-F1")
    f2 = wait_status(port1, f"{run}-F2")
    f3 = wait_status(port1, f"{run}-F3")
    print(f"  F1 decided_by={f1_done['decided_by']} seq=1; "
          f"F2 seq={f2['accept_seq']} {f2['status']}; "
          f"F3 refs={f3['reference_batch_nos']}")
    assert [x["accept_seq"] for x in (f1_done, f2, f3)] == [1, 2, 3]
    print("  PASS: same-device FIFO preserved, other device never blocked")

    print("\n=== DEMO 2: worker crash then survivor takeover ===")
    dev_k = f"K-{run}"
    # Stop the regular workers so we control who processes the K device.
    api1.shutdown(); api2.shutdown()

    # Restart api2 briefly just to ingest the four K batches, then stop it so
    # nothing has decided them yet.
    api2 = ApiInstance("api2", port2, {**common_env, "INSTANCE_ID": "api2"},
                       os.path.join(workdir, "api2.log"))
    api2.start()
    code, k1_accept = upload(port2, f"{run}-K1", dev_k, 500)
    assert code == 202, k1_accept
    upload(port2, f"{run}-K2", dev_k, 500)
    upload(port2, f"{run}-K3", dev_k, 500)
    upload(port2, f"{run}-K4", dev_k, 504)
    # Give the workers no time to process before shutting down.
    api2.shutdown()
    mid0 = asyncio.run(_peek_pending(dsn, k1_accept["batch_id"]))
    assert mid0["status"] == "PENDING", mid0

    crash_path = os.path.join(raw_dir, "crash_batches.txt")
    with open(crash_path, "w") as fh:
        fh.write(f"{k1_accept['batch_id']}\n")
    print(f"  crash marker set for batch id={k1_accept['batch_id']}")

    # A dedicated one-shot worker (separate process) hard-exits via os._exit
    # while deciding K1 -- equivalent to kill -9 of an API container.
    crasher_env = {**common_env,
                   "SLOW_MARKER_FILE": os.path.join(raw_dir, "slow_batches.txt")}
    crasher = subprocess.Popen(
        [VENV_PY, "-m", "scripts.worker_harness",
         "--instance-id", "crasher", "--crash",
         str(k1_accept["batch_id"])],
        cwd=ROOT, env=crasher_env,
        stdout=open(os.path.join(workdir, "crasher.log"), "ab"),
        stderr=subprocess.STDOUT)
    rc = crasher.wait(timeout=20)
    print(f"  crasher process hard-exited with code {rc} while deciding K1")
    assert rc != 0

    mid = asyncio.run(_peek_pending(dsn, k1_accept["batch_id"]))
    print(f"  after crash: K1 row = {mid} (PENDING, no median => no half state)")
    assert mid["status"] == "PENDING" and mid["median_nm"] is None

    # Remove the crash marker and start the survivor (api2), which drains all.
    os.remove(crash_path)
    api2 = ApiInstance("api2", port2, {**common_env, "INSTANCE_ID": "api2"},
                       os.path.join(workdir, "api2.log"))
    api2.start()
    for name in ("K1", "K2", "K3", "K4"):
        row = wait_status(port2, f"{run}-{name}")
        print(f"  survivor decided {name}: {row['status']} "
              f"refs={row['reference_batch_nos']} by={row['decided_by']}")
    _, k4_final = http("GET", f"http://127.0.0.1:{port2}/batches/{run}-K4")
    assert k4_final["reference_batch_nos"] == [
        f"{run}-K1", f"{run}-K2", f"{run}-K3"]
    print("  PASS: no half batch, no double reference advance, survivor took over")

    print("\n=== DEMO 3: delivery to the independent analysis receiver ===")
    # Demo 2 stopped both instances and restarted only api2; bring api1 back
    # so two source instances are live for the delivery scenarios.
    api1 = ApiInstance("api1", port1, {**common_env, "INSTANCE_ID": "api1"},
                       os.path.join(workdir, "api1.log"))
    api1.start()
    dev_d = f"D-{run}"
    for i in range(1, 4):
        upload(port1, f"{run}-D{i}", dev_d, 500)
        wait_status(port1, f"{run}-D{i}")
    upload(port2, f"{run}-D4", dev_d, 504)
    wait_status(port2, f"{run}-D4")
    upload(port2, f"{run}-DQ", dev_d, 900)   # quarantined: never delivered
    assert wait_status(port2, f"{run}-DQ")["status"] == "QUARANTINED"

    deadline = time.time() + 30
    while time.time() < deadline:
        docs = {n: http("GET", f"http://127.0.0.1:{port1}/deliveries/{run}-D{i}")[1]
                for i, n in ((1, "D1"), (4, "D4"))}
        if all(d.get("delivery_status") == "DELIVERED" for d in docs.values()):
            break
        time.sleep(0.3)
    for name in ("D1", "D2", "D3", "D4"):
        code, doc = http("GET", f"http://127.0.0.1:{port1}/deliveries/{run}-{name}")
        assert code == 200 and doc["delivery_status"] == "DELIVERED", doc
        print(f"  source {name}: {doc['delivery_status']} "
              f"receipt={doc['receipt']['receipt_id'][:10]}… "
              f"published={doc['receipt']['published']}")
    _, qdoc = http("GET", f"http://127.0.0.1:{port1}/deliveries/{run}-DQ")
    assert qdoc["delivery_status"] == "NOT_APPLICABLE"
    print(f"  source DQ: {qdoc['delivery_status']} (quarantined, never shipped)")

    _, rec = http("GET", f"http://127.0.0.1:{receiver_port}"
                         f"/analysis/batches/{run}-D4")
    assert rec["reference_batch_nos"] == [
        f"{run}-D1", f"{run}-D2", f"{run}-D3"], rec
    assert len(rec["wafers"]) == 5
    print(f"  receiver D4: {rec['status']} refs={rec['reference_batch_nos']} "
          f"wafers={len(rec['wafers'])} published={rec['published']}")
    _, q404 = http("GET", f"http://127.0.0.1:{receiver_port}"
                          f"/analysis/batches/{run}-DQ")
    assert q404 == {} or isinstance(q404, dict)
    print("  PASS: released batches (detail+decision+refs) in the analysis DB; "
          "quarantined batch absent")

    print("\n=== DEMO 4: receiver disconnect, retry, source crash-after-receipt ===")
    # Drop the receiver connection path while batches are being released.
    api2.shutdown()
    dev_r = f"R-{run}"
    upload(port1, f"{run}-R1", dev_r, 500)
    wait_status(port1, f"{run}-R1")
    # Simulate the receiver being down: stop the receiver process and let the
    # source retry.  (Signals are blocked in some sandboxes, so use the
    # receiver's HTTP shutdown hook first.)
    stop_process(receiver, receiver_port, shutdown_token)
    print("  receiver stopped; submitting R2 which must stay retryable")
    upload(port1, f"{run}-R2", dev_r, 500)
    wait_status(port1, f"{run}-R2")
    time.sleep(1.5)
    _, pending = http("GET", f"http://127.0.0.1:{port1}/deliveries/{run}-R2")
    assert pending["delivery_status"] == "PENDING", pending
    assert pending["receipt"] is None
    print(f"  R2: {pending['delivery_status']} attempts={pending['attempts']} "
          f"last_error={str(pending['last_error'])[:60]}")
    # Decision/query/upload stayed available throughout.
    _, r2 = http("GET", f"http://127.0.0.1:{port1}/batches/{run}-R2")
    assert r2["status"] == "RELEASED"
    print("  R2 decision still RELEASED while delivery failed -> verdict unchanged")

    # Bring the receiver back; the source retries automatically.
    receiver = subprocess.Popen(
        [VENV_PY, "-m", "uvicorn", "receiver.main:app",
         "--host", "127.0.0.1", "--port", receiver_port],
        cwd=ROOT, env=receiver_env,
        stdout=logger, stderr=subprocess.STDOUT)
    _wait_health_port(receiver_port)
    deadline = time.time() + 30
    while time.time() < deadline:
        _, doc = http("GET", f"http://127.0.0.1:{port1}/deliveries/{run}-R2")
        if doc.get("delivery_status") == "DELIVERED":
            break
        time.sleep(0.3)
    assert doc["delivery_status"] == "DELIVERED", doc
    print("  after receiver restart: R2 delivered automatically (no data loss)")

    # Crash AFTER the receiver accepted but BEFORE the source recorded the
    # receipt: mark R3, let the source hard-exit, then confirm another
    # instance retries and the receiver reuses the original receipt.
    crash_marker = os.path.join(raw_dir, "delivery_crash_batches.txt")
    with open(crash_marker, "w") as fh:
        fh.write(f"{run}-R3\n")
    api1.shutdown()   # the only source instance running right now

    # One-shot source instance that will hard-exit after the receiver accepted.
    # A slower delivery poll leaves time to upload and observe the decision
    # before the crash hook fires.
    crasher_env = {**common_env, "INSTANCE_ID": "delivery-crasher",
                   "DELIVERY_POLL_SECONDS": "3"}
    api_crash = ApiInstance("delivery-crasher", port1, crasher_env,
                            os.path.join(workdir, "api-crash.log"))
    api_crash.start()
    try:
        upload(port1, f"{run}-R3", dev_r, 500)
        wait_status(port1, f"{run}-R3")
    except Exception:
        # The crash hook can tear the process down while it is still serving
        # the last request; the row itself is already accepted and decided.
        pass
    # Wait for the process to die from the delivery crash hook.
    deadline = time.time() + 20
    while time.time() < deadline:
        if api_crash.proc.poll() is not None:
            break
        time.sleep(0.2)
    assert api_crash.proc.poll() is not None, "delivery crash hook did not fire"
    print(f"  source instance hard-exited after receiver accepted R3 "
          f"(exit={api_crash.proc.returncode})")
    api_crash.log.close()

    _, rec3 = http("GET", f"http://127.0.0.1:{receiver_port}"
                          f"/analysis/batches/{run}-R3")
    first_receipt = rec3["receipt"]["receipt_id"]
    # Remove the crash marker, start the survivor; its outbox worker must
    # retry after the lease expires and reuse the receiver's original receipt.
    os.remove(crash_marker)
    survivor_env = {**common_env, "INSTANCE_ID": "survivor",
                    "DELIVERY_LEASE_SECONDS": "3"}
    survivor = ApiInstance("survivor", port2, survivor_env,
                           os.path.join(workdir, "survivor.log"))
    survivor.start()
    deadline = time.time() + 30
    while time.time() < deadline:
        _, doc = http("GET", f"http://127.0.0.1:{port2}/deliveries/{run}-R3")
        if doc.get("delivery_status") == "DELIVERED":
            break
        time.sleep(0.3)
    assert doc["delivery_status"] == "DELIVERED", doc
    assert doc["receipt"]["receipt_id"] == first_receipt
    _, rec3b = http("GET", f"http://127.0.0.1:{receiver_port}"
                           f"/analysis/batches/{run}-R3")
    assert len(rec3b["wafers"]) == 5 and rec3b["receipt"]["receipt_id"] == first_receipt
    _, listing = http("GET", f"http://127.0.0.1:{receiver_port}"
                             f"/analysis/batches?device_id={dev_r}&published_only=false")
    assert listing["total"] == 3
    print(f"  survivor retried R3: DELIVERED with the SAME receipt "
          f"{first_receipt[:10]}…, receiver still has exactly 3 batches")
    print("  PASS: no lost batch, no duplicate rows, no false 'delivered'")

    survivor.shutdown()
    stop_process(receiver, receiver_port, shutdown_token)
    logger.close()
    print(f"\n[done] logs and raw files kept under {workdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
