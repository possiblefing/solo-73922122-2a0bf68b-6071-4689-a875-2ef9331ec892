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
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VENV_PY = sys.executable


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

    shutdown_token = "demo-token"
    common_env = {**os.environ, "DATABASE_DSN": dsn, "STORAGE_DIR": raw_dir,
                  "SLOW_MARKER_FILE": os.path.join(raw_dir, "slow_batches.txt"),
                  "SHUTDOWN_TOKEN": shutdown_token,
                  "PYTHONPATH": ROOT}
    port1 = os.environ.get("DEMO_PORT1", "8101")
    port2 = os.environ.get("DEMO_PORT2", "8102")
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

    api2.shutdown()
    print(f"\n[done] logs and raw files kept under {workdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
