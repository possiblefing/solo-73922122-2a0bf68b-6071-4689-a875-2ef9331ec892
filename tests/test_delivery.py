"""Delivery end-to-end tests: source outbox -> independent analysis receiver.

Every test uses the real source FastAPI app + real source PostgreSQL (the
``client`` fixture) and the real receiver app + a separate PostgreSQL database
(the ``receiver`` fixture).  A deterministic in-process sender is injected so
we can simulate receiver outages without a network listener; the HTTP transport
itself is covered by a dedicated test that starts a real uvicorn receiver.
"""
import asyncio
import time

import pytest

from app.db import create_pool
from app.delivery import DeliveryWorker, claim_due, enqueue_batch_by_no
from tests.conftest import InProcessSender, submit, uniform_csv, wait_status


async def _drain_delivery(ac, receiver_client, *, max_sweeps=40,
                          fail_times=0, monkeypatch=None):
    """Run delivery sweeps until the outbox has nothing in flight."""
    pool = await create_pool()
    sender = InProcessSender(receiver_client, fail_times=fail_times)
    worker = DeliveryWorker(pool, "test-delivery", sender=sender)
    for _ in range(max_sweeps):
        await worker.run_once()
        async with pool.acquire() as conn:
            pending = await conn.fetchval(
                "SELECT count(*) FROM delivery_outbox "
                "WHERE status IN ('PENDING','IN_FLIGHT')")
            due = await conn.fetchval(
                "SELECT count(*) FROM delivery_outbox "
                "WHERE status='PENDING' AND next_attempt_at <= now()")
        if pending == 0:
            break
        if due == 0:
            # Backoff pending: shorten so the next sweep retries promptly.
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE delivery_outbox SET next_attempt_at=now() "
                    "WHERE status='PENDING'")
        await asyncio.sleep(0.01)
    return sender, pool


@pytest.mark.asyncio
async def test_released_batches_delivered_and_queryable(client, receiver):
    ac = client
    for i in range(1, 4):
        await submit(ac, f"DB{i}", "EQ-DB", uniform_csv(f"db{i}", 4, 500))
        await wait_status(ac, f"DB{i}", "RELEASED")
    # A quarantined batch: never enqueued, never delivered.
    await submit(ac, "DBQ", "EQ-DB", uniform_csv("dbq", 4, 900))
    await wait_status(ac, "DBQ", "QUARANTINED")

    # A dependent released batch (accept_seq 5, refs DB1..DB3).
    await submit(ac, "DB4", "EQ-DB", uniform_csv("db4", 4, 504))
    await wait_status(ac, "DB4", "RELEASED")

    await _drain_delivery(ac, receiver)

    # Source: every released batch reports DELIVERED with a receipt; the
    # quarantined batch is explicitly not applicable.
    for name in ("DB1", "DB2", "DB3", "DB4"):
        r = await ac.get(f"/deliveries/{name}")
        assert r.status_code == 200, r.text
        doc = r.json()
        assert doc["delivery_status"] == "DELIVERED", doc
        assert doc["receipt"]["batch_no"] == name
        assert len(doc["receipt"]["content_sha256"]) == 64
    q = (await ac.get("/deliveries/DBQ")).json()
    assert q["delivery_status"] == "NOT_APPLICABLE"
    assert q["receipt"] is None

    # Receiver: full detail, decision and reference batch numbers per batch.
    r = await receiver.get("/analysis/batches/DB4")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "RELEASED"
    assert body["reference_batch_nos"] == ["DB1", "DB2", "DB3"]
    assert body["median_nm"] == 504
    assert [w["wafer_no"] for w in body["wafers"]] == [
        "db4-W001", "db4-W002", "db4-W003", "db4-W004"]
    assert body["published"] is True

    # Quarantined batch is absent from the analysis database entirely.
    assert (await receiver.get("/analysis/batches/DBQ")).status_code == 404
    data = (await receiver.get("/analysis/data")).json()
    assert {i["batch_no"] for i in data["items"]} == {"DB1", "DB2", "DB3", "DB4"}


@pytest.mark.asyncio
async def test_quarantined_never_gets_outbox_row(client, receiver):
    ac = client
    for i in range(1, 4):
        await submit(ac, f"N{i}", "EQ-N", uniform_csv(f"n{i}", 3, 500))
        await wait_status(ac, f"N{i}", "RELEASED")
    await submit(ac, "NQ", "EQ-N", uniform_csv("nq", 3, 900))
    await wait_status(ac, "NQ", "QUARANTINED")
    await _drain_delivery(ac, receiver)
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT batch_no, status FROM delivery_outbox ORDER BY batch_no")
    assert [r["batch_no"] for r in rows] == ["N1", "N2", "N3"]
    assert all(r["status"] == "DELIVERED" for r in rows)


@pytest.mark.asyncio
async def test_identical_redelivery_reuses_receipt_different_content_rejected(
        client, receiver):
    ac = client
    await submit(ac, "RC1", "EQ-RC", uniform_csv("rc1", 3, 500))
    detail = await wait_status(ac, "RC1", "RELEASED")
    await _drain_delivery(ac, receiver)

    pool = await create_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT payload, content_sha256 FROM delivery_outbox "
            "WHERE batch_no='RC1'")
    payload = row["payload"]

    # First receipt, from the receiver.
    first = (await receiver.get("/analysis/receipts/RC1")).json()
    assert first["content_sha256"] == row["content_sha256"]

    # Re-deliver identical content: 201, original receipt, no duplicate rows.
    r = await receiver.post(
        "/analysis/batches",
        json={"source_instance": "dup", "batch": payload})
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["reused"] is True
    assert body["receipt"]["receipt_id"] == first["receipt_id"]
    assert body["receipt"]["content_sha256"] == first["content_sha256"]

    # Same batch number, different content -> 409.
    tampered = dict(payload)
    tampered["wafers"] = [{"wafer_no": "x", "thickness_nm": 1}]
    r = await receiver.post(
        "/analysis/batches",
        json={"source_instance": "bad", "batch": tampered})
    assert r.status_code == 409, r.text
    assert r.json()["existing"]["batch_no"] == "RC1"

    # Still exactly one ordered listing and one wafer set.
    detail = (await receiver.get("/analysis/batches/RC1")).json()
    assert [w["wafer_no"] for w in detail["wafers"]] == [
        "rc1-W001", "rc1-W002", "rc1-W003"]


@pytest.mark.asyncio
async def test_dependent_batch_held_until_reference_delivered(client, receiver):
    ac = client
    for i in range(1, 4):
        await submit(ac, f"P{i}", "EQ-PD", uniform_csv(f"p{i}", 3, 500))
        await wait_status(ac, f"P{i}", "RELEASED")
    await submit(ac, "P4", "EQ-PD", uniform_csv("p4", 3, 504))
    await wait_status(ac, "P4", "RELEASED")

    # Build the outbox without delivering anything yet.
    await _drain_no_send()
    pool = await create_pool()
    async with pool.acquire() as conn:
        p4 = await conn.fetchrow(
            "SELECT payload FROM delivery_outbox WHERE batch_no='P4'")

    # Deliver ONLY the dependent batch first: it must be stored but not
    # published because P1..P3 are missing.
    r = await receiver.post(
        "/analysis/batches", json={"batch": p4["payload"]})
    assert r.status_code == 201, r.text
    receipt = r.json()["receipt"]
    assert receipt["published"] is False
    assert (await receiver.get("/analysis/batches/P4")).json()["published"] is False
    assert (await receiver.get("/analysis/data")).json()["total"] == 0
    blocked = (await receiver.get("/analysis/blocked")).json()
    assert blocked["count"] == 1
    assert blocked["items"][0]["missing_reference_batch_nos"] == ["P1", "P2", "P3"]

    # Now deliver the references out of order; P4 is published only after all
    # three are present.
    async with pool.acquire() as conn:
        refs = await conn.fetch(
            "SELECT payload FROM delivery_outbox "
            "WHERE batch_no IN ('P1','P2','P3') ORDER BY batch_no DESC")
    for i, ref in enumerate(refs):
        r = await receiver.post("/analysis/batches", json={"batch": ref["payload"]})
        assert r.status_code == 201
        if i < 2:
            assert (await receiver.get("/analysis/batches/P4")).json()["published"] is False
    assert (await receiver.get("/analysis/batches/P4")).json()["published"] is True
    assert (await receiver.get("/analysis/blocked")).json()["count"] == 0

    # The reused receipt now reports the current published state truthfully.
    r = await receiver.post(
        "/analysis/batches", json={"batch": p4["payload"]})
    assert r.json()["reused"] is True
    assert r.json()["receipt"]["published"] is True

    # End-to-end delivery sweep also reports success for the whole chain.
    await _drain_delivery(ac, receiver)
    doc = (await ac.get("/deliveries/P4")).json()
    assert doc["delivery_status"] == "DELIVERED"
    assert doc["receipt"]["published"] is True


async def _drain_no_send():
    """Create outbox rows for all released batches without delivering them."""
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT batch_no FROM batches WHERE status='RELEASED'")
    for row in rows:
        await enqueue_batch_by_no(pool, row["batch_no"])


@pytest.mark.asyncio
async def test_failed_delivery_retries_and_rule_is_unchanged(client, receiver):
    ac = client
    for i in range(1, 4):
        await submit(ac, f"RT{i}", "EQ-RT", uniform_csv(f"rt{i}", 3, 500))
        await wait_status(ac, f"RT{i}", "RELEASED")
    before = (await ac.get("/batches/RT1")).json()
    ok, pool = await _drain_delivery(ac, receiver, fail_times=2)
    assert ok.calls >= 3

    # Decision untouched by failed deliveries.
    after = (await ac.get("/batches/RT1")).json()
    for key in ("status", "median_nm", "deviation_nm", "reference_batch_nos"):
        assert after[key] == before[key]

    # All three eventually delivered; no duplicates on the receiver.
    for name in ("RT1", "RT2", "RT3"):
        doc = (await ac.get(f"/deliveries/{name}")).json()
        assert doc["delivery_status"] == "DELIVERED"
        assert doc["attempts"] >= 1
    listing = (await receiver.get("/analysis/batches")).json()
    assert listing["total"] == 3

    # A receiver outage leaves rows retryable, never falsely DELIVERED.
    await submit(ac, "RT4", "EQ-RT", uniform_csv("rt4", 3, 500))
    await wait_status(ac, "RT4", "RELEASED")
    pool2 = await create_pool()
    worker = DeliveryWorker(pool2, "fail-demo", sender=_AlwaysFail())
    await worker.run_once()
    doc = (await ac.get("/deliveries/RT4")).json()
    assert doc["delivery_status"] == "PENDING"
    assert doc["receipt"] is None
    assert doc["last_error"]


class _AlwaysFail:
    def __init__(self):
        self.calls = 0

    async def __call__(self, claim, source_instance, attempt):
        from app.delivery import SendOutcome
        self.calls += 1
        return SendOutcome("retryable", error="receiver down")


class _DropAfterCommitSender:
    """POSTs successfully, then behaves as if the response never arrived."""

    def __init__(self, receiver_client):
        self.client = receiver_client
        self.dropped = 0

    async def __call__(self, claim, source_instance, attempt):
        from app.delivery import SendOutcome
        resp = await self.client.post(
            "/analysis/batches",
            json={"source_instance": source_instance, "batch": claim.payload})
        assert resp.status_code == 201
        if self.dropped == 0:
            self.dropped += 1
            return SendOutcome("retryable",
                               error="transport: connection reset after send")
        return SendOutcome("delivered", receipt=resp.json()["receipt"])


@pytest.mark.asyncio
async def test_connection_drop_after_receiver_commit_is_retried_safely(
        client, receiver):
    """Receiver committed, the HTTP response was lost -> retry, no duplicate."""
    ac = client
    await submit(ac, "DP1", "EQ-DP", uniform_csv("dp1", 3, 500))
    await wait_status(ac, "DP1", "RELEASED")
    await _drain_no_send()

    pool = await create_pool()
    sender = _DropAfterCommitSender(receiver)
    worker = DeliveryWorker(pool, "dropper", sender=sender)
    await worker.run_once()

    doc = (await ac.get("/deliveries/DP1")).json()
    assert doc["delivery_status"] == "PENDING", doc
    assert doc["receipt"] is None
    first = (await receiver.get("/analysis/receipts/DP1")).json()

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE delivery_outbox SET next_attempt_at=now() WHERE batch_no='DP1'")
    await worker.run_once()
    doc = (await ac.get("/deliveries/DP1")).json()
    assert doc["delivery_status"] == "DELIVERED", doc
    assert doc["receipt"]["receipt_id"] == first["receipt_id"]
    assert (await receiver.get("/analysis/batches")).json()["total"] == 1
    detail = (await receiver.get("/analysis/batches/DP1")).json()
    assert len(detail["wafers"]) == 3


class _SlowSender:
    def __init__(self, receiver_client, delay: float):
        self.client = receiver_client
        self.delay = delay

    async def __call__(self, claim, source_instance, attempt):
        from app.delivery import SendOutcome
        await asyncio.sleep(self.delay)
        resp = await self.client.post(
            "/analysis/batches",
            json={"source_instance": source_instance, "batch": claim.payload})
        if not 200 <= resp.status_code < 300:
            return SendOutcome("retryable", error=resp.text)
        return SendOutcome("delivered", receipt=resp.json()["receipt"])


@pytest.mark.asyncio
async def test_slow_delivery_does_not_block_upload_decision_or_query(
        client, receiver, monkeypatch):
    """A 3s delivery call must not delay intake, decisions or queries."""
    ac = client
    pool = await create_pool()
    sender = _SlowSender(receiver, delay=3.0)
    worker = DeliveryWorker(pool, "slow-delivery", sender=sender)

    # Seed three released batches, then keep the delivery worker busy on them.
    for i in range(1, 4):
        await submit(ac, f"SL{i}", "EQ-SL", uniform_csv(f"sl{i}", 3, 500))
        await wait_status(ac, f"SL{i}", "RELEASED")

    started = time.monotonic()
    sweep = asyncio.create_task(worker.run_once())
    await asyncio.sleep(0.1)  # let the worker pick up its first claim

    # While delivery is sleeping in its own task, everything else responds.
    r = await asyncio.wait_for(
        submit(ac, "SL4", "EQ-SL", uniform_csv("sl4", 3, 500)), timeout=2)
    assert r.status_code == 202
    detail = await asyncio.wait_for(
        wait_status(ac, "SL4", "RELEASED", timeout=5), timeout=5)
    assert detail["status"] == "RELEASED"
    got = await asyncio.wait_for(ac.get("/batches/SL1"), timeout=2)
    assert got.status_code == 200
    data = await asyncio.wait_for(ac.get("/data/released"), timeout=2)
    assert data.status_code == 200
    elapsed_while_delivering = time.monotonic() - started
    assert elapsed_while_delivering < 2.9, elapsed_while_delivering

    await sweep
    for name in ("SL1", "SL2", "SL3"):
        assert (await ac.get(f"/deliveries/{name}")).json()["delivery_status"] \
            == "DELIVERED"


@pytest.mark.asyncio
async def test_delivery_worker_crash_after_accept_recovered_by_harness(
        client, receiver, tmp_path):
    """Real ``os._exit`` in a separate process after the receiver accepted.

    The harness process dies while recording the receipt; a second harness
    process (the surviving source instance) retries after the lease expires,
    the receiver returns the original receipt and storage is unchanged.
    """
    import os
    import socket
    import sys

    ac = client
    await submit(ac, "HX1", "EQ-HX", uniform_csv("hx1", 3, 500))
    await wait_status(ac, "HX1", "RELEASED")
    await _drain_no_send()

    # The harness needs a real HTTP listener for the receiver.  It shares the
    # same analysis database and global pool as the ``receiver`` fixture, so
    # the fixture's ASGI client remains valid for assertions.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    import uvicorn
    from receiver.main import app as receiver_app

    config = uvicorn.Config(receiver_app, host="127.0.0.1", port=port,
                            log_level="warning", lifespan="on")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.05)
    assert server.started

    env = {
        **os.environ,
        "DELIVERY_RECEIVER_URL": f"http://127.0.0.1:{port}",
        "DELIVERY_LEASE_SECONDS": "2",
        "DELIVERY_POLL_SECONDS": "0.2",
        "SLOW_MARKER_FILE": str(tmp_path / "slow.txt"),
        "PYTHONPATH": os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    }
    marker = tmp_path / "delivery_crash_batches.txt"
    marker.write_text("HX1\n")

    harness = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "scripts", "delivery_harness.py")
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, harness, "--instance-id", "crasher",
            "--until-idle", "--idle-timeout", "10",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            env=env)
        out, err = await asyncio.wait_for(proc.communicate(), timeout=30)
        assert proc.returncode == 1, (proc.returncode, out, err)

        # Receiver accepted it; the source still reports IN_FLIGHT (never
        # falsely DELIVERED).
        doc = (await ac.get("/deliveries/HX1")).json()
        assert doc["delivery_status"] == "IN_FLIGHT", doc
        assert doc["receipt"] is None
        accepted = (await receiver.get("/analysis/batches/HX1")).json()
        original_receipt = accepted["receipt"]["receipt_id"]

        # Survivor: remove crash marker, wait for lease expiry, retry.
        marker.unlink()
        pool = await create_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE delivery_outbox SET lease_expires_at=now() - interval "
                "'1 second' WHERE batch_no='HX1'")
        proc2 = await asyncio.create_subprocess_exec(
            sys.executable, harness, "--instance-id", "survivor",
            "--until-idle", "--idle-timeout", "20",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            env=env)
        out2, err2 = await asyncio.wait_for(proc2.communicate(), timeout=40)
        assert proc2.returncode == 0, (proc2.returncode, out2, err2)

        doc = (await ac.get("/deliveries/HX1")).json()
        assert doc["delivery_status"] == "DELIVERED"
        assert doc["receipt"]["receipt_id"] == original_receipt
        again = (await receiver.get("/analysis/batches/HX1")).json()
        assert again["receipt"]["receipt_id"] == original_receipt
        assert len(again["wafers"]) == 3
        listing = (await receiver.get("/analysis/batches")).json()
        assert listing["total"] == 1
    finally:
        server.should_exit = True
        await server_task


@pytest.mark.asyncio
async def test_source_crash_after_receiver_commit_is_retried_without_duplicate(
        client, receiver, monkeypatch):
    """Simulate: receiver commits, source dies before recording the receipt.

    The outbox row is claimed and marked IN_FLIGHT, the receiver accepts the
    batch, but the source process "dies" (we simply do not mark DELIVERED).
    After the lease expires another instance retries; the receiver returns the
    original receipt and storage is unchanged.
    """
    ac = client
    await submit(ac, "CR1", "EQ-CR", uniform_csv("cr1", 3, 500))
    await wait_status(ac, "CR1", "RELEASED")
    await _drain_no_send()

    pool = await create_pool()
    # Claim as instance A, deliver, but never record the receipt.
    claims = await claim_due(pool, "instance-A")
    assert [c.batch_no for c in claims] == ["CR1"]
    r = await receiver.post("/analysis/batches",
                            json={"source_instance": "instance-A",
                                  "batch": claims[0].payload})
    assert r.status_code == 201
    first_receipt_id = r.json()["receipt"]["receipt_id"]

    # Row is still IN_FLIGHT, so it is not reported as delivered.
    doc = (await ac.get("/deliveries/CR1")).json()
    assert doc["delivery_status"] == "IN_FLIGHT"
    assert doc["receipt"] is None

    # Lease expires -> instance B claims and retries.
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE delivery_outbox SET lease_expires_at=now() - interval '1 second' "
            "WHERE batch_no='CR1'")
    claims_b = await claim_due(pool, "instance-B")
    assert [c.batch_no for c in claims_b] == ["CR1"]
    r = await receiver.post("/analysis/batches",
                            json={"source_instance": "instance-B",
                                  "batch": claims_b[0].payload})
    assert r.status_code == 201
    assert r.json()["reused"] is True
    assert r.json()["receipt"]["receipt_id"] == first_receipt_id

    from app.delivery import mark_delivered
    assert await mark_delivered(pool, claims_b[0], "instance-B", r.json()["receipt"])
    doc = (await ac.get("/deliveries/CR1")).json()
    assert doc["delivery_status"] == "DELIVERED"
    assert doc["receipt"]["receipt_id"] == first_receipt_id

    # Exactly one stored batch and one wafer set.
    detail = (await receiver.get("/analysis/batches/CR1")).json()
    assert len(detail["wafers"]) == 3


@pytest.mark.asyncio
async def test_two_source_instances_do_not_double_claim(client, receiver):
    ac = client
    for i in range(1, 5):
        await submit(ac, f"TW{i}", "EQ-TW", uniform_csv(f"tw{i}", 2, 500))
        await wait_status(ac, f"TW{i}", "RELEASED")
    await _drain_no_send()

    pool = await create_pool()
    a = await claim_due(pool, "inst-A", limit=10)
    b = await claim_due(pool, "inst-B", limit=10)
    assert a and not b, (len(a), len(b))
    assert {c.batch_no for c in a} == {"TW1", "TW2", "TW3", "TW4"}

    # Concurrent posting of the same four claims from both instances still
    # yields exactly four stored batches (receiver idempotency).
    async def post_many(claims, tag):
        results = []
        for claim in claims:
            r = await receiver.post(
                "/analysis/batches",
                json={"source_instance": tag, "batch": claim.payload})
            results.append(r.status_code)
        return results

    codes = await asyncio.gather(post_many(a, "A"), post_many(a, "A-dup"))
    assert all(c == 201 for pair in codes for c in pair)
    listing = (await receiver.get("/analysis/batches")).json()
    assert listing["total"] == 4


@pytest.mark.asyncio
async def test_reconcile_backfills_preexisting_released_batches(
        client, receiver):
    ac = client
    for i in range(1, 4):
        await submit(ac, f"BF{i}", "EQ-BF", uniform_csv(f"bf{i}", 2, 500))
        await wait_status(ac, f"BF{i}", "RELEASED")
    # Simulate pre-deployment state: released batches without outbox rows.
    pool = await create_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM delivery_outbox")
    r = await ac.post("/deliveries/reconcile")
    assert r.status_code == 202
    assert r.json()["created"] == 3
    # Idempotent: a second call creates nothing.
    r = await ac.post("/deliveries/reconcile")
    assert r.json()["created"] == 0
    await _drain_delivery(ac, receiver)
    listing = (await receiver.get("/analysis/batches")).json()
    assert listing["total"] == 3


@pytest.mark.asyncio
async def test_delivery_listing_and_status_filter(client, receiver):
    ac = client
    for i in range(1, 4):
        await submit(ac, f"LS{i}", "EQ-LS", uniform_csv(f"ls{i}", 2, 500))
        await wait_status(ac, f"LS{i}", "RELEASED")
    await _drain_no_send()

    all_rows = (await ac.get("/deliveries")).json()
    assert all_rows["total"] == 3
    assert {r["batch_no"] for r in all_rows["items"]} == {"LS1", "LS2", "LS3"}
    # The test app was started without DELIVERY_RECEIVER_URL; delivery is
    # driven explicitly with an injected sender in these tests.
    assert all_rows["receiver_configured"] is False

    pending = (await ac.get("/deliveries", params={"status": "PENDING"})).json()
    assert pending["total"] == 3
    delivered = (await ac.get("/deliveries", params={"status": "DELIVERED"})).json()
    assert delivered["total"] == 0

    # Invalid status filter is rejected by FastAPI validation.
    bad = await ac.get("/deliveries", params={"status": "BOGUS"})
    assert bad.status_code == 422

    await _drain_delivery(ac, receiver)
    delivered = (await ac.get("/deliveries", params={"status": "DELIVERED"})).json()
    assert delivered["total"] == 3
    assert all(r["receipt"] for r in delivered["items"])
    by_device = (await ac.get("/deliveries", params={"device_id": "EQ-LS"})).json()
    assert by_device["total"] == 3


@pytest.mark.asyncio
async def test_two_delivery_workers_partition_work_exactly_once(
        client, receiver):
    ac = client
    n = 8
    for i in range(1, n + 1):
        await submit(ac, f"CW{i}", "EQ-CW", uniform_csv(f"cw{i}", 2, 500))
        await wait_status(ac, f"CW{i}", "RELEASED")
    await _drain_no_send()

    pool = await create_pool()
    seen = []

    class RecordingSender:
        def __init__(self):
            self.lock = asyncio.Lock()

        async def __call__(self, claim, source_instance, attempt):
            from app.delivery import SendOutcome
            async with self.lock:
                seen.append(claim.batch_no)
            await asyncio.sleep(0.02)
            resp = await receiver.post(
                "/analysis/batches",
                json={"source_instance": source_instance,
                      "batch": claim.payload})
            if not 200 <= resp.status_code < 300:
                return SendOutcome("retryable", error=resp.text)
            return SendOutcome("delivered", receipt=resp.json()["receipt"])

    sender = RecordingSender()
    w1 = DeliveryWorker(pool, "cw-1", sender=sender)
    w2 = DeliveryWorker(pool, "cw-2", sender=sender)
    for _ in range(10):
        await asyncio.gather(w1.run_once(), w2.run_once())
        async with pool.acquire() as conn:
            remaining = await conn.fetchval(
                "SELECT count(*) FROM delivery_outbox WHERE status<>'DELIVERED'")
        if remaining == 0:
            break
        await asyncio.sleep(0.01)

    assert sorted(seen) == [f"CW{i}" for i in range(1, n + 1)]
    listing = (await receiver.get("/analysis/batches")).json()
    assert listing["total"] == n
    async with pool.acquire() as conn:
        delivered = await conn.fetchval(
            "SELECT count(*) FROM delivery_outbox WHERE status='DELIVERED'")
    assert delivered == n
