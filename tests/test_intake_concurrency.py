"""Regression test for connection-pool starvation under same-device intake
contention.

Earlier the intake serialised accept_seq allocation with a *blocking*
transaction-scoped advisory lock.  When many same-device submissions ran at
once they each held a pool connection while queued on that lock; once the
pool was exhausted the lock holder itself could not get a connection to
commit, a self-deadlock that hung every intake.  Sequence allocation now
relies on the ``(device_id, accept_seq)`` unique constraint plus retry, so no
connection is ever held while blocked.
"""
import asyncio
import os
import sys
import time

import pytest

from app.config import settings
from app.csvutil import parse_thickness_csv
from app.db import close_pool, create_pool
from app.service import accept_submission
from app.storage import persist_original
from app.worker import DecisionWorker

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def _one(pool, n, device):
    raw = (f"wafer_no,thickness_nm\nW1,{500 + (n % 7)}\n"
           f"W2,{501 + (n % 5)}\n").encode()
    parsed = parse_thickness_csv(raw)
    artifact = persist_original(raw)
    return await accept_submission(
        pool, batch_no=f"STARVE-{n:03d}", device_id=device,
        raw=raw, parsed=parsed, artifact=artifact)


@pytest.mark.asyncio
async def test_many_concurrent_same_device_intakes_no_deadlock(
        tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "slow_marker_file",
                        str(tmp_path / "slow.txt"))
    # Deliberately small pool: the old blocking-lock design deadlocked here.
    monkeypatch.setattr(settings, "db_pool_max", 5)
    monkeypatch.setattr(settings, "db_pool_min", 2)

    pool = await create_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE wafer_measurements, batches, device_state")
            await conn.execute("ALTER SEQUENCE batches_id_seq RESTART WITH 1")

        worker = DecisionWorker(pool, "w-starve")
        worker.start()
        try:
            n = 24
            results = await asyncio.wait_for(
                asyncio.gather(*(_one(pool, i, "EQ-S") for i in range(n))),
                timeout=20,
            )
            # Let the worker drain the queue before stopping it.
            deadline = time.time() + 20
            while time.time() < deadline:
                async with pool.acquire() as conn:
                    pending = await conn.fetchval(
                        "SELECT count(*) FROM batches WHERE status='PENDING'")
                if pending == 0:
                    break
                await asyncio.sleep(0.1)
            assert pending == 0
        finally:
            await worker.stop()

        # Every submission got a unique, gapless per-device sequence.
        seqs = sorted(r.accept_seq for r in results)
        assert seqs == list(range(1, n + 1))
        assert len({r.batch_id for r in results}) == n
        assert all(not r.reused for r in results)

        async with pool.acquire() as conn:
            decided = await conn.fetch(
                "SELECT status, count(*) c FROM batches WHERE device_id='EQ-S' "
                "GROUP BY status")
            stats = {r["status"]: r["c"] for r in decided}
            assert stats.get("PENDING", 0) == 0
            assert sum(stats.values()) == n
            state = await conn.fetchrow(
                "SELECT * FROM device_state WHERE device_id='EQ-S'")
            assert state["last_decided_seq"] == n
    finally:
        await close_pool()


@pytest.mark.asyncio
async def test_concurrent_intakes_across_two_devices(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "slow_marker_file",
                        str(tmp_path / "slow.txt"))
    settings.db_pool_max = 5
    pool = await create_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE wafer_measurements, batches, device_state")
            await conn.execute("ALTER SEQUENCE batches_id_seq RESTART WITH 1")
        coros = []
        for i in range(10):
            coros.append(_one(pool, i, "EQ-A"))
            coros.append(_one(pool, 100 + i, "EQ-B"))
        results = await asyncio.wait_for(asyncio.gather(*coros), timeout=20)
        by_device = {"EQ-A": [], "EQ-B": []}
        for r in results:
            by_device[r.device_id].append(r.accept_seq)
        assert sorted(by_device["EQ-A"]) == list(range(1, 11))
        assert sorted(by_device["EQ-B"]) == list(range(1, 11))
    finally:
        await close_pool()
