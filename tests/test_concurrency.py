"""Concurrency tests with two real workers sharing one PostgreSQL.

These instantiate two :class:`DecisionWorker` instances directly (the same
object each API container runs) and prove:
* a slow head-of-line batch cannot be overtaken by later batches of the same
  device, while other devices keep flowing;
* two workers never double-decide or double-advance the reference window;
* per-device advisory locks let different devices progress concurrently.
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


async def _submit(pool, batch_no, device, thickness, count=5):
    lines = ["wafer_no,thickness_nm"] + [
        f"{batch_no}-W{i:03d},{thickness}" for i in range(1, count + 1)]
    raw = ("\n".join(lines) + "\n").encode()
    parsed = parse_thickness_csv(raw)
    artifact = persist_original(raw)
    acc = await accept_submission(
        pool, batch_no=batch_no, device_id=device,
        raw=raw, parsed=parsed, artifact=artifact)
    return acc.batch_id


@pytest.mark.asyncio
async def test_slow_head_batch_blocks_same_device_only(tmp_path, monkeypatch):
    slow_file = tmp_path / "slow.txt"
    monkeypatch.setattr(settings, "slow_marker_file", str(slow_file))

    pool = await create_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE wafer_measurements, batches, device_state")
            await conn.execute("ALTER SEQUENCE batches_id_seq RESTART WITH 1")

        # Same device EQ-F: head batch F1 will be slow; F2..F4 must wait.
        f1 = await _submit(pool, "F1", "EQ-F", 500)
        f2 = await _submit(pool, "F2", "EQ-F", 500)
        f3 = await _submit(pool, "F3", "EQ-F", 500)
        f4 = await _submit(pool, "F4", "EQ-F", 504)
        # A different device must not be blocked by F1's slow processing.
        await _submit(pool, "G1", "EQ-G", 700)
        g2 = await _submit(pool, "G2", "EQ-G", 700)

        # 4-second artificial delay on the head batch only.
        slow_file.write_text(f"{f1}:4\n")

        w1 = DecisionWorker(pool, "w1")
        w2 = DecisionWorker(pool, "w2")
        w1.start(); w2.start()

        # While F1 is still in progress, the other device must advance.
        g_done = False
        deadline = time.time() + 12
        while time.time() < deadline:
            async with pool.acquire() as conn:
                f1_status = await conn.fetchval(
                    "SELECT status FROM batches WHERE id=$1", f1)
                g2_status = await conn.fetchval(
                    "SELECT status FROM batches WHERE id=$1", g2)
            if f1_status == "PENDING" and g2_status == "RELEASED":
                g_done = True
                break
            await asyncio.sleep(0.15)
        assert g_done, "other device was blocked by the slow same-device batch"

        # Drain everything.
        deadline = time.time() + 20
        while time.time() < deadline:
            async with pool.acquire() as conn:
                pending = await conn.fetchval(
                    "SELECT count(*) FROM batches WHERE status='PENDING'")
            if pending == 0:
                break
            await asyncio.sleep(0.2)
        assert pending == 0

        await w1.stop(); await w2.stop()

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT batch_no, accept_seq, status, decided_by FROM batches "
                "WHERE device_id='EQ-F' ORDER BY accept_seq")
            assert [r["batch_no"] for r in rows] == ["F1", "F2", "F3", "F4"]
            assert all(r["status"] == "RELEASED" for r in rows)
            # FIFO: exactly one decision per sequence, no double advancement.
            seqs = [r["accept_seq"] for r in rows]
            assert seqs == [1, 2, 3, 4]
            state = await conn.fetchrow(
                "SELECT * FROM device_state WHERE device_id='EQ-F'")
            assert state["last_decided_seq"] == 4
            assert state["ref_batch_ids"] == [f2, f3, f4]
            # F4 was compared against F1,F2,F3 (rolling window before F4).
            f4_refs = await conn.fetchval(
                "SELECT reference_ids FROM batches WHERE id=$1", f4)
            assert f4_refs == [f1, f2, f3]
    finally:
        await close_pool()


@pytest.mark.asyncio
async def test_two_workers_never_double_decide(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "slow_marker_file",
                        str(tmp_path / "slow.txt"))
    pool = await create_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE wafer_measurements, batches, device_state")
            await conn.execute("ALTER SEQUENCE batches_id_seq RESTART WITH 1")
        ids = []
        for i in range(1, 9):
            ids.append(await _submit(pool, f"H{i}", "EQ-H", 500))
        # Include one drifting batch.
        await _submit(pool, "H9", "EQ-H", 900)

        w1 = DecisionWorker(pool, "w1")
        w2 = DecisionWorker(pool, "w2")
        w1.start(); w2.start()
        deadline = time.time() + 20
        while time.time() < deadline:
            async with pool.acquire() as conn:
                pending = await conn.fetchval(
                    "SELECT count(*) FROM batches WHERE status='PENDING'")
            if pending == 0:
                break
            await asyncio.sleep(0.15)
        assert pending == 0
        await w1.stop(); await w2.stop()

        async with pool.acquire() as conn:
            # Every batch decided exactly once; attempts can be >0 only via
            # explicit retries, never duplicated here.
            dupes = await conn.fetchval(
                "SELECT count(*) FROM (SELECT accept_seq FROM batches "
                "WHERE device_id='EQ-H' GROUP BY accept_seq HAVING count(*)>1) s")
            assert dupes == 0
            decided = await conn.fetch(
                "SELECT status, count(*) c FROM batches WHERE device_id='EQ-H' "
                "GROUP BY status")
            stats = {r["status"]: r["c"] for r in decided}
            assert stats["RELEASED"] == 8
            assert stats["QUARANTINED"] == 1
            state = await conn.fetchrow(
                "SELECT * FROM device_state WHERE device_id='EQ-H'")
            # Reference window holds exactly the three most recent RELEASED ids.
            assert state["ref_batch_ids"] == ids[-3:]
    finally:
        await close_pool()
