"""Crash-recovery test: a worker process dying mid-batch leaves no half
state and the surviving worker finishes the queue, with the decision,
reference history and data visibility consistent afterwards.

The crash is a real separate OS process (``scripts/worker_harness.py --crash``)
that calls ``os._exit(1)`` while holding the device lock, analogous to
``kill -9``.  PostgreSQL then drops the session and its advisory lock; the
second harness takes over on its next poll.
"""
import asyncio
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HARNESS = os.path.join(ROOT, "scripts", "worker_harness.py")
DSN = open("/tmp/pg_dsn.txt").read().strip()
BASE_ENV = {
    **os.environ,
    "DATABASE_DSN": DSN,
    "STORAGE_DIR": "/tmp/fabtest-crash-raw",
    "PYTHONPATH": ROOT,
}


async def _run_harness(*args, env=None, timeout=60):
    proc = await asyncio.create_subprocess_exec(
        sys.executable, HARNESS, *args,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        env=env or BASE_ENV)
    out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    return proc.returncode, out.decode(), err.decode()


async def _sql(query: str):
    code = (
        "import asyncio, asyncpg, os, json\n"
        "async def m():\n"
        "    c = await asyncpg.connect(os.environ['DATABASE_DSN'])\n"
        "    await c.set_type_codec('jsonb', encoder=json.dumps, "
        "decoder=json.loads, schema='pg_catalog')\n"
        f"    r = await c.fetch({query!r})\n"
        "    print(json.dumps([dict(x) for x in r], default=str))\n"
        "    await c.close()\n"
        "asyncio.run(m())\n")
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-I", "-c", code,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        env=BASE_ENV)
    out, err = await proc.communicate()
    assert proc.returncode == 0, err.decode()
    import json
    return json.loads(out.decode())


@pytest.mark.asyncio
async def test_crashed_worker_is_taken_over_by_survivor(tmp_path):
    # Fresh schema contents for an isolated story.
    await _sql("TRUNCATE wafer_measurements, batches, device_state")
    await _sql("ALTER SEQUENCE batches_id_seq RESTART WITH 1")
    os.makedirs(BASE_ENV["STORAGE_DIR"], exist_ok=True)

    # Submit four batches without starting any worker yet.  The first three
    # are baseline releases; K4 is the first drift-checked batch and its
    # reference window proves the survivor advanced history correctly.
    rc, out, err = await _run_harness(
        "--instance-id", "submitter", "--no-worker",
        "--submit",
        "K1:EQ-K:500:5,K2:EQ-K:500:5,K3:EQ-K:500:5,K4:EQ-K:504:5")
    assert rc == 0, (rc, out, err)
    rows = await _sql("SELECT id, batch_no, status FROM batches ORDER BY id")
    assert [r["batch_no"] for r in rows] == ["K1", "K2", "K3", "K4"]
    assert all(r["status"] == "PENDING" for r in rows)

    # Marker file that makes the first worker hard-exit while deciding batch 1.
    crash_env = dict(BASE_ENV)
    crash_env["SLOW_MARKER_FILE"] = str(tmp_path / "slow.txt")
    rc, out, err = await _run_harness(
        "--instance-id", "crasher", "--crash", "1",
        "--idle-timeout", "5", env=crash_env)
    assert rc != 0, (rc, out, err)
    assert "CRASH demo" in (out + err)

    # No half state: K1 still PENDING, no decision columns, device cursor at 0.
    rows = await _sql("SELECT id, batch_no, status, median_nm, reference_ids "
                      "FROM batches ORDER BY id")
    k1 = next(r for r in rows if r["batch_no"] == "K1")
    assert k1["status"] == "PENDING" and k1["median_nm"] is None
    states = await _sql("SELECT * FROM device_state WHERE device_id='EQ-K'")
    assert states[0]["last_decided_seq"] == 0
    assert states[0]["ref_batch_ids"] == []

    # The lock must be releasable by another session (proves no stuck lock).
    lock_rows = await _sql(
        "SELECT pg_try_advisory_lock(78219001, hashtext('EQ-K')) AS got")
    assert lock_rows[0]["got"] is True
    await _sql("SELECT pg_advisory_unlock(78219001, hashtext('EQ-K'))")

    # Survivor instance takes over and drains the queue.
    rc, out, err = await _run_harness(
        "--instance-id", "survivor", "--exit-when-idle",
        "--idle-timeout", "15")
    assert rc == 0, (rc, out, err)

    rows = await _sql(
        "SELECT id, batch_no, status, median_nm, reference_ids, attempts "
        "FROM batches ORDER BY id")
    assert [r["status"] for r in rows] == \
        ["RELEASED", "RELEASED", "RELEASED", "RELEASED"]
    # Baseline batches carry no reference set; the post-baseline K4 was
    # compared against exactly K1,K2,K3 after the survivor took over.
    assert rows[0]["reference_ids"] == []
    assert rows[3]["reference_ids"] == [1, 2, 3]
    states = await _sql("SELECT * FROM device_state WHERE device_id='EQ-K'")
    assert states[0]["last_decided_seq"] == 4
    # Rolling window keeps only the three most recent released batches.
    assert states[0]["ref_batch_ids"] == [2, 3, 4]
    # The crashed batch shows one attempt from the doomed process plus the
    # successful survivor attempt, while later batches have exactly one.
    assert rows[0]["attempts"] == 2
    assert rows[1]["attempts"] == 1 and rows[2]["attempts"] == 1
    assert rows[3]["attempts"] == 1
