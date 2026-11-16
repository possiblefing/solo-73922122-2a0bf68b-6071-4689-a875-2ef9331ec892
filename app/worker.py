"""Background decision worker.

Concurrency model (two API instances each run one worker)
---------------------------------------------------------
* Each device is serialised by a *session-level* PostgreSQL advisory lock
  (``pg_try_advisory_lock`` keyed by the device).  A worker that cannot get
  the lock simply moves on to another device, so:
    - a slow batch blocks later batches **of the same device** (head-of-line
      ordering is enforced by always picking the smallest ``accept_seq``);
    - it never blocks other devices, uploads or queries;
    - two workers never decide the same batch.
* The decision itself is one transaction: row locks on ``device_state`` and
  the batch, median/drift calculation, decision write, and -- for released
  batches only -- appending to ``ref_batch_ids``.  Either everything becomes
  visible together or nothing does, hence no half batch and no double
  advancement of the reference window.
* If a process dies mid-batch (before COMMIT), the row stays ``PENDING`` and
  ``device_state`` is untouched; the surviving instance's next poll picks it
  up and finishes it.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Dict, List, Optional, Set

import asyncpg

from .config import settings
from .csvutil import median_nm

log = logging.getLogger("fab.worker")


def _read_marker_file(path: str) -> Dict[int, float]:
    """Parse ``batch_id`` or ``batch_id:seconds`` lines; missing file => {}."""
    out: Dict[int, float] = {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if ":" in line:
                    bid, _, secs = line.partition(":")
                    out[int(bid.strip())] = float(secs.strip())
                else:
                    out[int(line)] = 8.0
    except FileNotFoundError:
        return {}
    except ValueError as exc:
        log.warning("ignoring bad marker file %s: %s", path, exc)
    return out


class DecisionWorker:
    def __init__(self, pool: asyncpg.pool.Pool, instance_id: str):
        self._pool = pool
        self._instance_id = instance_id
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._conn: Optional[asyncpg.Connection] = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"worker-{self._instance_id}")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=15)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

    # ------------------------------------------------------------------ loop
    async def _run(self) -> None:
        # Outer loop: if the dedicated connection dies (e.g. database
        # restart), acquire a fresh one instead of terminating the worker.
        # Advisory locks held by a dead session are released by PostgreSQL
        # itself, so a reconnect can never inherit a stale device lock.
        while not self._stop.is_set():
            try:
                conn = await self._pool.acquire()
            except Exception:
                log.warning("[%s] cannot acquire worker connection; retrying",
                            self._instance_id)
                await self._pause()
                continue
            self._conn = conn
            log.info("[%s] worker started", self._instance_id)
            try:
                while not self._stop.is_set():
                    try:
                        worked = await self._tick(conn)
                    except asyncio.CancelledError:
                        raise
                    except asyncpg.PostgresError as exc:
                        # Connection/query-level failure: drop this connection
                        # and reconnect rather than killing the worker.
                        log.warning("[%s] database error in worker: %s; reconnecting",
                                    self._instance_id, exc)
                        break
                    except Exception:  # never die on a transient application bug
                        log.exception("[%s] worker tick failed; retrying",
                                      self._instance_id)
                        worked = False
                    if not worked:
                        await self._pause()
            finally:
                try:
                    await conn.execute("SELECT pg_advisory_unlock_all()")
                except Exception:
                    pass
                try:
                    await self._pool.release(conn)
                except Exception:
                    pass
                self._conn = None
        log.info("[%s] worker stopped", self._instance_id)

    async def _pause(self) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(),
                                   timeout=settings.worker_poll_seconds)
        except asyncio.TimeoutError:
            pass

    async def _tick(self, conn: asyncpg.Connection) -> bool:
        """One sweep over devices that still have pending batches."""
        devices = await conn.fetch(
            "SELECT DISTINCT device_id FROM batches WHERE status = 'PENDING'"
        )
        did_work = False
        for dev_row in devices:
            if self._stop.is_set():
                break
            device_id = dev_row["device_id"]
            got = await conn.fetchval(
                "SELECT pg_try_advisory_lock($1, hashtext($2))",
                settings.worker_lock_id, device_id,
            )
            if not got:
                # Another instance owns this device right now.
                continue
            try:
                head = await conn.fetchrow(
                    "SELECT * FROM batches WHERE device_id = $1 AND status = 'PENDING'"
                    " ORDER BY accept_seq LIMIT 1",
                    device_id,
                )
                if head is not None:
                    await self._process_batch(conn, head)
                    did_work = True
            finally:
                await conn.execute(
                    "SELECT pg_advisory_unlock($1, hashtext($2))",
                    settings.worker_lock_id, device_id,
                )
        return did_work

    # ------------------------------------------------------------- one batch
    async def _process_batch(self, conn: asyncpg.Connection,
                             head: asyncpg.Record) -> None:
        batch_id: int = head["id"]
        log.info("[%s] picked up batch %s (id=%d, seq=%d)",
                 self._instance_id, head["batch_no"], batch_id, head["accept_seq"])

        # Record an attempt in its own transaction: if the process dies after
        # this point, the retry will show attempts > 1 while the decision is
        # still guaranteed to be written exactly once.
        await conn.execute(
            "UPDATE batches SET attempts = attempts + 1 WHERE id = $1 AND status='PENDING'",
            batch_id,
        )

        # Demo hooks (marker files managed by the demonstration scripts).
        slow = _read_marker_file(settings.slow_marker_file)
        if batch_id in slow:
            log.info("[%s] SLOW batch id=%d sleeping %.1fs while holding device lock",
                     self._instance_id, batch_id, slow[batch_id])
            await asyncio.sleep(slow[batch_id])
        crashes = self._crash_ids()
        if batch_id in crashes:
            log.error("[%s] CRASH demo: exiting before decision of batch id=%d "
                      "(row stays PENDING, survivor will take over)",
                      self._instance_id, batch_id)
            # Hard exit, like kill -9: no COMMIT, no cleanup.
            os._exit(1)

        await self._decide(conn, head)

    def _crash_ids(self) -> Set[int]:
        out: Set[int] = set()
        try:
            with open(settings.crash_marker_file, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        out.add(int(line.partition(":")[0].strip()))
        except FileNotFoundError:
            return set()
        except ValueError:
            return set()
        return out

    async def _decide(self, conn: asyncpg.Connection, head: asyncpg.Record) -> None:
        batch_id: int = head["id"]
        device_id: str = head["device_id"]
        accept_seq: int = head["accept_seq"]

        async with conn.transaction():
            # Serialise against anyone else touching this device's state.
            ds = await conn.fetchrow(
                "SELECT * FROM device_state WHERE device_id = $1 FOR UPDATE",
                device_id,
            )
            if ds is None:
                await conn.execute(
                    "INSERT INTO device_state(device_id) VALUES ($1) "
                    "ON CONFLICT (device_id) DO NOTHING", device_id)
                ds = await conn.fetchrow(
                    "SELECT * FROM device_state WHERE device_id = $1 FOR UPDATE",
                    device_id,
                )

            # Re-lock and re-check the batch inside the transaction.
            batch = await conn.fetchrow(
                "SELECT * FROM batches WHERE id = $1 FOR UPDATE", batch_id)
            if batch is None or batch["status"] != "PENDING":
                log.info("batch id=%d already decided, skipping", batch_id)
                return

            # Head-of-line safety: no earlier undecided batch may exist.
            if accept_seq != ds["last_decided_seq"] + 1:
                raise RuntimeError(
                    f"ordering violation on {device_id}: seq={accept_seq} "
                    f"last_decided={ds['last_decided_seq']}")

            wafers = await conn.fetch(
                "SELECT thickness_nm FROM wafer_measurements WHERE batch_id = $1",
                batch_id,
            )
            med = median_nm([w["thickness_nm"] for w in wafers])

            # Baseline batches are released without any drift comparison, so
            # they carry no reference set.  Only post-baseline batches compute
            # the rolling reference window and the deviation.
            ref_ids: List[int] = []
            ref_mean: Optional[float] = None
            deviation: Optional[float] = None
            if accept_seq <= settings.baseline_count:
                status, reason = "RELEASED", "baseline"
            else:
                ref_ids = list(ds["ref_batch_ids"])[-settings.reference_count:]
                if not ref_ids:
                    # Defensive: no reference yet -> treat as baseline release.
                    status, reason = "RELEASED", "no-reference"
                else:
                    ref_rows = await conn.fetch(
                        "SELECT id, median_nm FROM batches WHERE id = ANY($1::bigint[])",
                        ref_ids,
                    )
                    by_id = {r["id"]: float(r["median_nm"]) for r in ref_rows}
                    ref_medians = [by_id[i] for i in ref_ids if i in by_id]
                    ref_mean = sum(ref_medians) / len(ref_medians)
                    deviation = abs(med - ref_mean)
                    if deviation > settings.drift_threshold_nm:
                        status, reason = "QUARANTINED", "drift"
                    else:
                        status, reason = "RELEASED", "within-tolerance"

            new_refs = list(ds["ref_batch_ids"])
            if status == "RELEASED":
                new_refs = (new_refs + [batch_id])[-settings.reference_count:]

            await conn.execute(
                """
                UPDATE batches
                   SET status=$2, median_nm=$3, ref_mean_nm=$4, deviation_nm=$5,
                       reference_ids=$6::jsonb, decided_at=now(), decided_by=$7
                 WHERE id=$1
                """,
                batch_id, status, med, ref_mean, deviation,
                ref_ids, self._instance_id,
            )
            await conn.execute(
                """
                UPDATE device_state
                   SET last_decided_seq=$2, ref_batch_ids=$3::jsonb, updated_at=now()
                 WHERE device_id=$1
                """,
                device_id, accept_seq, new_refs,
            )
            log.info(
                "[%s] decided batch id=%d (%s/%s) median=%.2f ref_mean=%s dev=%s "
                "refs=%s -> %s (%s)",
                self._instance_id, batch_id, device_id, head["batch_no"], med,
                f"{ref_mean:.3f}" if ref_mean is not None else "-",
                f"{deviation:.3f}" if deviation is not None else "-",
                ref_ids, status, reason,
            )
