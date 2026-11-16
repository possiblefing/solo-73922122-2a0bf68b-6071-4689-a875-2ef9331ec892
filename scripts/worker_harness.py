"""Single-instance worker harness used by tests and the slow/crash demos.

Examples
--------
Submit two batches then run one worker until the queue drains::

    python -m scripts.worker_harness --instance-id api1 \\
        --submit B1:EQ-A:500:5,B2:EQ-A:506:5 --exit-when-idle

Make batch id 1 slow (holds its device lock for 6 seconds)::

    python -m scripts.worker_harness --instance-id api1 --slow 1:6 --exit-when-idle

Simulate kill -9 while deciding batch id 1 (hard exit, no COMMIT)::

    python -m scripts.worker_harness --instance-id api1 --crash 1
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

# Allow `python scripts/worker_harness.py` from the repository root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings  # noqa: E402
from app.csvutil import parse_thickness_csv  # noqa: E402
from app.db import close_pool, create_pool  # noqa: E402
from app.service import accept_submission  # noqa: E402
from app.storage import persist_original  # noqa: E402
from app.worker import DecisionWorker  # noqa: E402


def _write_marker(path: str, entries: list[str]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(entries) + "\n")


async def _submit_specs(pool, specs: str) -> None:
    for spec in specs.split(","):
        batch_no, device, thickness, count = spec.split(":")
        lines = ["wafer_no,thickness_nm"]
        for i in range(1, int(count) + 1):
            lines.append(f"{batch_no}-W{i:03d},{thickness}")
        raw = ("\n".join(lines) + "\n").encode()
        parsed = parse_thickness_csv(raw)
        artifact = persist_original(raw)
        acc = await accept_submission(
            pool, batch_no=batch_no, device_id=device,
            raw=raw, parsed=parsed, artifact=artifact)
        print(f"submitted batch_no={batch_no} id={acc.batch_id} seq={acc.accept_seq}",
              flush=True)


async def amain() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--instance-id", default=settings.instance_id)
    ap.add_argument("--submit", help="comma list of batch_no:device:thickness:count")
    ap.add_argument("--slow", help="comma list of id:seconds slow markers")
    ap.add_argument("--crash", help="comma list of batch ids that hard-exit the process")
    ap.add_argument("--exit-when-idle", action="store_true",
                    help="stop once no PENDING batch remains")
    ap.add_argument("--no-worker", action="store_true",
                    help="only run --submit, never start a decision worker")
    ap.add_argument("--idle-timeout", type=float, default=30.0)
    args = ap.parse_args()

    if args.slow:
        _write_marker(settings.slow_marker_file, args.slow.split(","))
    if args.crash:
        _write_marker(settings.crash_marker_file, args.crash.split(","))

    pool = await create_pool()
    if args.submit:
        await _submit_specs(pool, args.submit)
    if args.no_worker:
        await close_pool()
        return 0

    worker = DecisionWorker(pool, args.instance_id)
    worker.start()
    try:
        if args.exit_when_idle:
            deadline = time.time() + args.idle_timeout
            while time.time() < deadline:
                async with pool.acquire() as conn:
                    pending = await conn.fetchval(
                        "SELECT count(*) FROM batches WHERE status='PENDING'")
                if pending == 0:
                    # Extra settle window so the final decision is visible.
                    await asyncio.sleep(0.2)
                    break
                await asyncio.sleep(0.2)
            else:
                print("idle timeout reached with batches still pending", flush=True)
                return 2
        else:
            # No idle draining requested (e.g. crash demo): stay alive until the
            # worker task itself ends -- os._exit() in crash mode never returns.
            while not worker._task.done():
                await asyncio.sleep(0.2)
    finally:
        await worker.stop()
        await close_pool()
    print(f"harness {args.instance_id} finished", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(amain()))
