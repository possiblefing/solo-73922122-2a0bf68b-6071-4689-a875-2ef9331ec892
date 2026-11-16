"""Standalone delivery-worker harness used by tests and the compose demo.

Runs the same :class:`app.delivery.DeliveryWorker` the API process runs, in
its own OS process, so a simulated "source dies after the receiver accepted
the batch but before recording the receipt" is a genuine process death.

Examples::

    # Deliver everything currently due, then exit.
    python -m scripts.delivery_harness --instance-id w1 --until-idle

    # Die after the receiver accepts the batch listed in the crash marker file
    # (DELIVERY_CRASH_MARKER_FILE / SLOW_MARKER_FILE directory).
    python -m scripts.delivery_harness --instance-id w1 --until-idle
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings  # noqa: E402
from app.db import close_pool, create_pool  # noqa: E402
from app.delivery import DeliveryWorker  # noqa: E402


async def amain() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--instance-id", default=settings.instance_id)
    ap.add_argument("--until-idle", action="store_true",
                    help="sweep until nothing is due, then exit 0")
    ap.add_argument("--idle-timeout", type=float, default=30.0)
    args = ap.parse_args()

    if not settings.delivery_enabled:
        print("DELIVERY_RECEIVER_URL is not set", file=sys.stderr, flush=True)
        return 2

    pool = await create_pool()
    worker = DeliveryWorker(pool, args.instance_id)
    try:
        if args.until_idle:
            deadline = time.time() + args.idle_timeout
            while time.time() < deadline:
                await worker.run_once()
                async with pool.acquire() as conn:
                    remaining = await conn.fetchval(
                        "SELECT count(*) FROM delivery_outbox "
                        "WHERE status IN ('PENDING','IN_FLIGHT')")
                if remaining == 0:
                    return 0
                await asyncio.sleep(0.2)
            print("idle timeout with deliveries still pending", flush=True)
            return 3
        worker.start()
        await asyncio.sleep(1e9)
        return 0
    finally:
        await worker.stop()
        await close_pool()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(amain()))
