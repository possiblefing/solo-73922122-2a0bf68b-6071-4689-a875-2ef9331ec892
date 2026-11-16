"""Pytest fixtures against a real PostgreSQL (embedded via pgserver locally).

The DSN is taken from ``DATABASE_DSN`` when present, otherwise the
session-scoped embedded cluster started by ``scripts/start_test_db.py`` is
used.  Every test starts with empty tables; the marker files used by the
slow/crash demonstrations are redirected into per-test temp directories.
"""
from __future__ import annotations

import asyncio
import os
import tempfile
import time

import pytest
import pytest_asyncio

_DSN_FILE = "/tmp/pg_dsn.txt"
if "DATABASE_DSN" not in os.environ and os.path.exists(_DSN_FILE):
    os.environ["DATABASE_DSN"] = open(_DSN_FILE).read().strip()

os.environ.setdefault("STORAGE_DIR", tempfile.mkdtemp(prefix="fabraw_"))
os.environ.setdefault("INSTANCE_ID", "pytest")

from app.config import settings  # noqa: E402
from app.db import close_pool, create_pool  # noqa: E402


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def client(tmp_path, monkeypatch):
    # Redirect demo marker files into the test's own temp directory.
    slow = tmp_path / "slow_batches.txt"
    crash = tmp_path / "crash_batches.txt"
    monkeypatch.setattr(settings, "slow_marker_file", str(slow))
    monkeypatch.setattr(settings, "instance_id", "pytest-worker")
    # crash marker path is derived; patch the property via env-less override:
    monkeypatch.setattr(
        type(settings), "crash_marker_file",
        property(lambda self: str(crash)),
    )

    from app.main import app
    from httpx import ASGITransport, AsyncClient

    pool = await create_pool()
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE wafer_measurements, batches, device_state")
        await conn.execute("ALTER SEQUENCE batches_id_seq RESTART WITH 1")

    transport = ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=transport, base_url="http://t") as ac:
            yield ac

    await close_pool()


def make_csv(rows, header=True):
    lines = ["wafer_no,thickness_nm"] if header else []
    for wafer, thick in rows:
        lines.append(f"{wafer},{thick}")
    return ("\n".join(lines) + "\n").encode()


def uniform_csv(prefix, n, thickness):
    return make_csv([(f"{prefix}-W{i:03d}", thickness) for i in range(1, n + 1)])


async def submit(ac, batch_no, device, content):
    return await ac.post(
        "/batches",
        data={"batch_no": batch_no, "device_id": device},
        files={"file": ("b.csv", content, "text/csv")},
    )


async def wait_status(ac, batch_no, expected, timeout=10.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        r = await ac.get(f"/batches/{batch_no}")
        last = r
        if r.status_code == 200 and r.json()["status"] == expected:
            return r.json()
        await asyncio.sleep(0.1)
    raise AssertionError(f"batch {batch_no} did not reach {expected}: {last}")
