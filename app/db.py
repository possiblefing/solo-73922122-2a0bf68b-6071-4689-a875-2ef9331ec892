"""Shared asyncpg connection pool."""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import asyncpg

from .config import settings
from .schema import init_schema

log = logging.getLogger("fab.db")

_pool: Optional[asyncpg.pool.Pool] = None


async def create_pool(dsn: Optional[str] = None) -> asyncpg.pool.Pool:
    global _pool
    if _pool is not None:
        return _pool
    dsn = dsn or settings.database_dsn
    last_err: Optional[Exception] = None
    # The database container may still be starting; retry briefly.
    for attempt in range(30):
        try:
            pool = await asyncpg.create_pool(
                dsn, min_size=settings.db_pool_min,
                max_size=settings.db_pool_max,
                command_timeout=60,
                init=_init_connection,
            )
            break
        except (OSError, asyncpg.PostgresError) as exc:  # pragma: no cover
            last_err = exc
            log.warning("database not ready (attempt %d): %s", attempt + 1, exc)
            await asyncio.sleep(1.0)
    else:  # pragma: no cover
        raise RuntimeError(f"cannot connect to database: {last_err}")

    async with pool.acquire() as conn:
        await init_schema(conn)
    _pool = pool
    log.info("database pool ready")
    return pool


async def _init_connection(conn: asyncpg.Connection) -> None:
    # Make JSONB round-trip as plain Python objects.
    await conn.set_type_codec(
        "jsonb",
        encoder=lambda v: __import__("json").dumps(v),
        decoder=lambda v: __import__("json").loads(v),
        schema="pg_catalog",
    )


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.pool.Pool:
    if _pool is None:
        raise RuntimeError("pool not initialised; call create_pool() first")
    return _pool
