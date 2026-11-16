"""Independent analysis receiver API.

Runs as its own service against its own PostgreSQL.  The source service ships
released batches here; this API stores the full wafer detail, decision and
reference batch numbers, and exposes per-batch queries and published data.

Endpoints
---------
``POST /analysis/batches``      accept one delivered released batch (idempotent)
``GET  /analysis/batches``      list batches (``published_only=true`` by default)
``GET  /analysis/batches/{no}`` one batch with full wafer detail
``GET  /analysis/data``         published batches only (analysis read model)
``GET  /analysis/blocked``      stored but unpublished batches + missing refs
``GET  /analysis/receipts/{no}`` original receipt for a delivered batch
``GET  /health``                liveness + analysis DB connectivity
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, Optional

import asyncpg
from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.responses import JSONResponse

from app.delivery_protocol import PayloadValidationError

from .config import settings
from .schema import init_schema
from . import service

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("fab.receiver.api")

_pool: Optional[asyncpg.pool.Pool] = None


async def _init_connection(conn: asyncpg.Connection) -> None:
    import json
    await conn.set_type_codec(
        "jsonb",
        encoder=lambda v: json.dumps(v),
        decoder=lambda v: json.loads(v),
        schema="pg_catalog",
    )


async def create_pool(dsn: Optional[str] = None) -> asyncpg.pool.Pool:
    global _pool
    if _pool is not None:
        return _pool
    import asyncio
    dsn = dsn or settings.database_dsn
    last_err: Optional[Exception] = None
    for attempt in range(30):
        try:
            pool = await asyncpg.create_pool(
                dsn, min_size=settings.db_pool_min, max_size=settings.db_pool_max,
                command_timeout=60, init=_init_connection)
            break
        except (OSError, asyncpg.PostgresError) as exc:  # pragma: no cover
            last_err = exc
            log.warning("analysis database not ready (attempt %d): %s",
                        attempt + 1, exc)
            await asyncio.sleep(1.0)
    else:  # pragma: no cover
        raise RuntimeError(f"cannot connect to analysis database: {last_err}")
    async with pool.acquire() as conn:
        await init_schema(conn)
    _pool = pool
    log.info("analysis database pool ready")
    return pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.pool.Pool:
    if _pool is None:
        raise RuntimeError("receiver pool not initialised")
    return _pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_pool()
    # A crash between commit and publication can leave a stored batch
    # unpublished; re-evaluate the dependency graph on startup.
    promoted = await service.publish_ready(get_pool())
    if promoted:
        log.info("startup publication promoted %d batch(es)", promoted)
    yield
    await close_pool()


app = FastAPI(
    title="厚度批次分析接收端 / Wafer Thickness Analysis Receiver",
    version="1.0.0",
    description=(
        "独立分析库接收端。源端把已放行批次（含整批晶圆明细、判定与参考批号）"
        "投递到这里。同批同内容重投复用原回执；异内容拒绝；参考批未确认交付前"
        "不发布依赖它的批次。"
    ),
    lifespan=lifespan,
)


def _authorize(authorization: Optional[str] = Header(None)) -> None:
    if not settings.token:
        return
    expected = f"Bearer {settings.token}"
    if authorization != expected:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED,
                            "invalid or missing delivery token")


@app.exception_handler(PayloadValidationError)
async def _invalid_payload(_request, exc: PayloadValidationError):
    return JSONResponse(status_code=422, content={"error": str(exc)})


@app.exception_handler(service.Conflict)
async def _conflict(_request, exc: service.Conflict):
    return JSONResponse(
        status_code=409,
        content={"error": str(exc), "existing": exc.existing},
    )


@app.get("/health")
async def health():
    try:
        async with get_pool().acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_ok = True
    except Exception:  # pragma: no cover
        db_ok = False
    return {"status": "ok" if db_ok else "degraded",
            "instance_id": settings.instance_id, "database": db_ok,
            "role": "analysis-receiver"}


@app.post("/analysis/batches", status_code=status.HTTP_201_CREATED,
          summary="接收一个已放行批次（整批明细 + 判定 + 参考批号）")
async def receive_batch(
    body: dict[str, Any],
    _auth: None = Depends(_authorize),
):
    if not isinstance(body, dict) or not isinstance(body.get("batch"), dict):
        raise PayloadValidationError("request must contain a 'batch' object")
    receipt, reused = await service.accept_batch(
        get_pool(), body["batch"], body.get("source_instance"))
    return {
        "receipt": receipt,
        "reused": reused,
        "message": ("同批同内容，复用原回执 / identical content, original "
                    "receipt reused" if reused else "已接收 / accepted"),
    }


@app.get("/analysis/batches", summary="按批查询（默认仅已发布）")
async def list_batches(
    device_id: Optional[str] = Query(None),
    published_only: bool = Query(True),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    _auth: None = Depends(_authorize),
):
    total, items = await service.list_batches(
        get_pool(), device_id=device_id, published_only=published_only,
        limit=limit, offset=offset)
    return {"total": total, "limit": limit, "offset": offset, "items": items}


@app.get("/analysis/batches/{batch_no}", summary="单批完整明细（晶圆 + 判定 + 回执）")
async def get_batch(batch_no: str, _auth: None = Depends(_authorize)):
    view = await service.get_batch(get_pool(), batch_no)
    if view is None:
        raise HTTPException(404, f"未接收该批 / batch not received: {batch_no}")
    return view


@app.get("/analysis/data", summary="分析读模型：仅发布批次及其晶圆明细")
async def analysis_data(
    device_id: Optional[str] = Query(None),
    batch_no: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    _auth: None = Depends(_authorize),
):
    if batch_no:
        item = await service.get_batch_data(get_pool(), batch_no)
        items = [item] if item is not None else []
        return {"total": len(items), "limit": limit, "offset": offset,
                "items": items}
    total, rows = await service.list_batches(
        get_pool(), device_id=device_id, published_only=True,
        limit=limit, offset=offset)
    items = []
    for row in rows:
        detail = await service.get_batch_data(get_pool(), row["batch_no"])
        if detail is not None:
            items.append(detail)
    return {"total": total, "limit": limit, "offset": offset, "items": items}


@app.get("/analysis/blocked", summary="已收但未发布的批次及缺失参考批号")
async def blocked(_auth: None = Depends(_authorize)):
    items = await service.list_blocked(get_pool())
    return {"count": len(items), "items": items}


@app.get("/analysis/receipts/{batch_no}", summary="查询投递回执")
async def receipt(batch_no: str, _auth: None = Depends(_authorize)):
    doc = await service.fetch_receipt(get_pool(), batch_no)
    if doc is None:
        raise HTTPException(404, f"无回执 / no receipt: {batch_no}")
    return doc


@app.post("/internal/shutdown", include_in_schema=False)
async def internal_shutdown(token: str = Query(...)):
    """Graceful shutdown hook used by the compose/local demonstrations.

    Disabled (404) unless ``SHUTDOWN_TOKEN`` is configured.
    """
    import os
    import signal
    expected = os.environ.get("SHUTDOWN_TOKEN", "")
    if not expected or token != expected:
        raise HTTPException(404, "not found")
    await close_pool()
    try:
        os.kill(os.getpid(), signal.SIGTERM)
    except PermissionError:  # pragma: no cover - sandboxed demo environments
        os._exit(0)
    return {"status": "shutting down"}
