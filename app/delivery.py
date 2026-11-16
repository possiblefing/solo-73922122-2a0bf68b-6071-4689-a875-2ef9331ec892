"""Delivery of released batches to an independent analysis receiver.

Design
------
* **Outbox.** Every released batch gets one ``delivery_outbox`` row.  Rows are
  written inside the decision transaction for newly decided batches and by a
  reconciler that backfills batches released before this feature existed.
  Quarantined batches never get a row, so they can never be shipped.
* **Delivery never touches the decision.** The outbox is a separate table;
  a failed, slow or rejected delivery neither changes ``batches`` nor blocks
  intake, decisions or queries.  The delivery worker holds a database
  connection only for short claim/record transactions, never while the HTTP
  call is in flight.
* **At-least-once delivery, idempotent receiver.** A worker claims due rows
  with ``FOR UPDATE SKIP LOCKED`` and a lease.  Only a 2xx response carrying a
  receipt marks a row ``DELIVERED``; anything else is retried with exponential
  backoff.  If a source process dies between the receiver committing and the
  source recording the receipt, the lease expires and any instance retries;
  the receiver returns the original receipt for identical content.
* **Two source instances.** Leases plus ``SKIP LOCKED`` mean two instances can
  poll concurrently without sending the same row at the same time; a rare
  duplicate caused by a lease expiring during a slow call is harmless because
  the receiver is idempotent per ``batch_no`` + content digest.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import asyncpg

from .config import settings
from .delivery_protocol import canonical_batch, content_digest

log = logging.getLogger("fab.delivery")


def _read_marker_set(path: str) -> set:
    out = set()
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#"):
                    out.add(line.partition(":")[0].strip())
    except FileNotFoundError:
        return set()
    return out


def _read_delivery_slow() -> Dict[str, float]:
    out: Dict[str, float] = {}
    try:
        with open(settings.delivery_slow_marker_file, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if ":" in line:
                    name, _, secs = line.partition(":")
                    out[name.strip()] = float(secs.strip())
                else:
                    out[line] = 8.0
    except FileNotFoundError:
        return {}
    except ValueError as exc:
        log.warning("ignoring bad delivery slow marker file: %s", exc)
    return out


# --------------------------------------------------------------- outbox rows
@dataclass(frozen=True)
class DeliveryClaim:
    batch_no: str
    batch_id: int
    device_id: str
    accept_seq: int
    payload: Dict[str, Any]
    content_sha256: str
    attempts: int


def _iso(value) -> Optional[str]:
    return value.isoformat() if value is not None else None


async def build_payload(conn: asyncpg.Connection, batch_id: int) -> Optional[Dict[str, Any]]:
    """Load a *released* batch as a canonical delivery payload.

    Returns ``None`` when the batch is unknown or not released.  Quarantined
    and pending batches are never deliverable.
    """
    row = await conn.fetchrow(
        "SELECT id, batch_no, device_id, accept_seq, status, median_nm, "
        "ref_mean_nm, deviation_nm, reference_ids, sha256, decided_at "
        "FROM batches WHERE id = $1",
        batch_id,
    )
    if row is None or row["status"] != "RELEASED":
        return None

    wafers = await conn.fetch(
        "SELECT wafer_no, thickness_nm FROM wafer_measurements "
        "WHERE batch_id = $1 ORDER BY wafer_no",
        batch_id,
    )
    ref_ids = list(row["reference_ids"] or [])
    ref_nos: List[str] = []
    if ref_ids:
        ref_rows = await conn.fetch(
            "SELECT id, batch_no FROM batches WHERE id = ANY($1::bigint[])",
            ref_ids,
        )
        by_id = {r["id"]: r["batch_no"] for r in ref_rows}
        ref_nos = [by_id[i] for i in ref_ids if i in by_id]

    return canonical_batch({
        "batch_no": row["batch_no"],
        "device_id": row["device_id"],
        "accept_seq": row["accept_seq"],
        "status": "RELEASED",
        "median_nm": row["median_nm"],
        "reference_mean_nm": row["ref_mean_nm"],
        "deviation_nm": row["deviation_nm"],
        "reference_batch_nos": ref_nos,
        "sha256": row["sha256"],
        "decided_at": _iso(row["decided_at"]),
        "wafers": [
            {"wafer_no": w["wafer_no"], "thickness_nm": w["thickness_nm"]}
            for w in wafers
        ],
    })


async def enqueue_batch(conn: asyncpg.Connection, batch_id: int) -> bool:
    """Insert the outbox row for one released batch (idempotent).

    Callable inside the decision transaction.  Returns True if a row was
    created.
    """
    payload = await build_payload(conn, batch_id)
    if payload is None:
        return False
    digest = content_digest(payload)
    result = await conn.execute(
        """
        INSERT INTO delivery_outbox
            (batch_no, batch_id, device_id, accept_seq, payload, content_sha256)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6)
        ON CONFLICT (batch_no) DO NOTHING
        """,
        payload["batch_no"], batch_id, payload["device_id"],
        payload["accept_seq"], payload, digest,
    )
    return result.endswith(" 1")


async def enqueue_batch_by_no(pool: asyncpg.pool.Pool, batch_no: str) -> bool:
    async with pool.acquire() as conn:
        async with conn.transaction():
            batch_id = await conn.fetchval(
                "SELECT id FROM batches WHERE batch_no=$1 AND status='RELEASED'",
                batch_no)
            if batch_id is None:
                return False
            return await enqueue_batch(conn, batch_id)


async def reconcile(pool: asyncpg.pool.Pool,
                    limit: Optional[int] = None) -> int:
    """Backfill outbox rows for released batches that do not have one.

    Covers batches released before delivery was deployed, rows lost to an
    operational reset, and any batch whose enqueue was skipped.  Runs before
    every claim sweep; each insert is idempotent.
    """
    limit = limit or settings.delivery_reconcile_limit
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT b.id FROM batches b
            LEFT JOIN delivery_outbox o ON o.batch_no = b.batch_no
            WHERE b.status = 'RELEASED' AND o.batch_no IS NULL
            ORDER BY b.id LIMIT $1
            """,
            limit,
        )
    created = 0
    if rows:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for row in rows:
                    if await enqueue_batch(conn, row["id"]):
                        created += 1
    if created:
        log.info("reconciled %d released batch(es) into the delivery outbox", created)
    return created


# ------------------------------------------------------------------- claiming
CLAIM_SQL = """
WITH due AS (
    SELECT batch_no FROM delivery_outbox
     WHERE (status = 'PENDING' AND next_attempt_at <= now())
        OR (status = 'IN_FLIGHT' AND lease_expires_at IS NOT NULL
            AND lease_expires_at <= now())
     ORDER BY next_attempt_at, accept_seq, batch_no
     FOR UPDATE SKIP LOCKED
     LIMIT $3
)
UPDATE delivery_outbox o
   SET status = 'IN_FLIGHT',
       attempts = o.attempts + 1,
       lease_owner = $1,
       lease_expires_at = now() + make_interval(secs => $2),
       updated_at = now()
  FROM due
 WHERE o.batch_no = due.batch_no
RETURNING o.batch_no, o.batch_id, o.device_id, o.accept_seq, o.payload,
          o.content_sha256, o.attempts
"""


async def claim_due(pool: asyncpg.pool.Pool, owner: str,
                    limit: int = 8) -> List[DeliveryClaim]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            CLAIM_SQL, owner, float(settings.delivery_lease_seconds), limit)
    return [
        DeliveryClaim(
            batch_no=r["batch_no"], batch_id=r["batch_id"],
            device_id=r["device_id"], accept_seq=r["accept_seq"],
            payload=r["payload"], content_sha256=r["content_sha256"],
            attempts=r["attempts"],
        )
        for r in rows
    ]


async def mark_delivered(pool: asyncpg.pool.Pool, claim: DeliveryClaim,
                         owner: str, receipt: Dict[str, Any]) -> bool:
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE delivery_outbox
               SET status='DELIVERED', receipt=$3::jsonb, delivered_at=now(),
                   lease_owner=NULL, lease_expires_at=NULL, last_error=NULL,
                   updated_at=now()
             WHERE batch_no=$1 AND lease_owner=$2 AND status='IN_FLIGHT'
            """,
            claim.batch_no, owner, receipt,
        )
    return result.endswith(" 1")


async def mark_retry(pool: asyncpg.pool.Pool, claim: DeliveryClaim,
                     owner: str, error: str) -> bool:
    delay = min(
        settings.delivery_retry_max_seconds,
        settings.delivery_retry_base_seconds * (2 ** max(0, claim.attempts - 1)),
    )
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE delivery_outbox
               SET status='PENDING', last_error=$3,
                   next_attempt_at = now() + make_interval(secs => $4),
                   lease_owner=NULL, lease_expires_at=NULL, updated_at=now()
             WHERE batch_no=$1 AND lease_owner=$2 AND status='IN_FLIGHT'
            """,
            claim.batch_no, owner, error[:2000], float(delay),
        )
    return result.endswith(" 1")


async def mark_rejected(pool: asyncpg.pool.Pool, claim: DeliveryClaim,
                        owner: str, error: str) -> bool:
    """Permanent failure (receiver rejected the same batch number/content).

    The batch stays RELEASED locally; only its delivery is marked rejected and
    surfaced through the delivery-status API for an operator to inspect.
    """
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE delivery_outbox
               SET status='REJECTED', last_error=$3,
                   lease_owner=NULL, lease_expires_at=NULL, updated_at=now()
             WHERE batch_no=$1 AND lease_owner=$2 AND status='IN_FLIGHT'
            """,
            claim.batch_no, owner, error[:2000],
        )
    return result.endswith(" 1")


# -------------------------------------------------------------------- sending
@dataclass
class SendOutcome:
    kind: str                     # "delivered" | "retryable" | "rejected"
    receipt: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


SendFn = Callable[[DeliveryClaim, str, int], Awaitable[SendOutcome]]


class HttpSender:
    """POSTs a claim to the receiver and classifies the response."""

    def __init__(self, base_url: str, token: str, timeout: float):
        import httpx
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._client = httpx.AsyncClient(
            base_url=base_url, timeout=timeout, headers=headers)

    async def __call__(self, claim: DeliveryClaim, source_instance: str,
                       attempt: int) -> SendOutcome:
        body = {
            "source_instance": source_instance,
            "sent_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "batch": claim.payload,
        }
        try:
            resp = await self._client.post("/analysis/batches", json=body)
        except Exception as exc:  # network error, timeout, DNS, ...
            return SendOutcome("retryable", error=f"transport: {exc!r}")
        if resp.status_code == 409:
            return SendOutcome("rejected", error=f"409: {resp.text[:1000]}")
        if 200 <= resp.status_code < 300:
            try:
                data = resp.json()
            except ValueError:
                return SendOutcome("retryable", error="2xx without JSON receipt")
            receipt = data.get("receipt") if isinstance(data, dict) else None
            if not isinstance(receipt, dict):
                return SendOutcome("retryable", error="2xx without receipt object")
            if receipt.get("batch_no") != claim.batch_no:
                return SendOutcome(
                    "retryable",
                    error="receipt batch_no mismatch (receiver state?)")
            if receipt.get("content_sha256") != claim.content_sha256:
                return SendOutcome(
                    "retryable",
                    error="receipt content digest mismatch (receiver state?)")
            return SendOutcome("delivered", receipt=receipt)
        if resp.status_code in (400, 422):
            # Structurally invalid payload: retrying cannot fix it.
            return SendOutcome("rejected", error=f"{resp.status_code}: {resp.text[:1000]}")
        return SendOutcome(
            "retryable", error=f"{resp.status_code}: {resp.text[:500]}")

    async def close(self) -> None:
        await self._client.aclose()


# --------------------------------------------------------------------- worker
class DeliveryWorker:
    """Background outbox shipper; one per source API instance."""

    def __init__(self, pool: asyncpg.pool.Pool, instance_id: str,
                 sender: Optional[SendFn] = None,
                 receiver_url: Optional[str] = None,
                 token: Optional[str] = None):
        self._pool = pool
        self._instance_id = instance_id
        self._url = receiver_url if receiver_url is not None else settings.delivery_receiver_url
        self._token = token if token is not None else settings.delivery_token
        self._sender: Optional[SendFn] = sender
        self._owns_sender = sender is None
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._last_reconcile = 0.0

    def start(self) -> None:
        self._task = asyncio.create_task(
            self._run(), name=f"delivery-{self._instance_id}")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=15)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
        if self._sender is not None and self._owns_sender:
            close = getattr(self._sender, "close", None)
            if close is not None:
                await close()

    async def _pause(self) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(),
                                   timeout=settings.delivery_poll_seconds)
        except asyncio.TimeoutError:
            pass

    async def _run(self) -> None:
        if not self._url and self._sender is None:
            log.info("[%s] delivery disabled (no receiver URL)", self._instance_id)
            return
        self._ensure_sender()
        log.info("[%s] delivery worker started -> %s",
                 self._instance_id, self._url)
        while not self._stop.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # A transient database/application failure must never kill the
                # worker; the outbox remains durable and is retried.
                log.exception("[%s] delivery sweep failed; retrying",
                              self._instance_id)
                await self._pause()
            else:
                await self._pause()
        log.info("[%s] delivery worker stopped", self._instance_id)

    def _ensure_sender(self) -> None:
        """Create the HTTP sender on first use (start() or run_once())."""
        if self._sender is None and self._url:
            self._sender = HttpSender(
                self._url, self._token, settings.delivery_timeout_seconds)

    async def run_once(self) -> int:
        """One reconcile + claim + send sweep.  Returns rows processed."""
        self._ensure_sender()
        if self._sender is None:
            return 0
        now = time.monotonic()
        if now - self._last_reconcile >= max(5.0, settings.delivery_poll_seconds * 5):
            self._last_reconcile = now
            await reconcile(self._pool)

        claims = await claim_due(self._pool, self._instance_id)
        for claim in claims:
            if self._stop.is_set():
                break
            slow = _read_delivery_slow()
            if claim.batch_no in slow:
                log.info("[%s] SLOW delivery %s sleeping %.1fs (upload, decide "
                         "and query paths are unaffected)", self._instance_id,
                         claim.batch_no, slow[claim.batch_no])
                await asyncio.sleep(slow[claim.batch_no])
            try:
                outcome = await self._sender(claim, self._instance_id, claim.attempts)
            except Exception as exc:
                outcome = SendOutcome("retryable", error=f"sender crashed: {exc!r}")
            if outcome.kind == "delivered":
                # Demo hook: die after the receiver committed but before the
                # receipt is recorded.  The lease expires and another instance
                # retries; the receiver returns the same original receipt.
                if claim.batch_no in _read_marker_set(
                        settings.delivery_crash_marker_file):
                    log.error("[%s] CRASH demo: exiting after receiver accepted "
                              "%s but before recording its receipt",
                              self._instance_id, claim.batch_no)
                    os._exit(1)
                if await mark_delivered(self._pool, claim, self._instance_id,
                                        outcome.receipt or {}):
                    log.info("[%s] delivered %s (attempt %d, sha=%s)",
                             self._instance_id, claim.batch_no, claim.attempts,
                             claim.content_sha256[:12])
                else:
                    # Another instance took over the expired lease while the
                    # call was in flight.  The receiver already has the batch;
                    # that instance will record the (identical) receipt.
                    log.warning("[%s] lease on %s was taken over before the "
                                "receipt could be recorded; not marking "
                                "delivered locally", self._instance_id,
                                claim.batch_no)
            elif outcome.kind == "rejected":
                await mark_rejected(self._pool, claim, self._instance_id,
                                    outcome.error or "rejected")
                log.error("[%s] receiver REJECTED %s: %s", self._instance_id,
                          claim.batch_no, outcome.error)
            else:
                await mark_retry(self._pool, claim, self._instance_id,
                                 outcome.error or "retryable")
                log.warning("[%s] delivery of %s failed (attempt %d): %s",
                            self._instance_id, claim.batch_no, claim.attempts,
                            outcome.error)
        return len(claims)


# ---------------------------------------------------------------- status read
def _outbox_view(row: asyncpg.Record) -> Dict[str, Any]:
    return {
        "batch_no": row["batch_no"],
        "device_id": row["device_id"],
        "accept_seq": row["accept_seq"],
        "delivery_status": row["status"],
        "attempts": row["attempts"],
        "last_error": row["last_error"],
        "next_attempt_at": _iso(row["next_attempt_at"]),
        "lease_owner": row["lease_owner"],
        "lease_expires_at": _iso(row["lease_expires_at"]),
        "receipt": row["receipt"],
        "delivered_at": _iso(row["delivered_at"]),
        "content_sha256": row["content_sha256"],
        "created_at": _iso(row["created_at"]),
        "updated_at": _iso(row["updated_at"]),
    }


async def get_delivery_status(pool: asyncpg.pool.Pool,
                              batch_no: str) -> Optional[Dict[str, Any]]:
    """Delivery state for one batch; lazily backfills a released batch's row."""
    async with pool.acquire() as conn:
        batch = await conn.fetchrow(
            "SELECT batch_no, status FROM batches WHERE batch_no=$1", batch_no)
        if batch is None:
            return None
        row = await conn.fetchrow(
            "SELECT * FROM delivery_outbox WHERE batch_no=$1", batch_no)
    if row is None and batch["status"] == "RELEASED":
        # The enqueue is idempotent; this also makes newly released batches
        # visible even if the worker has not reconciled yet.
        await enqueue_batch_by_no(pool, batch_no)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM delivery_outbox WHERE batch_no=$1", batch_no)
    if row is None:
        return {
            "batch_no": batch_no,
            "decision_status": batch["status"],
            "delivery_status": "NOT_APPLICABLE",
            "receiver_configured": settings.delivery_enabled,
            "message": "隔离批不投递 / quarantined batches are never delivered"
            if batch["status"] == "QUARANTINED" else "尚未判定 / not decided yet",
            "receipt": None,
        }
    view = _outbox_view(row)
    view["decision_status"] = batch["status"]
    view["receiver_configured"] = settings.delivery_enabled
    return view


async def list_deliveries(pool: asyncpg.pool.Pool, *,
                          status_filter: Optional[str],
                          device_id: Optional[str],
                          limit: int,
                          offset: int) -> Tuple[int, List[Dict[str, Any]]]:
    where: List[str] = []
    params: List[Any] = []
    if status_filter:
        params.append(status_filter)
        where.append(f"status = ${len(params)}")
    if device_id:
        params.append(device_id)
        where.append(f"device_id = ${len(params)}")
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    async with pool.acquire() as conn:
        total = await conn.fetchval(
            f"SELECT count(*) FROM delivery_outbox{where_sql}", *params)
        params.extend([limit, offset])
        rows = await conn.fetch(
            f"SELECT * FROM delivery_outbox{where_sql} "
            f"ORDER BY created_at, batch_no "
            f"LIMIT ${len(params) - 1} OFFSET ${len(params)}",
            *params)
    return total, [_outbox_view(r) for r in rows]
