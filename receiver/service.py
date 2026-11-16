"""Receiver service: accept released batches into the analysis database.

Acceptance contract
-------------------
* The wafer detail and the receipt become visible **together**: one
  transaction inserts the batch, its wafers and its receipt, then publishes
  it only when all reference batches are present and published.
* ``batch_no`` + content digest is the idempotency key.  Re-delivering the
  same batch with the same content returns the *original* receipt and does not
  add rows; the same ``batch_no`` with different content is rejected 409.
* A batch whose reference batch numbers have not all been received and
  published stays stored but ``published = FALSE``.  It is promoted
  automatically when the missing references arrive (see :func:`publish_ready`).
* A reference that will never arrive (quarantined / not released) cannot be
  satisfied; such a batch is surfaced in ``/analysis/blocked`` for operators,
  and never appears in the published read APIs.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from app.delivery_protocol import canonical_batch, content_digest

log = logging.getLogger("fab.receiver")


class Conflict(Exception):
    """Same ``batch_no`` with a different content digest."""

    def __init__(self, message: str, existing: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.existing = existing or {}


def _batch_view(row: asyncpg.Record) -> Dict[str, Any]:
    return {
        "batch_no": row["batch_no"],
        "device_id": row["device_id"],
        "accept_seq": row["accept_seq"],
        "status": row["status"],
        "median_nm": row["median_nm"],
        "reference_mean_nm": row["reference_mean_nm"],
        "deviation_nm": row["deviation_nm"],
        "reference_batch_nos": list(row["reference_batch_nos"] or []),
        "source_sha256": row["source_sha256"],
        "decided_at": row["decided_at"].isoformat() if row["decided_at"] else None,
        "content_sha256": row["content_sha256"],
        "receipt": row["receipt"],
        "received_at": row["received_at"].isoformat() if row["received_at"] else None,
        "published": row["published"],
        "published_at": row["published_at"].isoformat() if row["published_at"] else None,
    }


def _receipt_doc(batch_no: str, digest: str,
                 published: bool) -> Dict[str, Any]:
    return {
        "receipt_id": uuid.uuid4().hex,
        "batch_no": batch_no,
        "content_sha256": digest,
        "status": "ACCEPTED",
        "published": published,
        "received_at": None,  # filled from DB on the way out
    }


async def accept_batch(pool: asyncpg.pool.Pool, raw_batch: Dict[str, Any],
                       source_instance: Optional[str]) -> Tuple[Dict[str, Any], bool]:
    """Accept one batch.  Returns ``(receipt, reused)``.

    Raises :class:`PayloadValidationError` (HTTP 422) for invalid structure
    and :class:`Conflict` (HTTP 409) for a same-number/different-content
    re-delivery.
    """
    batch = canonical_batch(raw_batch)
    digest = content_digest(batch)
    batch_no = batch["batch_no"]

    # Two source instances (or a retry racing the first attempt) can insert
    # the same batch number concurrently.  The primary key serialises them:
    # the loser gets a unique violation and re-reads the winner's row, which
    # turns into the normal identical-content reuse (or 409) decision.
    for attempt in range(4):
        try:
            receipt, reused = await _accept_once(
                pool, batch, digest, source_instance)
            break
        except asyncpg.UniqueViolationError:
            if attempt == 3:
                raise
            continue
    else:  # pragma: no cover
        raise RuntimeError("unreachable")

    if not reused:
        # Post-commit safety net for publication: two accepts committing
        # concurrently can each miss the other's freshly inserted row while
        # promoting dependents in their own transactions.  Re-evaluating the
        # dependency graph after commit makes publication eventually complete
        # without weakening the "references first" guarantee.
        await publish_ready(pool)
    return await fetch_receipt(pool, batch_no) or receipt, reused


async def _accept_once(pool: asyncpg.pool.Pool, batch: Dict[str, Any],
                       digest: str,
                       source_instance: Optional[str]) -> Tuple[Dict[str, Any], bool]:
    batch_no = batch["batch_no"]
    promoted = 0
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetchrow(
                "SELECT * FROM analysis_batches WHERE batch_no=$1 FOR UPDATE",
                batch_no,
            )
            if existing is not None:
                if existing["content_sha256"] != digest:
                    raise Conflict(
                        f"batch {batch_no} already received with different "
                        "content / 同批号异内容",
                        {"batch_no": batch_no,
                         "content_sha256": existing["content_sha256"],
                         "receipt": existing["receipt"]},
                    )
                # Identical re-delivery: reuse the original receipt verbatim.
                # Its "published" field may have advanced since first receipt,
                # so refresh it from the row to stay truthful.
                receipt = dict(existing["receipt"])
                receipt["published"] = existing["published"]
                receipt["reused"] = True
                await conn.execute(
                    "UPDATE analysis_batches SET received_count=received_count+1, "
                    "last_received_at=now() WHERE batch_no=$1",
                    batch_no,
                )
                return receipt, True

            # Insert the batch and wafers and receipt atomically.  Publishing
            # is evaluated in the same transaction but only flips if every
            # reference is already present and published.
            refs = list(batch["reference_batch_nos"])
            ready = await _references_published(conn, refs)
            received_at = await conn.fetchval("SELECT now()")
            receipt_doc = _receipt_doc(batch_no, digest, ready)
            receipt_doc["received_at"] = received_at.isoformat()

            await conn.execute(
                """
                INSERT INTO analysis_batches
                    (batch_no, device_id, accept_seq, status, median_nm,
                     reference_mean_nm, deviation_nm, reference_batch_nos,
                     source_sha256, decided_at, content_sha256, receipt_id,
                     receipt, source_instance, published, published_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12,$13::jsonb,
                        $14,$15,$16)
                """,
                batch_no, batch["device_id"], batch["accept_seq"], batch["status"],
                batch["median_nm"], batch["reference_mean_nm"],
                batch["deviation_nm"], batch["reference_batch_nos"],
                batch["sha256"], _parse_ts(batch["decided_at"]), digest,
                receipt_doc["receipt_id"], receipt_doc, source_instance,
                ready, received_at if ready else None,
            )
            await conn.executemany(
                "INSERT INTO analysis_wafers(batch_no, wafer_no, thickness_nm) "
                "VALUES ($1,$2,$3)",
                [(batch_no, w["wafer_no"], w["thickness_nm"])
                 for w in batch["wafers"]],
            )
            # Promote any batch (including this one and earlier dependents)
            # whose references are now all published, inside the same
            # transaction.  A crash can never leave a ready batch invisible.
            promoted = await _promote_ready(conn)
    if promoted:
        log.info("published %d batch(es) while accepting %s", promoted, batch_no)
    return await fetch_receipt(pool, batch_no) or receipt_doc, False


def _parse_ts(value: Optional[str]):
    if not value:
        return None
    from datetime import datetime
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


async def _references_published(conn: asyncpg.Connection,
                                refs: List[str]) -> bool:
    if not refs:
        return True
    rows = await conn.fetch(
        "SELECT batch_no, published FROM analysis_batches "
        "WHERE batch_no = ANY($1::text[])",
        refs,
    )
    published = {r["batch_no"] for r in rows if r["published"]}
    return all(ref in published for ref in refs)


async def _promote_ready(conn: asyncpg.Connection) -> int:
    """Promote unpublished batches whose references are all published.

    Runs to a fixpoint so that out-of-order delivery of a dependency chain is
    resolved in one pass.  Callable inside the acceptance transaction, which
    makes "detail + receipt + publication" one atomic effect.
    """
    total = 0
    for _ in range(1000):
        rows = await conn.fetch(
            "SELECT batch_no, reference_batch_nos FROM analysis_batches "
            "WHERE NOT published ORDER BY batch_no"
        )
        if not rows:
            break
        promoted: List[str] = []
        for row in rows:
            if await _references_published(conn, list(row["reference_batch_nos"] or [])):
                result = await conn.execute(
                    "UPDATE analysis_batches SET published=TRUE, "
                    "published_at=now() WHERE batch_no=$1 AND NOT published",
                    row["batch_no"])
                if result.endswith(" 1"):
                    promoted.append(row["batch_no"])
        if not promoted:
            break
        total += len(promoted)
        # Keep the stored receipt truthful about the current publish state.
        for batch_no in promoted:
            await _refresh_receipt(conn, batch_no)
    return total


async def publish_ready(pool: asyncpg.pool.Pool) -> int:
    """Promote ready batches outside an acceptance (startup reconciliation)."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            return await _promote_ready(conn)


async def _refresh_receipt(conn: asyncpg.Connection, batch_no: str) -> None:
    await conn.execute(
        """
        UPDATE analysis_batches
           SET receipt = jsonb_set(receipt, '{published}', 'true'::jsonb)
         WHERE batch_no=$1
        """,
        batch_no,
    )


async def fetch_receipt(pool: asyncpg.pool.Pool,
                        batch_no: str) -> Optional[Dict[str, Any]]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT receipt, published FROM analysis_batches WHERE batch_no=$1",
            batch_no)
    if row is None:
        return None
    receipt = dict(row["receipt"])
    receipt["published"] = row["published"]
    return receipt


async def get_batch(pool: asyncpg.pool.Pool,
                    batch_no: str) -> Optional[Dict[str, Any]]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM analysis_batches WHERE batch_no=$1", batch_no)
        if row is None:
            return None
        wafers = await conn.fetch(
            "SELECT wafer_no, thickness_nm FROM analysis_wafers "
            "WHERE batch_no=$1 ORDER BY wafer_no", batch_no)
    view = _batch_view(row)
    view["wafers"] = [
        {"wafer_no": w["wafer_no"], "thickness_nm": w["thickness_nm"]}
        for w in wafers
    ]
    return view


async def list_batches(pool: asyncpg.pool.Pool, *,
                       device_id: Optional[str],
                       published_only: bool,
                       limit: int,
                       offset: int) -> Tuple[int, List[Dict[str, Any]]]:
    where: List[str] = []
    params: List[Any] = []
    if device_id:
        params.append(device_id)
        where.append(f"device_id = ${len(params)}")
    if published_only:
        where.append("published = TRUE")
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    async with pool.acquire() as conn:
        total = await conn.fetchval(
            f"SELECT count(*) FROM analysis_batches{where_sql}", *params)
        params.extend([limit, offset])
        rows = await conn.fetch(
            f"SELECT * FROM analysis_batches{where_sql} "
            f"ORDER BY received_at, batch_no "
            f"LIMIT ${len(params) - 1} OFFSET ${len(params)}",
            *params)
    return total, [_batch_view(r) for r in rows]


async def get_batch_data(pool: asyncpg.pool.Pool,
                         batch_no: str) -> Optional[Dict[str, Any]]:
    """Published batch with full wafer detail (the analysis read model)."""
    view = await get_batch(pool, batch_no)
    if view is None or not view["published"]:
        return None
    return view


async def list_blocked(pool: asyncpg.pool.Pool) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM analysis_batches WHERE NOT published "
            "ORDER BY received_at, batch_no")
        out: List[Dict[str, Any]] = []
        for row in rows:
            refs = list(row["reference_batch_nos"] or [])
            missing = await _missing_refs(conn, refs)
            item = _batch_view(row)
            item["unpublished_reason"] = (
                "reference_not_received" if missing else "pending_publication")
            item["missing_reference_batch_nos"] = missing
            out.append(item)
        return out


async def _missing_refs(conn: asyncpg.Connection, refs: List[str]) -> List[str]:
    if not refs:
        return []
    rows = await conn.fetch(
        "SELECT batch_no FROM analysis_batches WHERE batch_no=ANY($1::text[]) "
        "AND published", refs)
    present = {r["batch_no"] for r in rows}
    return [r for r in refs if r not in present]
