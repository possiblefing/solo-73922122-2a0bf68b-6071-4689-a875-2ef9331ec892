"""Canonical wire format shared by the delivery sender and the receiver.

The payload carries everything the analysis database needs for one released
batch: the full wafer detail, the decision snapshot and the batch numbers of
the reference window that produced the decision.  A deterministic SHA-256 over
the canonical JSON is the content identity used by the receiver to make
re-delivery idempotent and to reject a same-batch-number/different-content
submission.

This module deliberately has no application dependencies so the standalone
receiver can import it without pulling in the source's configuration.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, List

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
MAX_WAFERS = 1000


class PayloadValidationError(ValueError):
    """The delivery payload is structurally invalid."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PayloadValidationError(message)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def canonical_batch(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Validate a batch payload and return its canonical form.

    Raises :class:`PayloadValidationError` for anything that is not a complete
    released-batch snapshot.  Extra keys are ignored so later protocol
    extensions do not change the content identity of existing batches.
    """
    _require(isinstance(raw, dict), "batch must be an object")
    batch_no = raw.get("batch_no")
    device_id = raw.get("device_id")
    accept_seq = raw.get("accept_seq")
    status = raw.get("status")
    sha256 = raw.get("sha256")
    refs = raw.get("reference_batch_nos")
    wafers = raw.get("wafers")

    _require(isinstance(batch_no, str) and batch_no.strip() != "",
             "batch_no is required")
    _require(isinstance(device_id, str) and device_id.strip() != "",
             "device_id is required")
    _require(isinstance(accept_seq, int) and not isinstance(accept_seq, bool)
             and accept_seq >= 1, "accept_seq must be a positive integer")
    # The receiver only accepts released batches: quarantined batches are
    # never shipped by the sender and must be rejected if they arrive.
    _require(status == "RELEASED", "only RELEASED batches may be delivered")
    _require(isinstance(sha256, str) and bool(_HEX64.match(sha256)),
             "sha256 must be a lowercase hex digest")
    _require(isinstance(refs, list) and all(isinstance(r, str) and r for r in refs),
             "reference_batch_nos must be a list of batch numbers")
    _require(len(set(refs)) == len(refs), "duplicate reference batch number")
    _require(batch_no not in refs, "a batch cannot reference itself")

    _require(isinstance(wafers, list) and 1 <= len(wafers) <= MAX_WAFERS,
             "wafers must contain 1..1000 entries")
    seen = set()
    clean_wafers: List[Dict[str, Any]] = []
    for wafer in wafers:
        _require(isinstance(wafer, dict), "each wafer must be an object")
        wafer_no = wafer.get("wafer_no")
        thickness = wafer.get("thickness_nm")
        _require(isinstance(wafer_no, str) and wafer_no.strip() != "",
                 "wafer_no is required")
        _require(isinstance(thickness, int) and not isinstance(thickness, bool)
                 and thickness > 0,
                 "thickness_nm must be a positive integer")
        _require(wafer_no not in seen, f"duplicate wafer number within batch: {wafer_no}")
        seen.add(wafer_no)
        clean_wafers.append({"wafer_no": wafer_no, "thickness_nm": thickness})

    for field in ("median_nm", "reference_mean_nm", "deviation_nm"):
        value = raw.get(field)
        _require(value is None or _is_number(value),
                 f"{field} must be a number or null")
    decided_at = raw.get("decided_at")
    _require(decided_at is None or isinstance(decided_at, str),
             "decided_at must be an ISO timestamp string or null")

    return {
        "batch_no": batch_no,
        "device_id": device_id,
        "accept_seq": accept_seq,
        "status": "RELEASED",
        "median_nm": raw.get("median_nm"),
        "reference_mean_nm": raw.get("reference_mean_nm"),
        "deviation_nm": raw.get("deviation_nm"),
        "reference_batch_nos": list(refs),
        "sha256": sha256,
        "decided_at": decided_at,
        "wafers": clean_wafers,
    }


def canonical_json(batch: Dict[str, Any]) -> str:
    return json.dumps(batch, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


def content_digest(batch: Dict[str, Any]) -> str:
    """SHA-256 of the canonical payload.  ``batch`` must already be canonical."""
    return hashlib.sha256(canonical_json(batch).encode("utf-8")).hexdigest()


