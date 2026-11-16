"""Receiver-side contract tests: validation, auth, publication and receipts."""
import asyncio

import pytest

from app.delivery import HttpSender
from receiver.config import settings as receiver_settings

GOOD_BATCH = {
    "batch_no": "V1",
    "device_id": "EQ-V",
    "accept_seq": 1,
    "status": "RELEASED",
    "median_nm": 500.0,
    "reference_mean_nm": None,
    "deviation_nm": None,
    "reference_batch_nos": [],
    "sha256": "a" * 64,
    "decided_at": "2026-01-01T00:00:00+00:00",
    "wafers": [
        {"wafer_no": "W1", "thickness_nm": 500},
        {"wafer_no": "W2", "thickness_nm": 502},
    ],
}


def _with(**overrides):
    batch = dict(GOOD_BATCH)
    batch.update(overrides)
    return {"source_instance": "pytest", "batch": batch}


@pytest.mark.asyncio
async def test_receiver_rejects_invalid_payloads(receiver):
    cases = [
        _with(status="QUARANTINED"),                      # never ship quarantined
        _with(status="PENDING"),
        _with(wafers=[]),                                 # empty detail
        _with(wafers=[{"wafer_no": "W1", "thickness_nm": 0}]),
        _with(wafers=[{"wafer_no": "W1", "thickness_nm": 1.5}]),
        _with(wafers=[{"wafer_no": "W1", "thickness_nm": 5},
                      {"wafer_no": "W1", "thickness_nm": 6}]),
        _with(accept_seq=0),
        _with(sha256="not-a-digest"),
        _with(reference_batch_nos=["V1"]),                # self reference
        {"source_instance": "pytest"},                    # missing batch
    ]
    for payload in cases:
        r = await receiver.post("/analysis/batches", json=payload)
        assert r.status_code == 422, (payload, r.status_code, r.text)
    listing = (await receiver.get("/analysis/batches")).json()
    assert listing["total"] == 0


@pytest.mark.asyncio
async def test_receiver_bearer_token_enforced(receiver, monkeypatch):
    monkeypatch.setattr(receiver_settings, "token", "s3cret")
    r = await receiver.post("/analysis/batches", json=_with())
    assert r.status_code == 401
    r = await receiver.post(
        "/analysis/batches", json=_with(),
        headers={"Authorization": "Bearer wrong"})
    assert r.status_code == 401
    r = await receiver.post(
        "/analysis/batches", json=_with(),
        headers={"Authorization": "Bearer s3cret"})
    assert r.status_code == 201, r.text


@pytest.mark.asyncio
async def test_receiver_query_lists_only_published_by_default(receiver):
    # Receive a dependent batch without its reference: stored, not published.
    dependent = dict(GOOD_BATCH)
    dependent.update(batch_no="V2", accept_seq=2,
                     reference_batch_nos=["V-MISSING"])
    r = await receiver.post("/analysis/batches", json={"batch": dependent})
    assert r.status_code == 201
    assert r.json()["receipt"]["published"] is False

    assert (await receiver.get("/analysis/batches")).json()["total"] == 0
    all_batches = (await receiver.get(
        "/analysis/batches", params={"published_only": False})).json()
    assert all_batches["total"] == 1
    assert all_batches["items"][0]["batch_no"] == "V2"

    # Direct per-batch query works for stored-but-unpublished batches too,
    # flagged explicitly, while the published read model hides it.
    one = (await receiver.get("/analysis/batches/V2")).json()
    assert one["published"] is False
    assert (await receiver.get("/analysis/data")).json()["total"] == 0
    assert (await receiver.get("/analysis/data",
                               params={"batch_no": "V2"})).json()["total"] == 0


@pytest.mark.asyncio
async def test_receiver_receipt_lookup_404(receiver):
    assert (await receiver.get("/analysis/receipts/NOPE")).status_code == 404
    assert (await receiver.get("/analysis/batches/NOPE")).status_code == 404


@pytest.mark.asyncio
async def test_http_sender_against_real_uvicorn_listener(receiver):
    """Exercise the real httpx transport, not just the in-process ASGI app."""
    import uvicorn
    from receiver.main import app as receiver_app

    port = 8399
    config = uvicorn.Config(receiver_app, host="127.0.0.1", port=port,
                            log_level="warning", lifespan="on")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    try:
        for _ in range(100):
            if server.started:
                break
            await asyncio.sleep(0.05)
        assert server.started

        sender = HttpSender(f"http://127.0.0.1:{port}", "", 5.0)
        try:
            from app.delivery_protocol import canonical_batch, content_digest
            batch = canonical_batch(GOOD_BATCH)
            from app.delivery import DeliveryClaim
            claim = DeliveryClaim(
                batch_no="V1", batch_id=1, device_id="EQ-V", accept_seq=1,
                payload=batch, content_sha256=content_digest(batch), attempts=1)
            outcome = await sender(claim, "src", 1)
            assert outcome.kind == "delivered", outcome
            assert outcome.receipt["batch_no"] == "V1"

            # Same content again -> delivered with the same receipt id.
            outcome2 = await sender(claim, "src", 2)
            assert outcome2.kind == "delivered"
            assert outcome2.receipt["receipt_id"] == outcome.receipt["receipt_id"]

            # Different content, same batch number -> permanent rejection.
            bad = canonical_batch(dict(GOOD_BATCH, wafers=[
                {"wafer_no": "W1", "thickness_nm": 1}]))
            bad_claim = DeliveryClaim(
                batch_no="V1", batch_id=1, device_id="EQ-V", accept_seq=1,
                payload=bad, content_sha256=content_digest(bad), attempts=1)
            outcome3 = await sender(bad_claim, "src", 1)
            assert outcome3.kind == "rejected", outcome3
        finally:
            await sender.close()
    finally:
        server.should_exit = True
        await task


@pytest.mark.asyncio
async def test_http_sender_network_failure_is_retryable():
    sender = HttpSender("http://127.0.0.1:1", "", 0.5)
    try:
        from app.delivery import DeliveryClaim
        from app.delivery_protocol import canonical_batch, content_digest
        batch = canonical_batch(GOOD_BATCH)
        claim = DeliveryClaim(
            batch_no="V1", batch_id=1, device_id="EQ-V", accept_seq=1,
            payload=batch, content_sha256=content_digest(batch), attempts=1)
        outcome = await sender(claim, "src", 1)
        assert outcome.kind == "retryable"
        assert outcome.receipt is None
    finally:
        await sender.close()
