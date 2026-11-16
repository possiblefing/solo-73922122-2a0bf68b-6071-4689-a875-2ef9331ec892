"""Receiver-side schema: the independent analysis database.

Two tables mirror the delivered fact exactly: one row per accepted released
batch (decision snapshot, reference batch numbers, receipt) and one row per
wafer measurement.  ``published`` gates visibility: a batch is only published
once every batch number in its reference window has itself been received and
published, so a dependent batch can never become visible before the reference
batches it depends on.
"""
from __future__ import annotations

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS analysis_batches (
    batch_no            TEXT PRIMARY KEY,
    device_id           TEXT        NOT NULL,
    accept_seq          BIGINT      NOT NULL,
    status              TEXT        NOT NULL CHECK (status = 'RELEASED'),
    median_nm           DOUBLE PRECISION,
    reference_mean_nm   DOUBLE PRECISION,
    deviation_nm        DOUBLE PRECISION,
    reference_batch_nos JSONB       NOT NULL DEFAULT '[]'::jsonb,
    source_sha256       TEXT        NOT NULL,
    decided_at          TIMESTAMPTZ,
    content_sha256      TEXT        NOT NULL,
    receipt_id          TEXT        NOT NULL,
    receipt             JSONB       NOT NULL,
    source_instance     TEXT,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_received_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    received_count      INTEGER     NOT NULL DEFAULT 1,
    published           BOOLEAN     NOT NULL DEFAULT FALSE,
    published_at        TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS analysis_wafers (
    batch_no     TEXT    NOT NULL REFERENCES analysis_batches(batch_no)
                 ON DELETE CASCADE,
    wafer_no     TEXT    NOT NULL,
    thickness_nm INTEGER NOT NULL CHECK (thickness_nm > 0),
    CONSTRAINT analysis_wafers_pkey PRIMARY KEY (batch_no, wafer_no)
);

CREATE INDEX IF NOT EXISTS idx_analysis_device_seq
    ON analysis_batches (device_id, accept_seq);
CREATE INDEX IF NOT EXISTS idx_analysis_published
    ON analysis_batches (published, received_at);
CREATE INDEX IF NOT EXISTS idx_analysis_wafers_batch
    ON analysis_wafers (batch_no);
"""


async def init_schema(conn) -> None:
    await conn.execute(SCHEMA_SQL)
