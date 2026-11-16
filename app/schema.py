"""PostgreSQL schema definition and initialisation.

Durability model
----------------
* ``batches`` carries every persisted fact about a submission and its
  decision.  Wafers are stored in a child table ``wafer_measurements``.
* A batch is processed inside a *single* transaction that inserts the
  decision and either appends the batch id to ``device_state.ref_batch_ids``
  (released only) or leaves it untouched.  Hence the decision, the reference
  history and data-lake visibility become visible atomically -- there is no
  observable intermediate ("half batch") state and the reference list can
  never be advanced twice.
* Row-level locking (``SELECT ... FOR UPDATE`` on ``device_state`` plus a
  transaction-scoped advisory lock on the device) serialises workers for one
  device while different devices proceed independently.
"""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS batches (
    id              BIGGENERATED_PLACEHOLDER,
    batch_no        TEXT        NOT NULL,
    device_id       TEXT        NOT NULL,
    sha256          TEXT        NOT NULL,
    size_bytes      BIGINT      NOT NULL,
    storage_path    TEXT        NOT NULL,
    accept_seq      BIGINT      NOT NULL,
    status          TEXT        NOT NULL CHECK (status IN ('PENDING','RELEASED','QUARANTINED')),
    median_nm       DOUBLE PRECISION,
    ref_mean_nm     DOUBLE PRECISION,
    deviation_nm    DOUBLE PRECISION,
    reference_ids   JSONB,
    decided_at      TIMESTAMPTZ,
    decided_by      TEXT,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT batches_pkey PRIMARY KEY (id),
    CONSTRAINT batches_batch_no_uniq UNIQUE (batch_no),
    CONSTRAINT batches_device_accept_uniq UNIQUE (device_id, accept_seq)
);

CREATE TABLE IF NOT EXISTS wafer_measurements (
    batch_id    BIGINT  NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    wafer_no    TEXT    NOT NULL,
    thickness_nm INTEGER NOT NULL CHECK (thickness_nm > 0),
    CONSTRAINT wafer_measurements_pkey PRIMARY KEY (batch_id, wafer_no)
);

CREATE INDEX IF NOT EXISTS idx_batches_device_seq
    ON batches (device_id, accept_seq);
CREATE INDEX IF NOT EXISTS idx_batches_status
    ON batches (status) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_wafers_batch ON wafer_measurements (batch_id);

-- One row per device: the durable processing cursor (last accepted sequence
-- that has been decided) and the FIFO list of released batch ids that form
-- the rolling reference window (most recent last).
CREATE TABLE IF NOT EXISTS device_state (
    device_id        TEXT PRIMARY KEY,
    last_decided_seq BIGINT NOT NULL DEFAULT 0,
    ref_batch_ids    JSONB  NOT NULL DEFAULT '[]'::jsonb,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Delivery outbox: one row per released batch that must be shipped to the
-- analysis receiver.  Rows are created for *released* batches only -- either
-- inside the decision transaction (best effort new batches) or by the
-- reconciler, which also backfills batches released before this feature was
-- deployed.  The original decision lives in ``batches`` and is never modified
-- by delivery, so a delivery failure cannot change a release/quarantine
-- verdict.
--
-- ``batch_no`` is the natural key (immutable, globally unique).  Concurrency
-- between two source instances is handled by a lease: a claim is an atomic
-- ``UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED)``.  ``DELIVERED`` is
-- only ever set after a 2xx response carrying a valid receipt; anything else
-- stays retryable with exponential backoff.
--
-- No foreign key to ``batches`` is used so operational resets that truncate
-- the decision tables keep working; the reconciler re-creates missing rows.
CREATE TABLE IF NOT EXISTS delivery_outbox (
    batch_no        TEXT PRIMARY KEY,
    batch_id        BIGINT      NOT NULL,
    device_id       TEXT        NOT NULL,
    accept_seq      BIGINT      NOT NULL,
    payload         JSONB       NOT NULL,
    content_sha256  TEXT        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING','IN_FLIGHT','DELIVERED','REJECTED')),
    attempts        INTEGER     NOT NULL DEFAULT 0,
    last_error      TEXT,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner     TEXT,
    lease_expires_at TIMESTAMPTZ,
    receipt         JSONB,
    delivered_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_delivery_due
    ON delivery_outbox (status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_delivery_device_seq
    ON delivery_outbox (device_id, accept_seq);
"""


async def init_schema(conn) -> None:
    # BIGSERIAL spelled out so the file stays readable on its own.
    sql = SCHEMA_SQL.replace("BIGGENERATED_PLACEHOLDER",
                             "BIGINT GENERATED ALWAYS AS IDENTITY")
    await conn.execute(sql)
