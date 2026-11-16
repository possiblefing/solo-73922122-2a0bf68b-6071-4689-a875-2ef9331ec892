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
"""


async def init_schema(conn) -> None:
    # BIGSERIAL spelled out so the file stays readable on its own.
    sql = SCHEMA_SQL.replace("BIGGENERATED_PLACEHOLDER",
                             "BIGINT GENERATED ALWAYS AS IDENTITY")
    await conn.execute(sql)
