"""Application configuration loaded from environment variables.

Works both under Docker Compose (PostgreSQL reached over TCP) and in the
local self-test (embedded PostgreSQL reached over a unix socket).
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _dsn_from_parts() -> str:
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    db = os.environ.get("POSTGRES_DB", "fab")
    host = os.environ.get("POSTGRES_HOST", "db")
    port = os.environ.get("POSTGRES_PORT", "5432")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


@dataclass
class Settings:
    database_dsn: str = os.environ.get("DATABASE_DSN", "") or _dsn_from_parts()
    # Directory where the original uploaded CSV files are persisted.
    storage_dir: str = os.environ.get("STORAGE_DIR", "/data/raw")
    worker_lock_id: int = int(os.environ.get("WORKER_LOCK_ID", "78219001"))
    worker_poll_seconds: float = float(os.environ.get("WORKER_POLL_SECONDS", "0.25"))
    # Artificial processing delay applied to batch ids listed in this file.
    # Used by the slow-batch demonstration; empty/absent file disables it.
    slow_marker_file: str = os.environ.get("SLOW_MARKER_FILE", "/data/slow_batches.txt")
    instance_id: str = os.environ.get("INSTANCE_ID", os.environ.get("HOSTNAME", "instance"))
    max_wafers: int = 1000
    # Connection pool sizing.  Intake transactions never block on a lock
    # (sequence allocation relies on a unique constraint + retry), so a small
    # pool is safe from starvation; the two extra headroom slots cover the
    # dedicated worker connection per process.
    db_pool_min: int = int(os.environ.get("DB_POOL_MIN", "2"))
    db_pool_max: int = int(os.environ.get("DB_POOL_MAX", "20"))
    # Decision rule knobs.
    baseline_count: int = 3          # first N accepted batches per device are released
    reference_count: int = 3         # compare against the N most recent released batches
    drift_threshold_nm: float = 5.0  # strictly greater than this => quarantine
    # Delivery to the independent analysis database.  An empty receiver URL
    # disables the feature entirely (no outbox worker, no network calls).
    delivery_receiver_url: str = os.environ.get("DELIVERY_RECEIVER_URL", "").rstrip("/")
    delivery_token: str = os.environ.get("DELIVERY_TOKEN", "")
    delivery_poll_seconds: float = float(os.environ.get("DELIVERY_POLL_SECONDS", "1.0"))
    delivery_timeout_seconds: float = float(
        os.environ.get("DELIVERY_TIMEOUT_SECONDS", "10"))
    delivery_lease_seconds: float = float(os.environ.get("DELIVERY_LEASE_SECONDS", "30"))
    delivery_retry_base_seconds: float = float(
        os.environ.get("DELIVERY_RETRY_BASE_SECONDS", "1.0"))
    delivery_retry_max_seconds: float = float(
        os.environ.get("DELIVERY_RETRY_MAX_SECONDS", "60"))
    delivery_reconcile_limit: int = int(
        os.environ.get("DELIVERY_RECONCILE_LIMIT", "200"))

    @property
    def delivery_enabled(self) -> bool:
        return bool(self.delivery_receiver_url)

    @property
    def crash_marker_file(self) -> str:
        return os.path.join(os.path.dirname(self.slow_marker_file), "crash_batches.txt")

    # Delivery demo hooks (same convention as the decision-worker markers:
    # absent file => feature disabled).
    @property
    def delivery_slow_marker_file(self) -> str:
        return os.path.join(os.path.dirname(self.slow_marker_file),
                            "delivery_slow_batches.txt")

    @property
    def delivery_crash_marker_file(self) -> str:
        """Batch numbers for which the source hard-exits *after* the receiver
        accepted the batch but *before* the receipt is recorded locally."""
        return os.path.join(os.path.dirname(self.slow_marker_file),
                            "delivery_crash_batches.txt")


settings = Settings()
