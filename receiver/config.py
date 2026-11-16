"""Receiver configuration (independent analysis database)."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _dsn_from_parts() -> str:
    user = os.environ.get("ANALYSIS_PG_USER", os.environ.get("POSTGRES_USER", "postgres"))
    password = os.environ.get("ANALYSIS_PG_PASSWORD",
                              os.environ.get("POSTGRES_PASSWORD", "postgres"))
    db = os.environ.get("ANALYSIS_PG_DB", "analysis")
    host = os.environ.get("ANALYSIS_PG_HOST", "analysis-db")
    port = os.environ.get("ANALYSIS_PG_PORT", "5432")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


@dataclass
class ReceiverSettings:
    database_dsn: str = os.environ.get("ANALYSIS_DATABASE_DSN", "") or _dsn_from_parts()
    # When set, every request must send ``Authorization: Bearer <token>``.
    token: str = os.environ.get("DELIVERY_TOKEN", "")
    db_pool_min: int = int(os.environ.get("ANALYSIS_DB_POOL_MIN", "1"))
    db_pool_max: int = int(os.environ.get("ANALYSIS_DB_POOL_MAX", "10"))
    instance_id: str = os.environ.get("ANALYSIS_INSTANCE_ID",
                                      os.environ.get("HOSTNAME", "receiver"))


settings = ReceiverSettings()
