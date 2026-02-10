"""
Shared configuration for all stock DAGs.
Centralizes symbol lists, DB engine, and default_args to avoid duplication.
"""

import os
from datetime import timedelta

from sqlalchemy import create_engine

# ── Symbols ────────────────────────────────────────────────────────────────
US_SYMBOLS = [
    "AVGO", "BE", "VRT", "SMR", "OKLO",
    "GEV", "MRVL", "COHR", "LITE", "VST", "ETN",
]
KR_SYMBOLS = [
    "267260.KS", "034020.KS", "028260.KS", "267270.KS", "010120.KS",
]
ADR_SYMBOLS = ["SBGSY", "HTHIY"]
ALL_SYMBOLS = US_SYMBOLS + KR_SYMBOLS + ADR_SYMBOLS  # 18 total

# ── Default args ───────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── DB engine (module-level singleton per process) ─────────────────────────
# Each Airflow task runs in its own subprocess (LocalExecutor), so this
# singleton is created once per task process — not shared across tasks.
_engine = None


def get_engine():
    global _engine
    if _engine is None:
        conn_str = os.environ["DATA_DB_CONN"]
        _engine = create_engine(conn_str, pool_pre_ping=True, pool_size=2, max_overflow=2)
    return _engine
