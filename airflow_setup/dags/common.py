"""
Shared configuration for all stock DAGs.
Centralizes symbol lists, DB engine, default_args, and Neon sync to avoid duplication.
"""

import csv
import io
import json
import logging
import os
import ssl
import urllib.request
import urllib.error
from datetime import timedelta

from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

# ── Symbols ────────────────────────────────────────────────────────────────
# DEPRECATED: Use get_symbols() instead to load from database
# These are kept as fallback if database is unavailable
_US_SYMBOLS_FALLBACK = [
    # 기존 테마주 (에너지/반도체/인프라)
    "AVGO", "BE", "VRT", "SMR", "OKLO",
    "GEV", "MRVL", "COHR", "LITE", "VST", "ETN",
    # 빅테크
    "AAPL", "MSFT", "AMZN", "NVDA", "META", "GOOGL",
    # 금융/헬스케어/소비재
    "BRK-B", "JPM", "UNH", "JNJ", "LLY", "PFE", "MRK", "ABBV",
    "AMGN", "ISRG", "PEP", "KO", "VZ", "CSCO",
    # 반도체/소재
    "AMD", "MU", "AMAT", "MP",
    # 글로벌 (US 거래소 상장)
    "TSM", "ASML", "ABBNY",
    # 신규
    "UBER",
]
_KR_SYMBOLS_FALLBACK = [
    "267260.KS", "034020.KS", "028260.KS", "267270.KS", "010120.KS",
]
_ADR_SYMBOLS_FALLBACK = [
    "SBGSY",   # Schneider Electric
    "HTHIY",   # Hitachi
    "FANUY",   # Fanuc
    "KYOCY",   # Keyence
    "SMCAY",   # SMC Corp
]

# Cached symbols loaded from database
_SYMBOLS_CACHE = None


def get_symbols(category: str = "ALL", force_reload: bool = False) -> list[str]:
    """
    Load active stock symbols from database.

    Args:
        category: "US", "KR", "ADR", or "ALL" (default)
        force_reload: Clear cache and reload from database

    Returns:
        List of active stock symbols
    """
    global _SYMBOLS_CACHE

    if force_reload:
        _SYMBOLS_CACHE = None

    if _SYMBOLS_CACHE is None:
        try:
            engine = get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT symbol, category FROM stock_symbols WHERE active = TRUE ORDER BY symbol"
                ))
                rows = result.fetchall()
                _SYMBOLS_CACHE = {
                    "US": [r[0] for r in rows if r[1] == "US"],
                    "KR": [r[0] for r in rows if r[1] == "KR"],
                    "ADR": [r[0] for r in rows if r[1] == "ADR"],
                }
            log.info("Loaded %d symbols from database", sum(len(v) for v in _SYMBOLS_CACHE.values()))
        except Exception as e:
            log.warning("Failed to load symbols from database, using fallback: %s", e)
            _SYMBOLS_CACHE = {
                "US": _US_SYMBOLS_FALLBACK,
                "KR": _KR_SYMBOLS_FALLBACK,
                "ADR": _ADR_SYMBOLS_FALLBACK,
            }

    if category == "ALL":
        return _SYMBOLS_CACHE["US"] + _SYMBOLS_CACHE["KR"] + _SYMBOLS_CACHE["ADR"]
    else:
        return _SYMBOLS_CACHE.get(category, [])


# Backward compatibility: maintain old variable names
US_SYMBOLS = get_symbols("US")
KR_SYMBOLS = get_symbols("KR")
ADR_SYMBOLS = get_symbols("ADR")
ALL_SYMBOLS = get_symbols("ALL")

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


# ── Neon HTTP sync ─────────────────────────────────────────────────────────
# Docker 컨테이너는 포트 5432가 차단돼있어 HTTPS(443)로 Neon HTTP API를 사용.

_NEON_HOST = "ep-small-firefly-ai94k736-pooler.c-4.us-east-1.aws.neon.tech"
_NEON_CONN = f"postgresql://neondb_owner:npg_TiDj4zKScRt7@{_NEON_HOST}/neondb"
_NEON_URL  = f"https://{_NEON_HOST}/sql"
_SSL = ssl.create_default_context()
_SSL.check_hostname = False
_SSL.verify_mode = ssl.CERT_NONE

_BATCH      = 200   # rows per INSERT for numeric-only tables
_BATCH_TEXT = 20    # rows per INSERT for tables with large text columns


def _neon_request(sql: str, params=None) -> dict:
    payload = json.dumps({"query": sql, **({"params": params} if params else {})}).encode()
    req = urllib.request.Request(
        _NEON_URL, data=payload,
        headers={"Content-Type": "application/json", "Neon-Connection-String": _NEON_CONN},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, context=_SSL, timeout=60) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        raise RuntimeError(f"Neon HTTP {e.code}: {body[:400]}") from e


def _local_rows(table: str) -> tuple[list[str], list[dict]]:
    """Read all rows as strings via PostgreSQL COPY TO CSV (type-safe)."""
    raw_conn = get_engine().raw_connection()
    try:
        buf = io.StringIO()
        cur = raw_conn.cursor()
        cur.copy_expert(f"COPY {table} TO STDOUT WITH CSV HEADER", buf)
        raw_conn.commit()
    finally:
        raw_conn.close()
    buf.seek(0)
    reader = csv.DictReader(buf)
    rows = list(reader)
    cols = list(reader.fieldnames or [])
    return cols, rows


def sync_table_to_neon(table: str, not_null_cols: list[str] | None = None):
    """Truncate the Neon table and re-insert all rows from local PostgreSQL."""
    cols, rows = _local_rows(table)
    if not rows:
        log.info("sync_to_neon: %s — empty, skipping", table)
        return

    # Filter rows where required columns are None
    if not_null_cols:
        before = len(rows)
        rows = [r for r in rows if all(r.get(c) not in (None, "") for c in not_null_cols)]
        if len(rows) < before:
            log.warning("sync_to_neon: %s — filtered %d NULL rows", table, before - len(rows))

    _neon_request(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")

    col_str = ", ".join(cols)
    n_cols  = len(cols)
    chunk   = _BATCH_TEXT if n_cols > 8 else _BATCH
    total   = 0

    for i in range(0, len(rows), chunk):
        batch = rows[i : i + chunk]
        placeholders, params = [], []
        for idx, row in enumerate(batch):
            base = idx * n_cols
            ph   = ", ".join(f"${base + j + 1}" for j in range(n_cols))
            placeholders.append(f"({ph})")
            for c in cols:
                v = row[c]
                params.append(None if (v is None or v == "") else v)
        sql = f"INSERT INTO {table} ({col_str}) VALUES {', '.join(placeholders)}"
        _neon_request(sql, params)
        total += len(batch)

    log.info("sync_to_neon: %s — %d rows synced", table, total)


def make_neon_sync_task(tables: list[str], not_null_map: dict | None = None):
    """Return a callable suitable for PythonOperator that syncs given tables."""
    def _sync():
        for table in tables:
            nn = (not_null_map or {}).get(table)
            try:
                sync_table_to_neon(table, not_null_cols=nn)
            except Exception as e:
                log.error("sync_to_neon: %s failed — %s", table, e)
                raise
    return _sync
