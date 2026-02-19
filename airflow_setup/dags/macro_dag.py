"""
Macro Indicators Collection DAG
Schedule: 0 2 * * * (daily at 02:00 UTC, after US markets close)

Collects:
  - Market indices, FX, commodities, crypto via yfinance
  - US interest rates, yield curve, M2, HY spread via FRED API
  - Merges all series, forward-fills (Bitcoin 24/7 vs weekday markets)
  - Upserts into macro_indicators table → syncs to Neon
"""

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
from sqlalchemy import text

from airflow import DAG
from airflow.operators.python import PythonOperator

from common import DEFAULT_ARGS, get_engine, make_neon_sync_task

log = logging.getLogger(__name__)

# ── Ticker definitions ────────────────────────────────────────────────────────

YF_TICKERS = {
    "SP500":     "^GSPC",
    "Nasdaq100": "^NDX",
    "DowJones":  "^DJI",
    "KOSPI":     "^KS11",
    "KOSDAQ":    "^KQ11",
    "DXY":       "DX-Y.NYB",
    "USD_KRW":   "KRW=X",
    "WTI_Oil":   "CL=F",
    "Gold":      "GC=F",
    "Silver":    "SI=F",
    "Copper":    "HG=F",
    "Bitcoin":   "BTC-USD",
    "Ethereum":  "ETH-USD",
    "VIX":       "^VIX",
}

FRED_SERIES = {
    "US10Y":          "DGS10",
    "US2Y":           "DGS2",
    "YieldCurve":     "T10Y2Y",
    "M2_Supply":      "M2SL",
    "HighYield_Spread": "BAMLH0A0HYM2",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _yf_session():
    try:
        from curl_cffi import requests as r
        return r.Session(impersonate="chrome110", verify=False)
    except ImportError:
        return None


def _ensure_macro_table():
    """Create macro_indicators if it doesn't exist."""
    with get_engine().begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS macro_indicators (
                id          SERIAL PRIMARY KEY,
                series_key  VARCHAR(50)  NOT NULL,
                trade_date  DATE         NOT NULL,
                value       NUMERIC(20, 6),
                created_at  TIMESTAMP    DEFAULT NOW(),
                updated_at  TIMESTAMP    DEFAULT NOW(),
                UNIQUE (series_key, trade_date)
            )
        """))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_macro_series ON macro_indicators (series_key)"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_macro_date   ON macro_indicators (trade_date DESC)"
        ))


# ── Task ─────────────────────────────────────────────────────────────────────

def fetch_and_store_macro(**context):
    dag_run = context.get("dag_run")
    conf    = (dag_run.conf or {}) if dag_run else {}
    period  = conf.get("period", "60d")

    # Derive observation_start for FRED
    days_back = 730 if period == "2y" else 90
    obs_start = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    all_series: dict[str, pd.Series] = {}

    # ── 1. Yahoo Finance ──────────────────────────────────────────────────────
    session = _yf_session()
    tickers = list(YF_TICKERS.values())
    log.info("Downloading %d Yahoo Finance tickers (period=%s)", len(tickers), period)
    try:
        kwargs = dict(period=period, auto_adjust=True, progress=False)
        if session:
            kwargs["session"] = session
        raw = yf.download(tickers, **kwargs)

        # MultiIndex → pick Close level; single-ticker → flat
        if isinstance(raw.columns, pd.MultiIndex):
            close = raw["Close"]
        else:
            close = raw[["Close"]].rename(columns={"Close": tickers[0]})

        ticker_to_key = {v: k for k, v in YF_TICKERS.items()}
        for col in close.columns:
            key = ticker_to_key.get(col)
            if key:
                all_series[key] = close[col].dropna()
                log.info("YF %s (%s): %d rows", key, col, len(all_series[key]))
    except Exception as e:
        log.error("Yahoo Finance download failed: %s", e)

    # ── 2. FRED ───────────────────────────────────────────────────────────────
    fred_key = os.environ.get("FRED_API_KEY", "")
    if not fred_key:
        log.warning("FRED_API_KEY not set — skipping FRED data")
    else:
        try:
            import ssl as _ssl
            _ssl._create_default_https_context = _ssl._create_unverified_context
            from fredapi import Fred
            fred = Fred(api_key=fred_key)
            for key, series_id in FRED_SERIES.items():
                try:
                    s = fred.get_series(series_id, observation_start=obs_start)
                    s.name = key
                    all_series[key] = s.dropna()
                    log.info("FRED %s (%s): %d observations", key, series_id, len(s))
                except Exception as e:
                    log.error("FRED %s failed: %s", key, e)
        except Exception as e:
            log.error("FRED API initialisation failed: %s", e)

    if not all_series:
        log.warning("No data fetched — aborting")
        return

    # ── 3. Merge & forward-fill ───────────────────────────────────────────────
    merged = pd.DataFrame(all_series)
    merged.index = pd.to_datetime(merged.index).normalize()  # date-only index

    # Extend to continuous daily range, forward-fill gaps (weekends, holidays)
    date_range = pd.date_range(merged.index.min(), merged.index.max(), freq="D")
    merged = merged.reindex(date_range).ffill()
    log.info("Merged shape after ffill: %s", merged.shape)

    # ── 4. Melt → long format ─────────────────────────────────────────────────
    merged.index.name = "trade_date"
    long = (
        merged.reset_index()
        .melt(id_vars="trade_date", var_name="series_key", value_name="value")
        .dropna(subset=["value"])
    )
    long["trade_date"] = pd.to_datetime(long["trade_date"]).dt.date

    # ── 5. Ensure table & upsert ──────────────────────────────────────────────
    _ensure_macro_table()

    rows = [
        {"key": r["series_key"], "date": r["trade_date"], "val": float(r["value"])}
        for _, r in long.iterrows()
    ]
    with get_engine().begin() as conn:
        conn.execute(
            text("""
                INSERT INTO macro_indicators (series_key, trade_date, value)
                VALUES (:key, :date, :val)
                ON CONFLICT (series_key, trade_date)
                DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
            """),
            rows,
        )
    log.info("Upserted %d macro data points (%d series)", len(rows), long["series_key"].nunique())


def _macro_complete():
    log.info("macro_collection DAG finished successfully")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="macro_collection",
    default_args={**DEFAULT_ARGS, "retries": 1, "retry_delay": timedelta(minutes=10)},
    schedule_interval="0 2 * * *",   # 02:00 UTC daily (after US close)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["macro", "fred", "yfinance"],
    doc_md=__doc__,
) as dag:

    fetch = PythonOperator(
        task_id="fetch_and_store_macro",
        python_callable=fetch_and_store_macro,
    )

    complete = PythonOperator(
        task_id="macro_complete",
        python_callable=_macro_complete,
    )

    sync_neon = PythonOperator(
        task_id="sync_to_neon",
        python_callable=make_neon_sync_task(["macro_indicators"]),
    )

    fetch >> complete >> sync_neon
