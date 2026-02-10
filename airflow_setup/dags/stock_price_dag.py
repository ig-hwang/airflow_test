"""
Stock Price Collection DAG
Schedule: 0 23 * * * (daily at 23:00 UTC)
Collects OHLCV data, technical indicators, and fundamentals for all symbols.
"""

import logging
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import ta as ta_lib
import yfinance as yf
from sqlalchemy import text

# Build a reusable yfinance session that bypasses SSL cert issues in Docker
try:
    from curl_cffi import requests as _cffi_requests
    _YF_SESSION = _cffi_requests.Session(verify=False)
except ImportError:
    import requests as _requests
    _YF_SESSION = _requests.Session()
    _YF_SESSION.verify = False

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from common import ALL_SYMBOLS, DEFAULT_ARGS, get_engine

log = logging.getLogger(__name__)


# ── Task functions ─────────────────────────────────────────────────────────

def validate_db():
    """Check that stock_price and fundamental tables exist."""
    engine = get_engine()
    with engine.connect() as conn:
        for table in ("stock_prices", "stock_fundamentals"):
            row = conn.execute(
                text("SELECT to_regclass(:t)"), {"t": f"public.{table}"}
            ).fetchone()
            if row[0] is None:
                raise RuntimeError(f"Table '{table}' not found in stock_data DB")
    log.info("DB validation passed")


def fetch_and_upsert_prices(symbol: str, bootstrap: bool = False, **context):
    """Download OHLCV for *symbol* and bulk-upsert into stock_prices.

    Supports dag_run.conf override:
      - {"period": "3y"}   → fetch 3 years (full historical bootstrap)
      - {"bootstrap": true} → fetch 2 years
    Trigger via Airflow UI: Trigger DAG w/ config → {"period": "3y"}
    """
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}
    period_override = conf.get("period")
    if period_override:
        period = period_override
    else:
        period = "2y" if (bootstrap or conf.get("bootstrap", False)) else "60d"
    log.info("Fetching %s prices (period=%s)", symbol, period)

    df = yf.Ticker(symbol, session=_YF_SESSION).history(period=period)
    if df.empty:
        log.warning("No price data for %s", symbol)
        return

    df = df.reset_index()
    df.rename(columns={
        "Date": "trade_date", "Open": "open", "High": "high",
        "Low": "low", "Close": "close", "Volume": "volume",
    }, inplace=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    rows = [
        {
            "symbol":     symbol,
            "trade_date": r["trade_date"],
            "open":       float(r["open"])   if pd.notna(r["open"])   else None,
            "high":       float(r["high"])   if pd.notna(r["high"])   else None,
            "low":        float(r["low"])    if pd.notna(r["low"])    else None,
            "close":      float(r["close"])  if pd.notna(r["close"])  else None,
            "volume":     int(r["volume"])   if pd.notna(r["volume"]) else None,
        }
        for _, r in df.iterrows()
    ]

    with get_engine().begin() as conn:
        conn.execute(
            text("""
                INSERT INTO stock_prices (symbol, trade_date, open, high, low, close, volume)
                VALUES (:symbol, :trade_date, :open, :high, :low, :close, :volume)
                ON CONFLICT (symbol, trade_date) DO UPDATE SET
                    open       = EXCLUDED.open,
                    high       = EXCLUDED.high,
                    low        = EXCLUDED.low,
                    close      = EXCLUDED.close,
                    volume     = EXCLUDED.volume,
                    updated_at = NOW()
            """),
            rows,  # SQLAlchemy executemany — single round-trip
        )
    log.info("Upserted %d rows for %s", len(rows), symbol)
    time.sleep(1)  # yfinance rate-limit guard


def calculate_and_store_indicators(symbol: str):
    """Calculate technical indicators and bulk-update stock_prices.

    Fetches at most 250 rows (enough for SMA-200) instead of full history.
    """
    with get_engine().connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT id, trade_date, open, high, low, close, volume
                FROM stock_prices
                WHERE symbol = :s
                ORDER BY trade_date DESC
                LIMIT 250
            """),
            conn,
            params={"s": symbol},
        )

    if len(df) < 30:
        log.warning("Not enough rows (%d) for %s indicators", len(df), symbol)
        return

    df = df.sort_values("trade_date").reset_index(drop=True)
    close, high, low, volume = df["close"], df["high"], df["low"], df["volume"]

    # ── Indicators ────────────────────────────────────────────────────────
    df["sma_20"]     = ta_lib.trend.SMAIndicator(close, window=20).sma_indicator()
    df["sma_50"]     = ta_lib.trend.SMAIndicator(close, window=50).sma_indicator()
    df["sma_200"]    = ta_lib.trend.SMAIndicator(close, window=200).sma_indicator()

    bb = ta_lib.volatility.BollingerBands(close, window=20, window_dev=2)
    df["bb_upper"]   = bb.bollinger_hband()
    df["bb_middle"]  = bb.bollinger_mavg()
    df["bb_lower"]   = bb.bollinger_lband()

    df["rsi_14"]     = ta_lib.momentum.RSIIndicator(close, window=14).rsi()

    macd = ta_lib.trend.MACD(close, window_slow=26, window_fast=12, window_sign=9)
    df["macd"]        = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["macd_hist"]   = macd.macd_diff()

    df["cci_20"]     = ta_lib.trend.CCIIndicator(high, low, close, window=20).cci()
    df["atr_14"]     = ta_lib.volatility.AverageTrueRange(high, low, close, window=14).average_true_range()
    df["obv"]        = ta_lib.volume.OnBalanceVolumeIndicator(close, volume).on_balance_volume()
    df["mfi_14"]     = ta_lib.volume.MFIIndicator(high, low, close, volume, window=14).money_flow_index()

    indicator_cols = [
        "sma_20", "sma_50", "sma_200",
        "bb_upper", "bb_middle", "bb_lower",
        "rsi_14", "macd", "macd_signal", "macd_hist",
        "cci_20", "atr_14", "obv", "mfi_14",
    ]

    rows = [
        {
            "symbol":     symbol,
            "trade_date": r["trade_date"],
            **{col: (float(r[col]) if pd.notna(r[col]) else None) for col in indicator_cols},
        }
        for _, r in df.iterrows()
    ]

    with get_engine().begin() as conn:
        conn.execute(
            text("""
                UPDATE stock_prices SET
                    sma_20 = :sma_20, sma_50 = :sma_50, sma_200 = :sma_200,
                    bb_upper = :bb_upper, bb_middle = :bb_middle, bb_lower = :bb_lower,
                    rsi_14 = :rsi_14,
                    macd = :macd, macd_signal = :macd_signal, macd_hist = :macd_hist,
                    cci_20 = :cci_20, atr_14 = :atr_14, obv = :obv, mfi_14 = :mfi_14,
                    updated_at = NOW()
                WHERE symbol = :symbol AND trade_date = :trade_date
            """),
            rows,  # executemany
        )
    log.info("Indicators updated for %s (%d rows)", symbol, len(rows))


def fetch_and_store_fundamentals(symbol: str):
    """Fetch fundamental data via yfinance and upsert into stock_fundamentals."""
    try:
        info = yf.Ticker(symbol, session=_YF_SESSION).info
    except Exception as exc:
        log.warning("Fundamentals fetch failed for %s: %s", symbol, exc)
        return

    today = datetime.now(timezone.utc).date()

    with get_engine().begin() as conn:
        conn.execute(
            text("""
                INSERT INTO stock_fundamentals
                    (symbol, fetch_date, market_cap, pe_ratio, pb_ratio, roe,
                     eps, dividend_yield, sector, industry)
                VALUES (:symbol, :fetch_date, :market_cap, :pe_ratio, :pb_ratio, :roe,
                        :eps, :dividend_yield, :sector, :industry)
                ON CONFLICT (symbol, fetch_date) DO UPDATE SET
                    market_cap     = EXCLUDED.market_cap,
                    pe_ratio       = EXCLUDED.pe_ratio,
                    pb_ratio       = EXCLUDED.pb_ratio,
                    roe            = EXCLUDED.roe,
                    eps            = EXCLUDED.eps,
                    dividend_yield = EXCLUDED.dividend_yield,
                    sector         = EXCLUDED.sector,
                    industry       = EXCLUDED.industry,
                    updated_at     = NOW()
            """),
            {
                "symbol":         symbol,
                "fetch_date":     today,
                "market_cap":     info.get("marketCap"),
                "pe_ratio":       info.get("trailingPE"),
                "pb_ratio":       info.get("priceToBook"),
                "roe":            info.get("returnOnEquity"),
                "eps":            info.get("trailingEps"),
                "dividend_yield": info.get("dividendYield"),
                "sector":         info.get("sector"),
                "industry":       info.get("industry"),
            },
        )
    log.info("Fundamentals upserted for %s", symbol)
    time.sleep(1)


def _dag_complete():
    log.info("stock_price_collection DAG finished successfully")


# ── DAG definition ─────────────────────────────────────────────────────────

with DAG(
    dag_id="stock_price_collection",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 23 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "prices"],
    doc_md=__doc__,
) as dag:

    validate = PythonOperator(task_id="validate_db", python_callable=validate_db)

    with TaskGroup("fetch_prices_group") as fetch_group:
        [
            PythonOperator(
                task_id=f"fetch_{sym.replace('.', '_')}",
                python_callable=fetch_and_upsert_prices,
                op_kwargs={"symbol": sym},
                provide_context=True,
            )
            for sym in ALL_SYMBOLS
        ]

    with TaskGroup("calculate_indicators_group") as indicators_group:
        [
            PythonOperator(
                task_id=f"indicators_{sym.replace('.', '_')}",
                python_callable=calculate_and_store_indicators,
                op_kwargs={"symbol": sym},
            )
            for sym in ALL_SYMBOLS
        ]

    with TaskGroup("fetch_fundamentals_group") as fundamentals_group:
        [
            PythonOperator(
                task_id=f"fundamentals_{sym.replace('.', '_')}",
                python_callable=fetch_and_store_fundamentals,
                op_kwargs={"symbol": sym},
            )
            for sym in ALL_SYMBOLS
        ]

    complete = PythonOperator(task_id="dag_complete", python_callable=_dag_complete)

    validate >> fetch_group >> indicators_group >> fundamentals_group >> complete
