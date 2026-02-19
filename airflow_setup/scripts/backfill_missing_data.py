"""
Standalone backfill script — fills missing stock price data on Neon DB.
Uses Neon HTTP API (port 443) since port 5432 is blocked.
Run with: .venv/bin/python3 airflow_setup/scripts/backfill_missing_data.py
"""

import json
import logging
import ssl
import time
import urllib.error
import urllib.request

import pandas as pd
import ta as ta_lib
import yfinance as yf

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Neon HTTP API ──────────────────────────────────────────────────────────
_NEON_HOST = "ep-small-firefly-ai94k736-pooler.c-4.us-east-1.aws.neon.tech"
_NEON_CONN = f"postgresql://neondb_owner:npg_TiDj4zKScRt7@{_NEON_HOST}/neondb"
_NEON_URL  = f"https://{_NEON_HOST}/sql"
_SSL = ssl.create_default_context()
_SSL.check_hostname = False
_SSL.verify_mode = ssl.CERT_NONE


def _neon(sql: str, params=None) -> dict:
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


def neon_query_df(sql: str, params=None) -> pd.DataFrame:
    """Run SELECT via Neon HTTP API and return a DataFrame."""
    result = _neon(sql, params)
    rows = result.get("rows", [])
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── yfinance session ──────────────────────────────────────────────────────
try:
    from curl_cffi import requests as _cffi_requests
    _YF_SESSION = _cffi_requests.Session(impersonate="chrome110", verify=False)
    log.info("Using curl_cffi session")
except ImportError:
    import requests as _requests
    _YF_SESSION = _requests.Session()
    _YF_SESSION.verify = False
    log.info("Using requests session (no curl_cffi)")

# ── Symbols ───────────────────────────────────────────────────────────────
US_SYMBOLS = [
    "AVGO", "BE", "VRT", "SMR", "OKLO",
    "GEV", "MRVL", "COHR", "LITE", "VST", "ETN",
    "AAPL", "MSFT", "AMZN", "NVDA", "META", "GOOGL",
    "BRK-B", "JPM", "UNH", "JNJ", "LLY", "PFE", "MRK", "ABBV",
    "AMGN", "ISRG", "PEP", "KO", "VZ", "CSCO",
    "AMD", "MU", "AMAT", "MP",
    "TSM", "ASML", "ABBNY",
]
KR_SYMBOLS = [
    "267260.KS", "034020.KS", "028260.KS", "267270.KS", "010120.KS",
]
ADR_SYMBOLS = [
    "SBGSY", "HTHIY", "FANUY", "KYOCY", "SMCAY",
]
ALL_SYMBOLS = US_SYMBOLS + KR_SYMBOLS + ADR_SYMBOLS


# ── Batch upsert helper ────────────────────────────────────────────────────
def _upsert_prices_batch(rows: list[dict]):
    """Batch upsert into stock_prices via Neon HTTP."""
    if not rows:
        return
    BATCH = 100
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        n_cols = 7  # symbol, trade_date, open, high, low, close, volume
        placeholders, params = [], []
        for idx, r in enumerate(batch):
            base = idx * n_cols
            placeholders.append(
                f"(${base+1}, ${base+2}, ${base+3}, ${base+4}, ${base+5}, ${base+6}, ${base+7})"
            )
            params.extend([
                r["symbol"], str(r["trade_date"]),
                str(r["open"]) if r["open"] is not None else None,
                str(r["high"]) if r["high"] is not None else None,
                str(r["low"])  if r["low"]  is not None else None,
                str(r["close"]) if r["close"] is not None else None,
                str(r["volume"]) if r["volume"] is not None else None,
            ])
        sql = f"""
            INSERT INTO stock_prices (symbol, trade_date, open, high, low, close, volume)
            VALUES {', '.join(placeholders)}
            ON CONFLICT (symbol, trade_date) DO UPDATE SET
                open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                close = EXCLUDED.close, volume = EXCLUDED.volume, updated_at = NOW()
        """
        _neon(sql, params)


def _update_indicators_batch(symbol: str, rows: list[dict]):
    """Batch update indicator columns via a single UPDATE ... FROM (VALUES ...) per batch."""
    if not rows:
        return
    # Filter to rows where at least rsi_14 is present (not all-NULL)
    rows = [r for r in rows if r.get("rsi_14") is not None]
    if not rows:
        return

    BATCH = 50  # rows per HTTP request
    ind_cols = [
        "sma_20", "sma_50", "sma_200",
        "bb_upper", "bb_middle", "bb_lower",
        "rsi_14", "macd", "macd_signal", "macd_hist",
        "cci_20", "atr_14", "obv", "mfi_14",
    ]

    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        placeholders, params = [], []
        n_cols = 2 + len(ind_cols)  # trade_date + symbol + indicators

        for idx, r in enumerate(batch):
            base = idx * n_cols
            ph = ", ".join(f"${base + j + 1}" for j in range(n_cols))
            placeholders.append(f"({ph})")
            params.append(str(r["trade_date"]))
            params.append(symbol)
            for col in ind_cols:
                v = r.get(col)
                if v is None:
                    params.append(None)
                elif col == "obv":
                    params.append(str(int(v)))
                else:
                    params.append(str(v))

        vals_clause = ", ".join(placeholders)
        set_clause = ", ".join(
            f"{col} = v.{col}::numeric" for col in ind_cols
        )
        col_defs = (
            "trade_date date, symbol text, "
            + ", ".join(f"{col} text" for col in ind_cols)
        )

        sql = f"""
            UPDATE stock_prices sp
            SET {set_clause}, updated_at = NOW()
            FROM (VALUES {vals_clause}) AS v({col_defs.replace('text', '').replace('date', '').replace(':', '').replace('symbol', '').replace('trade_date', '').strip()})
            WHERE sp.symbol = v.symbol AND sp.trade_date = v.trade_date
        """
        # Build column list for the VALUES alias
        alias_cols = "trade_date, symbol, " + ", ".join(ind_cols)
        sql = f"""
            UPDATE stock_prices sp
            SET {set_clause}, updated_at = NOW()
            FROM (VALUES {vals_clause}) AS v({alias_cols})
            WHERE sp.symbol = v.symbol::text AND sp.trade_date = v.trade_date::date
        """
        _neon(sql, params)


# ── Main tasks ────────────────────────────────────────────────────────────

def fetch_and_upsert_prices(symbol: str):
    log.info("Fetching prices for %s ...", symbol)
    try:
        df = yf.Ticker(symbol, session=_YF_SESSION).history(period="60d")
    except Exception as e:
        log.error("Failed to fetch %s: %s", symbol, e)
        return

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
    _upsert_prices_batch(rows)
    log.info("Upserted %d rows for %s", len(rows), symbol)


def calculate_and_store_indicators(symbol: str):
    log.info("Calculating indicators for %s ...", symbol)
    df = neon_query_df(
        "SELECT trade_date, open, high, low, close, volume "
        "FROM stock_prices WHERE symbol = $1 "
        "ORDER BY trade_date DESC LIMIT 250",
        [symbol],
    )

    if df.empty or len(df) < 30:
        log.warning("Not enough rows (%d) for %s indicators", len(df), symbol)
        return

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("trade_date").reset_index(drop=True)
    close, high, low, volume = df["close"], df["high"], df["low"], df["volume"]

    df["sma_20"]  = ta_lib.trend.SMAIndicator(close, window=20).sma_indicator()
    df["sma_50"]  = ta_lib.trend.SMAIndicator(close, window=50).sma_indicator()
    df["sma_200"] = ta_lib.trend.SMAIndicator(close, window=200).sma_indicator()

    bb = ta_lib.volatility.BollingerBands(close, window=20, window_dev=2)
    df["bb_upper"]  = bb.bollinger_hband()
    df["bb_middle"] = bb.bollinger_mavg()
    df["bb_lower"]  = bb.bollinger_lband()

    df["rsi_14"] = ta_lib.momentum.RSIIndicator(close, window=14).rsi()

    macd = ta_lib.trend.MACD(close, window_slow=26, window_fast=12, window_sign=9)
    df["macd"]        = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["macd_hist"]   = macd.macd_diff()

    df["cci_20"] = ta_lib.trend.CCIIndicator(high, low, close, window=20).cci()
    df["atr_14"] = ta_lib.volatility.AverageTrueRange(high, low, close, window=14).average_true_range()
    df["obv"]    = ta_lib.volume.OnBalanceVolumeIndicator(close, volume).on_balance_volume()
    df["mfi_14"] = ta_lib.volume.MFIIndicator(high, low, close, volume, window=14).money_flow_index()

    indicator_cols = [
        "sma_20", "sma_50", "sma_200",
        "bb_upper", "bb_middle", "bb_lower",
        "rsi_14", "macd", "macd_signal", "macd_hist",
        "cci_20", "atr_14", "obv", "mfi_14",
    ]
    rows = [
        {
            "trade_date": r["trade_date"],
            **{col: (float(r[col]) if pd.notna(r[col]) else None) for col in indicator_cols},
        }
        for _, r in df.iterrows()
    ]
    _update_indicators_batch(symbol, rows)
    log.info("Indicators updated for %s (%d rows)", symbol, len(rows))


def fix_sequences():
    """Sync Neon sequences to max(id) — needed after TRUNCATE RESTART IDENTITY + explicit-id inserts."""
    for table in ("stock_prices", "stock_fundamentals"):
        try:
            result = _neon(f"SELECT MAX(id) AS mx FROM {table}")
            max_id = result["rows"][0]["mx"]
            if max_id is not None:
                seq = f"{table}_id_seq"
                _neon(f"SELECT setval('{seq}', {max_id})")
                log.info("Fixed sequence %s → %s", seq, max_id)
        except Exception as e:
            log.warning("Could not fix sequence for %s: %s", table, e)


def main():
    # Quick connectivity check
    result = _neon("SELECT COUNT(*) AS cnt FROM stock_prices")
    count = result["rows"][0]["cnt"]
    log.info("Connected to Neon. stock_prices has %s rows total.", count)

    # Fix sequences so SERIAL inserts don't conflict with existing IDs
    fix_sequences()

    failed = []
    for i, sym in enumerate(ALL_SYMBOLS, 1):
        log.info("── [%d/%d] %s ──", i, len(ALL_SYMBOLS), sym)
        try:
            fetch_and_upsert_prices(sym)
            calculate_and_store_indicators(sym)
        except Exception as e:
            log.error("Error processing %s: %s", sym, e)
            failed.append(sym)
        time.sleep(1)

    result = _neon("SELECT COUNT(*) AS cnt FROM stock_prices")
    count = result["rows"][0]["cnt"]
    log.info("Done. stock_prices now has %s rows total.", count)
    if failed:
        log.warning("Failed symbols: %s", failed)
    else:
        log.info("All symbols processed successfully.")


if __name__ == "__main__":
    main()
