"""
News Collection DAG
Schedule: 0 6 * * * (daily at 06:00 UTC)
Collects news via yfinance (listed symbols) and Google News RSS (all + private companies).
"""

import logging
import time
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import feedparser
import yfinance as yf
from sqlalchemy import text

# Reusable yfinance session that bypasses SSL cert issues in Docker
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

# Google News search queries → (symbol_key, query_string)
GOOGLE_NEWS_QUERIES = [
    ("AVGO",       "Broadcom AVGO stock"),
    ("BE",         "Bloom Energy stock"),
    ("VRT",        "Vertiv Holdings stock"),
    ("SMR",        "NuScale Power SMR stock"),
    ("OKLO",       "Oklo nuclear stock"),
    ("GEV",        "GE Vernova stock"),
    ("MRVL",       "Marvell Technology stock"),
    ("COHR",       "Coherent Corp stock"),
    ("LITE",       "Lumentum Holdings stock"),
    ("VST",        "Vistra Energy stock"),
    ("ETN",        "Eaton Corporation stock"),
    ("267260.KS",  "HD현대일렉트릭"),
    ("034020.KS",  "두산에너빌리티"),
    ("028260.KS",  "삼성물산"),
    ("267270.KS",  "HD현대중공업"),
    ("010120.KS",  "LS ELECTRIC"),
    ("SBGSY",      "Schneider Electric SBGSY"),
    ("HTHIY",      "Hitachi HTHIY stock"),
    # Private companies — Google News only (no ticker)
    ("TerraPower", "TerraPower nuclear"),
    ("X-Energy",   "X-energy reactor"),
]

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

_NEWS_ARGS = {**DEFAULT_ARGS, "retries": 1, "retry_delay": timedelta(minutes=10)}


def _upsert_news(rows: list[dict]) -> int:
    """Bulk-insert news rows, ignoring duplicates by (symbol, url)."""
    if not rows:
        return 0
    with get_engine().begin() as conn:
        result = conn.execute(
            text("""
                INSERT INTO stock_news (symbol, title, url, source, published, summary)
                VALUES (:symbol, :title, :url, :source, :published, :summary)
                ON CONFLICT (symbol, url) DO NOTHING
            """),
            rows,  # executemany
        )
    return result.rowcount


def fetch_yfinance_news(symbol: str):
    """Fetch news from yfinance for listed symbols."""
    try:
        news_items = yf.Ticker(symbol, session=_YF_SESSION).news or []
    except Exception as exc:
        log.warning("yfinance news failed for %s: %s", symbol, exc)
        return

    rows = []
    for item in news_items:
        pub_ts = item.get("providerPublishTime")
        rows.append({
            "symbol":    symbol,
            "title":     (item.get("title") or "")[:500],
            "url":       (item.get("link") or "")[:1000],
            "source":    (item.get("publisher") or "")[:200],
            "published": datetime.utcfromtimestamp(pub_ts) if pub_ts else None,
            "summary":   (item.get("summary") or "")[:2000] or None,
        })

    inserted = _upsert_news(rows)
    log.info("yfinance: %d new / %d fetched for %s", inserted, len(rows), symbol)
    time.sleep(0.5)


def fetch_google_news(symbol_key: str, query: str):
    """Fetch news from Google News RSS."""
    url = GOOGLE_NEWS_RSS.format(query=quote_plus(query))
    try:
        feed = feedparser.parse(url)
    except Exception as exc:
        log.warning("feedparser failed for %s: %s", symbol_key, exc)
        return

    rows = []
    for entry in feed.entries:
        published = None
        if getattr(entry, "published_parsed", None):
            published = datetime(*entry.published_parsed[:6])

        # feedparser source is a FeedParserDict or may be absent
        source_obj = getattr(entry, "source", None) or {}
        source = (source_obj.get("title") if isinstance(source_obj, dict) else str(source_obj))
        source = (source or "Google News")[:200]

        summary = getattr(entry, "summary", None)
        rows.append({
            "symbol":    symbol_key,
            "title":     (getattr(entry, "title", "") or "")[:500],
            "url":       (getattr(entry, "link", "") or "")[:1000],
            "source":    source,
            "published": published,
            "summary":   (summary or "")[:2000] or None,
        })

    inserted = _upsert_news(rows)
    log.info("Google News: %d new / %d fetched for %s", inserted, len(rows), symbol_key)
    time.sleep(1)


def _news_complete():
    log.info("news_collection DAG finished successfully")


# ── DAG definition ─────────────────────────────────────────────────────────

with DAG(
    dag_id="news_collection",
    default_args=_NEWS_ARGS,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "news"],
    doc_md=__doc__,
) as dag:

    with TaskGroup("yfinance_news_group") as yf_group:
        [
            PythonOperator(
                task_id=f"yf_news_{sym.replace('.', '_')}",
                python_callable=fetch_yfinance_news,
                op_kwargs={"symbol": sym},
            )
            for sym in ALL_SYMBOLS
        ]

    with TaskGroup("google_news_group") as gn_group:
        [
            PythonOperator(
                task_id=f"gnews_{sym_key.replace('-', '_').replace('.', '_')}",
                python_callable=fetch_google_news,
                op_kwargs={"symbol_key": sym_key, "query": query},
            )
            for sym_key, query in GOOGLE_NEWS_QUERIES
        ]

    complete = PythonOperator(task_id="news_complete", python_callable=_news_complete)

    [yf_group, gn_group] >> complete
