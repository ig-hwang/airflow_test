"""
News Collection DAG
Schedule: 0 6 * * * (daily at 06:00 UTC)
Collects news via yfinance (listed symbols) and Google News RSS (all + private companies).
After collection, runs Claude AI analysis for each article (summary + expert analysis + sentiment).
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import feedparser
import yfinance as yf
from sqlalchemy import text

# Reusable yfinance session that bypasses SSL cert issues in Docker
try:
    from curl_cffi import requests as _cffi_requests
    _YF_SESSION = _cffi_requests.Session(impersonate="chrome110", verify=False)
except ImportError:
    import requests as _requests
    _YF_SESSION = _requests.Session()
    _YF_SESSION.verify = False

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from common import ALL_SYMBOLS, DEFAULT_ARGS, get_engine, make_neon_sync_task

log = logging.getLogger(__name__)

# Company names for AI prompt context
SYMBOL_NAMES = {
    "AVGO": "Broadcom", "BE": "Bloom Energy", "VRT": "Vertiv",
    "SMR": "NuScale Power", "OKLO": "Oklo", "GEV": "GE Vernova",
    "MRVL": "Marvell Technology", "COHR": "Coherent Corp", "LITE": "Lumentum",
    "VST": "Vistra Energy", "ETN": "Eaton Corporation",
    "267260.KS": "HD현대일렉트릭", "034020.KS": "두산에너빌리티",
    "028260.KS": "삼성물산", "267270.KS": "HD현대중공업", "010120.KS": "LS ELECTRIC",
    "SBGSY": "Schneider Electric", "HTHIY": "Hitachi",
    "TerraPower": "TerraPower", "X-Energy": "X-Energy",
}

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


def analyze_news_batch():
    """
    Call Claude API to generate expert analysis for news articles without ai_summary.
    Processes up to 60 articles per run (newest first) to stay within rate limits.
    Stores: summary (plain language) + expert analysis + sentiment (호재/악재/중립).
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        log.warning("ANTHROPIC_API_KEY not set — skipping AI analysis")
        return

    try:
        import anthropic
    except ImportError:
        log.warning("anthropic package not installed — skipping AI analysis")
        return

    client = anthropic.Anthropic(api_key=api_key)

    with get_engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT id, symbol, title, summary
            FROM stock_news
            WHERE ai_summary IS NULL
            ORDER BY published DESC NULLS LAST
            LIMIT 60
        """)).fetchall()

    if not rows:
        log.info("No articles need AI analysis")
        return

    log.info("Running AI analysis for %d articles", len(rows))

    updates = []
    for row in rows:
        article_id, symbol, title, raw_summary = row[0], row[1], row[2], row[3] or ""

        company = SYMBOL_NAMES.get(symbol, symbol)
        context = f"원문 요약: {raw_summary[:400]}" if raw_summary else ""

        prompt = f"""너는 주식·경제·IT·기업가치 분야의 전문 애널리스트야.
아래 뉴스 기사를 분석하고 반드시 JSON 형식으로만 응답해. 다른 텍스트는 절대 포함하지 마.

종목: {symbol} ({company})
제목: {title}
{context}

응답 형식 (JSON만):
{{
  "summary": "이 기사가 무엇에 관한 것인지 투자자가 이해하기 쉽게 2~3문장으로 설명 (한국어)",
  "analysis": "경제·주가·IT·기업가치 전문가 관점에서 이 뉴스가 갖는 의미와 시사점 2~3문장 (한국어)",
  "sentiment": "호재 또는 악재 또는 중립",
  "reason": "호재/악재/중립으로 판단한 핵심 근거 한 문장 (한국어)"
}}"""

        try:
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}],
            )
            result = json.loads(resp.content[0].text.strip())

            sentiment = result.get("sentiment", "중립")
            if sentiment not in ("호재", "악재", "중립"):
                sentiment = "중립"

            ai_text = (
                f"📌 내용 요약\n{result.get('summary', '')}\n\n"
                f"💡 전문가 분석\n{result.get('analysis', '')}\n\n"
                f"📊 판단 근거\n{result.get('reason', '')}"
            )

            updates.append({
                "id": article_id,
                "ai_summary": ai_text,
                "sentiment": sentiment,
            })
        except Exception as exc:
            log.warning("AI analysis failed for id=%d (%s): %s", article_id, title[:40], exc)

        time.sleep(0.3)  # Anthropic API rate-limit guard

    if updates:
        with get_engine().begin() as conn:
            conn.execute(
                text("UPDATE stock_news SET ai_summary=:ai_summary, sentiment=:sentiment WHERE id=:id"),
                updates,
            )
        log.info("AI analysis saved for %d articles", len(updates))


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

    analyze = PythonOperator(
        task_id="analyze_news",
        python_callable=analyze_news_batch,
    )

    complete = PythonOperator(task_id="news_complete", python_callable=_news_complete)

    sync_neon = PythonOperator(
        task_id="sync_to_neon",
        python_callable=make_neon_sync_task(
            ["stock_news"],
            not_null_map={"stock_news": ["title", "url"]},
        ),
    )

    [yf_group, gn_group] >> analyze >> complete >> sync_neon
