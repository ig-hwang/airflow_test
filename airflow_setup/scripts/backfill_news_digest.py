"""
Standalone backfill for news + weekly digest on Neon DB.
- Fetches news via yfinance + Google News RSS (2/12~today)
- Runs Claude AI analysis if ANTHROPIC_API_KEY is set
- Generates weekly digest for 2026-02-12 ~ 2026-02-18

Run: ANTHROPIC_API_KEY=sk-... .venv/bin/python3 airflow_setup/scripts/backfill_news_digest.py
"""

import json
import logging
import os
import ssl
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import feedparser
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


def neon_rows(sql: str, params=None) -> list[dict]:
    return _neon(sql, params).get("rows", [])


# ── yfinance session ──────────────────────────────────────────────────────
import requests as _requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from curl_cffi import requests as _cffi_requests
    _YF_SESSION = _cffi_requests.Session(impersonate="chrome110", verify=False)
    log.info("Using curl_cffi session")
except ImportError:
    _YF_SESSION = _requests.Session()
    _YF_SESSION.verify = False

# Shared session for RSS fetching (SSL disabled)
_RSS_SESSION = _requests.Session()
_RSS_SESSION.verify = False
_RSS_SESSION.headers.update({"User-Agent": "Mozilla/5.0 (compatible; RSS reader)"})

# ── Constants (from news_dag.py) ──────────────────────────────────────────
ALL_SYMBOLS = [
    "AVGO", "BE", "VRT", "SMR", "OKLO", "GEV", "MRVL", "COHR", "LITE", "VST", "ETN",
    "AAPL", "MSFT", "AMZN", "NVDA", "META", "GOOGL",
    "BRK-B", "JPM", "UNH", "JNJ", "LLY", "PFE", "MRK", "ABBV",
    "AMGN", "ISRG", "PEP", "KO", "VZ", "CSCO",
    "AMD", "MU", "AMAT", "MP",
    "TSM", "ASML", "ABBNY",
    "267260.KS", "034020.KS", "028260.KS", "267270.KS", "010120.KS",
    "SBGSY", "HTHIY", "FANUY", "KYOCY", "SMCAY",
]

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
    ("TerraPower", "TerraPower nuclear"),
    ("X-Energy",   "X-energy reactor"),
]

MARKET_FEEDS = [
    ("미국 경제 & 연준",  "Federal Reserve interest rates CPI inflation GDP US economy"),
    ("주식시장 동향",      "S&P 500 NASDAQ Dow Jones stock market weekly"),
    ("기업 실적 발표",     "earnings report quarterly results EPS revenue beat miss"),
    ("AI & 반도체",        "AI semiconductor Broadcom Marvell Nvidia chip demand"),
    ("에너지 & 원자력",    "nuclear energy SMR power grid electricity utility"),
    ("글로벌 & 지정학",    "global economy trade tariff China geopolitics currency"),
]

GNEWS_URL = "https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"


# ── News helpers ───────────────────────────────────────────────────────────

def _fix_news_sequence():
    result = neon_rows("SELECT MAX(id) AS mx FROM stock_news")
    max_id = result[0]["mx"] if result else None
    if max_id:
        _neon(f"SELECT setval('stock_news_id_seq', {max_id})")
        log.info("Fixed stock_news sequence → %s", max_id)


def _insert_news_batch(rows: list[dict]) -> int:
    """Insert news rows to Neon, ignoring duplicates by (symbol, url)."""
    if not rows:
        return 0
    inserted = 0
    BATCH = 20
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        n_cols = 6
        placeholders, params = [], []
        for idx, r in enumerate(batch):
            base = idx * n_cols
            placeholders.append(
                f"(${base+1}, ${base+2}, ${base+3}, ${base+4}, ${base+5}, ${base+6})"
            )
            params.extend([
                r["symbol"],
                r["title"],
                r["url"],
                r["source"],
                str(r["published"]) if r["published"] else None,
                r["summary"],
            ])
        sql = f"""
            INSERT INTO stock_news (symbol, title, url, source, published, summary)
            VALUES {', '.join(placeholders)}
            ON CONFLICT (symbol, url) DO NOTHING
        """
        result = _neon(sql, params)
        inserted += result.get("rowCount", 0)
    return inserted


def fetch_yfinance_news_to_neon(symbol: str):
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
            "published": datetime.utcfromtimestamp(pub_ts).isoformat() if pub_ts else None,
            "summary":   (item.get("summary") or "")[:2000] or None,
        })

    inserted = _insert_news_batch(rows)
    log.info("yfinance %s: %d new / %d fetched", symbol, inserted, len(rows))
    time.sleep(0.5)


def fetch_google_news_to_neon(symbol_key: str, query: str):
    url = GNEWS_URL.format(q=quote_plus(query))
    try:
        resp = _RSS_SESSION.get(url, timeout=10)
        feed = feedparser.parse(resp.content)
    except Exception as exc:
        log.warning("feedparser failed for %s: %s", symbol_key, exc)
        return

    rows = []
    for entry in feed.entries:
        published = None
        if getattr(entry, "published_parsed", None):
            published = datetime(*entry.published_parsed[:6]).isoformat()

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

    inserted = _insert_news_batch(rows)
    log.info("Google News %s: %d new / %d fetched", symbol_key, inserted, len(rows))
    time.sleep(1)


def analyze_news_with_ai(api_key: str, limit: int = 100):
    """Run Claude AI analysis for articles missing ai_summary."""
    try:
        import anthropic
    except ImportError:
        log.warning("anthropic package not installed — skipping AI analysis")
        return

    rows = neon_rows("""
        SELECT id, symbol, title, summary
        FROM stock_news
        WHERE ai_summary IS NULL
        ORDER BY published DESC NULLS LAST
        LIMIT $1
    """, [str(limit)])

    if not rows:
        log.info("No articles need AI analysis")
        return

    log.info("Running AI analysis for %d articles ...", len(rows))
    client = anthropic.Anthropic(api_key=api_key)

    for row in rows:
        article_id = row["id"]
        symbol = row["symbol"]
        title = row["title"]
        raw_summary = row.get("summary") or ""
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
            raw = resp.content[0].text.strip()
            # Strip markdown code fences if present (```json ... ```)
            if raw.startswith("```"):
                raw = raw.split("```", 2)[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.rsplit("```", 1)[0].strip()
            result = json.loads(raw)
            sentiment = result.get("sentiment", "중립")
            if sentiment not in ("호재", "악재", "중립"):
                sentiment = "중립"
            ai_text = (
                f"📌 내용 요약\n{result.get('summary', '')}\n\n"
                f"💡 전문가 분석\n{result.get('analysis', '')}\n\n"
                f"📊 판단 근거\n{result.get('reason', '')}"
            )
            _neon(
                "UPDATE stock_news SET ai_summary=$1, sentiment=$2 WHERE id=$3",
                [ai_text, sentiment, str(article_id)],
            )
            log.info("AI done: id=%s %s", article_id, title[:40])
        except Exception as exc:
            log.warning("AI failed id=%s: %s", article_id, exc)

        time.sleep(0.3)


# ── Weekly digest ──────────────────────────────────────────────────────────

def _rss_headlines(query: str, n: int = 8) -> list[str]:
    try:
        resp = _RSS_SESSION.get(GNEWS_URL.format(q=quote_plus(query)), timeout=10)
        feed = feedparser.parse(resp.content)
        return [e.title for e in feed.entries[:n]]
    except Exception as exc:
        log.warning("RSS fetch failed (%s): %s", query[:30], exc)
        return []


def generate_weekly_digest(week_start_str: str, week_end_str: str, api_key: str = ""):
    """Generate and upsert weekly digest for the given date range."""
    from datetime import date as _date
    week_start = _date.fromisoformat(week_start_str)
    week_end   = _date.fromisoformat(week_end_str)
    log.info("Generating weekly digest for %s ~ %s", week_start, week_end)

    # Collect price returns
    price_rows = neon_rows("""
        SELECT symbol, trade_date, close::float
        FROM stock_prices
        WHERE trade_date BETWEEN $1 AND $2
        ORDER BY symbol, trade_date
    """, [week_start_str, week_end_str])

    import pandas as pd
    price_df = pd.DataFrame(price_rows)

    returns = {}
    if not price_df.empty:
        for sym, grp in price_df.groupby("symbol"):
            grp = grp.sort_values("trade_date")
            if len(grp) >= 2:
                returns[sym] = (grp["close"].iloc[-1] / grp["close"].iloc[0] - 1) * 100

    # Collect news from Neon
    news_rows = neon_rows("""
        SELECT symbol, title, published
        FROM stock_news
        WHERE published >= $1 AND published < $2::date + INTERVAL '1 day'
        ORDER BY published DESC
        LIMIT 80
    """, [week_start_str, week_end_str])
    news_df = pd.DataFrame(news_rows)

    # RSS headlines
    rss_data = {}
    for topic, query in MARKET_FEEDS:
        headlines = _rss_headlines(query, n=7)
        if headlines:
            rss_data[topic] = headlines

    if api_key:
        try:
            import anthropic
            content = _ai_digest(api_key, week_start, week_end, returns, news_df, rss_data)
        except Exception as e:
            log.error("AI digest failed: %s — falling back to template", e)
            content = _template_digest(week_start, week_end, returns, news_df, rss_data)
    else:
        log.info("No ANTHROPIC_API_KEY — using template digest")
        content = _template_digest(week_start, week_end, returns, news_df, rss_data)

    headline = (
        f"{week_start.strftime('%Y년 %m월 %d일')} ~ "
        f"{week_end.strftime('%m월 %d일')} 주간 시장 이슈"
    )

    # Fix sequence before upsert
    result = neon_rows("SELECT MAX(id) AS mx FROM weekly_digest")
    max_id = result[0]["mx"] if result else None
    if max_id:
        _neon(f"SELECT setval('weekly_digest_id_seq', {max_id})")

    _neon("""
        INSERT INTO weekly_digest (week_start, week_end, headline, content)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (week_start) DO UPDATE SET
            headline = EXCLUDED.headline,
            content  = EXCLUDED.content,
            updated_at = NOW()
    """, [week_start_str, week_end_str, headline, content])

    log.info("Weekly digest saved: %s", headline)


def _ai_digest(api_key, week_start, week_end, returns, news_df, rss_data) -> str:
    import anthropic

    price_block = "데이터 없음"
    if returns:
        sorted_rets = sorted(returns.items(), key=lambda x: x[1], reverse=True)
        price_block = "\n".join(
            f"  {sym:12s} ({SYMBOL_NAMES.get(sym, sym):20s})  {ret:+.1f}%"
            for sym, ret in sorted_rets
        )

    news_block = "없음"
    if not news_df.empty:
        lines = []
        for _, row in news_df.head(40).iterrows():
            sym  = row["symbol"]
            name = SYMBOL_NAMES.get(sym, sym)
            lines.append(f"  [{sym}/{name}] {row['title']}")
        news_block = "\n".join(lines)

    rss_block = ""
    for topic, headlines in rss_data.items():
        rss_block += f"\n[{topic}]\n" + "\n".join(f"  - {h}" for h in headlines) + "\n"
    if not rss_block:
        rss_block = "RSS 수집 실패"

    prompt = f"""너는 미국·글로벌 주식시장과 경제를 전문적으로 분석하는 시니어 애널리스트야.
아래 데이터를 바탕으로 이번 주({week_start} ~ {week_end}) 주간 시장 이슈 모음을 한국어로 작성해줘.
투자자가 한눈에 파악할 수 있도록 핵심만 명확하게, 인사이트 있게 써줘.

──────────────────────────────────────────
▶ 추적 종목 주간 수익률
{price_block}

▶ 이번 주 수집된 뉴스 헤드라인 (DB)
{news_block}

▶ 시장 전반 뉴스 헤드라인 (Google News RSS, 영문)
{rss_block}
──────────────────────────────────────────

아래 마크다운 형식으로 작성해줘. 각 섹션은 충분히 구체적으로(3~6개 포인트):

## 📊 이번 주 시장 한눈에 보기
(전반적인 시장 분위기·주요 지수 흐름·투자 심리 요약 3~4줄)

## 🏦 거시경제 & 연준 동향
(금리·인플레이션·GDP·고용 관련 주요 이슈, 불릿 포인트)

## 📈 주요 실적 발표 & 기업 이슈
(이번 주 주요 실적·어닝 서프라이즈·기업별 이슈, 추적 종목 언급 포함)

## ⚡ 섹터별 핵심 이슈
(AI·반도체, 에너지·원자력·전력인프라, 산업재 등 섹터별 하이라이트)

## 🌏 글로벌 & 거시 리스크
(지정학, 무역, 달러·원화, 중국·유럽 등 글로벌 변수)

## 📅 다음 주 주목 이벤트
(예정된 경제지표 발표, 실적 발표, FOMC·중앙은행 일정, 주목할 이벤트 리스트)

## 💡 이번 주 핵심 한 줄 요약
(전체를 한 문장으로 압축)"""

    response = anthropic.Anthropic(api_key=api_key).messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=2500,
        messages=[{"role": "user", "content": prompt}],
    )
    log.info("AI digest generated via Claude Sonnet")
    return response.content[0].text.strip()


def _template_digest(week_start, week_end, returns, news_df, rss_data) -> str:
    lines = []
    lines.append("## 📊 이번 주 추적 종목 수익률\n")
    if returns:
        sorted_rets = sorted(returns.items(), key=lambda x: x[1], reverse=True)
        gainers = [(s, r) for s, r in sorted_rets if r > 0]
        losers  = [(s, r) for s, r in sorted_rets if r <= 0]
        lines.append("**상승 종목**")
        for sym, ret in gainers:
            lines.append(f"- {sym} ({SYMBOL_NAMES.get(sym, sym)}): **{ret:+.1f}%**")
        if not gainers:
            lines.append("- 없음")
        lines.append("\n**하락 종목**")
        for sym, ret in losers:
            lines.append(f"- {sym} ({SYMBOL_NAMES.get(sym, sym)}): **{ret:+.1f}%**")
        if not losers:
            lines.append("- 없음")
    else:
        lines.append("_이번 주 가격 데이터 없음_")
    lines.append("")

    SECTION_ICONS = {
        "미국 경제 & 연준":  "🏦 거시경제 & 연준 동향",
        "주식시장 동향":      "📈 주식시장 동향",
        "기업 실적 발표":     "📋 주요 실적 발표",
        "AI & 반도체":        "⚡ AI & 반도체",
        "에너지 & 원자력":    "⚡ 에너지 & 원자력",
        "글로벌 & 지정학":    "🌏 글로벌 & 지정학",
    }
    for topic, _ in MARKET_FEEDS:
        headlines = rss_data.get(topic, [])
        lines.append(f"## {SECTION_ICONS.get(topic, topic)}\n")
        if headlines:
            for h in headlines:
                lines.append(f"- {h}")
        else:
            lines.append("_헤드라인 수집 실패_")
        lines.append("")

    lines.append("## 📰 추적 종목 주간 뉴스\n")
    if not news_df.empty:
        for _, row in news_df.head(30).iterrows():
            sym = row["symbol"]
            lines.append(f"- **[{sym}]** {row['title']}")
    else:
        lines.append("_이번 주 수집된 뉴스 없음_")

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key:
        log.info("ANTHROPIC_API_KEY found — AI analysis enabled")
    else:
        log.info("ANTHROPIC_API_KEY not set — AI analysis disabled")

    # 1. Fix news sequence
    _fix_news_sequence()

    # 2. Fetch news via yfinance for all symbols
    log.info("=== Fetching yfinance news for %d symbols ===", len(ALL_SYMBOLS))
    for sym in ALL_SYMBOLS:
        fetch_yfinance_news_to_neon(sym)

    # 3. Fetch news via Google News RSS
    log.info("=== Fetching Google News for %d queries ===", len(GOOGLE_NEWS_QUERIES))
    for sym_key, query in GOOGLE_NEWS_QUERIES:
        fetch_google_news_to_neon(sym_key, query)

    # Count total news
    result = neon_rows("SELECT COUNT(*) AS cnt, MAX(published) AS latest FROM stock_news")
    log.info("stock_news total: %s rows, latest: %s", result[0]["cnt"], result[0]["latest"])

    # 4. AI analysis for unanalyzed articles (if API key available)
    if api_key:
        log.info("=== Running AI analysis on unanalyzed articles ===")
        analyze_news_with_ai(api_key, limit=100)
    else:
        unanalyzed = neon_rows("SELECT COUNT(*) AS cnt FROM stock_news WHERE ai_summary IS NULL")
        log.info("Skipping AI analysis (%s articles without ai_summary)", unanalyzed[0]["cnt"])

    # 5. Generate weekly digest for 2/12~2/18
    log.info("=== Generating weekly digest for 2026-02-12 ~ 2026-02-18 ===")
    generate_weekly_digest("2026-02-12", "2026-02-18", api_key)

    # Also generate current week (2/19~today) if today > 2/18
    today = datetime.utcnow().date()
    if today > datetime(2026, 2, 18).date():
        current_week_start = "2026-02-19"
        current_week_end   = str(today)
        log.info("=== Generating current week digest %s ~ %s ===", current_week_start, current_week_end)
        generate_weekly_digest(current_week_start, current_week_end, api_key)

    log.info("=== Done! ===")


if __name__ == "__main__":
    main()
