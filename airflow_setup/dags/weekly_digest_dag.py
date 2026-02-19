"""
Weekly Market Digest DAG
Schedule: 0 8 * * 1 (every Monday 08:00 UTC — covers previous Mon~Sun)

Generates a comprehensive Korean weekly summary covering:
  - US macroeconomic conditions (Fed, rates, CPI, GDP, jobs)
  - Major stock index performance
  - Key earnings reports and corporate news
  - Sector highlights (energy/nuclear, AI/semis, infrastructure)
  - Global macro risks
  - Next-week calendar
"""

import logging
import os
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import feedparser
import pandas as pd
from sqlalchemy import text

from airflow import DAG
from airflow.operators.python import PythonOperator

from common import DEFAULT_ARGS, get_engine, make_neon_sync_task

log = logging.getLogger(__name__)

SYMBOL_NAMES = {
    "AVGO": "Broadcom", "BE": "Bloom Energy", "VRT": "Vertiv",
    "SMR": "NuScale Power", "OKLO": "Oklo", "GEV": "GE Vernova",
    "MRVL": "Marvell Technology", "COHR": "Coherent Corp", "LITE": "Lumentum",
    "VST": "Vistra Energy", "ETN": "Eaton Corporation",
    "267260.KS": "HD현대일렉트릭", "034020.KS": "두산에너빌리티",
    "028260.KS": "삼성물산", "267270.KS": "HD현대중공업", "010120.KS": "LS ELECTRIC",
    "SBGSY": "Schneider Electric", "HTHIY": "Hitachi",
}

# Google News RSS queries covering broader market context
MARKET_FEEDS = [
    ("미국 경제 & 연준",    "Federal Reserve interest rates CPI inflation GDP US economy"),
    ("주식시장 동향",        "S&P 500 NASDAQ Dow Jones stock market weekly"),
    ("기업 실적 발표",       "earnings report quarterly results EPS revenue beat miss"),
    ("AI & 반도체",          "AI semiconductor Broadcom Marvell Nvidia chip demand"),
    ("에너지 & 원자력",      "nuclear energy SMR power grid electricity utility"),
    ("글로벌 & 지정학",      "global economy trade tariff China geopolitics currency"),
]
GNEWS_URL = "https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"


# ── Helpers ─────────────────────────────────────────────────────────────────

def _week_range() -> tuple:
    """Return (week_start, week_end) as date objects for the previous 7 days."""
    today = datetime.utcnow().date()
    week_end   = today - timedelta(days=1)
    week_start = week_end - timedelta(days=6)
    return week_start, week_end


def _rss_headlines(query: str, n: int = 8) -> list[str]:
    try:
        feed = feedparser.parse(GNEWS_URL.format(q=quote_plus(query)))
        return [e.title for e in feed.entries[:n]]
    except Exception as exc:
        log.warning("RSS fetch failed (%s): %s", query[:30], exc)
        return []


# ── Task functions ───────────────────────────────────────────────────────────

def _collect_digest_data(week_start, week_end) -> dict:
    """Collect all data needed for digest (DB prices, DB news, RSS headlines)."""
    with get_engine().connect() as conn:
        price_df = pd.read_sql(
            text("""
                SELECT symbol, trade_date, close
                FROM stock_prices
                WHERE trade_date BETWEEN :s AND :e
                ORDER BY symbol, trade_date
            """),
            conn, params={"s": week_start, "e": week_end},
        )
        news_df = pd.read_sql(
            text("""
                SELECT symbol, title, published
                FROM stock_news
                WHERE published >= :s
                  AND published <  :e + INTERVAL '1 day'
                ORDER BY published DESC
                LIMIT 80
            """),
            conn, params={"s": week_start, "e": week_end},
        )

    # Weekly return per symbol
    returns = {}
    for sym, grp in price_df.groupby("symbol"):
        grp = grp.sort_values("trade_date")
        if len(grp) >= 2:
            returns[sym] = (grp["close"].iloc[-1] / grp["close"].iloc[0] - 1) * 100

    # RSS headlines per topic
    rss_data = {}
    for topic, query in MARKET_FEEDS:
        headlines = _rss_headlines(query, n=7)
        if headlines:
            rss_data[topic] = headlines

    return {"returns": returns, "news_df": news_df, "rss_data": rss_data}


def _build_basic_digest(week_start, week_end, data: dict) -> str:
    """Build a template-based digest (no AI) from collected data."""
    returns  = data["returns"]
    news_df  = data["news_df"]
    rss_data = data["rss_data"]

    lines = []

    # ── 섹션 1: 추적 종목 주간 수익률 ───────────────────────────────────────
    lines.append("## 📊 이번 주 추적 종목 수익률\n")
    if returns:
        sorted_rets = sorted(returns.items(), key=lambda x: x[1], reverse=True)
        gainers = [(s, r) for s, r in sorted_rets if r > 0]
        losers  = [(s, r) for s, r in sorted_rets if r <= 0]

        lines.append("**상승 종목**")
        if gainers:
            for sym, ret in gainers:
                name = SYMBOL_NAMES.get(sym, sym)
                lines.append(f"- {sym} ({name}): **{ret:+.1f}%**")
        else:
            lines.append("- 없음")

        lines.append("\n**하락 종목**")
        if losers:
            for sym, ret in losers:
                name = SYMBOL_NAMES.get(sym, sym)
                lines.append(f"- {sym} ({name}): **{ret:+.1f}%**")
        else:
            lines.append("- 없음")
    else:
        lines.append("_이번 주 가격 데이터 없음 (stock_price_collection DAG 실행 필요)_")

    lines.append("")

    # ── 섹션 2~6: RSS 뉴스 헤드라인 (topic별) ───────────────────────────────
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
        section_title = SECTION_ICONS.get(topic, topic)
        lines.append(f"## {section_title}\n")
        if headlines:
            for h in headlines:
                lines.append(f"- {h}")
        else:
            lines.append("_헤드라인 수집 실패_")
        lines.append("")

    # ── 섹션 7: 추적 종목 뉴스 헤드라인 (DB) ───────────────────────────────
    lines.append("## 📰 추적 종목 주간 뉴스\n")
    if not news_df.empty:
        for _, row in news_df.head(30).iterrows():
            sym  = row["symbol"]
            name = SYMBOL_NAMES.get(sym, sym)
            lines.append(f"- **[{sym}]** {row['title']}")
    else:
        lines.append("_이번 주 수집된 뉴스 없음_")
    lines.append("")

    # ── 안내 메시지 ──────────────────────────────────────────────────────────
    lines.append("---")
    lines.append(
        "> ℹ️ **기본 모드**: ANTHROPIC_API_KEY가 설정되지 않아 AI 분석 없이 "
        "원본 데이터를 그대로 표시합니다. API 키를 `.env`에 추가하면 "
        "Claude AI가 심층 분석·요약·인사이트를 포함한 전문 리포트를 생성합니다."
    )

    return "\n".join(lines)


def generate_weekly_digest():
    """
    1) Pull tracked-symbol weekly returns + news from DB
    2) Fetch general market headlines via Google News RSS
    3) Call Claude Sonnet to write a comprehensive Korean digest
       (fallback: template-based digest when API key not set)
    4) Upsert result into weekly_digest table
    """
    week_start, week_end = _week_range()
    log.info("Generating weekly digest for %s ~ %s", week_start, week_end)

    data = _collect_digest_data(week_start, week_end)

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    ai_available = bool(api_key)
    if ai_available:
        try:
            import anthropic
        except ImportError:
            log.warning("anthropic package not installed — using basic digest")
            ai_available = False

    if ai_available:
        # ── AI digest via Claude Sonnet ──────────────────────────────────────
        returns  = data["returns"]
        news_df  = data["news_df"]
        rss_data = data["rss_data"]

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
        content = response.content[0].text.strip()
        log.info("AI digest generated via Claude Sonnet")

    else:
        # ── Fallback: template-based digest ──────────────────────────────────
        log.info("ANTHROPIC_API_KEY not set — building basic template digest")
        content = _build_basic_digest(week_start, week_end, data)

    headline = (
        f"{week_start.strftime('%Y년 %m월 %d일')} ~ "
        f"{week_end.strftime('%m월 %d일')} 주간 시장 이슈"
    )

    # ── Upsert into DB ────────────────────────────────────────────────────
    with get_engine().begin() as conn:
        conn.execute(
            text("""
                INSERT INTO weekly_digest (week_start, week_end, headline, content)
                VALUES (:week_start, :week_end, :headline, :content)
                ON CONFLICT (week_start) DO UPDATE SET
                    headline   = EXCLUDED.headline,
                    content    = EXCLUDED.content,
                    updated_at = NOW()
            """),
            {
                "week_start": week_start,
                "week_end":   week_end,
                "headline":   headline,
                "content":    content,
            },
        )
    log.info("Weekly digest saved: %s", headline)


def _digest_complete():
    log.info("weekly_digest DAG finished successfully")


# ── DAG definition ───────────────────────────────────────────────────────────

_DIGEST_ARGS = {**DEFAULT_ARGS, "retries": 1, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id="weekly_digest",
    default_args=_DIGEST_ARGS,
    schedule_interval="0 8 * * 1",   # Every Monday 08:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "digest", "weekly"],
    doc_md=__doc__,
) as dag:

    generate = PythonOperator(
        task_id="generate_weekly_digest",
        python_callable=generate_weekly_digest,
    )

    complete = PythonOperator(
        task_id="digest_complete",
        python_callable=_digest_complete,
    )

    sync_neon = PythonOperator(
        task_id="sync_to_neon",
        python_callable=make_neon_sync_task(["weekly_digest"]),
    )

    generate >> complete >> sync_neon
