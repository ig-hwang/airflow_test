"""
News Feed — Aggregated news for all tracked symbols with filtering.
AI-powered expert analysis (summary + economic/stock/IT perspective + 호재/악재 judgment).
"""

from datetime import datetime

import pandas as pd
import streamlit as st

from db import ALL_SYMBOLS, SYMBOL_NAMES, load_news

# ── Sentiment badge helpers ────────────────────────────────────────────────────
_SENTIMENT_STYLE = {
    "호재": ("background:#1b5e20;color:#a5d6a7", "📈 호재"),
    "악재": ("background:#b71c1c;color:#ef9a9a", "📉 악재"),
    "중립": ("background:#37474f;color:#b0bec5", "➖ 중립"),
}

def _sentiment_badge(sentiment: str) -> str:
    style, label = _SENTIMENT_STYLE.get(sentiment, ("background:#37474f;color:#b0bec5", "➖ 중립"))
    return (
        f'<span style="{style};padding:2px 9px;border-radius:4px;'
        f'font-size:0.78em;font-weight:bold;margin-right:6px">{label}</span>'
    )

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("뉴스 필터")
    sym_filter = st.multiselect(
        "종목 필터",
        ALL_SYMBOLS,
        format_func=lambda s: f"{s} — {SYMBOL_NAMES.get(s, s)}",
    )
    sentiment_filter = st.multiselect(
        "호재/악재 필터",
        ["호재", "악재", "중립"],
        default=[],
    )
    limit = st.slider("최대 기사 수", 20, 200, 60, step=20)

    if st.button("🔄 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.title("📰 뉴스 피드")

# ── Load news ──────────────────────────────────────────────────────────────────
if sym_filter:
    frames = [load_news(s, limit=limit) for s in sym_filter]
    news_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not news_df.empty:
        news_df = (
            news_df.drop_duplicates(subset=["url"])
            .sort_values("published", ascending=False, na_position="last")
            .head(limit)
        )
else:
    news_df = load_news(limit=limit)

if news_df.empty:
    st.info("뉴스 없음. `news_collection` DAG를 실행하세요.")
    st.stop()

# Apply sentiment filter
if sentiment_filter:
    news_df = news_df[news_df["sentiment"].isin(sentiment_filter)]
    if news_df.empty:
        st.info(f"선택한 필터({', '.join(sentiment_filter)})에 해당하는 기사가 없습니다.")
        st.stop()

# ── Header metrics ─────────────────────────────────────────────────────────────
total      = len(news_df)
ai_count   = news_df["ai_summary"].notna().sum() if "ai_summary" in news_df.columns else 0
bullish    = (news_df["sentiment"] == "호재").sum() if "sentiment" in news_df.columns else 0
bearish    = (news_df["sentiment"] == "악재").sum() if "sentiment" in news_df.columns else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("총 기사 수", total)
c2.metric("AI 분석 완료", ai_count)
c3.metric("📈 호재", bullish)
c4.metric("📉 악재", bearish)
st.divider()

# ── Group by date ──────────────────────────────────────────────────────────────
news_df["pub_date"] = pd.to_datetime(news_df["published"]).dt.date
dates   = sorted(news_df["pub_date"].dropna().unique(), reverse=True)
undated = news_df[news_df["pub_date"].isna()]
dated   = news_df[news_df["pub_date"].notna()]


def _render_article(row):
    sym    = row.get("symbol", "")
    pub    = row["published"].strftime("%H:%M") if pd.notna(row["published"]) else ""
    source = row.get("source") or ""
    sentiment = row.get("sentiment") or ""
    ai_text   = row.get("ai_summary") or ""
    raw_sum   = row.get("summary") or ""

    sym_badge = (
        f'<span style="background:#1e3a5f;color:#90caf9;padding:2px 7px;'
        f'border-radius:4px;font-size:0.78em;margin-right:6px">{sym}</span>'
    )
    sent_badge = _sentiment_badge(sentiment) if sentiment else ""

    st.markdown(
        f"{sym_badge}{sent_badge} **[{row['title']}]({row['url']})**  \n"
        f"<span style='color:gray;font-size:0.8em'>{source} &nbsp;·&nbsp; {pub}</span>",
        unsafe_allow_html=True,
    )

    if ai_text:
        with st.expander("AI 분석 보기"):
            # Parse the three sections
            parts = ai_text.split("\n\n")
            for part in parts:
                if part.startswith("📌"):
                    header, _, body = part.partition("\n")
                    st.markdown(f"**{header}**")
                    st.write(body)
                elif part.startswith("💡"):
                    header, _, body = part.partition("\n")
                    st.markdown(f"**{header}**")
                    st.write(body)
                elif part.startswith("📊"):
                    header, _, body = part.partition("\n")
                    st.markdown(f"**{header}**")
                    st.info(body)
                else:
                    st.write(part)
    elif raw_sum:
        with st.expander("원문 요약"):
            st.write(raw_sum)

    st.divider()


for date in dates:
    day_df = dated[dated["pub_date"] == date]
    if day_df.empty:
        continue

    # Count sentiments for this day
    day_bullish = (day_df["sentiment"] == "호재").sum()
    day_bearish = (day_df["sentiment"] == "악재").sum()
    sent_summary = ""
    if day_bullish or day_bearish:
        sent_summary = f"&nbsp;&nbsp;<span style='color:#a5d6a7;font-size:0.8em'>호재 {day_bullish}</span>&nbsp;<span style='color:#ef9a9a;font-size:0.8em'>악재 {day_bearish}</span>"

    st.markdown(
        f"<h4 style='color:#9e9e9e;margin:8px 0'>📅 {date.strftime('%Y년 %m월 %d일')}{sent_summary}</h4>",
        unsafe_allow_html=True,
    )

    for _, row in day_df.iterrows():
        _render_article(row)

# ── Undated articles ───────────────────────────────────────────────────────────
if not undated.empty:
    st.markdown("<h4 style='color:#9e9e9e'>날짜 미상</h4>", unsafe_allow_html=True)
    for _, row in undated.iterrows():
        _render_article(row)
