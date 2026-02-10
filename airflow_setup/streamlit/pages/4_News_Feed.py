"""
News Feed — Aggregated news for all tracked symbols with filtering.
"""

from datetime import datetime

import pandas as pd
import streamlit as st

from db import ALL_SYMBOLS, SYMBOL_NAMES, load_news

with st.sidebar:
    st.header("뉴스 필터")
    sym_filter = st.multiselect(
        "종목 필터",
        ALL_SYMBOLS,
        format_func=lambda s: f"{s} — {SYMBOL_NAMES.get(s, s)}",
    )
    limit = st.slider("최대 기사 수", 20, 200, 60, step=20)

    if st.button("🔄 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.title("📰 뉴스 피드")

# Load news (single or all symbols)
if sym_filter:
    # Load each symbol and concat
    frames = [load_news(s, limit=limit) for s in sym_filter]
    news_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not news_df.empty:
        news_df = news_df.drop_duplicates(subset=["url"]).sort_values(
            "published", ascending=False, na_position="last"
        ).head(limit)
else:
    news_df = load_news(limit=limit)

if news_df.empty:
    st.info("뉴스 없음. `news_collection` DAG를 실행하세요.")
    st.stop()

# ── Summary ───────────────────────────────────────────────────────────────────
total = len(news_df)
unique_syms = news_df["symbol"].nunique()
c1, c2 = st.columns(2)
c1.metric("총 기사 수", f"{total}")
c2.metric("종목 수",   f"{unique_syms}")
st.divider()

# ── Group by date ─────────────────────────────────────────────────────────────
news_df["pub_date"] = pd.to_datetime(news_df["published"]).dt.date
dates = sorted(news_df["pub_date"].dropna().unique(), reverse=True)

# Articles with no date at bottom
undated = news_df[news_df["pub_date"].isna()]
dated   = news_df[news_df["pub_date"].notna()]

for date in dates:
    day_df = dated[dated["pub_date"] == date]
    if day_df.empty:
        continue

    st.markdown(
        f"<h4 style='color:#9e9e9e;margin:8px 0'>📅 {date.strftime('%Y년 %m월 %d일')}</h4>",
        unsafe_allow_html=True,
    )

    for _, row in day_df.iterrows():
        sym   = row.get("symbol", "")
        name  = SYMBOL_NAMES.get(sym, sym)
        pub   = row["published"].strftime("%H:%M") if pd.notna(row["published"]) else ""
        source = row.get("source") or ""

        # Symbol badge
        badge = f'<span style="background:#1e3a5f;color:#90caf9;padding:2px 7px;border-radius:4px;font-size:0.78em;margin-right:6px">{sym}</span>'

        st.markdown(
            f"{badge} **[{row['title']}]({row['url']})**  \n"
            f"<span style='color:gray;font-size:0.8em'>{source} &nbsp;·&nbsp; {pub}</span>",
            unsafe_allow_html=True,
        )
        if pd.notna(row.get("summary")) and row["summary"]:
            with st.expander("요약"):
                st.write(row["summary"])
        st.divider()

# Undated articles
if not undated.empty:
    st.markdown("<h4 style='color:#9e9e9e'>날짜 미상</h4>", unsafe_allow_html=True)
    for _, row in undated.iterrows():
        sym    = row.get("symbol", "")
        source = row.get("source") or ""
        badge  = f'<span style="background:#1e3a5f;color:#90caf9;padding:2px 7px;border-radius:4px;font-size:0.78em;margin-right:6px">{sym}</span>'
        st.markdown(
            f"{badge} **[{row['title']}]({row['url']})**  \n"
            f"<span style='color:gray;font-size:0.8em'>{source}</span>",
            unsafe_allow_html=True,
        )
        st.divider()
