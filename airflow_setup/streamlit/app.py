"""
Stock Dashboard — Market Overview (Home)
Streamlit multipage entry point.
"""

import pandas as pd
import streamlit as st

import _nav
from db import (
    SYMBOL_CATEGORY, SYMBOL_NAMES,
    compute_overall_signal, detect_signals, load_overview_data,
    signal_badge_html,
)

st.set_page_config(
    page_title="AlphaBoard — Market Overview",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

_nav.inject()

# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    _nav.section("컨트롤")
    if st.button("↺  새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    _nav.status_bar("Yahoo Finance · 실시간 수집")


# ── Main ─────────────────────────────────────────────────────────────────────
st.header("시장 개요", divider="blue")

df = load_overview_data()
if df.empty:
    st.warning("데이터 없음. Airflow의 `stock_price_collection` DAG를 먼저 실행하세요.")
    st.stop()

# ── Compute signals & build display DataFrame ─────────────────────────────────
records = []
for _, row in df.iterrows():
    sym = row["symbol"]
    signals = detect_signals(row)
    overall, score = compute_overall_signal(signals)

    rsi_val = row.get("rsi_14")
    records.append({
        "종목":       sym,
        "회사명":      SYMBOL_NAMES.get(sym, sym),
        "분류":       SYMBOL_CATEGORY.get(sym, "?"),
        "현재가":      row.get("price"),
        "1일(%)":     row.get("ret_1d"),
        "1주(%)":     row.get("ret_1w"),
        "1개월(%)":   row.get("ret_1m"),
        "1년(%)":     row.get("ret_1y"),
        "RSI":        round(rsi_val, 1) if pd.notna(rsi_val) else None,
        "신호":        overall,
        "_score":     score,
        "_category":  SYMBOL_CATEGORY.get(sym, "?"),
    })

display_df = pd.DataFrame(records)

# ── Summary metrics ──────────────────────────────────────────────────────────
col_a, col_b, col_c, col_d = st.columns(4)

up_today   = display_df["1일(%)"].dropna().gt(0).sum()
down_today = display_df["1일(%)"].dropna().lt(0).sum()
buy_sigs   = display_df["신호"].isin(["매수", "강력매수"]).sum()
sell_sigs  = display_df["신호"].isin(["매도", "강력매도"]).sum()

col_a.metric("📈 상승 종목",  f"{up_today}개")
col_b.metric("📉 하락 종목",  f"{down_today}개")
col_c.metric("🟢 매수 신호",  f"{buy_sigs}개")
col_d.metric("🔴 매도 신호",  f"{sell_sigs}개")

st.divider()


# ── Helper: styled dataframe per category ─────────────────────────────────────
def _pct_color(val):
    if pd.isna(val):
        return ""
    return "color: #26a69a" if val > 0 else "color: #ef5350" if val < 0 else ""


def _signal_color(val):
    mapping = {
        "강력매수": "color: #a5d6a7; font-weight: bold",
        "매수":    "color: #c8e6c9",
        "중립":    "color: #b0bec5",
        "매도":    "color: #ffcdd2",
        "강력매도": "color: #ef9a9a; font-weight: bold",
    }
    return mapping.get(val, "")


def render_table(df_subset: pd.DataFrame):
    cols = ["종목", "회사명", "현재가", "1일(%)", "1주(%)", "1개월(%)", "1년(%)", "RSI", "신호"]
    sub = df_subset[cols].copy()

    def fmt_price(v):
        if pd.isna(v): return "—"
        return f"{v:,.2f}" if v < 10000 else f"{v:,.0f}"

    sub["현재가"] = sub["현재가"].apply(fmt_price)

    styled = (
        sub.style
        .map(_pct_color,    subset=["1일(%)", "1주(%)", "1개월(%)", "1년(%)"])
        .map(_signal_color, subset=["신호"])
        .format({
            "1일(%)":   lambda v: f"{v:+.2f}%" if pd.notna(v) else "—",
            "1주(%)":   lambda v: f"{v:+.2f}%" if pd.notna(v) else "—",
            "1개월(%)": lambda v: f"{v:+.2f}%" if pd.notna(v) else "—",
            "1년(%)":   lambda v: f"{v:+.2f}%" if pd.notna(v) else "—",
            "RSI":      lambda v: f"{v:.1f}"   if pd.notna(v) else "—",
        })
        .set_properties(**{"text-align": "right"})
        .set_properties(subset=["종목", "회사명", "신호"], **{"text-align": "left"})
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)


# ── Category tabs ─────────────────────────────────────────────────────────────
tab_all, tab_us, tab_kr, tab_adr = st.tabs(["전체", "🇺🇸 US", "🇰🇷 KR", "🌐 ADR"])

with tab_all:
    render_table(display_df)

with tab_us:
    render_table(display_df[display_df["_category"] == "US"])

with tab_kr:
    render_table(display_df[display_df["_category"] == "KR"])

with tab_adr:
    render_table(display_df[display_df["_category"] == "ADR"])

# ── Top movers ────────────────────────────────────────────────────────────────
st.divider()
st.subheader("오늘의 주요 움직임")
valid = display_df.dropna(subset=["1일(%)"])
if not valid.empty:
    top_gainers = valid.nlargest(3, "1일(%)")
    top_losers  = valid.nsmallest(3, "1일(%)")

    g_cols = st.columns(3)
    for i, (_, row) in enumerate(top_gainers.iterrows()):
        with g_cols[i]:
            pct = row["1일(%)"]
            st.metric(
                label=f"🟢 {row['종목']} — {row['회사명']}",
                value=f"{row['현재가']:,.2f}" if pd.notna(row['현재가']) else "—",
                delta=f"{pct:+.2f}%",
            )

    l_cols = st.columns(3)
    for i, (_, row) in enumerate(top_losers.iterrows()):
        with l_cols[i]:
            pct = row["1일(%)"]
            st.metric(
                label=f"🔴 {row['종목']} — {row['회사명']}",
                value=f"{row['현재가']:,.2f}" if pd.notna(row['현재가']) else "—",
                delta=f"{pct:+.2f}%",
            )

# ── Signal leaderboard ────────────────────────────────────────────────────────
st.divider()
st.subheader("신호 강도 순위")
ranked = display_df[["종목", "회사명", "현재가", "신호", "_score"]].copy()
ranked["점수"] = ranked["_score"].round(2)
ranked = ranked.sort_values("점수", ascending=False)
ranked = ranked.drop(columns=["_score"])

st.dataframe(
    ranked.style
    .map(_signal_color, subset=["신호"])
    .format({"현재가": lambda v: f"{v:,.2f}" if pd.notna(v) else "—"}),
    use_container_width=True,
    hide_index=True,
)
