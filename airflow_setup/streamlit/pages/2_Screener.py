"""
Screener — Filter all tracked symbols by technical conditions.
"""

import pandas as pd
import streamlit as st

from db import (
    SYMBOL_NAMES,
    compute_overall_signal, detect_signals, load_overview_data,
    signal_badge_html,
)

with st.sidebar:
    st.header("스크리너 필터")

    rsi_range = st.slider("RSI 범위", 0, 100, (0, 100))
    ma200_pos = st.multiselect("SMA200 기준", ["상방", "하방"], default=["상방", "하방"])
    macd_dir  = st.multiselect("MACD 방향", ["강세", "약세"], default=["강세", "약세"])
    bb_pos    = st.multiselect("BB 위치", ["상단 근접", "중간 구간", "하단 근접"], default=["상단 근접", "중간 구간", "하단 근접"])
    sig_filter = st.multiselect("종합 신호", ["강력매수", "매수", "중립", "매도", "강력매도"],
                                default=["강력매수", "매수", "중립", "매도", "강력매도"])

    if st.button("🔄 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.title("🔍 기술적 스크리너")
st.caption("현재 기술적 지표 기반으로 모든 종목을 필터링합니다.")

df = load_overview_data()
if df.empty:
    st.warning("데이터 없음. DAG를 먼저 실행하세요.")
    st.stop()

# ── Build screener rows ───────────────────────────────────────────────────────
records = []
for _, row in df.iterrows():
    sym     = row["symbol"]
    signals = detect_signals(row)
    overall, score = compute_overall_signal(signals)

    # Derive human-readable conditions
    rsi_val = row.get("rsi_14")

    # MA200 position
    sma200 = row.get("sma_200")
    close  = row.get("price")
    ma200_str = "상방" if (pd.notna(close) and pd.notna(sma200) and close > sma200) else "하방"

    # MACD direction
    macd_str = "강세" if (pd.notna(row.get("macd")) and pd.notna(row.get("macd_signal")) and
                          row.get("macd", 0) > row.get("macd_signal", 0)) else "약세"

    # BB position
    bb_u, bb_l, bb_m = row.get("bb_upper"), row.get("bb_lower"), row.get("bb_middle")
    bb_str = "중간 구간"
    if pd.notna(close) and pd.notna(bb_u) and pd.notna(bb_l) and pd.notna(bb_m):
        upper_z = bb_m + 0.7 * (bb_u - bb_m)
        lower_z = bb_m - 0.7 * (bb_m - bb_l)
        if close >= upper_z:
            bb_str = "상단 근접"
        elif close <= lower_z:
            bb_str = "하단 근접"

    records.append({
        "symbol":    sym,
        "종목":       sym,
        "회사명":      SYMBOL_NAMES.get(sym, sym),
        "현재가":      close,
        "RSI":        round(rsi_val, 1) if pd.notna(rsi_val) else None,
        "SMA200":     ma200_str,
        "MACD":       macd_str,
        "BB 위치":    bb_str,
        "종합 신호":   overall,
        "신호 점수":   round(score, 2),
        "_rsi":       rsi_val,
    })

screen_df = pd.DataFrame(records)

# ── Apply filters ─────────────────────────────────────────────────────────────
mask = pd.Series([True] * len(screen_df))

# RSI range
if screen_df["_rsi"].notna().any():
    mask &= (
        screen_df["_rsi"].isna() |
        ((screen_df["_rsi"] >= rsi_range[0]) & (screen_df["_rsi"] <= rsi_range[1]))
    )

# MA200 position
if ma200_pos:
    mask &= screen_df["SMA200"].isin(ma200_pos)

# MACD direction
if macd_dir:
    mask &= screen_df["MACD"].isin(macd_dir)

# BB position
if bb_pos:
    mask &= screen_df["BB 위치"].isin(bb_pos)

# Signal filter
if sig_filter:
    mask &= screen_df["종합 신호"].isin(sig_filter)

result = screen_df[mask].drop(columns=["symbol", "_rsi"])
result = result.sort_values("신호 점수", ascending=False)

# ── Display ───────────────────────────────────────────────────────────────────
st.metric("조건 충족 종목", f"{len(result)} / {len(screen_df)}")
st.divider()

def _signal_color(val):
    m = {"강력매수": "color:#a5d6a7;font-weight:bold", "매수": "color:#c8e6c9",
         "중립": "color:#b0bec5", "매도": "color:#ffcdd2", "강력매도": "color:#ef9a9a;font-weight:bold"}
    return m.get(val, "")

def _ma_color(val):
    return "color:#26a69a" if val == "상방" else "color:#ef5350"

def _macd_color(val):
    return "color:#26a69a" if val == "강세" else "color:#ef5350"

def _bb_color(val):
    if val == "상단 근접": return "color:#ef5350"
    if val == "하단 근접": return "color:#26a69a"
    return ""

if result.empty:
    st.info("조건에 맞는 종목이 없습니다. 필터를 조정해보세요.")
else:
    styled = (
        result.style
        .map(_signal_color, subset=["종합 신호"])
        .map(_ma_color,     subset=["SMA200"])
        .map(_macd_color,   subset=["MACD"])
        .map(_bb_color,     subset=["BB 위치"])
        .format({
            "현재가": lambda v: f"{v:,.2f}" if pd.notna(v) else "—",
            "RSI":    lambda v: f"{v:.1f}"  if pd.notna(v) else "—",
        })
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)

# ── Opportunity highlights ────────────────────────────────────────────────────
st.divider()
st.subheader("🟢 잠재 매수 기회")
buy_opps = screen_df[
    (screen_df["종합 신호"].isin(["매수", "강력매수"])) &
    (screen_df["_rsi"].fillna(50) < 50)
].sort_values("신호 점수", ascending=False)

if buy_opps.empty:
    st.info("현재 매수 조건 충족 종목 없음.")
else:
    cols = st.columns(min(3, len(buy_opps)))
    for i, (_, r) in enumerate(buy_opps.head(3).iterrows()):
        with cols[i]:
            st.markdown(
                f"""
                <div style="border:1px solid #2e7d32;border-radius:8px;padding:12px">
                  <b>{r['종목']}</b> {r['회사명']}<br>
                  가격: {f"{r['현재가']:,.2f}" if pd.notna(r['현재가']) else '—'}<br>
                  RSI: {f"{r['RSI']:.1f}" if pd.notna(r['RSI']) else '—'} &nbsp;|&nbsp;
                  {r['MACD']} MACD<br>
                  <span style="color:#a5d6a7;font-weight:bold">{r['종합 신호']} ({r['신호 점수']:+.1f})</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

st.divider()
st.subheader("🔴 주의 필요 (과매수)")
sell_opps = screen_df[
    (screen_df["종합 신호"].isin(["매도", "강력매도"])) &
    (screen_df["_rsi"].fillna(50) > 50)
].sort_values("신호 점수")

if sell_opps.empty:
    st.info("현재 과매수 조건 충족 종목 없음.")
else:
    cols = st.columns(min(3, len(sell_opps)))
    for i, (_, r) in enumerate(sell_opps.head(3).iterrows()):
        with cols[i]:
            st.markdown(
                f"""
                <div style="border:1px solid #b71c1c;border-radius:8px;padding:12px">
                  <b>{r['종목']}</b> {r['회사명']}<br>
                  가격: {f"{r['현재가']:,.2f}" if pd.notna(r['현재가']) else '—'}<br>
                  RSI: {f"{r['RSI']:.1f}" if pd.notna(r['RSI']) else '—'} &nbsp;|&nbsp;
                  {r['MACD']} MACD<br>
                  <span style="color:#ef9a9a;font-weight:bold">{r['종합 신호']} ({r['신호 점수']:+.1f})</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
