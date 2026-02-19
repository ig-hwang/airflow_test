"""
Comparison — Normalized price chart + side-by-side metrics for multiple symbols.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import _nav
from db import (
    SYMBOL_NAMES, TIMEFRAME_DAYS,
    compute_overall_signal, detect_signals,
    load_fundamentals, load_multi_prices, load_overview_data, load_symbols,
)

COLORS = [
    "#1976d2", "#f57c00", "#26a69a", "#ab47bc",
    "#ef5350", "#66bb6a", "#42a5f5", "#ffa726",
]

st.set_page_config(page_title="AlphaBoard — 종목 비교", page_icon="⚖️", layout="wide")
_nav.inject()

with st.sidebar:
    _nav.section("종목 선택")
    all_syms = load_symbols()
    if not all_syms:
        st.warning("데이터 없음.")
        st.stop()

    default_syms = all_syms[:3] if len(all_syms) >= 3 else all_syms
    selected = st.multiselect(
        "비교할 종목 선택 (2~5개)",
        all_syms,
        default=default_syms,
        format_func=lambda s: f"{s} — {SYMBOL_NAMES.get(s, s)}",
        max_selections=5,
        label_visibility="collapsed",
    )
    _nav.section("차트 설정")
    timeframe = st.select_slider(
        "기간", options=list(TIMEFRAME_DAYS.keys()), value="1Y",
        label_visibility="collapsed",
    )
    normalize = st.checkbox("수익률 정규화 (100 기준)", value=True)
    st.divider()
    if st.button("↺  새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    _nav.status_bar("상대 수익률 · 기술적 비교")

st.header("종목 비교 분석", divider="blue")

if len(selected) < 2:
    st.info("사이드바에서 2개 이상의 종목을 선택하세요.")
    st.stop()

days = TIMEFRAME_DAYS[timeframe]
raw_df = load_multi_prices(tuple(sorted(selected)), days)

if raw_df.empty:
    st.warning("선택된 종목의 데이터가 없습니다.")
    st.stop()

raw_df["trade_date"] = pd.to_datetime(raw_df["trade_date"])

# ── Normalized price chart ────────────────────────────────────────────────────
fig = go.Figure()

for i, sym in enumerate(selected):
    sym_df = raw_df[raw_df["symbol"] == sym].sort_values("trade_date")
    if sym_df.empty:
        continue
    color = COLORS[i % len(COLORS)]
    if normalize:
        base = sym_df["close"].dropna().iloc[0]
        y_values = sym_df["close"] / base * 100 if base else sym_df["close"]
        y_label  = "수익률 (기준=100)"
    else:
        y_values = sym_df["close"]
        y_label  = "가격"

    fig.add_trace(go.Scatter(
        x=sym_df["trade_date"],
        y=y_values,
        name=f"{sym} {SYMBOL_NAMES.get(sym, '')}",
        line=dict(color=color, width=2),
    ))

if normalize:
    fig.add_hline(y=100, line_dash="dash", line_color="rgba(255,255,255,0.3)", line_width=1)

fig.update_layout(
    height=500,
    template="plotly_dark",
    plot_bgcolor="#0e1117",
    paper_bgcolor="#0e1117",
    yaxis_title=y_label if normalize else "가격",
    xaxis_title="날짜",
    legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1),
    margin=dict(l=0, r=0, t=30, b=0),
)
fig.update_xaxes(showgrid=False, rangebreaks=[dict(bounds=["sat", "mon"])])
fig.update_yaxes(showgrid=True, gridcolor="#1e2130", gridwidth=0.5)
st.plotly_chart(fig, use_container_width=True)

# ── Period return summary ─────────────────────────────────────────────────────
st.divider()
st.subheader("기간별 수익률")

ov_df = load_overview_data()
ov_map = {r["symbol"]: r for _, r in ov_df.iterrows()} if not ov_df.empty else {}

ret_records = []
for sym in selected:
    ov = ov_map.get(sym, {})
    ret_records.append({
        "종목":      sym,
        "회사명":     SYMBOL_NAMES.get(sym, sym),
        "현재가":     ov.get("price"),
        "1일(%)":    ov.get("ret_1d"),
        "1주(%)":    ov.get("ret_1w"),
        "1개월(%)":  ov.get("ret_1m"),
        "1년(%)":    ov.get("ret_1y"),
    })
ret_df = pd.DataFrame(ret_records)

def _pct_color(v):
    if pd.isna(v): return ""
    return "color:#26a69a" if v > 0 else "color:#ef5350"

st.dataframe(
    ret_df.style
    .map(_pct_color, subset=["1일(%)", "1주(%)", "1개월(%)", "1년(%)"])
    .format({
        "현재가":    lambda v: f"{v:,.2f}" if pd.notna(v) else "—",
        "1일(%)":   lambda v: f"{v:+.2f}%" if pd.notna(v) else "—",
        "1주(%)":   lambda v: f"{v:+.2f}%" if pd.notna(v) else "—",
        "1개월(%)": lambda v: f"{v:+.2f}%" if pd.notna(v) else "—",
        "1년(%)":   lambda v: f"{v:+.2f}%" if pd.notna(v) else "—",
    }),
    use_container_width=True, hide_index=True,
)

# ── Key indicators comparison ─────────────────────────────────────────────────
st.divider()
st.subheader("주요 지표 비교")

ind_records = []
for sym in selected:
    ov = ov_map.get(sym, {})

    # ov 는 pd.Series 일 수 있으므로 `if ov` 대신 isinstance 로 확인
    # (pd.Series 에 대해 `if series:` 하면 ValueError: ambiguous truth value)
    if isinstance(ov, pd.Series):
        # detect_signals 는 "close" 컬럼을 기대하지만
        # load_overview_data() SQL 에서 `l.close AS price` 로 alias 됨 → 보정
        ov_sig = ov.copy()
        if "close" not in ov_sig.index and "price" in ov_sig.index:
            ov_sig["close"] = ov_sig["price"]
        sigs = detect_signals(ov_sig)
    else:
        sigs = {}

    overall, score = compute_overall_signal(sigs)

    rsi_v  = ov.get("rsi_14")
    sma200 = ov.get("sma_200")
    price  = ov.get("price")
    macd   = ov.get("macd")
    macd_s = ov.get("macd_signal")

    # 값을 미리 문자열로 포맷 — Styler .format() + NaN 조합의 ValueError 방지
    rsi_str = f"{float(rsi_v):.1f}" if pd.notna(rsi_v) else "—"
    sma_str = (
        f"{(float(price) / float(sma200) - 1) * 100:+.1f}%"
        if pd.notna(price) and pd.notna(sma200) and float(sma200) != 0
        else "—"
    )
    macd_str = (
        "강세"
        if pd.notna(macd) and pd.notna(macd_s) and float(macd) > float(macd_s)
        else "약세"
    )

    ind_records.append({
        "종목":      sym,
        "회사명":     SYMBOL_NAMES.get(sym, sym),
        "RSI":       rsi_str,
        "vs SMA200": sma_str,
        "MACD":      macd_str,
        "종합 신호":  overall,
        "신호 점수":  round(float(score), 2),
    })

ind_df = pd.DataFrame(ind_records)


def _signal_color(val):
    m = {
        "강력매수": "color:#a5d6a7;font-weight:bold",
        "매수":    "color:#c8e6c9",
        "중립":    "color:#b0bec5",
        "매도":    "color:#ffcdd2",
        "강력매도": "color:#ef9a9a;font-weight:bold",
    }
    return m.get(val, "")


st.dataframe(
    ind_df.style.map(_signal_color, subset=["종합 신호"]),
    use_container_width=True,
    hide_index=True,
)

# ── Fundamental comparison ────────────────────────────────────────────────────
st.divider()
st.subheader("펀더멘털 비교")

fund_records = []
for sym in selected:
    f = load_fundamentals(sym)
    if f.empty:
        fund_records.append({"종목": sym, "회사명": SYMBOL_NAMES.get(sym, sym),
                              "시가총액": "—", "PER": "—", "PBR": "—", "ROE": "—", "EPS": "—"})
    else:
        r = f.iloc[0]
        mc = r.get("market_cap")
        fund_records.append({
            "종목":    sym,
            "회사명":   SYMBOL_NAMES.get(sym, sym),
            "시가총액": f"${mc/1e9:.1f}B" if pd.notna(mc) else "—",
            "PER":     f"{r['pe_ratio']:.1f}"  if pd.notna(r.get("pe_ratio"))  else "—",
            "PBR":     f"{r['pb_ratio']:.2f}"  if pd.notna(r.get("pb_ratio"))  else "—",
            "ROE":     f"{r['roe']*100:.1f}%"  if pd.notna(r.get("roe"))       else "—",
            "EPS":     f"{r['eps']:.2f}"        if pd.notna(r.get("eps"))       else "—",
        })

st.dataframe(pd.DataFrame(fund_records), use_container_width=True, hide_index=True)

# ── Relative performance chart (bar) ──────────────────────────────────────────
st.divider()
st.subheader("수익률 비교 (기간별 막대)")

if not ret_df.empty:
    period_cols = ["1일(%)", "1주(%)", "1개월(%)", "1년(%)"]
    bar_fig = go.Figure()
    for i, sym in enumerate(selected):
        sym_row = ret_df[ret_df["종목"] == sym]
        if sym_row.empty:
            continue
        values = [sym_row[c].values[0] for c in period_cols]
        bar_fig.add_trace(go.Bar(
            name=sym,
            x=period_cols,
            y=values,
            marker_color=COLORS[i % len(COLORS)],
        ))

    bar_fig.add_hline(y=0, line_color="rgba(255,255,255,0.3)", line_width=1)
    bar_fig.update_layout(
        height=350,
        barmode="group",
        template="plotly_dark",
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
        margin=dict(l=0, r=0, t=10, b=0),
        yaxis_title="수익률 (%)",
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1),
    )
    st.plotly_chart(bar_fig, use_container_width=True)
