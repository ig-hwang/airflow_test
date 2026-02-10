"""
Stock Detail — Full chart + signals + fundamentals + news.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from db import (
    SYMBOL_NAMES, TIMEFRAME_DAYS,
    build_chart, compute_overall_signal, detect_signals,
    load_fundamentals, load_news, load_prices, load_symbols,
    signal_badge_html, signal_icon,
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("종목 설정")
    symbols = load_symbols()
    if not symbols:
        st.warning("데이터 없음. DAG를 먼저 실행하세요.")
        st.stop()

    symbol = st.selectbox(
        "종목",
        symbols,
        format_func=lambda s: f"{s}  {SYMBOL_NAMES.get(s, '')}",
    )
    timeframe = st.select_slider("기간", options=list(TIMEFRAME_DAYS.keys()), value="1Y")
    days = TIMEFRAME_DAYS[timeframe]

    st.divider()
    indicator_choice = st.multiselect(
        "추가 지표 패널",
        ["CCI", "ATR", "OBV", "MFI"],
        default=[],
    )

    if st.button("🔄 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ── Load data ─────────────────────────────────────────────────────────────────
df = load_prices(symbol, days)
if df.empty:
    st.warning(f"**{symbol}** 데이터 없음.")
    st.stop()

latest = df.iloc[-1]
prev   = df.iloc[-2] if len(df) > 1 else latest

chg     = latest["close"] - prev["close"]
chg_pct = chg / prev["close"] * 100 if prev["close"] else 0

# ── Header metrics ────────────────────────────────────────────────────────────
name = SYMBOL_NAMES.get(symbol, symbol)
st.title(f"📈 {symbol}  {name}")

m1, m2, m3, m4, m5, m6 = st.columns(6)
close_fmt = f"{latest['close']:,.2f}" if pd.notna(latest["close"]) else "—"
m1.metric("종가",   close_fmt,        f"{chg:+.2f} ({chg_pct:+.2f}%)")
m2.metric("고가",   f"{latest['high']:,.2f}" if pd.notna(latest["high"]) else "—")
m3.metric("저가",   f"{latest['low']:,.2f}"  if pd.notna(latest["low"])  else "—")
m4.metric("거래량", f"{int(latest['volume']):,}" if pd.notna(latest["volume"]) else "—")
m5.metric("RSI 14", f"{latest['rsi_14']:.1f}"  if pd.notna(latest["rsi_14"]) else "—")
m6.metric("MFI 14", f"{latest['mfi_14']:.1f}"  if pd.notna(latest["mfi_14"]) else "—")

# ── Main chart ────────────────────────────────────────────────────────────────
st.plotly_chart(build_chart(df, symbol), use_container_width=True)

# ── Additional indicator charts ───────────────────────────────────────────────
if indicator_choice:
    n = len(indicator_choice)
    extra_fig = make_subplots(
        rows=n, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        subplot_titles=indicator_choice,
    )
    colors = {"CCI": "#ff7043", "ATR": "#42a5f5", "OBV": "#66bb6a", "MFI": "#ab47bc"}
    hrefs = {"CCI": ("cci_20", -100, 100), "ATR": ("atr_14", None, None),
             "OBV": ("obv", None, None), "MFI": ("mfi_14", 20, 80)}

    for i, ind in enumerate(indicator_choice, 1):
        col, lo, hi = hrefs.get(ind, (ind.lower(), None, None))
        if col in df.columns and df[col].notna().sum() > 2:
            extra_fig.add_trace(go.Scatter(
                x=df["trade_date"], y=df[col],
                name=ind, line=dict(color=colors.get(ind, "#ffffff"), width=1.5),
            ), row=i, col=1)
            if lo is not None:
                extra_fig.add_hline(y=lo, line_dash="dash",
                                    line_color="rgba(38,166,154,0.5)", line_width=1, row=i, col=1)
            if hi is not None:
                extra_fig.add_hline(y=hi, line_dash="dash",
                                    line_color="rgba(239,83,80,0.5)", line_width=1, row=i, col=1)

    extra_fig.update_layout(
        height=200 * n,
        template="plotly_dark",
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
        margin=dict(l=0, r=0, t=30, b=0),
        showlegend=False,
    )
    extra_fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
    extra_fig.update_yaxes(showgrid=True, gridcolor="#1e2130", gridwidth=0.5)
    st.plotly_chart(extra_fig, use_container_width=True)

st.divider()

# ── Signal panel ──────────────────────────────────────────────────────────────
st.subheader("기술적 신호 분석")

signals = detect_signals(latest, prev_row=prev)
overall, score = compute_overall_signal(signals)

# Overall badge
ovr_col, score_col, _ = st.columns([2, 2, 6])
with ovr_col:
    st.markdown(f"**종합 신호:** {signal_badge_html(overall)}", unsafe_allow_html=True)
with score_col:
    st.metric("신호 점수", f"{score:+.1f}")

# Individual signal cards (2 per row)
signal_labels = {
    "rsi":    "RSI",
    "macd":   "MACD",
    "sma200": "SMA 200",
    "cross":  "MA 교차",
    "bb":     "볼린저밴드",
    "mfi":    "MFI",
}
items = [(k, v) for k, v in signal_labels.items() if k in signals]

for i in range(0, len(items), 3):
    cols = st.columns(3)
    for j, (key, label) in enumerate(items[i:i+3]):
        sig = signals[key]
        with cols[j]:
            icon = signal_icon(sig["signal"])
            st.markdown(
                f"""
                <div style="border:1px solid #2e2e3e;border-radius:8px;padding:12px;margin:4px 0">
                  <div style="font-size:0.8em;color:#9e9e9e">{label}</div>
                  <div style="font-size:1.1em;font-weight:bold">{icon} {sig['label']}</div>
                  <div style="font-size:0.75em;color:#616161;margin-top:4px">강도: {'●' * sig['strength']}{'○' * (2 - sig['strength'])}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

st.divider()

# ── Tabs: Fundamentals / Indicators / News ────────────────────────────────────
tab_fund, tab_ind, tab_news = st.tabs(["펀더멘털", "지표 테이블", "최근 뉴스"])

with tab_fund:
    fund = load_fundamentals(symbol)
    if fund.empty:
        st.info("펀더멘털 데이터 없음.")
    else:
        r = fund.iloc[0]
        fc1, fc2, fc3, fc4 = st.columns(4)
        mc = r.get("market_cap")
        fc1.metric("시가총액", f"${mc/1e9:.1f}B" if pd.notna(mc) else "—")
        fc2.metric("PER",     f"{r['pe_ratio']:.1f}"     if pd.notna(r.get("pe_ratio"))       else "—")
        fc3.metric("PBR",     f"{r['pb_ratio']:.2f}"     if pd.notna(r.get("pb_ratio"))       else "—")
        fc4.metric("ROE",     f"{r['roe']*100:.1f}%"     if pd.notna(r.get("roe"))            else "—")
        fc5, fc6 = st.columns(2)
        fc5.metric("EPS",     f"{r['eps']:.2f}"          if pd.notna(r.get("eps"))            else "—")
        fc6.metric("배당수익률", f"{r['dividend_yield']*100:.2f}%" if pd.notna(r.get("dividend_yield")) else "—")
        if pd.notna(r.get("sector")):
            st.caption(f"섹터: {r['sector']}  ·  업종: {r.get('industry', '—')}")

with tab_ind:
    show_cols = [
        "trade_date", "close",
        "sma_20", "sma_50", "sma_200",
        "rsi_14", "macd", "macd_signal",
        "cci_20", "atr_14", "mfi_14",
    ]
    st.dataframe(
        df[show_cols].tail(60).sort_values("trade_date", ascending=False),
        use_container_width=True, hide_index=True,
    )

with tab_news:
    news = load_news(symbol)
    if news.empty:
        st.info("뉴스 없음.")
    else:
        for _, row in news.iterrows():
            pub = row["published"].strftime("%Y-%m-%d %H:%M") if pd.notna(row["published"]) else ""
            st.markdown(
                f"**[{row['title']}]({row['url']})**  \n"
                f"<span style='color:gray;font-size:0.82em'>{row.get('source','')}&nbsp;·&nbsp;{pub}</span>",
                unsafe_allow_html=True,
            )
            if pd.notna(row.get("summary")) and row["summary"]:
                with st.expander("요약 보기"):
                    st.write(row["summary"])
            st.divider()
