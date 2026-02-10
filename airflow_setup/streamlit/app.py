"""
Stock Dashboard — Streamlit + Plotly
Reads directly from stock_data PostgreSQL DB.
"""

import os
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text

# ── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Stock Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── DB ─────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_engine():
    return create_engine(os.environ["DATA_DB_CONN"], pool_pre_ping=True)


@st.cache_data(ttl=300)
def load_symbols() -> list[str]:
    with get_engine().connect() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT symbol FROM stock_prices ORDER BY symbol")
        ).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300)
def load_prices(symbol: str, days: int) -> pd.DataFrame:
    with get_engine().connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT trade_date, open, high, low, close, volume,
                       sma_20, sma_50, sma_200,
                       bb_upper, bb_middle, bb_lower,
                       rsi_14, macd, macd_signal, macd_hist,
                       cci_20, atr_14, mfi_14
                FROM stock_prices
                WHERE symbol = :symbol
                  AND trade_date >= CURRENT_DATE - :days * INTERVAL '1 day'
                ORDER BY trade_date
            """),
            conn,
            params={"symbol": symbol, "days": days},
        )
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


@st.cache_data(ttl=3600)
def load_fundamentals(symbol: str) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(
            text("""
                SELECT * FROM stock_fundamentals
                WHERE symbol = :symbol
                ORDER BY fetch_date DESC
                LIMIT 1
            """),
            conn,
            params={"symbol": symbol},
        )


@st.cache_data(ttl=300)
def load_news(symbol: str, limit: int = 20) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(
            text("""
                SELECT title, source, published, url, summary
                FROM stock_news
                WHERE symbol = :symbol
                ORDER BY published DESC NULLS LAST
                LIMIT :limit
            """),
            conn,
            params={"symbol": symbol, "limit": limit},
        )


# ── Chart builder ──────────────────────────────────────────────────────────

def build_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    fig = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.55, 0.15, 0.15, 0.15],
        subplot_titles=(f"{symbol} Price", "Volume", "RSI (14)", "MACD"),
    )

    # ── Candlestick ───────────────────────────────────────────────────────
    fig.add_trace(
        go.Candlestick(
            x=df["trade_date"],
            open=df["open"], high=df["high"],
            low=df["low"],   close=df["close"],
            name="Price",
            increasing_line_color="#26a69a",
            decreasing_line_color="#ef5350",
        ),
        row=1, col=1,
    )

    # ── Moving averages ───────────────────────────────────────────────────
    for col, color, name in [
        ("sma_20",  "#1976d2", "SMA 20"),
        ("sma_50",  "#f57c00", "SMA 50"),
        ("sma_200", "#c62828", "SMA 200"),
    ]:
        if df[col].notna().sum() > 5:
            fig.add_trace(
                go.Scatter(
                    x=df["trade_date"], y=df[col],
                    name=name, line=dict(color=color, width=1.2), opacity=0.85,
                ),
                row=1, col=1,
            )

    # ── Bollinger Bands (fill between upper & lower) ───────────────────────
    if df["bb_upper"].notna().sum() > 5:
        fig.add_trace(
            go.Scatter(
                x=df["trade_date"], y=df["bb_upper"],
                name="BB Upper", line=dict(color="#9e9e9e", width=0.8, dash="dot"),
                showlegend=False,
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["trade_date"], y=df["bb_lower"],
                name="BB Bands", line=dict(color="#9e9e9e", width=0.8, dash="dot"),
                fill="tonexty", fillcolor="rgba(158,158,158,0.08)",
            ),
            row=1, col=1,
        )

    # ── Volume bars (red/green by candle direction) ────────────────────────
    vol_colors = [
        "#26a69a" if c >= o else "#ef5350"
        for c, o in zip(df["close"], df["open"])
    ]
    fig.add_trace(
        go.Bar(
            x=df["trade_date"], y=df["volume"],
            name="Volume", marker_color=vol_colors, showlegend=False,
        ),
        row=2, col=1,
    )

    # ── RSI ───────────────────────────────────────────────────────────────
    if df["rsi_14"].notna().sum() > 5:
        fig.add_trace(
            go.Scatter(
                x=df["trade_date"], y=df["rsi_14"],
                name="RSI 14", line=dict(color="#ab47bc", width=1.5),
            ),
            row=3, col=1,
        )
        fig.add_hrect(y0=70, y1=100, fillcolor="rgba(239,83,80,0.07)", line_width=0, row=3, col=1)
        fig.add_hrect(y0=0,  y1=30,  fillcolor="rgba(38,166,154,0.07)", line_width=0, row=3, col=1)
        fig.add_hline(y=70, line_dash="dash", line_color="rgba(239,83,80,0.5)",   line_width=1, row=3, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="rgba(38,166,154,0.5)",  line_width=1, row=3, col=1)

    # ── MACD ─────────────────────────────────────────────────────────────
    if df["macd"].notna().sum() > 5:
        hist_colors = [
            "#26a69a" if v >= 0 else "#ef5350"
            for v in df["macd_hist"].fillna(0)
        ]
        fig.add_trace(
            go.Bar(
                x=df["trade_date"], y=df["macd_hist"],
                name="Histogram", marker_color=hist_colors, showlegend=False,
            ),
            row=4, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["trade_date"], y=df["macd"],
                name="MACD", line=dict(color="#1565c0", width=1.5),
            ),
            row=4, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["trade_date"], y=df["macd_signal"],
                name="Signal", line=dict(color="#f57c00", width=1.5),
            ),
            row=4, col=1,
        )

    fig.update_layout(
        height=820,
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1),
        margin=dict(l=0, r=0, t=30, b=0),
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
    )
    fig.update_yaxes(showgrid=True, gridcolor="#1e2130", gridwidth=0.5)
    fig.update_xaxes(showgrid=False, rangebreaks=[
        dict(bounds=["sat", "mon"])  # hide weekends
    ])
    return fig


# ── Main app ───────────────────────────────────────────────────────────────

def main():
    st.title("📈 Stock Dashboard")

    # ── Sidebar ───────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("종목 설정")
        symbols = load_symbols()
        if not symbols:
            st.warning("데이터 없음. `stock_price_collection` DAG를 먼저 실행하세요.")
            return

        symbol = st.selectbox("종목", symbols)
        days   = st.select_slider("조회 기간", options=[30, 60, 90, 180, 365], value=90)

        if st.button("🔄 새로고침", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.caption("Airflow UI → http://localhost:8080")
        st.caption(f"Updated: {datetime.now().strftime('%H:%M:%S')}")

    # ── Load data ─────────────────────────────────────────────────────────
    df = load_prices(symbol, days)
    if df.empty:
        st.warning(f"**{symbol}** 데이터가 없습니다.")
        return

    # ── Summary metrics ───────────────────────────────────────────────────
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last
    chg     = last["close"] - prev["close"]
    chg_pct = chg / prev["close"] * 100 if prev["close"] else 0

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("종가",    f"{last['close']:,.2f}",
              f"{chg:+.2f} ({chg_pct:+.2f}%)")
    c2.metric("고가",    f"{last['high']:,.2f}")
    c3.metric("저가",    f"{last['low']:,.2f}")
    c4.metric("거래량",  f"{int(last['volume']):,}" if pd.notna(last["volume"]) else "—")
    c5.metric("RSI 14",  f"{last['rsi_14']:.1f}"   if pd.notna(last["rsi_14"]) else "—")
    c6.metric("MFI 14",  f"{last['mfi_14']:.1f}"   if pd.notna(last["mfi_14"]) else "—")

    # ── Chart ─────────────────────────────────────────────────────────────
    st.plotly_chart(build_chart(df, symbol), use_container_width=True)

    # ── Fundamentals / News tabs ───────────────────────────────────────────
    tab_fund, tab_ind, tab_news = st.tabs(["펀더멘털", "지표 테이블", "최근 뉴스"])

    with tab_fund:
        fund = load_fundamentals(symbol)
        if fund.empty:
            st.info("펀더멘털 데이터 없음. `stock_price_collection` DAG를 실행하세요.")
        else:
            r = fund.iloc[0]
            fc1, fc2, fc3, fc4 = st.columns(4)
            mc = r["market_cap"]
            fc1.metric("시가총액", f"${mc/1e9:.1f}B" if pd.notna(mc) else "—")
            fc2.metric("PER",      f"{r['pe_ratio']:.1f}"        if pd.notna(r["pe_ratio"])  else "—")
            fc3.metric("PBR",      f"{r['pb_ratio']:.2f}"        if pd.notna(r["pb_ratio"])  else "—")
            fc4.metric("ROE",      f"{r['roe']*100:.1f}%"        if pd.notna(r["roe"])       else "—")
            fc5, fc6 = st.columns(2)
            fc5.metric("EPS",      f"{r['eps']:.2f}"             if pd.notna(r["eps"])       else "—")
            fc6.metric("배당수익률", f"{r['dividend_yield']*100:.2f}%" if pd.notna(r["dividend_yield"]) else "—")
            if pd.notna(r.get("sector")):
                st.caption(f"섹터: {r['sector']} · 업종: {r.get('industry', '—')}")

    with tab_ind:
        show_cols = [
            "trade_date", "close",
            "sma_20", "sma_50", "sma_200",
            "rsi_14", "macd", "macd_signal",
            "cci_20", "atr_14", "mfi_14",
        ]
        st.dataframe(
            df[show_cols].tail(30).sort_values("trade_date", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

    with tab_news:
        news = load_news(symbol)
        if news.empty:
            st.info("뉴스 데이터 없음. `news_collection` DAG를 실행하세요.")
        else:
            for _, row in news.iterrows():
                pub = row["published"].strftime("%Y-%m-%d %H:%M") if pd.notna(row["published"]) else ""
                st.markdown(
                    f"**[{row['title']}]({row['url']})**  \n"
                    f"<span style='color:gray;font-size:0.82em'>"
                    f"{row.get('source', '')} · {pub}"
                    f"</span>",
                    unsafe_allow_html=True,
                )
                if pd.notna(row.get("summary")) and row["summary"]:
                    with st.expander("요약 보기"):
                        st.write(row["summary"])
                st.divider()


if __name__ == "__main__":
    main()
