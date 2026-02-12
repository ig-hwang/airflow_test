"""
Shared DB utilities, symbol metadata, signal detection, and chart builder.
Imported by app.py and all pages/*.py files.
"""

import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text

# ── Symbol metadata ─────────────────────────────────────────────────────────

SYMBOL_NAMES = {
    "AVGO": "Broadcom",
    "BE": "Bloom Energy",
    "VRT": "Vertiv Holdings",
    "SMR": "NuScale Power",
    "OKLO": "Oklo",
    "GEV": "GE Vernova",
    "MRVL": "Marvell Tech",
    "COHR": "Coherent",
    "LITE": "Lumentum",
    "VST": "Vistra Energy",
    "ETN": "Eaton",
    "267260.KS": "HD현대일렉트릭",
    "034020.KS": "두산에너빌리티",
    "028260.KS": "삼성물산",
    "267270.KS": "HD현대중공업",
    "010120.KS": "LS ELECTRIC",
    "SBGSY": "Schneider Electric",
    "HTHIY": "Hitachi",
}

SYMBOL_CATEGORY = {
    "AVGO": "US", "BE": "US", "VRT": "US", "SMR": "US", "OKLO": "US",
    "GEV": "US", "MRVL": "US", "COHR": "US", "LITE": "US", "VST": "US", "ETN": "US",
    "267260.KS": "KR", "034020.KS": "KR", "028260.KS": "KR",
    "267270.KS": "KR", "010120.KS": "KR",
    "SBGSY": "ADR", "HTHIY": "ADR",
}

US_SYMBOLS  = [s for s, c in SYMBOL_CATEGORY.items() if c == "US"]
KR_SYMBOLS  = [s for s, c in SYMBOL_CATEGORY.items() if c == "KR"]
ADR_SYMBOLS = [s for s, c in SYMBOL_CATEGORY.items() if c == "ADR"]
ALL_SYMBOLS = US_SYMBOLS + KR_SYMBOLS + ADR_SYMBOLS

TIMEFRAME_DAYS = {
    "1W": 7, "1M": 30, "3M": 90, "6M": 180,
    "1Y": 365, "2Y": 730, "3Y": 1095,
}

# ── DB connection ────────────────────────────────────────────────────────────

@st.cache_resource
def get_engine():
    return create_engine(
        os.environ["DATA_DB_CONN"],
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
    )


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
                       cci_20, atr_14, mfi_14, obv
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


@st.cache_data(ttl=300)
def load_overview_data() -> pd.DataFrame:
    """Single query: latest price + 1D/1W/1M/1Y returns + key indicators for all symbols."""
    with get_engine().connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT
                    l.symbol,
                    l.close                                                                 AS price,
                    l.rsi_14, l.macd, l.macd_signal,
                    l.sma_50, l.sma_200,
                    l.bb_upper, l.bb_lower, l.bb_middle,
                    l.mfi_14,
                    ROUND((l.close - d1.close)   / NULLIF(d1.close,   0) * 100, 2)        AS ret_1d,
                    ROUND((l.close - d7.close)   / NULLIF(d7.close,   0) * 100, 2)        AS ret_1w,
                    ROUND((l.close - d30.close)  / NULLIF(d30.close,  0) * 100, 2)        AS ret_1m,
                    ROUND((l.close - d365.close) / NULLIF(d365.close, 0) * 100, 2)        AS ret_1y
                FROM (
                    SELECT DISTINCT ON (symbol)
                        symbol, close, rsi_14, macd, macd_signal,
                        sma_50, sma_200, bb_upper, bb_lower, bb_middle, mfi_14
                    FROM stock_prices
                    ORDER BY symbol, trade_date DESC
                ) l
                LEFT JOIN LATERAL (
                    SELECT close FROM stock_prices
                    WHERE symbol = l.symbol AND trade_date <= CURRENT_DATE - INTERVAL '1 day'
                    ORDER BY trade_date DESC LIMIT 1
                ) d1 ON true
                LEFT JOIN LATERAL (
                    SELECT close FROM stock_prices
                    WHERE symbol = l.symbol AND trade_date <= CURRENT_DATE - INTERVAL '7 days'
                    ORDER BY trade_date DESC LIMIT 1
                ) d7 ON true
                LEFT JOIN LATERAL (
                    SELECT close FROM stock_prices
                    WHERE symbol = l.symbol AND trade_date <= CURRENT_DATE - INTERVAL '30 days'
                    ORDER BY trade_date DESC LIMIT 1
                ) d30 ON true
                LEFT JOIN LATERAL (
                    SELECT close FROM stock_prices
                    WHERE symbol = l.symbol AND trade_date <= CURRENT_DATE - INTERVAL '365 days'
                    ORDER BY trade_date DESC LIMIT 1
                ) d365 ON true
                ORDER BY l.symbol
            """),
            conn,
        )
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
def load_news(symbol: str = None, limit: int = 60) -> pd.DataFrame:
    with get_engine().connect() as conn:
        if symbol:
            return pd.read_sql(
                text("""
                    SELECT title, source, published, url, summary, symbol,
                           ai_summary, sentiment
                    FROM stock_news
                    WHERE symbol = :symbol
                    ORDER BY published DESC NULLS LAST
                    LIMIT :limit
                """),
                conn,
                params={"symbol": symbol, "limit": limit},
            )
        return pd.read_sql(
            text("""
                SELECT title, source, published, url, summary, symbol,
                       ai_summary, sentiment
                FROM stock_news
                ORDER BY published DESC NULLS LAST
                LIMIT :limit
            """),
            conn,
            params={"limit": limit},
        )


@st.cache_data(ttl=300)
def load_multi_prices(symbols: tuple, days: int) -> pd.DataFrame:
    """Load prices for multiple symbols — used by Comparison page."""
    with get_engine().connect() as conn:
        return pd.read_sql(
            text("""
                SELECT symbol, trade_date, close
                FROM stock_prices
                WHERE symbol = ANY(:syms)
                  AND trade_date >= CURRENT_DATE - :days * INTERVAL '1 day'
                ORDER BY symbol, trade_date
            """),
            conn,
            params={"syms": list(symbols), "days": days},
        )


# ── Signal detection ─────────────────────────────────────────────────────────

_SIGNAL_WEIGHTS = {
    "rsi":    1.5,
    "macd":   1.0,
    "sma200": 1.5,
    "cross":  2.0,
    "bb":     0.5,
    "mfi":    1.0,
}

SIGNAL_COLORS = {
    "강력매수": ("#1b5e20", "#a5d6a7"),
    "매수":    ("#2e7d32", "#c8e6c9"),
    "중립":    ("#37474f", "#b0bec5"),
    "매도":    ("#b71c1c", "#ffcdd2"),
    "강력매도": ("#7f0000", "#ef9a9a"),
}


def detect_signals(row: pd.Series, prev_row: pd.Series | None = None) -> dict:
    """
    Compute individual technical signals from a latest-row of stock_prices.
    Returns dict of {key: {value, signal, strength, label}}.
    signal: 'BUY' | 'SELL' | 'NEUTRAL'
    """
    results = {}
    close = row.get("close")

    # ── RSI ──────────────────────────────────────────────────────────────────
    rsi = row.get("rsi_14")
    if pd.notna(rsi):
        if rsi < 30:
            results["rsi"] = {"value": rsi, "signal": "BUY",     "strength": 2, "label": f"과매도 ({rsi:.1f})"}
        elif rsi < 45:
            results["rsi"] = {"value": rsi, "signal": "BUY",     "strength": 1, "label": f"매수 우위 ({rsi:.1f})"}
        elif rsi > 70:
            results["rsi"] = {"value": rsi, "signal": "SELL",    "strength": 2, "label": f"과매수 ({rsi:.1f})"}
        elif rsi > 55:
            results["rsi"] = {"value": rsi, "signal": "SELL",    "strength": 1, "label": f"매도 우위 ({rsi:.1f})"}
        else:
            results["rsi"] = {"value": rsi, "signal": "NEUTRAL", "strength": 0, "label": f"중립 ({rsi:.1f})"}

    # ── MACD ─────────────────────────────────────────────────────────────────
    macd, macd_sig = row.get("macd"), row.get("macd_signal")
    if pd.notna(macd) and pd.notna(macd_sig):
        diff = macd - macd_sig
        if diff > 0:
            results["macd"] = {"value": diff, "signal": "BUY",     "strength": 1, "label": "MACD 강세"}
        elif diff < 0:
            results["macd"] = {"value": diff, "signal": "SELL",    "strength": 1, "label": "MACD 약세"}
        else:
            results["macd"] = {"value": diff, "signal": "NEUTRAL", "strength": 0, "label": "MACD 중립"}

    # ── SMA 200 ──────────────────────────────────────────────────────────────
    sma200 = row.get("sma_200")
    if pd.notna(close) and pd.notna(sma200) and sma200 != 0:
        pct = (close - sma200) / sma200
        if pct > 0.05:
            results["sma200"] = {"value": pct, "signal": "BUY",  "strength": 2, "label": f"SMA200 +{pct*100:.1f}%"}
        elif pct > 0:
            results["sma200"] = {"value": pct, "signal": "BUY",  "strength": 1, "label": f"SMA200 +{pct*100:.1f}%"}
        elif pct < -0.05:
            results["sma200"] = {"value": pct, "signal": "SELL", "strength": 2, "label": f"SMA200 {pct*100:.1f}%"}
        else:
            results["sma200"] = {"value": pct, "signal": "SELL", "strength": 1, "label": f"SMA200 {pct*100:.1f}%"}

    # ── Golden / Death Cross ──────────────────────────────────────────────────
    sma50 = row.get("sma_50")
    if pd.notna(sma50) and pd.notna(sma200):
        if prev_row is not None:
            p50, p200 = prev_row.get("sma_50"), prev_row.get("sma_200")
            if pd.notna(p50) and pd.notna(p200):
                if p50 <= p200 and sma50 > sma200:
                    results["cross"] = {"value": sma50, "signal": "BUY",  "strength": 2, "label": "황금십자 발생!"}
                elif p50 >= p200 and sma50 < sma200:
                    results["cross"] = {"value": sma50, "signal": "SELL", "strength": 2, "label": "죽음십자 발생!"}
        if "cross" not in results:
            if sma50 > sma200:
                results["cross"] = {"value": sma50, "signal": "BUY",  "strength": 1, "label": "황금십자 유지"}
            else:
                results["cross"] = {"value": sma50, "signal": "SELL", "strength": 1, "label": "죽음십자 유지"}

    # ── Bollinger Bands ───────────────────────────────────────────────────────
    bb_u, bb_l, bb_m = row.get("bb_upper"), row.get("bb_lower"), row.get("bb_middle")
    if pd.notna(close) and pd.notna(bb_u) and pd.notna(bb_l) and pd.notna(bb_m):
        upper_zone = bb_m + 0.7 * (bb_u - bb_m)
        lower_zone = bb_m - 0.7 * (bb_m - bb_l)
        if close >= upper_zone:
            results["bb"] = {"value": close, "signal": "SELL",    "strength": 1, "label": "BB 상단 근접"}
        elif close <= lower_zone:
            results["bb"] = {"value": close, "signal": "BUY",     "strength": 1, "label": "BB 하단 근접"}
        else:
            results["bb"] = {"value": close, "signal": "NEUTRAL", "strength": 0, "label": "BB 중간 구간"}

    # ── MFI ──────────────────────────────────────────────────────────────────
    mfi = row.get("mfi_14")
    if pd.notna(mfi):
        if mfi < 20:
            results["mfi"] = {"value": mfi, "signal": "BUY",     "strength": 2, "label": f"MFI 과매도 ({mfi:.1f})"}
        elif mfi > 80:
            results["mfi"] = {"value": mfi, "signal": "SELL",    "strength": 2, "label": f"MFI 과매수 ({mfi:.1f})"}
        else:
            results["mfi"] = {"value": mfi, "signal": "NEUTRAL", "strength": 0, "label": f"MFI 중립 ({mfi:.1f})"}

    return results


def compute_overall_signal(signals: dict) -> tuple[str, float]:
    score = 0.0
    for key, weight in _SIGNAL_WEIGHTS.items():
        s = signals.get(key)
        if s is None:
            continue
        if s["signal"] == "BUY":
            score += weight * s["strength"]
        elif s["signal"] == "SELL":
            score -= weight * s["strength"]

    if score >= 5.0:
        return "강력매수", score
    elif score >= 2.0:
        return "매수", score
    elif score <= -5.0:
        return "강력매도", score
    elif score <= -2.0:
        return "매도", score
    return "중립", score


def signal_badge_html(label: str) -> str:
    bg, fg = SIGNAL_COLORS.get(label, SIGNAL_COLORS["중립"])
    return (
        f'<span style="background:{bg};color:{fg};padding:3px 10px;'
        f'border-radius:5px;font-size:0.82em;font-weight:bold">{label}</span>'
    )


def signal_icon(signal: str) -> str:
    return {"BUY": "🟢", "SELL": "🔴", "NEUTRAL": "⚪"}.get(signal, "⚪")


# ── Chart builder ─────────────────────────────────────────────────────────────

def build_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    """4-panel chart: Candlestick+BB+MA / Volume / RSI / MACD."""
    fig = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.55, 0.15, 0.15, 0.15],
        subplot_titles=(f"{symbol}  {SYMBOL_NAMES.get(symbol, '')}", "Volume", "RSI (14)", "MACD"),
    )

    # ── Candlestick ───────────────────────────────────────────────────────────
    fig.add_trace(go.Candlestick(
        x=df["trade_date"],
        open=df["open"], high=df["high"],
        low=df["low"],   close=df["close"],
        name="Price",
        increasing_line_color="#26a69a",
        decreasing_line_color="#ef5350",
    ), row=1, col=1)

    # ── Moving averages ────────────────────────────────────────────────────────
    for col, color, name in [
        ("sma_20",  "#1976d2", "SMA 20"),
        ("sma_50",  "#f57c00", "SMA 50"),
        ("sma_200", "#c62828", "SMA 200"),
    ]:
        if col in df.columns and df[col].notna().sum() > 5:
            fig.add_trace(go.Scatter(
                x=df["trade_date"], y=df[col],
                name=name, line=dict(color=color, width=1.2), opacity=0.85,
            ), row=1, col=1)

    # ── Bollinger Bands ────────────────────────────────────────────────────────
    if "bb_upper" in df.columns and df["bb_upper"].notna().sum() > 5:
        fig.add_trace(go.Scatter(
            x=df["trade_date"], y=df["bb_upper"],
            name="BB Upper", line=dict(color="#9e9e9e", width=0.8, dash="dot"),
            showlegend=False,
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=df["trade_date"], y=df["bb_lower"],
            name="BB Bands", line=dict(color="#9e9e9e", width=0.8, dash="dot"),
            fill="tonexty", fillcolor="rgba(158,158,158,0.08)",
        ), row=1, col=1)

    # ── MACD crossover arrows ──────────────────────────────────────────────────
    if "macd" in df.columns and df["macd"].notna().sum() > 5:
        cross_up   = (df["macd"] > df["macd_signal"]) & (df["macd"].shift(1) <= df["macd_signal"].shift(1))
        cross_down = (df["macd"] < df["macd_signal"]) & (df["macd"].shift(1) >= df["macd_signal"].shift(1))
        buy_pts  = df[cross_up]
        sell_pts = df[cross_down]
        if not buy_pts.empty:
            fig.add_trace(go.Scatter(
                x=buy_pts["trade_date"], y=buy_pts["low"] * 0.98,
                mode="markers", marker=dict(symbol="triangle-up", size=10, color="#26a69a"),
                name="MACD 매수", showlegend=True,
            ), row=1, col=1)
        if not sell_pts.empty:
            fig.add_trace(go.Scatter(
                x=sell_pts["trade_date"], y=sell_pts["high"] * 1.02,
                mode="markers", marker=dict(symbol="triangle-down", size=10, color="#ef5350"),
                name="MACD 매도", showlegend=True,
            ), row=1, col=1)

    # ── Volume ────────────────────────────────────────────────────────────────
    vol_colors = [
        "#26a69a" if c >= o else "#ef5350"
        for c, o in zip(df["close"], df["open"])
    ]
    fig.add_trace(go.Bar(
        x=df["trade_date"], y=df["volume"],
        name="Volume", marker_color=vol_colors, showlegend=False,
    ), row=2, col=1)

    # ── RSI ───────────────────────────────────────────────────────────────────
    if "rsi_14" in df.columns and df["rsi_14"].notna().sum() > 5:
        fig.add_trace(go.Scatter(
            x=df["trade_date"], y=df["rsi_14"],
            name="RSI 14", line=dict(color="#ab47bc", width=1.5),
        ), row=3, col=1)
        fig.add_hrect(y0=70, y1=100, fillcolor="rgba(239,83,80,0.07)",   line_width=0, row=3, col=1)
        fig.add_hrect(y0=0,  y1=30,  fillcolor="rgba(38,166,154,0.07)",  line_width=0, row=3, col=1)
        fig.add_hline(y=70, line_dash="dash", line_color="rgba(239,83,80,0.5)",  line_width=1, row=3, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="rgba(38,166,154,0.5)", line_width=1, row=3, col=1)

    # ── MACD ──────────────────────────────────────────────────────────────────
    if "macd" in df.columns and df["macd"].notna().sum() > 5:
        hist_colors = ["#26a69a" if v >= 0 else "#ef5350" for v in df["macd_hist"].fillna(0)]
        fig.add_trace(go.Bar(
            x=df["trade_date"], y=df["macd_hist"],
            name="Histogram", marker_color=hist_colors, showlegend=False,
        ), row=4, col=1)
        fig.add_trace(go.Scatter(
            x=df["trade_date"], y=df["macd"],
            name="MACD", line=dict(color="#1565c0", width=1.5),
        ), row=4, col=1)
        fig.add_trace(go.Scatter(
            x=df["trade_date"], y=df["macd_signal"],
            name="Signal", line=dict(color="#f57c00", width=1.5),
        ), row=4, col=1)

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
    fig.update_xaxes(showgrid=False, rangebreaks=[dict(bounds=["sat", "mon"])])
    return fig
