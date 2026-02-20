"""
📊 Financial Analysis - Institutional-Grade Fundamental Analysis

Analyzes financial health, growth trends, and valuation based on comprehensive
financial statement data. Uses frameworks from leading institutions:
- Piotroski F-Score (Stanford)
- Altman Z-Score (NYU Stern)
- DuPont Analysis (DuPont Corporation)
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from sqlalchemy import text
from datetime import datetime, timedelta

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from db import get_engine, ALL_SYMBOLS, SYMBOL_NAMES, SYMBOL_CATEGORIES

st.set_page_config(page_title="Financial Analysis", page_icon="📊", layout="wide")

# ── Data Loading Functions ────────────────────────────────────────────────────


@st.cache_data(ttl=3600)
def load_financial_data(symbol: str, period_type: str = 'quarterly') -> pd.DataFrame:
    """Load financial statement data."""
    with get_engine().connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT *
                FROM stock_financials
                WHERE symbol = :symbol AND period_type = :period_type
                ORDER BY report_date DESC
                LIMIT 20
            """),
            conn,
            params={"symbol": symbol, "period_type": period_type}
        )
    return df


@st.cache_data(ttl=3600)
def load_stock_prices(symbol: str, days: int = 730) -> pd.DataFrame:
    """Load stock price data."""
    with get_engine().connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT trade_date, close
                FROM stock_prices
                WHERE symbol = :symbol AND trade_date >= CURRENT_DATE - :days
                ORDER BY trade_date
            """),
            conn,
            params={"symbol": symbol, "days": days}
        )
    return df


# ── Analysis Functions ─────────────────────────────────────────────────────────


def calculate_piotroski_fscore(df: pd.DataFrame) -> dict:
    """
    Piotroski F-Score: 9-point financial strength score (Stanford, 2000)

    Profitability (4 points):
    1. Positive net income
    2. Positive operating cash flow
    3. Increasing ROA
    4. Quality of earnings (OCF > Net Income)

    Leverage/Liquidity (3 points):
    5. Decreasing debt-to-equity
    6. Increasing current ratio
    7. No new shares issued (not checked here)

    Operating Efficiency (2 points):
    8. Increasing gross margin
    9. Increasing asset turnover

    Score: 0-9 (8-9: Strong, 5-7: Moderate, 0-4: Weak)
    """
    if len(df) < 2:
        return {"score": None, "details": {}, "interpretation": "Insufficient data"}

    latest = df.iloc[0]
    previous = df.iloc[1]

    score = 0
    details = {}

    # 1. Positive net income
    if latest['net_income'] and latest['net_income'] > 0:
        score += 1
        details['net_income'] = {'value': True, 'point': 1}
    else:
        details['net_income'] = {'value': False, 'point': 0}

    # 2. Positive operating cash flow
    if latest['operating_cash_flow'] and latest['operating_cash_flow'] > 0:
        score += 1
        details['ocf_positive'] = {'value': True, 'point': 1}
    else:
        details['ocf_positive'] = {'value': False, 'point': 0}

    # 3. Increasing ROA
    if latest['roa'] and previous['roa'] and latest['roa'] > previous['roa']:
        score += 1
        details['roa_increasing'] = {'value': True, 'point': 1}
    else:
        details['roa_increasing'] = {'value': False, 'point': 0}

    # 4. Quality of earnings
    if (latest['operating_cash_flow'] and latest['net_income'] and
        latest['operating_cash_flow'] > latest['net_income']):
        score += 1
        details['earnings_quality'] = {'value': True, 'point': 1}
    else:
        details['earnings_quality'] = {'value': False, 'point': 0}

    # 5. Decreasing debt-to-equity
    if (latest['debt_to_equity'] and previous['debt_to_equity'] and
        latest['debt_to_equity'] < previous['debt_to_equity']):
        score += 1
        details['leverage_improving'] = {'value': True, 'point': 1}
    else:
        details['leverage_improving'] = {'value': False, 'point': 0}

    # 6. Increasing current ratio
    if (latest['current_ratio'] and previous['current_ratio'] and
        latest['current_ratio'] > previous['current_ratio']):
        score += 1
        details['liquidity_improving'] = {'value': True, 'point': 1}
    else:
        details['liquidity_improving'] = {'value': False, 'point': 0}

    # 8. Increasing gross margin
    if (latest['gross_margin'] and previous['gross_margin'] and
        latest['gross_margin'] > previous['gross_margin']):
        score += 1
        details['margin_improving'] = {'value': True, 'point': 1}
    else:
        details['margin_improving'] = {'value': False, 'point': 0}

    # 9. Asset turnover (Revenue / Total Assets)
    latest_turnover = latest['revenue'] / latest['total_assets'] if latest['revenue'] and latest['total_assets'] else 0
    prev_turnover = previous['revenue'] / previous['total_assets'] if previous['revenue'] and previous['total_assets'] else 0
    if latest_turnover > prev_turnover:
        score += 1
        details['efficiency_improving'] = {'value': True, 'point': 1}
    else:
        details['efficiency_improving'] = {'value': False, 'point': 0}

    # Interpretation
    if score >= 8:
        interpretation = "🟢 Strong (8-9점): 재무 건전성 우수"
    elif score >= 5:
        interpretation = "🟡 Moderate (5-7점): 재무 건전성 보통"
    else:
        interpretation = "🔴 Weak (0-4점): 재무 건전성 취약"

    return {
        "score": score,
        "details": details,
        "interpretation": interpretation
    }


def calculate_altman_zscore(latest: pd.Series) -> dict:
    """
    Altman Z-Score: Bankruptcy prediction model (NYU Stern, 1968)

    Z = 1.2*X1 + 1.4*X2 + 3.3*X3 + 0.6*X4 + 1.0*X5

    X1 = Working Capital / Total Assets
    X2 = Retained Earnings / Total Assets (approximated by Net Income)
    X3 = EBIT / Total Assets
    X4 = Market Cap / Total Liabilities
    X5 = Revenue / Total Assets

    Score > 2.99: Safe Zone
    1.81 < Score < 2.99: Grey Zone
    Score < 1.81: Distress Zone
    """
    try:
        working_capital = (latest['current_assets'] or 0) - (latest['current_liabilities'] or 0)
        total_assets = latest['total_assets'] or 1

        x1 = working_capital / total_assets
        x2 = (latest['net_income'] or 0) / total_assets
        x3 = (latest['operating_income'] or 0) / total_assets
        x4 = 0  # Market cap not in financials table
        x5 = (latest['revenue'] or 0) / total_assets

        z_score = 1.2*x1 + 1.4*x2 + 3.3*x3 + 0.6*x4 + 1.0*x5

        if z_score > 2.99:
            interpretation = "🟢 Safe Zone (>2.99): 파산 위험 낮음"
            risk = "Low"
        elif z_score > 1.81:
            interpretation = "🟡 Grey Zone (1.81-2.99): 주의 필요"
            risk = "Moderate"
        else:
            interpretation = "🔴 Distress Zone (<1.81): 파산 위험 높음"
            risk = "High"

        return {
            "score": round(z_score, 2),
            "components": {"X1": x1, "X2": x2, "X3": x3, "X4": x4, "X5": x5},
            "interpretation": interpretation,
            "risk": risk
        }
    except Exception as e:
        return {"score": None, "error": str(e)}


def analyze_growth_trends(df: pd.DataFrame) -> dict:
    """Analyze revenue and earnings growth trends."""
    if len(df) < 4:
        return {"error": "Insufficient data for growth analysis"}

    df = df.sort_values('report_date')

    # YoY growth (compare with same quarter last year)
    growth_metrics = {}

    # Revenue growth
    if df['revenue'].notna().sum() >= 4:
        latest_rev = df['revenue'].iloc[-1]
        yoy_rev = df['revenue'].iloc[-5] if len(df) >= 5 else df['revenue'].iloc[0]
        growth_metrics['revenue_yoy'] = ((latest_rev - yoy_rev) / yoy_rev * 100) if yoy_rev else None

    # Net income growth
    if df['net_income'].notna().sum() >= 4:
        latest_ni = df['net_income'].iloc[-1]
        yoy_ni = df['net_income'].iloc[-5] if len(df) >= 5 else df['net_income'].iloc[0]
        growth_metrics['netincome_yoy'] = ((latest_ni - yoy_ni) / yoy_ni * 100) if yoy_ni and yoy_ni != 0 else None

    # Operating cash flow growth
    if df['operating_cash_flow'].notna().sum() >= 4:
        latest_ocf = df['operating_cash_flow'].iloc[-1]
        yoy_ocf = df['operating_cash_flow'].iloc[-5] if len(df) >= 5 else df['operating_cash_flow'].iloc[0]
        growth_metrics['ocf_yoy'] = ((latest_ocf - yoy_ocf) / yoy_ocf * 100) if yoy_ocf else None

    # Trend (last 4 quarters)
    recent = df.tail(4)
    growth_metrics['revenue_trend'] = recent['revenue'].pct_change().mean() * 100 if len(recent) >= 2 else None
    growth_metrics['margin_trend'] = recent['net_margin'].diff().mean() if len(recent) >= 2 else None

    return growth_metrics


def analyze_cash_flow_quality(df: pd.DataFrame) -> dict:
    """Analyze cash flow quality metrics."""
    if df.empty:
        return {}

    latest = df.iloc[0]

    metrics = {}

    # Cash flow to net income ratio
    if latest['operating_cash_flow'] and latest['net_income']:
        metrics['cf_to_ni_ratio'] = latest['operating_cash_flow'] / latest['net_income']

    # Free cash flow margin
    if latest['free_cash_flow'] and latest['revenue']:
        metrics['fcf_margin'] = (latest['free_cash_flow'] / latest['revenue']) * 100

    # Cash conversion cycle (simplified)
    if latest['operating_cash_flow'] and latest['revenue']:
        metrics['cash_conversion'] = (latest['operating_cash_flow'] / latest['revenue']) * 100

    return metrics


# ── UI Components ──────────────────────────────────────────────────────────────


st.title("📊 Financial Analysis")
st.markdown("**Institutional-Grade Fundamental Analysis** • Financial Health • Growth • Valuation")

# Sidebar
symbol = st.sidebar.selectbox("Select Symbol", ALL_SYMBOLS, index=0)
period_type = st.sidebar.radio("Period", ["quarterly", "annual"], index=0)

# Load data
df_financials = load_financial_data(symbol, period_type)
df_prices = load_stock_prices(symbol)

if df_financials.empty:
    st.warning(f"⚠️ No financial data available for {symbol}. Please run `fetch_financials.py` first.")
    st.stop()

latest = df_financials.iloc[0]

# ── Overview Metrics ───────────────────────────────────────────────────────────

st.subheader(f"📈 {symbol} - Latest Financial Snapshot")
col1, col2, col3, col4 = st.columns(4)

with col1:
    revenue = latest['revenue'] / 1e9 if latest['revenue'] else 0
    st.metric("Revenue", f"${revenue:.2f}B",
              f"{analyze_growth_trends(df_financials).get('revenue_yoy', 0):.1f}% YoY" if analyze_growth_trends(df_financials).get('revenue_yoy') else None)

with col2:
    net_income = latest['net_income'] / 1e9 if latest['net_income'] else 0
    st.metric("Net Income", f"${net_income:.2f}B",
              latest['net_margin'] and f"{latest['net_margin']:.1f}% margin")

with col3:
    ocf = latest['operating_cash_flow'] / 1e9 if latest['operating_cash_flow'] else 0
    st.metric("Operating CF", f"${ocf:.2f}B")

with col4:
    fcf = latest['free_cash_flow'] / 1e9 if latest['free_cash_flow'] else 0
    st.metric("Free Cash Flow", f"${fcf:.2f}B")

st.markdown("---")

# ── Financial Health Scores ────────────────────────────────────────────────────

st.subheader("🏆 Financial Health Scores")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### Piotroski F-Score")
    st.caption("Stanford University - 9-Point Financial Strength Assessment")

    fscore_result = calculate_piotroski_fscore(df_financials)

    if fscore_result['score'] is not None:
        st.markdown(f"## **{fscore_result['score']}/9**")
        st.markdown(fscore_result['interpretation'])

        with st.expander("📋 Score Breakdown"):
            details = fscore_result['details']
            st.markdown("**Profitability (4 points)**")
            st.write(f"✓ Net Income > 0: {details.get('net_income', {}).get('point', 0)} pt")
            st.write(f"✓ OCF > 0: {details.get('ocf_positive', {}).get('point', 0)} pt")
            st.write(f"✓ ROA Increasing: {details.get('roa_increasing', {}).get('point', 0)} pt")
            st.write(f"✓ OCF > Net Income: {details.get('earnings_quality', {}).get('point', 0)} pt")

            st.markdown("**Leverage/Liquidity (2 points)**")
            st.write(f"✓ Debt/Equity Decreasing: {details.get('leverage_improving', {}).get('point', 0)} pt")
            st.write(f"✓ Current Ratio Increasing: {details.get('liquidity_improving', {}).get('point', 0)} pt")

            st.markdown("**Operating Efficiency (2 points)**")
            st.write(f"✓ Gross Margin Increasing: {details.get('margin_improving', {}).get('point', 0)} pt")
            st.write(f"✓ Asset Turnover Increasing: {details.get('efficiency_improving', {}).get('point', 0)} pt")
    else:
        st.info("Insufficient data for F-Score calculation")

with col2:
    st.markdown("### Altman Z-Score")
    st.caption("NYU Stern - Bankruptcy Prediction Model")

    zscore_result = calculate_altman_zscore(latest)

    if zscore_result.get('score'):
        st.markdown(f"## **{zscore_result['score']}**")
        st.markdown(zscore_result['interpretation'])

        with st.expander("📋 Formula Components"):
            comp = zscore_result['components']
            st.write(f"X1 (Working Capital / Assets): {comp['X1']:.3f}")
            st.write(f"X2 (Retained Earnings / Assets): {comp['X2']:.3f}")
            st.write(f"X3 (EBIT / Assets): {comp['X3']:.3f}")
            st.write(f"X5 (Revenue / Assets): {comp['X5']:.3f}")
            st.caption("Z = 1.2×X1 + 1.4×X2 + 3.3×X3 + 1.0×X5")
    else:
        st.info("Unable to calculate Z-Score")

st.markdown("---")

# ── Growth Analysis ────────────────────────────────────────────────────────────

st.subheader("📈 Growth & Profitability Trends")

growth = analyze_growth_trends(df_financials)

col1, col2, col3 = st.columns(3)
with col1:
    yoy_rev = growth.get('revenue_yoy')
    if yoy_rev:
        st.metric("Revenue Growth (YoY)", f"{yoy_rev:+.1f}%")
with col2:
    yoy_ni = growth.get('netincome_yoy')
    if yoy_ni:
        st.metric("Net Income Growth (YoY)", f"{yoy_ni:+.1f}%")
with col3:
    yoy_ocf = growth.get('ocf_yoy')
    if yoy_ocf:
        st.metric("OCF Growth (YoY)", f"{yoy_ocf:+.1f}%")

# Revenue & Margins Chart
df_chart = df_financials.sort_values('report_date').tail(8)

fig = make_subplots(
    rows=2, cols=1,
    subplot_titles=("Revenue Trend", "Profit Margins"),
    row_heights=[0.5, 0.5],
    vertical_spacing=0.15
)

# Revenue
fig.add_trace(
    go.Bar(x=df_chart['report_date'], y=df_chart['revenue']/1e9, name="Revenue",
           marker_color='lightblue'),
    row=1, col=1
)

# Margins
fig.add_trace(
    go.Scatter(x=df_chart['report_date'], y=df_chart['gross_margin'], name="Gross Margin",
               mode='lines+markers', line=dict(color='green')),
    row=2, col=1
)
fig.add_trace(
    go.Scatter(x=df_chart['report_date'], y=df_chart['operating_margin'], name="Operating Margin",
               mode='lines+markers', line=dict(color='orange')),
    row=2, col=1
)
fig.add_trace(
    go.Scatter(x=df_chart['report_date'], y=df_chart['net_margin'], name="Net Margin",
               mode='lines+markers', line=dict(color='red')),
    row=2, col=1
)

fig.update_yaxes(title_text="Revenue ($B)", row=1, col=1)
fig.update_yaxes(title_text="Margin (%)", row=2, col=1)
fig.update_layout(height=600, showlegend=True)

st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Cash Flow Analysis ─────────────────────────────────────────────────────────

st.subheader("💰 Cash Flow Quality")

cf_quality = analyze_cash_flow_quality(df_financials)

col1, col2, col3 = st.columns(3)
with col1:
    ratio = cf_quality.get('cf_to_ni_ratio')
    if ratio:
        st.metric("OCF / Net Income", f"{ratio:.2f}x")
        st.caption("Ideal: > 1.0 (high earnings quality)")
with col2:
    fcf_margin = cf_quality.get('fcf_margin')
    if fcf_margin:
        st.metric("FCF Margin", f"{fcf_margin:.1f}%")
        st.caption("% of revenue converted to free cash")
with col3:
    conversion = cf_quality.get('cash_conversion')
    if conversion:
        st.metric("Cash Conversion", f"{conversion:.1f}%")
        st.caption("Operating efficiency")

# Cash Flow Chart
df_cf = df_financials.sort_values('report_date').tail(8)

fig_cf = go.Figure()
fig_cf.add_trace(go.Bar(x=df_cf['report_date'], y=df_cf['operating_cash_flow']/1e9,
                         name="Operating CF", marker_color='green'))
fig_cf.add_trace(go.Bar(x=df_cf['report_date'], y=df_cf['investing_cash_flow']/1e9,
                         name="Investing CF", marker_color='orange'))
fig_cf.add_trace(go.Bar(x=df_cf['report_date'], y=df_cf['financing_cash_flow']/1e9,
                         name="Financing CF", marker_color='red'))
fig_cf.add_trace(go.Scatter(x=df_cf['report_date'], y=df_cf['free_cash_flow']/1e9,
                             name="Free Cash Flow", mode='lines+markers',
                             line=dict(color='blue', width=3)))

fig_cf.update_layout(
    title="Cash Flow Statement",
    yaxis_title="Cash Flow ($B)",
    barmode='group',
    height=400
)

st.plotly_chart(fig_cf, use_container_width=True)

st.markdown("---")

# ── Balance Sheet Strength ─────────────────────────────────────────────────────

st.subheader("🏦 Balance Sheet Strength")

col1, col2, col3, col4 = st.columns(4)
with col1:
    if latest['current_ratio']:
        st.metric("Current Ratio", f"{latest['current_ratio']:.2f}")
        st.caption("Liquidity (ideal: > 1.5)")
with col2:
    if latest['debt_to_equity']:
        st.metric("Debt/Equity", f"{latest['debt_to_equity']:.2f}")
        st.caption("Leverage (lower is better)")
with col3:
    if latest['roe']:
        st.metric("ROE", f"{latest['roe']:.1f}%")
        st.caption("Return on equity")
with col4:
    if latest['roa']:
        st.metric("ROA", f"{latest['roa']:.1f}%")
        st.caption("Return on assets")

# Assets vs Liabilities
if latest['total_assets'] and latest['total_liabilities']:
    fig_balance = go.Figure()

    fig_balance.add_trace(go.Bar(
        x=['Balance Sheet'],
        y=[latest['total_assets']/1e9],
        name='Total Assets',
        marker_color='lightblue'
    ))

    fig_balance.add_trace(go.Bar(
        x=['Balance Sheet'],
        y=[latest['total_liabilities']/1e9],
        name='Total Liabilities',
        marker_color='lightcoral'
    ))

    fig_balance.add_trace(go.Bar(
        x=['Balance Sheet'],
        y=[latest['total_equity']/1e9],
        name='Total Equity',
        marker_color='lightgreen'
    ))

    fig_balance.update_layout(
        title="Balance Sheet Composition",
        yaxis_title="Amount ($B)",
        barmode='group',
        height=400
    )

    st.plotly_chart(fig_balance, use_container_width=True)

st.markdown("---")

# ── Stock Price Correlation ────────────────────────────────────────────────────

st.subheader("📊 Financial Performance vs Stock Price")
st.caption("Analyze how fundamental improvements correlate with stock price movements")

if not df_prices.empty:
    # Merge financial metrics with stock prices
    df_merge = df_financials.sort_values('report_date').tail(8).copy()
    df_merge['report_date'] = pd.to_datetime(df_merge['report_date'])

    fig_corr = make_subplots(specs=[[{"secondary_y": True}]])

    # Stock price
    fig_corr.add_trace(
        go.Scatter(x=df_prices['trade_date'], y=df_prices['close'],
                   name="Stock Price", line=dict(color='black', width=2)),
        secondary_y=False
    )

    # ROE
    if df_merge['roe'].notna().any():
        fig_corr.add_trace(
            go.Scatter(x=df_merge['report_date'], y=df_merge['roe'],
                       name="ROE (%)", mode='lines+markers',
                       line=dict(color='green', dash='dot')),
            secondary_y=True
        )

    # Net Margin
    if df_merge['net_margin'].notna().any():
        fig_corr.add_trace(
            go.Scatter(x=df_merge['report_date'], y=df_merge['net_margin'],
                       name="Net Margin (%)", mode='lines+markers',
                       line=dict(color='blue', dash='dot')),
            secondary_y=True
        )

    fig_corr.update_xaxes(title_text="Date")
    fig_corr.update_yaxes(title_text="Stock Price ($)", secondary_y=False)
    fig_corr.update_yaxes(title_text="Financial Metrics (%)", secondary_y=True)
    fig_corr.update_layout(height=500, title="Stock Price vs Financial Metrics")

    st.plotly_chart(fig_corr, use_container_width=True)

# ── Insights ───────────────────────────────────────────────────────────────────

st.markdown("---")
st.subheader("💡 Key Insights")

# Generate insights based on scores
insights = []

fscore = fscore_result.get('score', 0)
if fscore >= 8:
    insights.append("✅ **Strong Financial Health**: Piotroski F-Score 8-9점으로 재무 건전성이 우수합니다.")
elif fscore <= 4:
    insights.append("⚠️ **Weak Financial Health**: Piotroski F-Score 4점 이하로 재무 개선이 필요합니다.")

zscore = zscore_result.get('score', 0)
if zscore and zscore < 1.81:
    insights.append("🚨 **High Bankruptcy Risk**: Altman Z-Score가 1.81 미만으로 재무적 스트레스가 높습니다.")
elif zscore and zscore > 2.99:
    insights.append("✅ **Low Bankruptcy Risk**: Altman Z-Score가 2.99 이상으로 안정적입니다.")

rev_growth = growth.get('revenue_yoy')
if rev_growth and rev_growth > 20:
    insights.append(f"📈 **Strong Revenue Growth**: YoY {rev_growth:.1f}% 성장으로 높은 성장세를 보이고 있습니다.")
elif rev_growth and rev_growth < 0:
    insights.append(f"📉 **Revenue Decline**: YoY {rev_growth:.1f}%로 매출이 감소하고 있습니다.")

cf_ratio = cf_quality.get('cf_to_ni_ratio')
if cf_ratio and cf_ratio > 1.2:
    insights.append("💰 **High Earnings Quality**: 영업현금흐름이 순이익을 크게 상회하여 이익의 질이 높습니다.")
elif cf_ratio and cf_ratio < 0.8:
    insights.append("⚠️ **Low Earnings Quality**: 영업현금흐름이 순이익에 미치지 못해 주의가 필요합니다.")

if insights:
    for insight in insights:
        st.markdown(insight)
else:
    st.info("데이터가 충분하지 않아 인사이트를 생성할 수 없습니다.")

# ── Data Table ─────────────────────────────────────────────────────────────────

with st.expander("📋 View Raw Financial Data"):
    display_cols = ['report_date', 'revenue', 'net_income', 'operating_cash_flow',
                    'free_cash_flow', 'gross_margin', 'net_margin', 'roe', 'current_ratio']
    st.dataframe(df_financials[display_cols].head(10), use_container_width=True)
