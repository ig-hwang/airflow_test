"""
Weekly Digest — AI-generated weekly summary of US/global market news & issues.
Covers: macro economy, stock indices, earnings, sector highlights, global risks.
"""

import pandas as pd
import streamlit as st

import _nav
from db import load_weekly_digests

st.set_page_config(page_title="AlphaBoard — 주간 리포트", page_icon="📋", layout="wide")
_nav.inject()

st.header("주간 시장 리포트", divider="blue")
st.caption("매주 월요일 자동 생성 · Claude AI 기반 종합 분석")

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    _nav.section("표시 설정")
    show_weeks = st.slider("불러올 주 수", 1, 12, 4, label_visibility="collapsed")
    st.divider()
    if st.button("↺  새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.markdown("""
    <div style="font-size:0.72rem;color:#2d3a52;line-height:1.9;padding:2px 2px;">
      생성 시점 &nbsp;·&nbsp; 매주 월요일 08:00 UTC<br>
      커버리지 &nbsp;&nbsp;·&nbsp; 직전 7일 (월~일)<br>
      출처 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;·&nbsp; DB + RSS + Claude AI
    </div>
    """, unsafe_allow_html=True)
    _nav.status_bar("Claude Sonnet · AI 분석")

# ── Load data ──────────────────────────────────────────────────────────────────
digests = load_weekly_digests(limit=show_weeks)

if digests.empty:
    st.info(
        "아직 생성된 주간 이슈가 없습니다.\n\n"
        "**수동 실행 방법**: Airflow UI → `weekly_digest` DAG → ▶ Trigger DAG"
    )
    st.divider()
    st.subheader("⚙️ 설정 확인")
    st.code(
        "# .env 파일에 API 키 추가 후 컨테이너 재시작\n"
        "ANTHROPIC_API_KEY=sk-ant-your_key_here\n\n"
        "# 재시작\n"
        "docker compose up -d airflow-scheduler airflow-webserver",
        language="bash",
    )
    st.stop()

# ── Render each week ───────────────────────────────────────────────────────────
for i, (_, row) in enumerate(digests.iterrows()):
    week_start = row["week_start"]
    week_end   = row["week_end"]
    headline   = row.get("headline") or f"{week_start} ~ {week_end} 주간 이슈"
    content    = row.get("content") or ""
    created_at = row.get("created_at")

    # Week header
    if i == 0:
        label = "🔴 최신"
        header_color = "#1976d2"
    else:
        label = f"{i + 1}주 전"
        header_color = "#546e7a"

    col_badge, col_title = st.columns([1, 9])
    with col_badge:
        st.markdown(
            f"<div style='background:{header_color};color:white;padding:4px 10px;"
            f"border-radius:6px;text-align:center;font-size:0.8em;margin-top:4px'>"
            f"{label}</div>",
            unsafe_allow_html=True,
        )
    with col_title:
        # Date range
        if hasattr(week_start, "strftime"):
            date_str = (
                f"{week_start.strftime('%Y년 %m월 %d일')} ~ "
                f"{week_end.strftime('%m월 %d일')}"
            )
        else:
            date_str = f"{week_start} ~ {week_end}"
        st.markdown(f"### {date_str}")

    if created_at is not None:
        st.caption(f"생성: {pd.Timestamp(created_at).strftime('%Y-%m-%d %H:%M UTC')}")

    # Render digest content
    if i == 0:
        # Latest week: fully expanded
        st.markdown(content)
    else:
        # Older weeks: inside expander
        with st.expander(f"📄 {headline}", expanded=False):
            st.markdown(content)

    st.divider()

# ── Footer ─────────────────────────────────────────────────────────────────────
st.caption(
    "⚠️ 본 분석은 AI가 생성한 참고 자료입니다. "
    "투자 결정은 반드시 공식 자료와 전문가 조언을 기반으로 하시기 바랍니다."
)
