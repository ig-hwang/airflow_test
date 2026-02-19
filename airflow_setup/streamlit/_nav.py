"""
Shared sidebar styling for all pages.
Call `inject()` at the top of every page (before any st.sidebar code).
"""
import streamlit as st

_CSS = """
<style>

/* ── Hide Streamlit default chrome ─────────────────────────────── */
#MainMenu { visibility: hidden; }
footer    { visibility: hidden; }
header    { visibility: hidden; }

/* ── Sidebar shell ─────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: #0a0e1a !important;
    border-right: 1px solid #1a2035 !important;
    min-width: 248px !important;
    max-width: 248px !important;
}
[data-testid="stSidebar"] > div:first-child {
    padding-top: 0 !important;
}

/* ── Brand header injected via CSS before the nav list ─────────── */
[data-testid="stSidebarNav"] {
    padding-top: 0 !important;
}
[data-testid="stSidebarNav"]::before {
    content: "";
    display: block;
    height: 72px;
    background:
        url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='20' height='20' viewBox='0 0 24 24' fill='none' stroke='%231565c0' stroke-width='2.5' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='22 7 13.5 15.5 8.5 10.5 2 17'/%3E%3Cpolyline points='16 7 22 7 22 13'/%3E%3C/svg%3E")
        no-repeat 18px 18px,
        linear-gradient(135deg, #0d1321 0%, #0a0e1a 100%);
    border-bottom: 1px solid #1a2035;
    margin-bottom: 4px;
}

/* ── Nav list ───────────────────────────────────────────────────── */
[data-testid="stSidebarNav"] ul {
    padding: 6px 10px 10px !important;
    display: flex !important;
    flex-direction: column !important;
    gap: 1px !important;
    list-style: none !important;
    margin: 0 !important;
}
[data-testid="stSidebarNav"] li {
    margin: 0 !important;
    padding: 0 !important;
}

/* ── Nav links ──────────────────────────────────────────────────── */
[data-testid="stSidebarNav"] a {
    display: flex !important;
    align-items: center !important;
    gap: 9px !important;
    padding: 9px 11px !important;
    border-radius: 7px !important;
    color: #5a6478 !important;
    font-size: 0.84rem !important;
    font-weight: 500 !important;
    text-decoration: none !important;
    letter-spacing: 0.01em !important;
    border-left: 2px solid transparent !important;
    transition: background 0.12s, color 0.12s, border-color 0.12s !important;
    white-space: nowrap !important;
    overflow: hidden !important;
}
[data-testid="stSidebarNav"] a:hover {
    background: rgba(21, 101, 192, 0.08) !important;
    color: #a0b0cc !important;
    border-left-color: #1565c0 !important;
}
[data-testid="stSidebarNav"] a[aria-current="page"] {
    background: rgba(21, 101, 192, 0.14) !important;
    color: #5b9cf6 !important;
    border-left-color: #1976d2 !important;
    font-weight: 600 !important;
}

/* ── Sidebar divider ────────────────────────────────────────────── */
[data-testid="stSidebar"] hr {
    border: none !important;
    border-top: 1px solid #1a2035 !important;
    margin: 8px 0 !important;
}

/* ── Section label ──────────────────────────────────────────────── */
.sb-label {
    font-size: 0.62rem !important;
    font-weight: 700 !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
    color: #2d3a52 !important;
    padding: 14px 12px 5px !important;
    display: block !important;
}

/* ── Widget labels ──────────────────────────────────────────────── */
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stMultiSelect label,
[data-testid="stSidebar"] .stSlider label,
[data-testid="stSidebar"] .stCheckbox label,
[data-testid="stSidebar"] .stRadio label {
    color: #6b7a99 !important;
    font-size: 0.79rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.015em !important;
}

/* ── Selectbox / multiselect ────────────────────────────────────── */
[data-testid="stSidebar"] [data-baseweb="select"] > div,
[data-testid="stSidebar"] [data-baseweb="select"] > div > div {
    background: #0f1626 !important;
    border-color: #1e2d45 !important;
    color: #a0b0cc !important;
    border-radius: 6px !important;
    font-size: 0.84rem !important;
}
[data-testid="stSidebar"] [data-baseweb="select"] > div:hover {
    border-color: #1976d2 !important;
}

/* ── Tag chips in multiselect ───────────────────────────────────── */
[data-testid="stSidebar"] [data-baseweb="tag"] {
    background: #132040 !important;
    border: 1px solid #1e3560 !important;
    border-radius: 4px !important;
}
[data-testid="stSidebar"] [data-baseweb="tag"] span {
    color: #5b9cf6 !important;
    font-size: 0.75rem !important;
}

/* ── Slider ─────────────────────────────────────────────────────── */
[data-testid="stSidebar"] [data-testid="stSlider"] > div > div > div {
    background: #1976d2 !important;
}

/* ── Refresh button ─────────────────────────────────────────────── */
[data-testid="stSidebar"] .stButton > button {
    background: transparent !important;
    border: 1px solid #1e2d45 !important;
    color: #5a6478 !important;
    border-radius: 7px !important;
    font-size: 0.8rem !important;
    font-weight: 500 !important;
    padding: 7px 14px !important;
    width: 100% !important;
    transition: all 0.12s !important;
    letter-spacing: 0.02em !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(21, 101, 192, 0.08) !important;
    border-color: #1976d2 !important;
    color: #a0b0cc !important;
}

/* ── Sidebar scrollbar ──────────────────────────────────────────── */
[data-testid="stSidebar"]::-webkit-scrollbar { width: 4px; }
[data-testid="stSidebar"]::-webkit-scrollbar-track { background: transparent; }
[data-testid="stSidebar"]::-webkit-scrollbar-thumb {
    background: #1a2035;
    border-radius: 2px;
}

/* ── Sidebar status badge ───────────────────────────────────────── */
.sb-status {
    display: flex;
    align-items: center;
    gap: 7px;
    font-size: 0.72rem;
    color: #2d3a52;
    padding: 4px 12px;
    line-height: 1.5;
}
.sb-status .live-dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: #22c55e;
    box-shadow: 0 0 4px #22c55e88;
    flex-shrink: 0;
}

</style>
"""

_BRAND_HTML = """
<div style="
    padding: 18px 16px 14px;
    border-bottom: 1px solid #1a2035;
    margin-bottom: 4px;
">
  <div style="display:flex; align-items:center; gap:8px;">
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none"
         stroke="#1976d2" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
      <polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/>
      <polyline points="16 7 22 7 22 13"/>
    </svg>
    <span style="font-size:1.05rem;font-weight:700;color:#c9d8f0;letter-spacing:-0.01em;">
      AlphaBoard
    </span>
  </div>
  <div style="font-size:0.62rem;color:#2d3a52;margin-top:4px;
              letter-spacing:0.09em;text-transform:uppercase;font-weight:600;">
    Market Intelligence
  </div>
</div>
"""


def inject():
    """Inject global sidebar CSS + render brand header in sidebar."""
    st.markdown(_CSS, unsafe_allow_html=True)
    with st.sidebar:
        st.markdown(_BRAND_HTML, unsafe_allow_html=True)


def section(label: str):
    """Render a section label inside the sidebar."""
    st.markdown(f'<span class="sb-label">{label}</span>', unsafe_allow_html=True)


def status_bar(text: str, live: bool = True):
    """Render a small status/data-source line at the bottom of the sidebar."""
    dot = '<span class="live-dot"></span>' if live else ""
    st.markdown(
        f'<div class="sb-status">{dot}{text}</div>',
        unsafe_allow_html=True,
    )