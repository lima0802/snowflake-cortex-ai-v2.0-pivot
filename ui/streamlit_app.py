"""
DIA v2 - Streamlit UI
=======================
Direct Marketing Analytics Agent — Volvo Cars visual identity.

Deployment modes (set via env var):
  DIA_MODE=direct  → agent runs in-process (Streamlit Cloud, single-container)
  DIA_MODE=api     → calls FastAPI backend at DIA_API_URL (Docker VM, multi-service)
Default: direct (no separate server needed).
"""

import streamlit as st
import plotly.graph_objects as go
import os
import uuid
import asyncio
import sys

# ── Deployment mode ───────────────────────────────────────────────────────────
# direct = run agent in-process (Streamlit Cloud)
# api    = call FastAPI backend (Docker / VM)
_MODE    = os.getenv("DIA_MODE", "direct").lower()
_API_URL = os.getenv("DIA_API_URL", "http://localhost:8000")

# In direct mode, add project root to path so agent/* imports resolve
if _MODE == "direct":
    _ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)

# ── SVG avatars — line art, no fill ──────────────────────────────────────────

USER_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" width="26" height="26" viewBox="0 0 24 24" '
    'fill="none" stroke="#111111" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">'
    '<circle cx="12" cy="7" r="4"/>'
    '<path d="M4 21c0-4 3.6-7 8-7s8 3 8 7"/>'
    '</svg>'
)

BOT_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" width="26" height="26" viewBox="0 0 24 24" '
    'fill="none" stroke="#111111" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">'
    '<rect x="3" y="8" width="18" height="12" rx="2"/>'
    '<path d="M12 2v6"/>'
    '<circle cx="12" cy="2" r="1"/>'
    '<circle cx="8.5" cy="14" r="1.2"/>'
    '<circle cx="15.5" cy="14" r="1.2"/>'
    '<path d="M8 18h8"/>'
    '</svg>'
)

# Bot SVG for tab favicon (data URI)
BOT_FAVICON = (
    "data:image/svg+xml,"
    "%3Csvg xmlns='http://www.w3.org/2000/svg' width='32' height='32' viewBox='0 0 24 24' "
    "fill='none' stroke='%23111111' stroke-width='1.6' stroke-linecap='round' stroke-linejoin='round'%3E"
    "%3Crect x='3' y='8' width='18' height='12' rx='2'/%3E"
    "%3Cpath d='M12 2v6'/%3E"
    "%3Ccircle cx='12' cy='2' r='1'/%3E"
    "%3Ccircle cx='8.5' cy='14' r='1.2'/%3E"
    "%3Ccircle cx='15.5' cy='14' r='1.2'/%3E"
    "%3Cpath d='M8 18h8'/%3E"
    "%3C/svg%3E"
)

st.set_page_config(
    page_title="DIA — Direct Marketing Analytics",
    page_icon=BOT_FAVICON,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:ital,wght@0,300;0,400;0,500;0,600;0,700;1,400&display=swap');

/* ── Global ─────────────────────────────── */
html, body, .stApp {
    background-color: #F5F5F5;
    font-family: 'Inter', 'Helvetica Neue', Arial, sans-serif !important;
    color: #111111;
}
.block-container {
    padding-top: 1.5rem !important;
    padding-bottom: 2rem !important;
    max-width: 1080px;
}

/* ── Sidebar ─────────────────────────────── */
[data-testid="stSidebar"] {
    background: #1A1A2E !important;
}
[data-testid="stSidebar"],
[data-testid="stSidebar"] * {
    font-family: 'Inter', 'Helvetica Neue', Arial, sans-serif !important;
    color: #FFFFFF !important;
}
[data-testid="stSidebar"] .stMarkdown p {
    color: rgba(255,255,255,0.42) !important;
    font-size: 10px !important;
    line-height: 1.6;
}

/* Volvo Cars brand name — large, uppercase */
[data-testid="stSidebar"] h1 {
    color: #FFFFFF !important;
    font-size: 18px !important;
    font-weight: 600 !important;
    letter-spacing: 0.12em !important;
    text-transform: uppercase !important;
    margin-bottom: 2px !important;
    line-height: 1.2 !important;
}
/* "Direct Marketing Analytics" subtitle */
[data-testid="stSidebar"] h3 {
    color: rgba(255,255,255,0.45) !important;
    font-size: 10px !important;
    font-weight: 400 !important;
    letter-spacing: 0.14em !important;
    text-transform: uppercase !important;
    margin-top: 0 !important;
    margin-bottom: 0 !important;
}
/* Section labels: New Session, Sample Queries */
[data-testid="stSidebar"] strong {
    font-family: 'Inter', 'Helvetica Neue', Arial, sans-serif !important;
    font-size: 10px !important;
    font-weight: 600 !important;
    letter-spacing: 0.12em !important;
    text-transform: uppercase !important;
    color: rgba(255,255,255,0.35) !important;
}
/* Sidebar buttons — all outlined boxes */
[data-testid="stSidebar"] .stButton button,
[data-testid="stSidebar"] .stButton > div > button {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.18) !important;
    box-shadow: none !important;
    outline: none !important;
    color: rgba(255,255,255,0.85) !important;
    font-family: 'Inter', 'Helvetica Neue', Arial, sans-serif !important;
    font-size: 13px !important;
    font-weight: 400 !important;
    border-radius: 3px !important;
    text-align: left !important;
    padding: 8px 12px !important;
    line-height: 1.5 !important;
    letter-spacing: 0 !important;
    width: 100% !important;
    transition: all 0.15s !important;
}
[data-testid="stSidebar"] .stButton button p,
[data-testid="stSidebar"] .stButton button span,
[data-testid="stSidebar"] .stButton > div > button p,
[data-testid="stSidebar"] .stButton > div > button span {
    font-family: 'Inter', 'Helvetica Neue', Arial, sans-serif !important;
    font-size: 13px !important;
    font-weight: 400 !important;
    color: rgba(255,255,255,0.85) !important;
    line-height: 1.5 !important;
}
[data-testid="stSidebar"] .stButton button:hover,
[data-testid="stSidebar"] .stButton > div > button:hover {
    background: rgba(255,255,255,0.09) !important;
    border-color: rgba(255,255,255,0.35) !important;
    color: #FFFFFF !important;
    box-shadow: none !important;
}
/* New Conversation — slightly more prominent */
[data-testid="stSidebar"] [data-testid="stButton-new_convo"] button,
[data-testid="stSidebar"] [data-testid="stButton-new_convo"] button p,
[data-testid="stSidebar"] [data-testid="stButton-new_convo"] button span {
    border-color: rgba(255,255,255,0.35) !important;
    color: #FFFFFF !important;
    font-weight: 500 !important;
}
[data-testid="stSidebar"] hr {
    border: none !important;
    border-top: 1px solid rgba(255,255,255,0.08) !important;
    margin: 10px 0 !important;
}
/* More questions toggle button */
[data-testid="stSidebar"] [data-testid="stButton-toggle_more"] button,
[data-testid="stSidebar"] [data-testid="stButton-toggle_more"] button p,
[data-testid="stSidebar"] [data-testid="stButton-toggle_more"] button span {
    color: rgba(255,255,255,0.4) !important;
    font-size: 11px !important;
    font-weight: 500 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    border-color: transparent !important;
    background: transparent !important;
    padding: 4px 0 !important;
}
[data-testid="stSidebar"] [data-testid="stButton-toggle_more"] button:hover,
[data-testid="stSidebar"] [data-testid="stButton-toggle_more"] button:hover p {
    color: rgba(255,255,255,0.7) !important;
    background: transparent !important;
    border-color: transparent !important;
    text-decoration: none !important;
}

/* ── Chat input ──────────────────────────── */
[data-testid="stChatInput"] textarea {
    font-family: 'Inter', sans-serif !important;
    font-size: 14px !important;
    border-radius: 2px !important;
    border: 1px solid #CCCCCC !important;
    background: #FFFFFF !important;
    color: #111111 !important;
}
[data-testid="stChatInput"] textarea:focus {
    border-color: #111111 !important;
    box-shadow: 0 0 0 2px rgba(0,0,0,0.06) !important;
}

/* ── Expander (SQL dropdown) ─────────────── */
[data-testid="stExpander"] {
    border: 1px solid #E0E0E0 !important;
    border-radius: 2px !important;
    background: #FAFAFA !important;
    margin-top: 8px !important;
}
[data-testid="stExpander"] summary {
    font-family: 'Inter', sans-serif !important;
    font-size: 11px !important;
    color: #666666 !important;
    font-weight: 500 !important;
    padding: 6px 10px !important;
}
[data-testid="stExpander"] summary:hover { color: #111111 !important; }

/* ── Code blocks ─────────────────────────── */
code, pre {
    font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace !important;
    font-size: 12px !important;
    background: #F0F0F0 !important;
    border-radius: 2px !important;
}

/* ── Hide Streamlit chrome ───────────────── */
#MainMenu { visibility: hidden; }
footer    { visibility: hidden; }
header    { visibility: hidden; }

/* ── Hide all sidebar collapse/expand controls ── */
[data-testid="collapsedControl"],
[data-testid="stSidebarCollapseButton"],
button[aria-label="Close sidebar"],
button[aria-label="Open sidebar"],
button[kind="header"][data-testid*="sidebar"] {
    display: none !important;
    visibility: hidden !important;
    pointer-events: none !important;
}

/* ── Page header ─────────────────────────── */
.dia-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 0 14px;
    border-bottom: 2px solid #111111;
    margin-bottom: 20px;
}
.dia-title {
    font-family: 'Inter', sans-serif;
    font-size: 18px;
    font-weight: 600;
    color: #111111;
    letter-spacing: -0.01em;
}
.dia-live {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-family: 'Inter', sans-serif;
    font-size: 11px;
    color: #888888;
}
.dia-dot { width:6px; height:6px; border-radius:50%; background:#22C55E; display:inline-block; }

/* ── User bubble: white text on black ──── */
.msg-user-wrap {
    background: #FFFFFF;
    border: 1px solid #E0E0E0;
    border-radius: 4px;
    padding: 12px 14px;
    margin: 6px 0;
}
.msg-user-wrap p, .msg-user-wrap * {
    font-family: 'Inter', sans-serif !important;
    color: #111111 !important;
    font-weight: 500 !important;
    font-size: 14px !important;
    line-height: 1.6 !important;
    margin: 0 !important;
}

/* ── Bot bubble: navy block (matches sidebar) ─────────── */
.msg-bot-wrap {
    background: #1A1A2E;
    border-radius: 4px;
    padding: 14px 16px;
    margin: 6px 0;
}
.msg-bot-wrap,
.msg-bot-wrap p,
.msg-bot-wrap li,
.msg-bot-wrap span,
.msg-bot-wrap div,
.msg-bot-wrap * {
    font-family: 'Inter', sans-serif !important;
    color: #FFFFFF !important;
    font-size: 14px !important;
    line-height: 1.7 !important;
}
.msg-bot-wrap strong,
.msg-bot-wrap b,
.msg-bot-wrap em,
.msg-bot-wrap a {
    color: #FFFFFF !important;
    font-weight: 600 !important;
}
.msg-bot-wrap table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 8px;
}
.msg-bot-wrap th {
    background: #252540;
    color: #FFFFFF !important;
    padding: 6px 10px;
    font-size: 12px;
    text-align: left;
    border-bottom: 1px solid #2E2E50;
}
.msg-bot-wrap td {
    color: #DDDDDD !important;
    padding: 5px 10px;
    font-size: 12px;
    border-bottom: 1px solid #252540;
}

/* Avatar column — no padding */
[data-testid="column"]:first-child .msg-avatar-col {
    padding-top: 4px;
}
.msg-avatar-col svg {
    display: block;
}

/* ── Footer ──────────────────────────────── */
.dia-footer {
    margin-top: 32px;
    padding-top: 10px;
    border-top: 1px solid #DDDDDD;
    display: flex;
    justify-content: space-between;
    font-family: 'Inter', sans-serif;
    font-size: 10px;
    color: #BBBBBB;
}

/* ── Badges ──────────────────────────────── */
.badge-excellent { background:#111111; color:#fff; padding:2px 8px; border-radius:2px; font-size:11px; font-weight:600; font-family:'Inter',sans-serif; }
.badge-good      { background:#444444; color:#fff; padding:2px 8px; border-radius:2px; font-size:11px; font-weight:600; font-family:'Inter',sans-serif; }
.badge-average   { background:#D97706; color:#fff; padding:2px 8px; border-radius:2px; font-size:11px; font-weight:600; font-family:'Inter',sans-serif; }
.badge-poor      { background:#B91C1C; color:#fff; padding:2px 8px; border-radius:2px; font-size:11px; font-weight:600; font-family:'Inter',sans-serif; }
</style>
""", unsafe_allow_html=True)

VOLVO_COLORS = ["#111111", "#444444", "#777777", "#003057", "#1D6A9E", "#8EABBE"]


# ─── Sidebar ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("# VOLVO CARS")
    st.markdown("### Direct Marketing Analytics")
    st.markdown("---")

    st.markdown("**New Session**")
    if st.button("＋ New Conversation", key="new_convo", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

    st.markdown("---")
    st.markdown("**Sample Queries**")

    primary_queries = [
        "What was the open rate for last month's global eDM campaign?",
        "Show me the click-through rate trend for the past six months in Europe",
        "How does Germany's email performance compare to the European average?",
        "Which campaign achieved the highest engagement in Q3 2025?",
        "Compare open rates for France Spain and Italy for the most recent campaign",
        "What is Spain's opt-out rate compared to the EU average in Q3 2025?",
        "Compare open and click rates for EX30 campaigns in NL versus BE",
        "Summarize all markets where the opt-out rate exceeds 0.5%",
        "Show me Link Tracking Alias performance for Global eNewsletter in France",
    ]

    more_queries = [
        "Click rate for EX30 in Spain last month",
        "Compare open rates across Nordic markets",
        "Top 5 campaigns by click rate last month",
        "Send volume trend for Germany last 6 months",
        "Which markets have the lowest open rates?",
        "Unsubscribe rate for EX90 campaigns last month",
        "Performance breakdown by car model last month",
        "How many emails were sent to Spain in Q1 2025?",
    ]

    for sq in primary_queries:
        if st.button(sq, key=f"sq_{sq}", use_container_width=True):
            st.session_state.pending_query = sq
            st.rerun()

    if "show_more_queries" not in st.session_state:
        st.session_state.show_more_queries = False

    if st.button("+ More questions" if not st.session_state.show_more_queries else "− More questions",
                 key="toggle_more", use_container_width=True):
        st.session_state.show_more_queries = not st.session_state.show_more_queries
        st.rerun()

    if st.session_state.show_more_queries:
        for sq in more_queries:
            if st.button(sq, key=f"sq_{sq}", use_container_width=True):
                st.session_state.pending_query = sq
                st.rerun()

    st.markdown("---")
    st.markdown(
        '<p style="font-size:10px;color:rgba(255,255,255,0.22);line-height:1.6;">'
        'VML MAP · Volvo Cars<br>Direct Marketing Analytics v2</p>',
        unsafe_allow_html=True,
    )



# ─── Header ──────────────────────────────────────────────────────────────────

st.markdown(
    '<div class="dia-header">'
    '  <span class="dia-title">Direct Marketing Analytics Agent</span>'
    '  <span class="dia-live"><span class="dia-dot"></span> Live · Snowflake</span>'
    '</div>',
    unsafe_allow_html=True,
)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _render_chart(msg: dict):
    if msg.get("chart_figure"):
        try:
            fig = go.Figure(msg["chart_figure"])
            fig.update_layout(
                paper_bgcolor="white", plot_bgcolor="white",
                font=dict(family="Inter, Helvetica Neue, Arial, sans-serif", size=12, color="#111111"),
                margin=dict(l=20, r=20, t=40, b=20),
            )
            st.plotly_chart(fig, use_container_width=True)
            return
        except Exception:
            pass
    if msg.get("chart_config") and msg.get("data"):
        try:
            from agent.charts import build_plotly_figure
            fd = build_plotly_figure(msg["data"], msg["chart_config"])
            if fd:
                fig = go.Figure(fd)
                fig.update_layout(
                    paper_bgcolor="white", plot_bgcolor="white",
                    font=dict(family="Inter, Helvetica Neue, Arial, sans-serif", size=12, color="#111111"),
                    margin=dict(l=20, r=20, t=40, b=20),
                )
                st.plotly_chart(fig, use_container_width=True)
        except Exception:
            pass


def _to_html(text: str) -> str:
    """Convert markdown text to HTML for embedding inside custom divs."""
    try:
        import markdown as md
        return md.markdown(text, extensions=["tables", "nl2br"])
    except ImportError:
        # Fallback: basic bold/newline conversion
        import re
        text = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', text)
        text = text.replace("\n\n", "<br><br>").replace("\n", "<br>")
        return text


def _send_feedback(rating: int, msg: dict, session_id: str, comment: str = ""):
    if _MODE == "direct":
        try:
            from agent.feedback import write_feedback
            write_feedback(
                rating=rating,
                query_text=msg.get("query", ""),
                answer_text=msg.get("content", ""),
                sql_generated=msg.get("sql"),
                intent=msg.get("intent"),
                feedback_text=comment or None,
                session_id=session_id,
            )
        except Exception:
            pass
    else:
        try:
            import requests
            requests.post(
                f"{_API_URL}/feedback",
                json={
                    "rating":        rating,
                    "query_text":    msg.get("query", ""),
                    "answer_text":   msg.get("content", ""),
                    "sql_generated": msg.get("sql"),
                    "intent":        msg.get("intent"),
                    "feedback_text": comment or None,
                    "session_id":    session_id,
                },
                timeout=10,
            )
        except Exception:
            pass


def _render_bot_message(msg: dict, idx: int):
    """Render bot message: avatar col + black content block side-by-side."""
    col_icon, col_body = st.columns([1, 19], gap="small")

    with col_icon:
        st.markdown(
            f'<div style="padding-top:6px;">{BOT_SVG}</div>',
            unsafe_allow_html=True,
        )

    with col_body:
        st.markdown(
            f'<div class="msg-bot-wrap">{_to_html(msg["content"])}</div>',
            unsafe_allow_html=True,
        )

        if msg.get("chart_figure") or (msg.get("chart_config") and msg.get("data")):
            _render_chart(msg)

        if msg.get("sql"):
            with st.expander("▸ View generated SQL", expanded=False):
                st.code(msg["sql"], language="sql")

        if msg.get("benchmark"):
            st.markdown(
                f'<span class="badge-{msg["benchmark"].lower()}">{msg["benchmark"]}</span>',
                unsafe_allow_html=True,
            )

        if idx > 0:
            already = st.session_state.feedback_given.get(idx)
            if already:
                st.caption("✓ Thanks for your feedback!")
            else:
                fc = st.columns([1, 1, 8])
                with fc[0]:
                    if st.button("👍", key=f"up_{idx}", help="Helpful"):
                        _send_feedback(1, msg, st.session_state.session_id)
                        st.session_state.feedback_given[idx] = "rated"
                        st.rerun()
                with fc[1]:
                    if st.button("👎", key=f"dn_{idx}", help="Not helpful"):
                        st.session_state.feedback_given[idx] = "commenting"
                        st.rerun()
            if st.session_state.feedback_given.get(idx) == "commenting":
                with st.form(key=f"comment_form_{idx}", clear_on_submit=True):
                    comment = st.text_area(
                        "What went wrong?",
                        placeholder="e.g. Wrong market, missing filter, incorrect metric...",
                        max_chars=500,
                        label_visibility="collapsed",
                    )
                    if st.form_submit_button("Submit feedback"):
                        _send_feedback(-1, msg, st.session_state.session_id, comment)
                        st.session_state.feedback_given[idx] = "rated"
                        st.rerun()


# ─── Session State ────────────────────────────────────────────────────────────

if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": (
                "Hello! I'm your **Direct Marketing Analytics Agent** for Volvo Cars email campaigns.\n\n"
                "Ask me about click rates, open rates, delivery metrics, market comparisons, "
                "send volumes, trends, or campaign performance.\n\n"
                "What would you like to know?"
            ),
        }
    ]

if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())

if "feedback_given" not in st.session_state:
    st.session_state.feedback_given = {}


# ─── Render Chat History ──────────────────────────────────────────────────────

for idx, msg in enumerate(st.session_state.messages):
    if msg["role"] == "user":
        col_icon, col_body = st.columns([1, 19], gap="small")
        with col_icon:
            st.markdown(
                f'<div style="padding-top:6px;">{USER_SVG}</div>',
                unsafe_allow_html=True,
            )
        with col_body:
            st.markdown(
                f'<div class="msg-user-wrap">{_to_html(msg["content"])}</div>',
                unsafe_allow_html=True,
            )
    else:
        _render_bot_message(msg, idx)


# ─── Handle Input ─────────────────────────────────────────────────────────────

prompt = st.chat_input("Ask about campaigns, markets, click rates, trends…")

if "pending_query" in st.session_state:
    prompt = st.session_state.pending_query
    del st.session_state.pending_query

if prompt:
    # Render user bubble immediately
    col_icon, col_body = st.columns([1, 19], gap="small")
    with col_icon:
        st.markdown(f'<div style="padding-top:6px;">{USER_SVG}</div>', unsafe_allow_html=True)
    with col_body:
        st.markdown(f'<div class="msg-user-wrap">{_to_html(prompt)}</div>', unsafe_allow_html=True)
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.spinner("Analysing…"):
        try:
            # Build conversation history (last 6 turns) for context
            history = [
                {"role": m["role"], "content": m["content"]}
                for m in st.session_state.messages[-6:]
                if m["role"] in ("user", "assistant")
            ]

            # Pass last assistant data for "plot that data" support
            last_data = None
            last_sql  = None
            for m in reversed(st.session_state.messages):
                if m["role"] == "assistant" and m.get("data"):
                    last_data = m["data"]
                    last_sql  = m.get("sql")
                    break
            context = {
                "history":       history,
                "previous_data": last_data,
                "previous_sql":  last_sql,
            }

            # ── Call agent: direct (in-process) or via API ────────────────
            if _MODE == "direct":
                from agent.graph import run_agent
                result = asyncio.run(run_agent(
                    query=prompt,
                    session_id=st.session_state.session_id,
                    context=context,
                ))
                ok = True
            else:
                import requests
                response = requests.post(
                    f"{_API_URL}/query",
                    json={"query": prompt, "session_id": st.session_state.session_id, "context": context},
                    timeout=90,
                )
                result = response.json()
                ok = response.status_code == 200

            # Only auto-show chart for trend/time-series or explicit plot requests
            import re as _re
            _PLOT_REQUEST = _re.compile(r"\b(plot|chart|graph|visuali[sz]e|show.{0,10}(chart|graph|plot))\b", _re.I)
            _TREND_INTENT = result.get("intent") in ("predictive",)
            _data = result.get("data") or []
            _col_keys = list(_data[0].keys()) if _data else []
            _has_date_col = any(
                _re.search(r"\b(month|send_date|send_month|week|period)\b", k, _re.I)
                for k in _col_keys
            )
            # Trend = date column present AND multiple rows (not a single-value lookup)
            _is_trend = _has_date_col and len(_data) > 1
            show_chart = (
                _PLOT_REQUEST.search(prompt)
                or _TREND_INTENT
                or _is_trend
            )

            if ok:
                new_msg = {
                    "role":         "assistant",
                    "content":      result["answer"],
                    "sql":          result.get("sql"),
                    "data":         result.get("data"),
                    "chart_config": result.get("chart_config") if show_chart else None,
                    "chart_figure": result.get("chart_figure") if show_chart else None,
                    "benchmark":    result.get("benchmark"),
                    "intent":       result.get("intent"),
                    "query":        prompt,
                }
                st.session_state.messages.append(new_msg)
                new_idx = len(st.session_state.messages) - 1
                _render_bot_message(new_msg, new_idx)
            else:
                st.error(f"Error: {result.get('detail', 'Unknown error')}")

        except Exception as e:
            st.error(f"Error: {str(e)}")


# ─── Footer ───────────────────────────────────────────────────────────────────

st.markdown(
    '<div class="dia-footer">'
    '  <span>VML MAP &nbsp;·&nbsp; Volvo Cars</span>'
    '  <span>Direct Marketing Analytics Agent &nbsp;·&nbsp; v2.0</span>'
    '</div>',
    unsafe_allow_html=True,
)
