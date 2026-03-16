"""
DIA v2 - Deterministic Chart Engine
=====================================
Auto-detects chart type from data shape + query keywords.
Zero LLM calls — instant, reliable, zero cost.

Usage:
    from agent.charts import recommend_chart
    config = recommend_chart(results, query)   # returns dict or None
"""

import re
import logging
from datetime import date, datetime

logger = logging.getLogger("dia-v2.charts")

# Volvo brand palette
VOLVO_COLORS = ["#003057", "#2389DA", "#4A6076", "#8EABBE", "#6B8299", "#C8D8E4"]

BENCHMARK_COLORS = {
    "excellent": "#003057",
    "good":      "#2389DA",
    "average":   "#F57F17",
    "poor":      "#BF360C",
}

# ── Column classifiers ────────────────────────────────────────────────────────

DATE_PATTERNS   = re.compile(r"date|month|week|year|period|time", re.I)
METRIC_PATTERNS = re.compile(r"rate|pct|percent|count|sends|clicks|opens|bounces|delivered|volume|total", re.I)
CATEGORY_PATTERNS = re.compile(r"market|name|business_unit|bu|region|country|program|type|category|intent|email_name", re.I)
BENCHMARK_PATTERNS = re.compile(r"benchmark|classification|rating|status|tier", re.I)

# Query keyword signals
TREND_WORDS    = re.compile(r"\btrend|over time|by month|monthly|weekly|ytd|yoy|yoy\b|time series", re.I)
RANK_WORDS     = re.compile(r"\btop \d+|bottom \d+|best|worst|highest|lowest|ranking|ranked\b", re.I)
COMPARE_WORDS  = re.compile(r"\bcompare|vs\.?|versus|across|between|by market|by region|breakdown\b", re.I)
SINGLE_WORDS   = re.compile(r"\boverall|total|average|what is|what was|how many\b", re.I)


def _col_type(col: str, sample_val) -> str:
    """Classify a column as 'date', 'metric', 'category', or 'benchmark'."""
    if isinstance(sample_val, (date, datetime)):
        return "date"
    if DATE_PATTERNS.search(col):
        return "date"
    if BENCHMARK_PATTERNS.search(col):
        return "benchmark"
    if METRIC_PATTERNS.search(col):
        return "metric"
    if CATEGORY_PATTERNS.search(col):
        return "category"
    if isinstance(sample_val, (int, float)) and not isinstance(sample_val, bool):
        return "metric"
    return "category"


def _classify_columns(results: list) -> dict:
    """Return lists of column names by type."""
    if not results:
        return {}
    first = results[0]
    classified = {"date": [], "metric": [], "category": [], "benchmark": []}
    for col, val in first.items():
        t = _col_type(col, val)
        classified[t].append(col)
    return classified


def _is_rate_col(col: str) -> bool:
    return bool(re.search(r"rate|pct|percent|ctor", col, re.I))


def _format_label(col: str) -> str:
    """Turn snake_case column into readable label."""
    return col.replace("_", " ").title().replace("Pct", "%").replace("Rate %", "Rate (%)")


# ── Core detection logic ──────────────────────────────────────────────────────

def recommend_chart(results: list, query: str = "", force: bool = False) -> dict | None:
    """
    Analyse result set + query text and return a Plotly-ready chart config,
    or None if the data isn't suitable for a chart.

    force=True skips the minimum-rows guard (used when user explicitly asks to plot).

    Returned dict keys:
        chart_type   : "bar" | "horizontal_bar" | "line" | "multi_line" | "donut"
        x_field      : column name for x-axis / labels
        y_fields     : list of column names for y-axis / values
        title        : chart title string
        x_label      : human-readable x-axis label
        y_label      : human-readable y-axis label
        is_pct       : bool — True if primary metric is a percentage
        colors       : list of hex colors
    """
    if not results:
        return None
    if not force and len(results) < 2:
        return None

    cols = _classify_columns(results)
    n_rows = len(results)
    q = query.lower()

    date_cols     = cols.get("date", [])
    metric_cols   = cols.get("metric", [])
    category_cols = cols.get("category", [])

    if not metric_cols:
        return None

    # Pick primary metric — prefer click_rate, then open_rate, then first metric
    primary_metric = next(
        (c for c in metric_cols if "click_rate" in c.lower()),
        next((c for c in metric_cols if "open_rate" in c.lower()), metric_cols[0]),
    )
    is_pct = _is_rate_col(primary_metric)

    # ── 1. TIME TREND — date column present + trend keywords or 3+ date rows ──
    if date_cols and (TREND_WORDS.search(query) or n_rows >= 3):
        x_field = date_cols[0]
        # Multi-line if multiple metrics
        rate_metrics = [c for c in metric_cols if _is_rate_col(c)]
        y_fields = rate_metrics[:3] if len(rate_metrics) > 1 else [primary_metric]
        chart_type = "multi_line" if len(y_fields) > 1 else "line"
        return {
            "chart_type": chart_type,
            "x_field":    x_field,
            "y_fields":   y_fields,
            "title":      _infer_title(query, y_fields),
            "x_label":    _format_label(x_field),
            "y_label":    "%" if is_pct else _format_label(primary_metric),
            "is_pct":     is_pct,
            "colors":     VOLVO_COLORS,
        }

    # ── 2. RANKING — "top N" / "best" / "worst" keywords ──────────────────────
    if RANK_WORDS.search(query) and category_cols:
        x_field = category_cols[0]
        return {
            "chart_type": "horizontal_bar",
            "x_field":    x_field,
            "y_fields":   [primary_metric],
            "title":      _infer_title(query, [primary_metric]),
            "x_label":    _format_label(x_field),
            "y_label":    "%" if is_pct else _format_label(primary_metric),
            "is_pct":     is_pct,
            "colors":     VOLVO_COLORS,
        }

    # ── 3. COMPARISON — compare / across / by market ──────────────────────────
    if COMPARE_WORDS.search(query) and category_cols and n_rows >= 2:
        x_field = category_cols[0]
        # Multiple rate metrics → grouped bar
        rate_metrics = [c for c in metric_cols if _is_rate_col(c)]
        y_fields = rate_metrics[:2] if len(rate_metrics) > 1 else [primary_metric]
        chart_type = "grouped_bar" if len(y_fields) > 1 else "bar"
        return {
            "chart_type": chart_type,
            "x_field":    x_field,
            "y_fields":   y_fields,
            "title":      _infer_title(query, y_fields),
            "x_label":    _format_label(x_field),
            "y_label":    "%" if is_pct else _format_label(primary_metric),
            "is_pct":     is_pct,
            "colors":     VOLVO_COLORS,
        }

    # ── 4. CATEGORY BREAKDOWN — category col + metric, ≤12 rows ──────────────
    if category_cols and 2 <= n_rows <= 12:
        x_field = category_cols[0]
        chart_type = "donut" if n_rows <= 8 and not is_pct else "bar"
        return {
            "chart_type": chart_type,
            "x_field":    x_field,
            "y_fields":   [primary_metric],
            "title":      _infer_title(query, [primary_metric]),
            "x_label":    _format_label(x_field),
            "y_label":    "%" if is_pct else _format_label(primary_metric),
            "is_pct":     is_pct,
            "colors":     VOLVO_COLORS,
        }

    # ── 5. MANY ROWS — bar chart, cap at 15 ───────────────────────────────────
    if category_cols and n_rows > 2:
        return {
            "chart_type": "bar",
            "x_field":    category_cols[0],
            "y_fields":   [primary_metric],
            "title":      _infer_title(query, [primary_metric]),
            "x_label":    _format_label(category_cols[0]),
            "y_label":    "%" if is_pct else _format_label(primary_metric),
            "is_pct":     is_pct,
            "colors":     VOLVO_COLORS,
        }

    return None


def _infer_title(query: str, y_fields: list) -> str:
    """Generate a short chart title from the query."""
    # Strip question words and trailing punctuation
    title = re.sub(r"^(show me|what (is|was|are)|how (many|much)|can you show|give me)\s+", "", query, flags=re.I)
    title = re.sub(r"[?.]$", "", title).strip().capitalize()
    if len(title) > 60:
        # Fall back to metric name
        title = " & ".join(_format_label(f) for f in y_fields)
    return title


# ── Plotly figure builder ─────────────────────────────────────────────────────

def build_plotly_figure(results: list, config: dict) -> dict | None:
    """
    Build a Plotly figure dict (JSON-serialisable) from results + config.
    Returns the figure as a dict so it can be serialised through the API.
    """
    try:
        import plotly.graph_objects as go

        chart_type = config["chart_type"]
        x_field    = config["x_field"]
        y_fields   = config["y_fields"]
        title      = config.get("title", "")
        is_pct     = config.get("is_pct", False)
        colors     = config.get("colors", VOLVO_COLORS)

        cap = 15  # max rows to render
        data = results[:cap]

        x_vals = [_safe_str(row.get(x_field)) for row in data]

        fig = go.Figure()

        layout_base = dict(
            title=dict(text=title, font=dict(family="Manrope, Segoe UI, sans-serif", size=14, color="#141E30")),
            font=dict(family="Manrope, Segoe UI, sans-serif", color="#4A6076", size=12),
            plot_bgcolor="#FFFFFF",
            paper_bgcolor="#F5F7FA",
            margin=dict(l=50, r=30, t=55, b=60),
            xaxis=dict(gridcolor="#E8EDF2", linecolor="#E8EDF2", tickfont=dict(size=11)),
            yaxis=dict(gridcolor="#E8EDF2", linecolor="#E8EDF2", tickfont=dict(size=11)),
            height=340,
            showlegend=len(y_fields) > 1,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )

        if is_pct:
            layout_base["yaxis"]["ticksuffix"] = "%"

        if chart_type in ("bar", "grouped_bar"):
            for i, yf in enumerate(y_fields):
                y_vals = [_safe_num(row.get(yf)) for row in data]
                text   = [f"{v:.1f}%" if is_pct else _fmt_num(v) for v in y_vals]
                fig.add_trace(go.Bar(
                    name=_format_label(yf),
                    x=x_vals, y=y_vals,
                    text=text, textposition="outside",
                    marker_color=colors[i % len(colors)],
                    textfont=dict(size=10),
                ))
            if chart_type == "grouped_bar":
                layout_base["barmode"] = "group"

        elif chart_type == "horizontal_bar":
            y_vals = [_safe_num(row.get(y_fields[0])) for row in data]
            text   = [f"{v:.1f}%" if is_pct else _fmt_num(v) for v in y_vals]
            fig.add_trace(go.Bar(
                name=_format_label(y_fields[0]),
                x=y_vals, y=x_vals,
                orientation="h",
                text=text, textposition="outside",
                marker_color=colors[0],
                textfont=dict(size=10),
            ))
            layout_base["xaxis"]["ticksuffix"] = "%" if is_pct else ""
            layout_base["yaxis"]["ticksuffix"] = ""
            layout_base["height"] = max(300, len(data) * 32 + 80)
            layout_base["margin"]["l"] = 160

        elif chart_type == "line":
            y_vals = [_safe_num(row.get(y_fields[0])) for row in data]
            text  = [f"{v:.1f}%" if is_pct else _fmt_num(v) for v in y_vals]
            fig.add_trace(go.Scatter(
                name=_format_label(y_fields[0]),
                x=x_vals, y=y_vals,
                mode="lines+markers+text",
                text=text,
                textposition="top center",
                textfont=dict(size=10, color=colors[0]),
                line=dict(color=colors[0], width=2.5),
                marker=dict(size=7, color=colors[1]),
            ))

        elif chart_type == "multi_line":
            for i, yf in enumerate(y_fields):
                y_vals = [_safe_num(row.get(yf)) for row in data]
                text  = [f"{v:.1f}%" if is_pct else _fmt_num(v) for v in y_vals]
                fig.add_trace(go.Scatter(
                    name=_format_label(yf),
                    x=x_vals, y=y_vals,
                    mode="lines+markers+text",
                    text=text,
                    textposition="top center",
                    textfont=dict(size=10, color=colors[i % len(colors)]),
                    line=dict(color=colors[i % len(colors)], width=2.5),
                    marker=dict(size=7),
                ))

        elif chart_type == "donut":
            y_vals = [_safe_num(row.get(y_fields[0])) for row in data]
            fig.add_trace(go.Pie(
                labels=x_vals, values=y_vals,
                hole=0.45,
                marker=dict(colors=colors[:len(x_vals)]),
                textfont=dict(size=11),
            ))
            layout_base.pop("xaxis", None)
            layout_base.pop("yaxis", None)

        fig.update_layout(**layout_base)
        return fig.to_dict()

    except Exception as e:
        logger.warning(f"Chart build failed: {e}")
        return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_str(val) -> str:
    if val is None:
        return ""
    if isinstance(val, (date, datetime)):
        return str(val)[:10]
    return str(val)


def _safe_num(val) -> float:
    try:
        return float(val) if val is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _fmt_num(val: float) -> str:
    if val >= 1_000_000:
        return f"{val/1_000_000:.1f}M"
    if val >= 1_000:
        return f"{val/1_000:.1f}K"
    return f"{val:,.0f}"
