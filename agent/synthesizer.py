"""
DIA v2 - Response Synthesizer
================================
Loads response instructions dynamically from:
  data/prompts/response_instruction.txt

Edit that file to change tone, formatting, benchmark rules, and output style.
No code changes needed — restart API to reload.
"""

import logging
import json
import os
from openai import AsyncOpenAI
from config import LLMConfig, AppConfig
from agent.charts import recommend_chart, build_plotly_figure

_openai_client: AsyncOpenAI | None = None

def _get_client() -> AsyncOpenAI:
    global _openai_client
    if _openai_client is None:
        _openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _openai_client

logger = logging.getLogger("dia-v2.synthesizer")

# Module-level cache
_response_prompt: str | None = None


def _load_response_prompt() -> str:
    """Load response instructions from file. Cached after first load."""
    global _response_prompt
    if _response_prompt is None:
        try:
            with open(AppConfig.RESPONSE_PROMPT_PATH, encoding="utf-8") as f:
                raw = f.read()
            # Strip comment lines (lines starting with #)
            lines = [l for l in raw.splitlines() if not l.strip().startswith("#")]
            _response_prompt = "\n".join(lines).strip()
            logger.info(f"Loaded response prompt from {AppConfig.RESPONSE_PROMPT_PATH}")
        except FileNotFoundError:
            logger.warning(f"response_instruction.txt not found — using default")
            _response_prompt = _default_response_prompt()
    return _response_prompt


def reload_prompt():
    """Force reload from disk (call after editing the file)."""
    global _response_prompt
    _response_prompt = None
    return _load_response_prompt()


# ── Prompt templates ──────────────────────────────────────────────────────────

SYNTHESIS_PROMPT = """{response_instructions}

═══════════════════════════════════
QUERY CONTEXT
═══════════════════════════════════
Intent classified as: {intent}

SQL Results (from Snowflake — this is the ground truth, use these exact numbers):
{sql_results}

ML / Anomaly / Forecast Results (if applicable):
{ml_results}

RAG Entity Context (resolved campaign/LTA names):
{rag_context}

═══════════════════════════════════
USER QUESTION
═══════════════════════════════════
{query}

═══════════════════════════════════
INSTRUCTIONS FOR THIS RESPONSE
═══════════════════════════════════
- Use the SQL Results above as your data source — never invent numbers
- If SQL Results are empty or null, say no data was found and suggest alternatives
- Follow all formatting, tone, and guardrail rules from the instructions above
- Do NOT include industry benchmarks unless the user explicitly asked for them
- Be concise — answer the question asked, nothing more"""




# ── Main function ─────────────────────────────────────────────────────────────

def _clarification_response(state: dict) -> dict:
    """Return a clarification question without calling the LLM or hitting Snowflake."""
    import re
    query = state.get("query", "")
    q = query.lower()

    # If a 4-digit year or 2-digit abbreviation is present, this should NOT be a clarification
    # (intent.py should have caught this, but guard here too)
    has_year = bool(re.search(r'20\d{2}', query) or re.search(r"'\s*\d{2}\b", query))
    if has_year:
        answer = (
            "Could you clarify your question a bit more? "
            "For example, please specify the market and metric you're interested in."
        )
        return {"answer": answer, "data": None, "chart_config": None, "chart_figure": None, "benchmark": None}

    # Quarter without year
    m = re.search(r'\b(q[1-4]|first quarter|second quarter|third quarter|fourth quarter)\b', q)
    if m:
        period = m.group(1).upper().replace("FIRST QUARTER", "Q1").replace(
            "SECOND QUARTER", "Q2").replace("THIRD QUARTER", "Q3").replace("FOURTH QUARTER", "Q4")
        answer = (
            f"You mentioned **{period}** but didn't specify a year. "
            f"Did you mean **{period} 2025**? Or a different year? "
            "Please confirm and I'll pull the data right away."
        )
        return {"answer": answer, "data": None, "chart_config": None, "chart_figure": None, "benchmark": None}

    # Month without year
    months_map = {
        "january": "January", "february": "February", "march": "March",
        "april": "April", "may": "May", "june": "June",
        "july": "July", "august": "August", "september": "September",
        "october": "October", "november": "November", "december": "December",
    }
    for short, full in months_map.items():
        if short in q:
            answer = (
                f"You mentioned **{full}** but didn't specify a year. "
                f"Did you mean **{full} 2025**? Or a different year? "
                "Please confirm and I'll pull the data."
            )
            return {"answer": answer, "data": None, "chart_config": None, "chart_figure": None, "benchmark": None}

    # Generic clarification
    answer = (
        "Could you clarify your question a bit more? "
        "For example, please specify the time period (including year) and any market or metric you're interested in."
    )
    return {"answer": answer, "data": None, "chart_config": None, "chart_figure": None, "benchmark": None}


async def synthesize_response(state: dict) -> dict:
    """Generate the final user-facing response."""

    # Short-circuit for clarification and out-of-scope — no LLM call needed
    intent = state.get("intent", "descriptive")
    if intent == "clarification_needed":
        return _clarification_response(state)
    if intent == "out_of_scope":
        # Identity/greeting questions should answer naturally
        q = state.get("query", "").lower().strip()
        if any(w in q for w in ("who are you", "what are you", "what can you do",
                                 "what is dia", "about you", "introduce yourself",
                                 "help me", "what do you do")):
            return {
                "answer": (
                    "I'm **DIA** — the Direct Marketing Analytics Agent for Volvo Cars, built by VML MAP.\n\n"
                    "I specialise in SFMC email campaign analytics. I can help you with:\n"
                    "- **Click rates, open rates, CTOR** — by market, car model, or campaign\n"
                    "- **Send volumes & delivery metrics** — trends over time\n"
                    "- **Market comparisons** — Nordic, EMEA, country vs country\n"
                    "- **Campaign & programme performance** — top/bottom performers, rankings\n"
                    "- **Anomaly detection** — unusual patterns in your data\n\n"
                    "What would you like to explore?"
                ),
                "data": None, "chart_config": None, "chart_figure": None, "benchmark": None,
            }
        return {
            "answer": "I'm focused on Volvo Cars email campaign analytics. I can help with click rates, open rates, delivery metrics, market comparisons, and campaign performance. What would you like to know?",
            "data": None, "chart_config": None, "chart_figure": None, "benchmark": None,
        }

    response_instructions = _load_response_prompt()

    sql_results_str = "No SQL results."
    if state.get("sql_results") is not None:
        if len(state["sql_results"]) == 0:
            sql_results_str = "Query executed successfully and returned 0 rows (empty result set). This is a valid result — interpret it as the answer (e.g. no items matched the criteria)."
        else:
            sql_results_str = json.dumps(state["sql_results"][:20], indent=2, default=str)

    ml_results_str = "No ML analysis performed."
    if state.get("ml_results"):
        ml_results_str = json.dumps(state["ml_results"], indent=2, default=str)

    rag_context_str = "No entities resolved."
    if state.get("rag_results"):
        rag_context_str = json.dumps(state["rag_results"], indent=2)

    prompt = SYNTHESIS_PROMPT.format(
        response_instructions=response_instructions,
        intent=state.get("intent", "unknown"),
        sql_results=sql_results_str,
        ml_results=ml_results_str,
        rag_context=rag_context_str,
        query=state["query"],
    )

    try:
        response = await _get_client().chat.completions.create(
            model=LLMConfig.get_model("synthesis"),
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=1000,
        )

        answer    = response.choices[0].message.content
        # Only show benchmark when user explicitly requested it
        query_lower = state.get("query", "").lower()
        benchmark_requested = any(w in query_lower for w in ("benchmark", "industry", "compare to", "vs industry", "standard"))
        benchmark = _classify_benchmark(state.get("sql_results")) if benchmark_requested else None

        # Deterministic chart — no LLM call, instant
        sql_results  = state.get("sql_results") or []
        chart_config = recommend_chart(sql_results, state["query"])
        chart_figure = build_plotly_figure(sql_results, chart_config) if chart_config else None

        return {
            "answer":       answer,
            "data":         sql_results,
            "chart_config": chart_config,
            "chart_figure": chart_figure,
            "benchmark":    benchmark,
        }

    except Exception as e:
        logger.error(f"Synthesis failed: {e}")
        sql_results  = state.get("sql_results") or []
        chart_config = recommend_chart(sql_results, state["query"])
        return {
            "answer":       _fallback_summary(state),
            "data":         sql_results,
            "chart_config": chart_config,
            "chart_figure": build_plotly_figure(sql_results, chart_config) if chart_config else None,
            "benchmark":    None,
        }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _classify_benchmark(results: list) -> str | None:
    """Classify the primary metric against VCC benchmarks."""
    if not results:
        return None
    first_row = results[0]
    thresholds = {
        "click_rate":         {"Excellent": 0.032, "Good": 0.025, "Average": 0.015},
        "open_rate":          {"Excellent": 0.30,  "Good": 0.22,  "Average": 0.15},
        "avg_click_rate_pct": {"Excellent": 3.2,   "Good": 2.5,   "Average": 1.5},
        "avg_open_rate_pct":  {"Excellent": 30,    "Good": 22,    "Average": 15},
    }
    for field, levels in thresholds.items():
        if field in first_row and first_row[field] is not None:
            val = float(first_row[field])
            if val >= levels["Excellent"]: return "Excellent"
            elif val >= levels["Good"]:    return "Good"
            elif val >= levels["Average"]: return "Average"
            else:                          return "Poor"
    return None



def _fallback_summary(state: dict) -> str:
    results = state.get("sql_results", [])
    if not results:
        return "I processed your query but didn't find matching data. Could you rephrase or specify a different time period or market?"
    n_rows = len(results)
    fields = list(results[0].keys()) if results else []
    return (
        f"I found {n_rows} result{'s' if n_rows != 1 else ''} for your query. "
        f"The data includes: {', '.join(fields[:5])}. "
        "Let me know if you'd like me to dig deeper into any specific aspect."
    )


def _default_response_prompt() -> str:
    return """You are the DIA (Data Intelligence Agent) for Volvo Cars Corporation.
Transform raw data into actionable marketing insights.
Be professional, use concrete numbers, include benchmark classifications, suggest next steps."""
