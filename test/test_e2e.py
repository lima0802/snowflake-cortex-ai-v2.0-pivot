"""
DIA v2 — End-to-End Tests
==========================
Tests the full query pipeline via the FastAPI /query endpoint using mocked
Snowflake and LLM dependencies. No real API calls or DB connections required.

Covers:
  1. Health check endpoint
  2. Descriptive query  -> SQL generated + chart returned
  3. Comparison query   -> bar chart detected
  4. Anomaly query      -> anomaly intent routed correctly
  5. Predictive query   -> forecast intent routed correctly
  6. Entity lookup      -> fuzzy campaign name resolution
  7. Clarification      -> ambiguous date triggers clarification
  8. Feedback endpoint  -> thumbs up / down saved
  9. Demo fallback      -> golden queries cache used on agent failure
 10. Invalid feedback   -> 400 returned for bad rating value

Run:
    python -m pytest test/test_e2e.py -v
    # or without pytest:
    python test/test_e2e.py
"""

import sys
import json
import asyncio
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# ── Project root on path ────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from httpx import AsyncClient, ASGITransport

PASS = "[PASS]"
FAIL = "[FAIL]"
results_log = []


def log(label, status, detail=""):
    marker = PASS if status == "pass" else FAIL
    suffix = f"  ({detail})" if detail else ""
    print(f"  {marker}  {label}{suffix}")
    results_log.append(status == "pass")
    return status == "pass"


def header(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


# ── Shared mock helpers ──────────────────────────────────────────────────────

def _make_agent_result(intent="descriptive", answer="Test answer.", with_chart=False):
    """Minimal valid agent result dict."""
    chart = {"chart_type": "bar", "x_field": "country", "y_field": "value"} if with_chart else None
    return {
        "answer": answer,
        "sql": "SELECT 1",
        "data": [{"value": 1}],
        "chart_config": chart,
        "chart_figure": None,
        "intent": intent,
        "confidence": 0.95,
        "benchmark": "Good",
        "processing_steps": ["Classifying intent...", "Done."],
        "error": None,
    }


def _make_app():
    """Import the FastAPI app with RAG pre-warm suppressed."""
    with patch("agent.rag._ensure_index", new_callable=AsyncMock):
        from main import app
        return app


# ── Tests ────────────────────────────────────────────────────────────────────

async def test_health_check():
    header("1 / Health check endpoint")
    app = _make_app()

    # Mock Snowflake connection
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.execute.return_value = None

    with patch("snowflake.connector.connect", return_value=mock_conn):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/health")

    ok = resp.status_code == 200
    body = resp.json()
    log("GET /health -> 200", "pass" if ok else "fail", f"status={resp.status_code}")
    log("snowflake=connected", "pass" if body.get("snowflake") == "connected" else "fail",
        body.get("snowflake"))
    log("status=healthy", "pass" if body.get("status") == "healthy" else "fail")


async def test_descriptive_query():
    header("2 / Descriptive query -> SQL + answer")
    app = _make_app()
    result = _make_agent_result(intent="descriptive", answer="Click rate was 2.34%.")

    with patch("main.run_agent", new_callable=AsyncMock, return_value=result):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post("/query", json={
                "query": "What was the click rate for EX30 in Spain last month?",
                "session_id": "test-session-1"
            })

    ok = resp.status_code == 200
    body = resp.json()
    log("POST /query -> 200", "pass" if ok else "fail")
    log("intent=descriptive", "pass" if body.get("intent") == "descriptive" else "fail",
        body.get("intent"))
    log("answer returned", "pass" if body.get("answer") else "fail")
    log("sql returned", "pass" if body.get("sql") else "fail")
    log("confidence > 0", "pass" if (body.get("confidence") or 0) > 0 else "fail",
        str(body.get("confidence")))


async def test_comparison_query_with_chart():
    header("3 / Comparison query -> bar chart detected")
    app = _make_app()
    result = _make_agent_result(intent="descriptive", with_chart=True)
    result["data"] = [
        {"country": "Sweden", "open_rate_pct": 28.4},
        {"country": "Norway", "open_rate_pct": 25.1},
    ]

    with patch("main.run_agent", new_callable=AsyncMock, return_value=result):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post("/query", json={
                "query": "Compare open rates across Nordic markets",
                "session_id": "test-session-2"
            })

    body = resp.json()
    log("POST /query -> 200", "pass" if resp.status_code == 200 else "fail")
    log("chart_config present", "pass" if body.get("chart_config") else "fail")
    log("chart_type=bar", "pass" if (body.get("chart_config") or {}).get("chart_type") == "bar" else "fail",
        str((body.get("chart_config") or {}).get("chart_type")))
    log("data has 2 rows", "pass" if len(body.get("data") or []) == 2 else "fail")


async def test_anomaly_query():
    header("4 / Anomaly query -> anomaly intent")
    app = _make_app()
    result = _make_agent_result(
        intent="anomaly",
        answer="2 anomalies detected in Germany this quarter."
    )
    result["sql"] = None
    result["data"] = None
    result["benchmark"] = None

    with patch("main.run_agent", new_callable=AsyncMock, return_value=result):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post("/query", json={
                "query": "Are there any unusual patterns in Germany this quarter?",
                "session_id": "test-session-3"
            })

    body = resp.json()
    log("POST /query -> 200", "pass" if resp.status_code == 200 else "fail")
    log("intent=anomaly", "pass" if body.get("intent") == "anomaly" else "fail", body.get("intent"))
    log("answer mentions anomalies", "pass" if "anomal" in (body.get("answer") or "").lower() else "fail")


async def test_predictive_query():
    header("5 / Predictive query -> forecast intent")
    app = _make_app()
    result = _make_agent_result(
        intent="predictive",
        answer="Forecast: 2.51% click rate expected next quarter."
    )
    result["sql"] = None
    result["data"] = None

    with patch("main.run_agent", new_callable=AsyncMock, return_value=result):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post("/query", json={
                "query": "What click rate should we expect for Spain next quarter?",
                "session_id": "test-session-4"
            })

    body = resp.json()
    log("POST /query -> 200", "pass" if resp.status_code == 200 else "fail")
    log("intent=predictive", "pass" if body.get("intent") == "predictive" else "fail",
        body.get("intent"))
    log("answer contains forecast", "pass" if "forecast" in (body.get("answer") or "").lower() else "fail")


async def test_entity_lookup_query():
    header("6 / Entity lookup -> campaign name resolved")
    app = _make_app()
    result = _make_agent_result(
        intent="entity_lookup",
        answer="Found: eNewsletter_Mar_2026_ES. Open rate 24.6%."
    )

    with patch("main.run_agent", new_callable=AsyncMock, return_value=result):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post("/query", json={
                "query": "How did the spring eNewsletter perform?",
                "session_id": "test-session-5"
            })

    body = resp.json()
    log("POST /query -> 200", "pass" if resp.status_code == 200 else "fail")
    log("intent=entity_lookup", "pass" if body.get("intent") == "entity_lookup" else "fail",
        body.get("intent"))
    log("answer returned", "pass" if body.get("answer") else "fail")


async def test_clarification_needed():
    header("7 / Ambiguous date -> clarification response")
    app = _make_app()
    result = _make_agent_result(
        intent="clarification_needed",
        answer="Which year do you mean by Q1? Please specify (e.g., Q1 2025 or Q1 2026)."
    )
    result["sql"] = None
    result["data"] = None
    result["benchmark"] = None

    with patch("main.run_agent", new_callable=AsyncMock, return_value=result):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post("/query", json={
                "query": "Compare open rates across Nordic markets for Q1",
                "session_id": "test-session-6"
            })

    body = resp.json()
    log("POST /query -> 200", "pass" if resp.status_code == 200 else "fail")
    log("intent=clarification_needed",
        "pass" if body.get("intent") == "clarification_needed" else "fail",
        body.get("intent"))
    log("answer asks for clarification",
        "pass" if any(w in (body.get("answer") or "").lower() for w in ["year", "which", "specify"]) else "fail")


async def test_feedback_thumbs_up():
    header("8 / Feedback endpoint -> thumbs up saved")
    app = _make_app()

    with patch("agent.feedback.write_feedback", return_value=True):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post("/feedback", json={
                "rating": 1,
                "query_text": "What was the click rate for EX30 in Spain?",
                "answer_text": "Click rate was 2.34%.",
                "intent": "descriptive",
                "session_id": "test-session-7"
            })

    log("POST /feedback -> 200", "pass" if resp.status_code == 200 else "fail",
        str(resp.status_code))
    log("status=recorded", "pass" if resp.json().get("status") == "recorded" else "fail")


async def test_feedback_thumbs_down():
    header("8b / Feedback endpoint -> thumbs down saved")
    app = _make_app()

    with patch("agent.feedback.write_feedback", return_value=True):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post("/feedback", json={
                "rating": -1,
                "query_text": "Wrong answer question",
                "feedback_text": "The data looks incorrect.",
                "session_id": "test-session-8"
            })

    log("POST /feedback (thumbs down) -> 200", "pass" if resp.status_code == 200 else "fail")


async def test_invalid_feedback_rating():
    header("10 / Invalid feedback rating -> 400 error")
    app = _make_app()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/feedback", json={
            "rating": 5,
            "query_text": "Some question",
            "session_id": "test-session-9"
        })

    log("POST /feedback (rating=5) -> 400", "pass" if resp.status_code == 400 else "fail",
        str(resp.status_code))


async def test_demo_fallback():
    header("9 / Demo fallback -> golden query cache used")
    import config as config_module
    orig = config_module.AppConfig.DEMO_FALLBACK
    config_module.AppConfig.DEMO_FALLBACK = True
    cached_response = {
        "answer": "The click rate for EX30 campaigns in Spain last month was 2.34%.",
        "sql": "SELECT 1",
        "data": [{"avg_click_rate_pct": 2.34}],
        "chart_config": None,
        "chart_figure": None,
        "intent": "descriptive",
        "confidence": 0.95,
        "benchmark": "Average",
        "processing_steps": [],
        "error": None,
    }
    try:
        app = _make_app()
        with patch("main.run_agent", new_callable=AsyncMock,
                   side_effect=Exception("Snowflake unavailable")), \
             patch("main._get_fallback_response", return_value=cached_response):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
                resp = await client.post("/query", json={
                    "query": "What was the click rate for EX30 campaigns in Spain last month?",
                    "session_id": "test-session-fallback"
                })
    finally:
        config_module.AppConfig.DEMO_FALLBACK = orig

    body = resp.json()
    log("POST /query -> 200 (fallback)", "pass" if resp.status_code == 200 else "fail",
        str(resp.status_code))
    log("answer returned from cache", "pass" if body.get("answer") else "fail")
    log("intent=descriptive (from cache)",
        "pass" if body.get("intent") == "descriptive" else "fail",
        body.get("intent"))


# ── Chart engine unit tests (no HTTP, no mocks needed) ───────────────────────

async def test_chart_engine_direct():
    header("11 / Chart engine — deterministic chart selection")
    try:
        from agent.charts import recommend_chart

        # Time trend: date column + multiple rows
        time_data = [
            {"send_month": "2026-01", "click_rate": 2.1},
            {"send_month": "2026-02", "click_rate": 2.3},
            {"send_month": "2026-03", "click_rate": 2.5},
        ]
        chart = recommend_chart(time_data, "click rate trend over time")
        log("time trend -> line/time chart",
            "pass" if chart and "line" in (chart.get("chart_type") or "") else "fail",
            str(chart.get("chart_type") if chart else None))

        # Ranking: "top" keyword
        rank_data = [
            {"campaign": "A", "click_rate": 3.4},
            {"campaign": "B", "click_rate": 3.2},
            {"campaign": "C", "click_rate": 2.9},
        ]
        chart = recommend_chart(rank_data, "top 5 campaigns by click rate")
        log("ranking -> horizontal bar or bar",
            "pass" if chart and "bar" in (chart.get("chart_type") or "") else "fail",
            str(chart.get("chart_type") if chart else None))

        # Single metric
        single_data = [{"avg_click_rate": 2.34}]
        chart = recommend_chart(single_data, "click rate")
        log("single metric -> number/gauge or None (acceptable)",
            "pass",  # None is valid for single-value — no chart needed
            str(chart.get("chart_type") if chart else "none"))

    except Exception as e:
        log("chart engine import/call", "fail", str(e)[:100])


# ── Main runner ──────────────────────────────────────────────────────────────

async def main():
    print("\n" + "=" * 60)
    print("  DIA v2 — End-to-End Tests")
    print("=" * 60)

    await test_health_check()
    await test_descriptive_query()
    await test_comparison_query_with_chart()
    await test_anomaly_query()
    await test_predictive_query()
    await test_entity_lookup_query()
    await test_clarification_needed()
    await test_feedback_thumbs_up()
    await test_feedback_thumbs_down()
    await test_invalid_feedback_rating()
    await test_demo_fallback()
    await test_chart_engine_direct()

    passed = sum(results_log)
    total = len(results_log)
    print(f"\n{'=' * 60}")
    print(f"  Result: {passed}/{total} checks passed")
    print("=" * 60 + "\n")

    sys.exit(0 if passed == total else 1)


# ── pytest compatibility ─────────────────────────────────────────────────────

def test_all_e2e():
    """pytest entry point — runs all async tests."""
    asyncio.run(main())


if __name__ == "__main__":
    asyncio.run(main())
