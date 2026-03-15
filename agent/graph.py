"""
DIA v2 - LangGraph Agent
==========================
Stateful agent that orchestrates intent classification, tool selection,
SQL generation, RAG search, ML features, and response synthesis.

This is the core intelligence layer — it replaces Cortex Agent.
"""

import logging
from typing import TypedDict, Optional, Annotated
from langgraph.graph import StateGraph, END

from agent.intent import classify_intent
from agent.text_to_sql import generate_and_execute_sql
from agent.rag import search_entities
from agent.ml_features import detect_anomalies, forecast_metric
from agent.synthesizer import synthesize_response

logger = logging.getLogger("dia-v2.agent")


# --- Agent State ---

class AgentState(TypedDict):
    """State passed between agent nodes."""
    query: str
    session_id: str
    context: Optional[dict]

    # Set by intent classifier
    intent: Optional[str]           # descriptive | diagnostic | predictive | anomaly | entity_lookup
    confidence: Optional[float]

    # Set by tools
    sql: Optional[str]
    sql_results: Optional[list]
    sql_error: Optional[str]
    retry_count: int

    rag_results: Optional[list]     # Resolved entity matches
    ml_results: Optional[dict]      # Anomaly or forecast results

    # Set by synthesizer
    answer: Optional[str]
    data: Optional[list]
    chart_config: Optional[dict]
    chart_figure: Optional[dict]
    benchmark: Optional[str]
    processing_steps: list
    error: Optional[str]


# --- Agent Nodes ---

async def classify_intent_node(state: AgentState) -> AgentState:
    """Classify user intent and kick off RAG index warm-up in parallel."""
    import asyncio
    from agent.rag import _ensure_index

    state["processing_steps"].append("Classifying intent...")
    # Run intent classification and RAG index warm-up concurrently
    intent_task = asyncio.create_task(classify_intent(state["query"]))
    warmup_task = asyncio.create_task(_ensure_index())
    result, _ = await asyncio.gather(intent_task, warmup_task)
    state["intent"] = result["intent"]
    state["confidence"] = result["confidence"]
    logger.info(f"Intent: {state['intent']} (confidence: {state['confidence']:.2f})")
    return state


async def entity_search_node(state: AgentState) -> AgentState:
    """RAG search for fuzzy entity resolution — index already warm from intent node."""
    state["processing_steps"].append("Searching entities...")
    results = await search_entities(state["query"])
    state["rag_results"] = results
    logger.info(f"RAG results: {len(results)} entities found")
    return state


async def sql_generation_node(state: AgentState) -> AgentState:
    """Generate SQL from natural language and execute against Snowflake."""
    state["processing_steps"].append("Generating SQL...")
    result = await generate_and_execute_sql(
        query=state["query"],
        rag_context=state.get("rag_results"),
        previous_error=state.get("sql_error"),
    )
    state["sql"] = result.get("sql")
    state["sql_results"] = result.get("results")
    state["sql_error"] = result.get("error")
    if result.get("error"):
        state["retry_count"] = state.get("retry_count", 0) + 1
        logger.warning(f"SQL error (attempt {state['retry_count']}): {result['error']}")
    else:
        state["processing_steps"].append("Executing query...")
        logger.info(f"SQL executed: {len(result.get('results', []))} rows returned")
    return state


async def anomaly_node(state: AgentState) -> AgentState:
    """Run anomaly detection on relevant metrics."""
    state["processing_steps"].append("Detecting anomalies...")
    result = await detect_anomalies(state["query"])
    state["ml_results"] = result
    return state


async def forecast_node(state: AgentState) -> AgentState:
    """Generate time-series forecast."""
    state["processing_steps"].append("Generating forecast...")
    result = await forecast_metric(state["query"])
    state["ml_results"] = result
    return state


async def synthesis_node(state: AgentState) -> AgentState:
    """Synthesize final natural language response with business context."""
    state["processing_steps"].append("Analyzing results...")
    result = await synthesize_response(state)
    state["answer"] = result["answer"]
    state["data"] = result.get("data")
    state["chart_config"] = result.get("chart_config")
    state["chart_figure"] = result.get("chart_figure")
    state["benchmark"] = result.get("benchmark")
    return state


# --- Routing Logic ---

def route_after_intent(state: AgentState) -> str:
    """Route to appropriate tool based on classified intent."""
    intent = state.get("intent", "descriptive")

    if intent in ("clarification_needed", "out_of_scope"):
        return "direct_synthesis"  # Skip SQL/RAG — go straight to response
    elif intent == "entity_lookup":
        return "entity_search"
    elif intent == "anomaly":
        return "anomaly_detection"
    elif intent == "predictive":
        return "forecast"
    else:
        # descriptive and diagnostic both go through entity search first
        return "entity_search"


def route_after_sql(state: AgentState) -> str:
    """Decide whether to retry SQL or proceed to synthesis."""
    if state.get("sql_error") and state.get("retry_count", 0) < 2:
        return "sql_generation"  # Retry with error context
    return "synthesis"


def route_after_entity_search(state: AgentState) -> str:
    """After entity search, go to SQL generation for most intents."""
    intent = state.get("intent", "descriptive")
    if intent in ("descriptive", "diagnostic", "entity_lookup"):
        return "sql_generation"
    return "synthesis"


# --- Build the Graph ---

def build_agent_graph() -> StateGraph:
    """Construct the LangGraph agent."""

    graph = StateGraph(AgentState)

    # Add nodes
    graph.add_node("classify_intent", classify_intent_node)
    graph.add_node("entity_search", entity_search_node)
    graph.add_node("sql_generation", sql_generation_node)
    graph.add_node("anomaly_detection", anomaly_node)
    graph.add_node("forecast", forecast_node)
    graph.add_node("synthesis", synthesis_node)

    # Define edges
    graph.set_entry_point("classify_intent")

    graph.add_conditional_edges("classify_intent", route_after_intent, {
        "entity_search":      "entity_search",
        "anomaly_detection":  "anomaly_detection",
        "forecast":           "forecast",
        "direct_synthesis":   "synthesis",   # clarification_needed / out_of_scope
    })

    graph.add_conditional_edges("entity_search", route_after_entity_search, {
        "sql_generation": "sql_generation",
        "synthesis":      "synthesis",
    })

    graph.add_conditional_edges("sql_generation", route_after_sql, {
        "sql_generation": "sql_generation",
        "synthesis":      "synthesis",
    })

    graph.add_edge("anomaly_detection", "synthesis")
    graph.add_edge("forecast", "synthesis")
    graph.add_edge("synthesis", END)

    return graph.compile()


# Singleton compiled graph
_agent = build_agent_graph()


async def run_agent(query: str, session_id: str = "default", context: dict = None) -> dict:
    """Execute the agent graph and return structured response."""

    initial_state: AgentState = {
        "query": query,
        "session_id": session_id,
        "context": context,
        "intent": None,
        "confidence": None,
        "sql": None,
        "sql_results": None,
        "sql_error": None,
        "retry_count": 0,
        "rag_results": None,
        "ml_results": None,
        "answer": None,
        "data": None,
        "chart_config": None,
        "chart_figure": None,
        "benchmark": None,
        "processing_steps": [],
        "error": None,
    }

    final_state = await _agent.ainvoke(initial_state)

    return {
        "answer": final_state.get("answer", "I wasn't able to process that query."),
        "sql": final_state.get("sql"),
        "data": final_state.get("data"),
        "chart_config": final_state.get("chart_config"),
        "chart_figure": final_state.get("chart_figure"),
        "intent": final_state.get("intent"),
        "confidence": final_state.get("confidence"),
        "benchmark": final_state.get("benchmark"),
        "processing_steps": final_state.get("processing_steps", []),
        "error": final_state.get("error"),
    }
