"""
DIA v2 - FastAPI Application
==============================
Main entry point. Exposes REST API that both the Streamlit UI and Teams bot call.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

from contextlib import asynccontextmanager
from config import AppConfig
from agent.graph import run_agent
from agent.feedback import write_feedback

logging.basicConfig(level=logging.DEBUG if AppConfig.DEBUG else logging.INFO)
logger = logging.getLogger("dia-v2")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: clear prompt caches and pre-warm RAG index."""
    # Clear cached prompts so file edits take effect on every restart
    from agent.intent import reload_prompt as reload_intent_prompt
    from agent.synthesizer import reload_prompt as reload_synth_prompt
    from agent.text_to_sql import reload_context
    reload_intent_prompt()
    reload_synth_prompt()
    reload_context()
    logger.info("Prompt caches cleared.")
    try:
        from agent.rag import _ensure_index
        logger.info("Pre-warming RAG entity index...")
        await _ensure_index()
        logger.info("RAG index ready.")
    except Exception as e:
        logger.warning(f"RAG pre-warm skipped: {e}")
    yield


app = FastAPI(
    title="DIA v2 - Direct Marketing Analytics Agent",
    description="Conversational analytics for VCC email campaign performance",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Lock down in production
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Request/Response Models ---

class QueryRequest(BaseModel):
    query: str
    session_id: Optional[str] = "default"
    context: Optional[dict] = None  # For multi-turn conversation state


class QueryResponse(BaseModel):
    answer: str
    sql: Optional[str] = None
    data: Optional[list] = None           # Raw query results
    chart_config: Optional[dict] = None   # Chart metadata (type, fields, title)
    chart_figure: Optional[dict] = None   # Full Plotly figure dict — render directly
    intent: Optional[str] = None
    confidence: Optional[float] = None
    benchmark: Optional[str] = None
    processing_steps: Optional[list] = None
    error: Optional[str] = None


class FeedbackRequest(BaseModel):
    rating: int                        # 1 = thumbs up, -1 = thumbs down
    query_text: str
    answer_text: Optional[str] = None
    sql_generated: Optional[str] = None
    intent: Optional[str] = None
    feedback_text: Optional[str] = None   # free-text comment
    session_id: Optional[str] = "default"


class HealthResponse(BaseModel):
    status: str
    snowflake: str
    llm_provider: str


# --- Endpoints ---

@app.post("/reload")
async def reload_all():
    """Hot-reload all YAML files and prompts without restarting the server."""
    from agent.intent import reload_prompt as reload_intent_prompt
    from agent.synthesizer import reload_prompt as reload_synth_prompt
    from agent.text_to_sql import reload_context
    reload_intent_prompt()
    reload_synth_prompt()
    reload_context()
    return {"status": "reloaded"}


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check for monitoring and Teams bot registration."""
    from config import LLMConfig, SnowflakeConfig
    import snowflake.connector

    sf_status = "unknown"
    try:
        conn = snowflake.connector.connect(**SnowflakeConfig.connection_params())
        conn.cursor().execute("SELECT 1")
        conn.close()
        sf_status = "connected"
    except Exception as e:
        sf_status = f"error: {str(e)[:50]}"

    return HealthResponse(
        status="healthy",
        snowflake=sf_status,
        llm_provider=LLMConfig.PROVIDER,
    )


@app.post("/query", response_model=QueryResponse)
async def process_query(request: QueryRequest):
    """
    Main query endpoint. Receives natural language, returns structured response.
    Called by Streamlit UI and Teams bot.
    """
    logger.info(f"Query received: {request.query[:100]}...")

    try:
        result = await run_agent(
            query=request.query,
            session_id=request.session_id,
            context=request.context,
        )
        return QueryResponse(**result)

    except Exception as e:
        logger.error(f"Agent error: {e}", exc_info=True)

        # Demo fallback: return cached result if available
        if AppConfig.DEMO_FALLBACK:
            fallback = _get_fallback_response(request.query)
            if fallback:
                logger.info("Returning demo fallback response")
                return QueryResponse(**fallback)

        raise HTTPException(status_code=500, detail=str(e))


def _get_fallback_response(query: str) -> Optional[dict]:
    """Load pre-cached demo response if available."""
    import json
    try:
        with open(AppConfig.GOLDEN_QUERIES_PATH) as f:
            golden = json.load(f)
        # Simple keyword matching for demo
        q_lower = query.lower()
        for item in golden:
            if any(kw in q_lower for kw in item.get("keywords", [])):
                return item["response"]
    except Exception:
        pass
    return None


@app.post("/feedback")
async def submit_feedback(request: FeedbackRequest):
    """Collect user feedback (thumbs up/down + optional comment) into Snowflake."""
    if request.rating not in (1, -1):
        raise HTTPException(status_code=400, detail="rating must be 1 (up) or -1 (down)")

    loop = asyncio.get_event_loop()
    ok = await loop.run_in_executor(
        None,
        lambda: write_feedback(
            rating=request.rating,
            query_text=request.query_text,
            answer_text=request.answer_text,
            sql_generated=request.sql_generated,
            intent=request.intent,
            feedback_text=request.feedback_text,
            session_id=request.session_id,
        ),
    )

    if not ok:
        raise HTTPException(status_code=500, detail="Failed to record feedback")

    logger.info(f"Feedback saved: rating={request.rating}, session={request.session_id}")
    return {"status": "recorded"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=AppConfig.HOST, port=AppConfig.PORT)
