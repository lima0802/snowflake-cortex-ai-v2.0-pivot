"""
DIA v2 - RAG Entity Search
=============================
Fuzzy matching of campaign/email names using FAISS + OpenAI embeddings.

Entity index is built from AGENT_V_DIM_SFMC_METADATA_JOB (live Snowflake data).
Rebuilt automatically on first call, or call rebuild_index() to refresh.
"""

import logging
import os
import asyncio
import numpy as np
import snowflake.connector
from config import AppConfig, SnowflakeConfig

logger = logging.getLogger("dia-v2.rag")

# Global index — built once at startup
_faiss_index = None
_metadata    = None
_embed_fn    = None


# ── Index builder ─────────────────────────────────────────────────────────────

def _fetch_entities() -> list:
    """
    Pull distinct email names + metadata from AGENT_V_DIM_SFMC_METADATA_JOB.
    Returns a list of dicts — one per unique email_name.
    """
    sql = """
        SELECT DISTINCT
            email_name,
            email_name_cleansed,
            business_unit,
            program_or_compaign  AS email_type,
            program_names,
            car_model,
            dashboard_car_model
        FROM AGENT_V_DIM_SFMC_METADATA_JOB
        WHERE email_name IS NOT NULL
          AND email_name NOT ILIKE '%sparkpost%'
        ORDER BY email_name
        LIMIT 5000
    """
    try:
        conn = snowflake.connector.connect(**SnowflakeConfig.connection_params())
        cur  = conn.cursor()
        cur.execute(f"USE SCHEMA {SnowflakeConfig.DATABASE}.{SnowflakeConfig.SCHEMA}")
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        rows = cur.fetchall()
        conn.close()
        entities = [dict(zip(cols, r)) for r in rows]
        logger.info(f"RAG: loaded {len(entities)} email entities from Snowflake")
        return entities
    except Exception as e:
        logger.error(f"RAG: failed to fetch entities from Snowflake: {e}")
        return []


def _build_search_text(e: dict) -> str:
    """Rich text representation for embedding — combines all searchable fields."""
    parts = [
        e.get("email_name_cleansed") or "",
        e.get("email_name") or "",
        e.get("business_unit") or "",
        e.get("email_type") or "",
        e.get("program_names") or "",
        e.get("car_model") or "",
        e.get("dashboard_car_model") or "",
    ]
    return " | ".join(p for p in parts if p).strip()


def _make_embed_fn():
    """Return async embedding function using OpenAI."""
    from openai import AsyncOpenAI
    client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    model  = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")

    async def embed(texts: list[str]) -> list[list[float]]:
        # Batch in chunks of 100 to stay within API limits
        results = []
        for i in range(0, len(texts), 100):
            batch = texts[i:i + 100]
            resp  = await client.embeddings.create(model=model, input=batch)
            results.extend(d.embedding for d in resp.data)
        return results

    return embed


async def _ensure_index():
    """Build FAISS index on first call. Thread-safe via asyncio lock."""
    global _faiss_index, _metadata, _embed_fn

    if _faiss_index is not None:
        return

    import faiss

    _embed_fn = _make_embed_fn()
    _metadata = _fetch_entities()

    if not _metadata:
        logger.warning("RAG: no entities loaded — entity search will return empty results")
        return

    texts        = [_build_search_text(e) for e in _metadata]
    embeddings   = await _embed_fn(texts)
    emb_np       = np.array(embeddings, dtype="float32")

    faiss.normalize_L2(emb_np)

    dim           = emb_np.shape[1]
    _faiss_index  = faiss.IndexFlatIP(dim)
    _faiss_index.add(emb_np)

    logger.info(f"RAG: FAISS index ready — {_faiss_index.ntotal} vectors, dim={dim}")


async def rebuild_index():
    """Force a full rebuild — call after new campaigns are loaded into Snowflake."""
    global _faiss_index, _metadata, _embed_fn
    _faiss_index = _metadata = _embed_fn = None
    await _ensure_index()
    return _faiss_index.ntotal if _faiss_index else 0


# ── Search ────────────────────────────────────────────────────────────────────

async def search_entities(query: str, top_k: int = 3) -> list:
    """
    Fuzzy-match a user query against the email entity index.
    Returns up to top_k resolved entities with confidence scores.
    Only returns results with score >= 0.35 (relevant matches only).
    """
    await _ensure_index()

    if _faiss_index is None or _embed_fn is None:
        return []

    import faiss

    q_emb = await _embed_fn([query])
    q_np  = np.array(q_emb, dtype="float32")
    faiss.normalize_L2(q_np)

    scores, indices = _faiss_index.search(q_np, top_k)

    results = []
    for score, idx in zip(scores[0], indices[0]):
        if idx < 0 or score < 0.35:
            continue
        e = _metadata[idx]
        results.append({
            "query_term":    query,
            "resolved_name": e.get("email_name", ""),
            "display_name":  e.get("email_name_cleansed") or e.get("email_name", ""),
            "business_unit": e.get("business_unit", ""),
            "email_type":    e.get("email_type", ""),
            "car_model":     e.get("car_model", ""),
            "score":         round(float(score), 3),
        })

    logger.info(f"RAG: '{query[:50]}' -> {len(results)} matches")
    return results
