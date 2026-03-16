"""
DIA v2 - Text-to-SQL Engine
==============================
Loads semantic model and SQL instructions dynamically from files.
To update prompts or schema: edit the files in data/semantic_views/ and
data/prompts/ — no code changes required, restart API to reload.

File loading map:
  SQL rules   ← data/semantic_views/dm_performance_model/custom_instructions/custom_instructions.yaml
  Schema      ← data/semantic_views/dm_performance_model/logical_tables/*.yaml
              ← data/semantic_views/dm_performance_model/derived_metrics/*.yaml
              ← data/semantic_views/dm_performance_model/relationships/*.yaml
  Few-shots   ← data/semantic_views/dm_performance_model/verified_queries/*.yaml
"""

import glob
import logging
import json
import yaml
from typing import Optional

import os
from openai import AsyncOpenAI
import snowflake.connector
from config import LLMConfig, SnowflakeConfig, AppConfig

_openai_client: AsyncOpenAI | None = None

def _get_client() -> AsyncOpenAI:
    global _openai_client
    if _openai_client is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            try:
                import streamlit as st
                api_key = st.secrets.get("OPENAI_API_KEY")
            except Exception:
                pass
        _openai_client = AsyncOpenAI(api_key=api_key)
    return _openai_client

logger = logging.getLogger("dia-v2.text_to_sql")

# Module-level cache — loaded once at startup, cleared on hot-reload
_cache: dict = {}


# ── Dynamic file loaders ──────────────────────────────────────────────────────

def _resolve_placeholders(text: str) -> str:
    """Replace {{DATABASE}} and {{SCHEMA}} with real Snowflake values."""
    return (
        text
        .replace("{{DATABASE}}", SnowflakeConfig.DATABASE or "PLAYGROUND_LM")
        .replace("{{SCHEMA}}",   SnowflakeConfig.SCHEMA   or "CORTEX_ANALYTICS_ORCHESTRATOR")
    )


def _load_yaml_dir(directory: str) -> list[dict]:
    """Load all YAML files in a directory, resolving DB/schema placeholders."""
    files = sorted(glob.glob(f"{directory}/*.yaml"))
    results = []
    for path in files:
        try:
            with open(path, encoding="utf-8") as f:
                raw = _resolve_placeholders(f.read())
            content = yaml.safe_load(raw)
            if content:
                results.append(content)
            logger.debug(f"Loaded: {path}")
        except Exception as e:
            logger.warning(f"Could not load {path}: {e}")
    return results


def _merge_semantic_model() -> dict:
    """
    Glob-merge all semantic YAML files into one context dict.
    Keeps each section separate so the LLM gets structured context.
    """
    merged = {
        "logical_tables": [],
        "derived_metrics": [],
        "relationships": [],
        "verified_queries": [],
    }

    for item in _load_yaml_dir(AppConfig.LOGICAL_TABLES_DIR):
        merged["logical_tables"].append(item)

    for item in _load_yaml_dir(AppConfig.DERIVED_METRICS_DIR):
        merged["derived_metrics"].append(item)

    for item in _load_yaml_dir(AppConfig.RELATIONSHIPS_DIR):
        merged["relationships"].append(item)

    for item in _load_yaml_dir(AppConfig.VERIFIED_QUERIES_DIR):
        merged["verified_queries"].append(item)

    total = sum(len(v) for v in merged.values())
    logger.info(f"Semantic model loaded: {total} files merged from {AppConfig.SEMANTIC_VIEWS_ROOT}")
    return merged


def _load_custom_instructions() -> str:
    """
    Load SQL generation rules from custom_instructions.yaml.
    Falls back to built-in default if file missing.
    """
    try:
        with open(AppConfig.CUSTOM_INSTRUCTIONS_PATH, encoding="utf-8") as f:
            content = yaml.safe_load(f)
            instructions = content.get("instructions", "")
            logger.info(f"Loaded SQL instructions from {AppConfig.CUSTOM_INSTRUCTIONS_PATH}")
            return instructions.strip()
    except FileNotFoundError:
        logger.warning(f"custom_instructions.yaml not found — using default SQL rules")
        return _default_sql_instructions()


def _load_context():
    """Load all semantic context. Cached after first load."""
    if not _cache:
        _cache["semantic_model"] = _merge_semantic_model()
        _cache["sql_instructions"] = _load_custom_instructions()
    return _cache["semantic_model"], _cache["sql_instructions"]


def reload_context():
    """Force reload all files from disk (call after editing prompt files)."""
    _cache.clear()
    logger.info("Semantic context cache cleared — reloading from disk")
    return _load_context()


# ── Prompt templates ──────────────────────────────────────────────────────────

SQL_GENERATION_PROMPT = """You are a Snowflake SQL expert for Volvo Cars email campaign analytics.

## Database Context
Schema: {database}.{schema}
All AGENT_V_* objects are VIEWS (not tables). Reference them WITHOUT any database/schema prefix.
The session is already set to USE SCHEMA {database}.{schema} — just write: FROM AGENT_V_FACT_SFMC_PERFORMANCE_TRACKING

## SQL Rules
{sql_instructions}

## Semantic Model — Logical Tables
{logical_tables}

## Semantic Model — Derived Metrics
{derived_metrics}

## Semantic Model — Relationships
{relationships}

## Verified Query Examples (Few-Shot)
{verified_queries}

## Entity Context (from RAG search)
{rag_context}

{error_context}

Now generate SQL for this question. Return ONLY the SQL, nothing else:
{query}"""


ERROR_RETRY_CONTEXT = """## Previous Attempt Failed
The previous SQL query produced this error:
```
{error}
```
Previous SQL:
```sql
{previous_sql}
```
Please fix the error and generate corrected SQL.
"""


# ── Main function ─────────────────────────────────────────────────────────────

async def generate_and_execute_sql(
    query: str,
    rag_context: Optional[list] = None,
    previous_error: Optional[str] = None,
    previous_sql: Optional[str] = None,
) -> dict:
    """Generate SQL from natural language and execute against Snowflake."""

    semantic_model, sql_instructions = _load_context()

    # RAG context string
    rag_str = "No specific entities resolved."
    if rag_context:
        rag_str = "Resolved entities:\n" + "\n".join(
            f"- '{r['query_term']}' → {r['resolved_name']} (confidence: {r['score']:.2f})"
            for r in rag_context
        )

    # Error context for retries
    error_ctx = ""
    if previous_error:
        error_ctx = ERROR_RETRY_CONTEXT.format(
            error=previous_error,
            previous_sql=previous_sql or "N/A",
        )

    # Serialize each section — cap to avoid token overflow
    def _dump(data: list, max_chars: int = 3000) -> str:
        raw = yaml.dump(data, default_flow_style=False, allow_unicode=True)
        return raw[:max_chars] + ("\n... (truncated)" if len(raw) > max_chars else "")

    prompt = SQL_GENERATION_PROMPT.format(
        database=SnowflakeConfig.DATABASE or "PLAYGROUND_LM",
        schema=SnowflakeConfig.SCHEMA or "CORTEX_ANALYTICS_ORCHESTRATOR",
        sql_instructions=sql_instructions,
        logical_tables=_dump(semantic_model["logical_tables"]),
        derived_metrics=_dump(semantic_model["derived_metrics"], 1000),
        relationships=_dump(semantic_model["relationships"], 4000),
        verified_queries=_dump(semantic_model["verified_queries"], 20000),
        rag_context=rag_str,
        error_context=error_ctx,
        query=query,
    )

    sql = None
    try:
        response = await _get_client().chat.completions.create(
            model=LLMConfig.get_model("sql"),
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=1000,
        )

        sql = _extract_sql(response.choices[0].message.content)
        logger.info(f"Generated SQL:\n{sql}")

        results = _execute_sql(sql)

        return {"sql": sql, "results": results, "error": None}

    except SnowflakeExecutionError as e:
        return {"sql": sql, "results": None, "error": str(e)}
    except Exception as e:
        logger.error(f"SQL generation/execution failed: {e}")
        return {"sql": None, "results": None, "error": str(e)}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_sql(llm_output: str) -> str:
    """Extract clean SQL from LLM response (handles markdown code blocks)."""
    sql = llm_output.strip()
    if sql.startswith("```"):
        lines = sql.split("\n")
        sql = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    return sql.strip().rstrip(";")


class SnowflakeExecutionError(Exception):
    pass


def _execute_sql(sql: str) -> list:
    """Execute SQL against Snowflake and return results as list of dicts."""
    try:
        conn = snowflake.connector.connect(**SnowflakeConfig.connection_params())
        cursor = conn.cursor()
        # Ensure correct schema context so views resolve without full prefix
        cursor.execute(f"USE SCHEMA {SnowflakeConfig.DATABASE}.{SnowflakeConfig.SCHEMA}")
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    except snowflake.connector.errors.ProgrammingError as e:
        raise SnowflakeExecutionError(f"Snowflake SQL error: {e.msg}")
    except Exception as e:
        raise SnowflakeExecutionError(f"Snowflake connection error: {str(e)}")


def _default_sql_instructions() -> str:
    return """You are a SQL expert for Volvo Cars Corporation email marketing analytics.
Generate Snowflake SQL based on the semantic model above.
Rules:
1. Only use tables and columns defined in the semantic model.
2. Always use full schema prefix.
3. Rates are decimals (0.025 = 2.5%) — display as percentages.
4. Use ROUND() for rates, 2 decimal places.
5. For "last month": DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
6. Always ORDER BY for ranked/comparison queries.
7. LIMIT 20 unless specified.
8. Return ONLY SQL — no explanation."""
