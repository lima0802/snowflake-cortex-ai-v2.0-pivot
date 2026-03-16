"""
DIA v2 - Centralized Configuration
===================================
Single source of truth for all settings. Swap LLM providers by changing
env vars — zero code changes required.

v2.0-β model strategy (demo):
  • OpenAI GPT-4o       — Text-to-SQL + Response Synthesis
  • OpenAI GPT-4o-mini  — Intent Classification (fast, cheap)
  • text-embedding-3-small — RAG Embeddings (same API key)
  Fallback: Google Gemini 1.5 Pro/Flash (secondary), Together AI open-source (tertiary)
"""

import os
from dotenv import load_dotenv

load_dotenv()



class LLMConfig:
    """LLM provider configuration. Supports hot-swapping via env vars."""

    PROVIDER = os.getenv("LLM_PROVIDER", "openai")

    # Model routing — different models for different tasks
    @staticmethod
    def get_model(task: str) -> str:
        """Get the right model for each task. Provider-agnostic via LiteLLM."""
        provider = LLMConfig.PROVIDER

        models = {
            # v2.0-β primary: single OpenAI key, best Text-to-SQL accuracy (BIRD-SQL 81.95%)
            "openai": {
                "sql":       os.getenv("OPENAI_MODEL_SQL",       "gpt-4o"),
                "fast":      os.getenv("OPENAI_MODEL_FAST",      "gpt-4o-mini"),
                "synthesis": os.getenv("OPENAI_MODEL_SYNTHESIS", "gpt-4o"),
            },
            # Fallback A: Google Gemini (secondary provider)
            "google": {
                "sql":       os.getenv("GOOGLE_MODEL_SQL",       "gemini/gemini-2.5-pro"),
                "fast":      os.getenv("GOOGLE_MODEL_FAST",      "gemini/gemini-2.5-flash"),
                "synthesis": os.getenv("GOOGLE_MODEL_SYNTHESIS", "gemini/gemini-2.5-pro"),
            },
            # Fallback B: Together AI open-source (tertiary / post-budget self-hosted path)
            "together": {
                "sql":       os.getenv("TOGETHER_MODEL_SQL",       "together_ai/deepseek-ai/DeepSeek-V3"),
                "fast":      os.getenv("TOGETHER_MODEL_FAST",      "together_ai/meta-llama/Llama-3.1-8B-Instruct-Turbo"),
                "synthesis": os.getenv("TOGETHER_MODEL_SYNTHESIS", "together_ai/mistralai/Mistral-Large-Instruct-2407"),
            },
            # Fallback C: Groq (ultra-low latency for intent classification)
            "groq": {
                "sql":       os.getenv("GROQ_MODEL_SQL",       "groq/llama-3.1-70b-versatile"),
                "fast":      os.getenv("GROQ_MODEL_FAST",      "groq/llama-3.1-8b-instant"),
                "synthesis": os.getenv("GROQ_MODEL_SYNTHESIS", "groq/llama-3.1-70b-versatile"),
            },
        }

        provider_models = models.get(provider, models["openai"])
        return provider_models.get(task, provider_models["sql"])


class EmbeddingConfig:
    """Embedding model configuration.
    v2.0-β: text-embedding-3-small (OpenAI) — same API key as LLM, $0.02/1M tokens.
    Fallback: local sentence-transformers BGE-M3 (no API key needed).
    """
    PROVIDER = os.getenv("EMBEDDING_PROVIDER", "openai")
    MODEL    = os.getenv("EMBEDDING_MODEL",    "text-embedding-3-small")

    # Local fallback (used when EMBEDDING_PROVIDER=local)
    LOCAL_MODEL = os.getenv("EMBEDDING_LOCAL_MODEL", "BAAI/bge-m3")


class SnowflakeConfig:
    """Snowflake connection settings. All values read at call time (not import time)."""
    WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "DIA_WH")
    DATABASE  = os.getenv("SNOWFLAKE_DATABASE",  "PLAYGROUND_LM")
    SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA",    "CORTEX_ANALYTICS_ORCHESTRATOR")
    ROLE      = os.getenv("SNOWFLAKE_ROLE",      "SYSADMIN")

    @staticmethod
    def _get(key: str, default: str = None) -> str:
        """Read from env first, then st.secrets (Streamlit Cloud), then default."""
        val = os.getenv(key)
        if not val:
            try:
                import streamlit as st
                val = st.secrets.get(key, default)
            except Exception:
                val = default
        return val

    @staticmethod
    def connection_params() -> dict:
        return {
            "account":   SnowflakeConfig._get("SNOWFLAKE_ACCOUNT"),
            "user":      SnowflakeConfig._get("SNOWFLAKE_USER"),
            "password":  SnowflakeConfig._get("SNOWFLAKE_PASSWORD"),
            "warehouse": SnowflakeConfig._get("SNOWFLAKE_WAREHOUSE", "DIA_WH"),
            "database":  SnowflakeConfig._get("SNOWFLAKE_DATABASE",  "PLAYGROUND_LM"),
            "schema":    SnowflakeConfig._get("SNOWFLAKE_SCHEMA",    "CORTEX_ANALYTICS_ORCHESTRATOR"),
            "role":      SnowflakeConfig._get("SNOWFLAKE_ROLE",      "SYSADMIN"),
        }


class AppConfig:
    HOST  = os.getenv("APP_HOST", "0.0.0.0")
    PORT  = int(os.getenv("APP_PORT", 8000))
    DEBUG = os.getenv("DEBUG", "false").lower() == "true"
    DEMO_FALLBACK = os.getenv("DEMO_FALLBACK_ENABLED", "true").lower() == "true"

    # --- Semantic model directories (glob-merged at runtime) ---
    SEMANTIC_VIEWS_ROOT     = os.getenv("SEMANTIC_VIEWS_ROOT",     "data/semantic_views/dm_performance_model")
    LOGICAL_TABLES_DIR      = os.getenv("LOGICAL_TABLES_DIR",      "data/semantic_views/dm_performance_model/logical_tables")
    DERIVED_METRICS_DIR     = os.getenv("DERIVED_METRICS_DIR",     "data/semantic_views/dm_performance_model/derived_metrics")
    RELATIONSHIPS_DIR       = os.getenv("RELATIONSHIPS_DIR",       "data/semantic_views/dm_performance_model/relationships")
    VERIFIED_QUERIES_DIR    = os.getenv("VERIFIED_QUERIES_DIR",    "data/semantic_views/dm_performance_model/verified_queries")
    CUSTOM_INSTRUCTIONS_PATH = os.getenv("CUSTOM_INSTRUCTIONS_PATH", "data/semantic_views/dm_performance_model/custom_instructions/custom_instructions.yaml")

    # --- Prompt files (loaded dynamically — edit without code changes) ---
    ORCHESTRATION_PROMPT_PATH = os.getenv("ORCHESTRATION_PROMPT_PATH", "data/prompts/orchestration_instruction.txt")
    RESPONSE_PROMPT_PATH      = os.getenv("RESPONSE_PROMPT_PATH",      "data/prompts/response_instruction.txt")

    # --- Other data files ---
    GOLDEN_QUERIES_PATH = os.getenv("GOLDEN_QUERIES_PATH", "data/golden_queries.json")
    # RAG entity index is built live from Snowflake (AGENT_V_DIM_SFMC_METADATA_JOB)

    # Performance
    SQL_MAX_RETRIES        = 2
    QUERY_TIMEOUT_SECONDS  = 30
