"""
DIA v2 - Intent Classification
================================
Loads orchestration instructions dynamically from:
  data/prompts/orchestration_instruction.txt

Edit that file to change intent categories and routing rules.
No code changes needed — restart API to reload.
"""

import logging
import json
import os
import re
from openai import AsyncOpenAI
from config import LLMConfig, AppConfig

_openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

logger = logging.getLogger("dia-v2.intent")

# Module-level cache
_orchestration_prompt: str | None = None


def _load_orchestration_prompt() -> str:
    """Load intent classification instructions from file. Cached after first load."""
    global _orchestration_prompt
    if _orchestration_prompt is None:
        try:
            with open(AppConfig.ORCHESTRATION_PROMPT_PATH, encoding="utf-8") as f:
                raw = f.read()
            # Strip comment lines (lines starting with #)
            lines = [l for l in raw.splitlines() if not l.strip().startswith("#")]
            _orchestration_prompt = "\n".join(lines).strip()
            logger.info(f"Loaded orchestration prompt from {AppConfig.ORCHESTRATION_PROMPT_PATH}")
        except FileNotFoundError:
            logger.warning(f"orchestration_instruction.txt not found — using default")
            _orchestration_prompt = _default_orchestration_prompt()
    return _orchestration_prompt


def reload_prompt():
    """Force reload from disk (call after editing the file)."""
    global _orchestration_prompt
    _orchestration_prompt = None
    return _load_orchestration_prompt()


def _needs_date_clarification(query: str) -> bool:
    """
    Rule-based pre-check: does the query mention a quarter or month WITHOUT a year?
    Returns True only if no year can be found anywhere in the query.
    """
    q = query.lower()

    # "last X" / "this X" / "current X" phrases are unambiguous — never clarify
    if re.search(r'\b(last|this|current|past|previous)\s+(month|quarter|year|week)\b', q):
        return False

    # Any 4-digit year present (2020-2029) — catches "2026", "Q1'2026", "Q1 2026", "Jan 2026"
    if re.search(r'20\d{2}', query):
        return False
    # 2-digit year abbreviations: Jan'26, Q1'26, '26
    if re.search(r"['\s\-]\d{2}\b", query):
        return False

    # Quarter without year: "Q1", "Q2", "Q3", "Q4"
    if re.search(r'\bq[1-4]\b', q):
        return True
    if re.search(r'\b(first|second|third|fourth)\s+quarter\b', q):
        return True

    # Full or abbreviated month name without any year
    months = (r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|'
              r'jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|'
              r'oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b')
    if re.search(months, q):
        return True

    return False


async def classify_intent(query: str) -> dict:
    """Classify query intent using a fast, cheap model."""

    # Hard rule: quarter/month without year must be clarified first
    if _needs_date_clarification(query):
        logger.info(f"Pre-classify: clarification_needed (ambiguous date) for: {query[:60]}")
        return {"intent": "clarification_needed", "confidence": 0.99}

    system_prompt = _load_orchestration_prompt()
    try:
        response = await _openai_client.chat.completions.create(
            model=LLMConfig.get_model("fast"),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": query},
            ],
            temperature=0.0,
            max_tokens=100,
            response_format={"type": "json_object"},
        )
        result = json.loads(response.choices[0].message.content)
        return {
            "intent":     result.get("intent",     "descriptive"),
            "confidence": result.get("confidence", 0.5),
        }
    except Exception as e:
        logger.error(f"Intent classification failed: {e}")
        return _keyword_fallback(query)


def _keyword_fallback(query: str) -> dict:
    """Rule-based fallback if LLM classification fails."""
    q = query.lower()
    if any(w in q for w in ["why", "explain", "root cause", "reason"]):
        return {"intent": "diagnostic",   "confidence": 0.6}
    elif any(w in q for w in ["forecast", "predict", "expect", "next quarter", "next month"]):
        return {"intent": "predictive",   "confidence": 0.7}
    elif any(w in q for w in ["unusual", "anomaly", "anomalies", "alert", "spike", "drop", "strange"]):
        return {"intent": "anomaly",      "confidence": 0.7}
    else:
        return {"intent": "descriptive",  "confidence": 0.5}


def _default_orchestration_prompt() -> str:
    return """You are an intent classifier for a marketing analytics assistant.
Classify the user query into exactly ONE of these categories:

- "descriptive": Questions about current or historical data (what, show, list, top, how many, compare)
- "diagnostic": Questions about why something happened (why, explain, root cause, reason)
- "predictive": Questions about future expectations (forecast, predict, expect, next quarter)
- "anomaly": Questions about unusual patterns (unusual, anomaly, alert, spike, drop, strange)
- "entity_lookup": Fuzzy references to specific campaigns or LTAs that need resolution

Respond with ONLY a JSON object: {"intent": "...", "confidence": 0.0-1.0}"""
