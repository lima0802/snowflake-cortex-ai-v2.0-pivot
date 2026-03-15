"""
DIA v2 — LLM Provider Connectivity Tests
==========================================
Tests OpenAI (GPT) and Google (Gemini) API keys and model calls
using the native SDKs directly (no litellm dependency needed).

Run:
    python test/test_llm_providers.py
"""

import os
import sys
import asyncio
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

PASS = "[PASS]"
FAIL = "[FAIL]"
SKIP = "[SKIP]"
PROMPT = "Reply with exactly one word: OK"

results_log = []


def header(title):
    print(f"\n{'=' * 55}")
    print(f"  {title}")
    print("=" * 55)


def log(label, status, detail=""):
    marker = PASS if status == "pass" else (SKIP if status == "skip" else FAIL)
    suffix = f"  ({detail})" if detail else ""
    print(f"  {marker}  {label}{suffix}")
    results_log.append(status != "fail")
    return status != "fail"


# ── 1. Keys ───────────────────────────────────────────────────────────────────

def test_env_keys():
    header("1 / API Keys")
    for key, label in [
        ("OPENAI_API_KEY", "OPENAI_API_KEY"),
        ("GOOGLE_API_KEY", "GOOGLE_API_KEY"),
    ]:
        val = os.getenv(key, "")
        if val and not val.startswith("#"):
            log(label, "pass", f"{val[:8]}...")
        else:
            log(label, "fail", "not set")


# ── 2. OpenAI ─────────────────────────────────────────────────────────────────

async def test_openai():
    header("2 / OpenAI (GPT)")
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        log("OpenAI", "skip", "OPENAI_API_KEY not set")
        return

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=api_key)
    except ImportError as e:
        log("import openai", "fail", str(e))
        return

    models_to_test = {
        "SQL model":   os.getenv("OPENAI_MODEL_SQL",       "gpt-4o"),
        "Fast model":  os.getenv("OPENAI_MODEL_FAST",      "gpt-4o-mini"),
    }
    # Only add synthesis if it differs from SQL
    synth = os.getenv("OPENAI_MODEL_SYNTHESIS", "gpt-4o")
    if synth not in models_to_test.values():
        models_to_test["Synthesis model"] = synth

    seen = set()
    for label, model in models_to_test.items():
        if model in seen:
            log(f"{label} ({model})", "skip", "already tested above")
            continue
        seen.add(model)
        try:
            resp = await client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": PROMPT}],
                temperature=0.0,
                max_tokens=10,
            )
            reply = resp.choices[0].message.content.strip()
            tokens = resp.usage.total_tokens
            log(f"{label} ({model})", "pass", f'reply="{reply}"  tokens={tokens}')
        except Exception as e:
            log(f"{label} ({model})", "fail", str(e)[:120])


# ── 3. Gemini ─────────────────────────────────────────────────────────────────

async def test_gemini():
    header("3 / Google (Gemini)")
    api_key = os.getenv("GOOGLE_API_KEY", "")
    if not api_key:
        log("Gemini", "skip", "GOOGLE_API_KEY not set")
        return

    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
    except ImportError as e:
        log("import google.generativeai", "fail", str(e))
        return

    # Strip "gemini/" prefix if present (litellm prefix, not needed here)
    def _model_name(m):
        return m.replace("gemini/", "")

    models_to_test = {
        "SQL model":  _model_name(os.getenv("GOOGLE_MODEL_SQL",  "gemini/gemini-2.5-pro")),
        "Fast model": _model_name(os.getenv("GOOGLE_MODEL_FAST", "gemini/gemini-2.5-flash")),
    }

    seen = set()
    for label, model in models_to_test.items():
        if model in seen:
            log(f"{label} ({model})", "skip", "already tested above")
            continue
        seen.add(model)
        try:
            client = genai.GenerativeModel(model)
            # Run sync call in executor to keep async flow
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None, lambda m=client: m.generate_content(PROMPT)
            )
            reply = resp.text.strip()
            log(f"{label} ({model})", "pass", f'reply="{reply[:30]}"')
        except Exception as e:
            log(f"{label} ({model})", "fail", str(e)[:120])


# ── 4. Config routing ─────────────────────────────────────────────────────────

def test_config_routing():
    header("4 / Config model routing (LLMConfig)")
    try:
        from config import LLMConfig
        for task in ("sql", "fast", "synthesis"):
            model = LLMConfig.get_model(task)
            log(f"LLMConfig.get_model('{task}')", "pass", model)
    except Exception as e:
        log("LLMConfig", "fail", str(e))


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    print("\n" + "=" * 55)
    print("  DIA v2 - LLM Provider Tests")
    print("=" * 55)

    test_env_keys()
    await test_openai()
    await test_gemini()
    test_config_routing()

    passed = sum(results_log)
    total  = len(results_log)
    print(f"\n{'=' * 55}")
    print(f"  Result: {passed}/{total} checks passed")
    print("=" * 55 + "\n")

    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    asyncio.run(main())
