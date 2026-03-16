# DIA v2 — Direct Marketing Analytics Agent

**Provider-agnostic conversational analytics for Volvo Cars Corporation**

> **Deployment Note**
> The current demo is deployed via **Streamlit Community Cloud** — a temporary setup for quick demo access and stakeholder review.
> Once the demo is approved, the production deployment will migrate to a **Cloud VM (Docker)** for full control, security, and scalability.
> This is a quick-win deployment, not the final architecture.

## Quick Start

```bash
# 1. Clone and configure
cp .env.example .env
# Edit .env with your credentials (Snowflake + LLM API key)

# 2. Install dependencies
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Start the API backend
uvicorn main:app --reload --port 8000 &

# 4. Start the Streamlit UI
DIA_API_URL=http://localhost:8000 streamlit run ui/streamlit_app.py

# 5. Open http://localhost:8501 in your browser
```

## Streamlit Cloud Deployment (Single URL, No Server)

1. Push this repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io) → New app
3. Set **Main file path**: `ui/streamlit_app.py`
4. Under **Advanced settings → Secrets**, add:

```toml
OPENAI_API_KEY      = "sk-..."
SNOWFLAKE_ACCOUNT   = "your-account.region"
SNOWFLAKE_USER      = "your_user"
SNOWFLAKE_PASSWORD  = "your_password"
SNOWFLAKE_WAREHOUSE = "DIA_WH"
SNOWFLAKE_DATABASE  = "DEV_MARCOM_DB"
SNOWFLAKE_SCHEMA    = "CORTEX_ANALYTICS_ORCHESTRATOR"
SNOWFLAKE_ROLE      = "DIA_ANALYST_ROLE"
DIA_MODE            = "direct"
```

5. Deploy → get a public URL instantly

## Docker / Cloud VM Deployment

```bash
docker-compose up --build -d
# API: http://your-vm:8002
# UI:  http://your-vm:8502
```

## Architecture

```
User (Streamlit)
  ↓
LangGraph Agent (Orchestration)
  ↓ classify intent → route to tools
  ├── Text-to-SQL → Snowflake (via Python connector)
  ├── RAG Search  → FAISS (entity resolution)
  ├── Anomaly Detection → scipy/IQR
  ├── Forecasting → Prophet
  └── Response Synthesis → LLM
  ↓
Natural language answer + charts + SQL
```

## Deployment Modes

| Mode | How | When |
|------|-----|------|
| `DIA_MODE=direct` | Agent runs inside Streamlit (no separate server) | Streamlit Cloud |
| `DIA_MODE=api` | Streamlit calls FastAPI backend | Docker / VM |

## LLM Provider Swap

Change `LLM_PROVIDER` in `.env` — zero code changes:

| Provider | Set in .env |
|----------|-------------|
| OpenAI (default) | `LLM_PROVIDER=openai` + `OPENAI_API_KEY=...` |
| Google | `LLM_PROVIDER=google` + `GOOGLE_API_KEY=...` |
| Together AI | `LLM_PROVIDER=together` + `TOGETHER_API_KEY=...` |
| Groq | `LLM_PROVIDER=groq` + `GROQ_API_KEY=...` |

## File Structure

```
├── main.py                    # FastAPI entry point
├── config.py                  # All configuration
├── agent/
│   ├── graph.py               # LangGraph agent (the brain)
│   ├── intent.py              # Intent classification
│   ├── text_to_sql.py         # SQL generation + execution
│   ├── rag.py                 # FAISS entity search
│   ├── ml_features.py         # Anomaly + forecasting
│   ├── synthesizer.py         # Response generation
│   ├── charts.py              # Deterministic chart engine
│   └── feedback.py            # Feedback writer (Snowflake)
├── ui/
│   ├── streamlit_app.py       # Streamlit frontend
│   └── requirements.txt       # Streamlit Cloud dependencies
├── data/
│   ├── prompts/               # LLM instruction files (hot-reload)
│   ├── semantic_views/        # YAML semantic model + verified queries
│   └── golden_queries.json    # Demo fallback cache
├── deploy/
│   ├── Dockerfile
│   ├── setup.sh               # Cloud VM setup script
│   └── create_feedback_table.sql
├── docker-compose.yml
└── requirements.txt
```
