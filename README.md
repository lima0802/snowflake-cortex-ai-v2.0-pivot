# DIA v2 — Data Intelligence Agent (Weekend Sprint)

**Provider-agnostic conversational analytics for Volvo Cars Corporation**

## Quick Start (15 minutes)

```bash
# 1. Clone and configure
cd dia-v2
cp .env.example .env
# Edit .env with your credentials (Snowflake + LLM API key)

# 2. Install dependencies
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Start the API backend
uvicorn main:app --reload --port 8000 &

# 4. Start the Streamlit UI
DIA_API_URL=http://localhost:8000 streamlit run ui/streamlit_app.py &

# 5. Open http://localhost:8501 in your browser
```

## Expose to Client (choose one)

```bash
# Option A: Cloudflare Tunnel (stable, free)
cloudflared tunnel --url http://localhost:8501

# Option B: ngrok (quick)
ngrok http 8501

# Option C: Docker on cloud VM
docker-compose up --build -d
```

## Architecture

```
User (Streamlit / Teams)
  ↓
FastAPI + LangGraph (Orchestration)
  ↓ classify intent → route to tools
  ├── Text-to-SQL → Snowflake (via Python connector)
  ├── RAG Search → FAISS (entity resolution)
  ├── Anomaly Detection → scipy/IQR
  ├── Forecasting → Prophet
  └── Response Synthesis → LLM
  ↓
Natural language answer + charts + SQL
```

## LLM Provider Swap

Change `LLM_PROVIDER` in `.env` — zero code changes:

| Provider | Best For | Set in .env |
|----------|----------|-------------|
| OpenAI | Highest accuracy | `LLM_PROVIDER=openai` + `OPENAI_API_KEY=...` |
| Google | Good alternative | `LLM_PROVIDER=google` + `GOOGLE_API_KEY=...` |
| Together AI | Open-source models | `LLM_PROVIDER=together` + `TOGETHER_API_KEY=...` |
| Groq | Fastest inference | `LLM_PROVIDER=groq` + `GROQ_API_KEY=...` |

## Demo Fallback

If `DEMO_FALLBACK_ENABLED=true` in `.env`, the system falls back to
pre-cached responses from `data/golden_queries.json` when live queries fail.
This ensures the Tuesday demo never crashes.

## File Structure

```
dia-v2/
├── main.py                    # FastAPI entry point
├── config.py                  # All configuration
├── agent/
│   ├── graph.py               # LangGraph agent (the brain)
│   ├── intent.py              # Intent classification
│   ├── text_to_sql.py         # SQL generation + execution
│   ├── rag.py                 # FAISS entity search
│   ├── ml_features.py         # Anomaly + forecasting
│   └── synthesizer.py         # Response generation
├── ui/streamlit_app.py        # Streamlit frontend
├── teams/bot.py               # Teams bot
├── data/golden_queries.json   # Demo fallback cache
├── docker-compose.yml         # Full stack deployment
└── deploy/setup.sh            # Cloud VM setup script
```
