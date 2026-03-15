"""
DIA v2 - Weekend Sprint Project Structure
==========================================

dia-v2/
├── .env.example              # Environment configuration template
├── requirements.txt          # Python dependencies
├── config.py                 # Centralized configuration
├── main.py                   # FastAPI application entry point
├── agent/
│   ├── __init__.py
│   ├── graph.py              # LangGraph agent definition
│   ├── intent.py             # Intent classification
│   ├── text_to_sql.py        # SQL generation + execution
│   ├── rag.py                # FAISS-based entity search
│   ├── ml_features.py        # Anomaly detection + forecasting
│   └── synthesizer.py        # Response synthesis + insight generation
├── data/
│   ├── semantic_model.yaml   # Reused from DIA v1
│   ├── system_prompt.txt     # Reused from DIA v1
│   ├── golden_queries.json   # Demo + test queries with expected results
│   └── lta_metadata.json     # LTA/campaign metadata for RAG index
├── ui/
│   ├── streamlit_app.py      # Streamlit frontend
│   ├── .streamlit/
│   │   └── config.toml       # Streamlit theme config
│   └── assets/
│       └── volvo_style.css   # Custom Volvo CSS
├── teams/
│   └── bot.py                # Teams bot (Bot Framework)
├── deploy/
│   ├── Dockerfile            # Container build
│   ├── docker-compose.yml    # Full stack deployment
│   └── setup.sh              # Quick start script
└── tests/
    └── test_golden_queries.py
"""
