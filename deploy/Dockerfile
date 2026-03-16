FROM python:3.11-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential && \
    rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY . .

# Ensure Streamlit config is discoverable from the working directory
RUN mkdir -p /app/.streamlit && \
    cp /app/ui/.streamlit/config.toml /app/.streamlit/config.toml

EXPOSE 8000 8501

# Default: run API (override in docker-compose for UI)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
