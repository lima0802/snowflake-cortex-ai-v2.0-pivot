#!/bin/bash
# ==============================================================================
# DIA v2 - Quick Start Deployment Script
# ==============================================================================
# This script sets up DIA v2 on a fresh cloud VM (Ubuntu 22.04+)
# and exposes it via a public HTTPS URL for the demo.
#
# Usage:
#   chmod +x deploy/setup.sh
#   ./deploy/setup.sh
#
# Prerequisites:
#   - Ubuntu 22.04+ VM (Azure/AWS/GCP) with 4GB+ RAM
#   - .env file configured with API keys and Snowflake credentials
# ==============================================================================

set -e

echo "============================================"
echo "  DIA v2 - Weekend Sprint Deployment"
echo "============================================"

# --- 1. System Setup ---
echo ""
echo "[1/6] Installing system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y -qq python3.11 python3.11-venv python3-pip \
    docker.io docker-compose curl wget unzip

# --- 2. Python Environment ---
echo ""
echo "[2/6] Setting up Python environment..."
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q

echo "  ✓ Python environment ready"

# --- 3. Verify Configuration ---
echo ""
echo "[3/6] Checking configuration..."

if [ ! -f .env ]; then
    echo "  ✗ ERROR: .env file not found!"
    echo "  Copy .env.example to .env and fill in your credentials:"
    echo "    cp .env.example .env"
    echo "    nano .env"
    exit 1
fi

# Quick validation
source .env
if [ -z "$SNOWFLAKE_ACCOUNT" ]; then
    echo "  ✗ WARNING: SNOWFLAKE_ACCOUNT not set in .env"
fi
if [ -z "$OPENAI_API_KEY" ] && [ -z "$GOOGLE_API_KEY" ] && [ -z "$TOGETHER_API_KEY" ]; then
    echo "  ✗ WARNING: No LLM API key found in .env"
fi
echo "  ✓ Configuration loaded (provider: ${LLM_PROVIDER:-openai})"

# --- 4. Test Snowflake Connection ---
echo ""
echo "[4/6] Testing Snowflake connection..."
python3 -c "
import snowflake.connector
from dotenv import load_dotenv
import os
load_dotenv()
try:
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
    )
    conn.cursor().execute('SELECT 1')
    conn.close()
    print('  ✓ Snowflake connection successful')
except Exception as e:
    print(f'  ✗ Snowflake connection failed: {e}')
    print('  → Check credentials in .env file')
"

# --- 5. Start Services ---
echo ""
echo "[5/6] Starting DIA v2 services..."

# Option A: Docker Compose (recommended)
if command -v docker-compose &> /dev/null; then
    echo "  Using Docker Compose..."
    docker-compose up -d --build
    echo "  ✓ API running on port 8000"
    echo "  ✓ Streamlit UI running on port 8501"
else
    # Option B: Direct Python (fallback)
    echo "  Using direct Python (no Docker)..."

    # Start API in background
    nohup uvicorn main:app --host 0.0.0.0 --port 8000 > /tmp/dia-api.log 2>&1 &
    echo "  ✓ API started (PID: $!, log: /tmp/dia-api.log)"

    # Start Streamlit in background
    export DIA_API_URL=http://localhost:8000
    nohup streamlit run ui/streamlit_app.py \
        --server.port=8501 \
        --server.address=0.0.0.0 \
        --server.headless=true \
        > /tmp/dia-ui.log 2>&1 &
    echo "  ✓ Streamlit started (PID: $!, log: /tmp/dia-ui.log)"
fi

# --- 6. Expose via Public URL ---
echo ""
echo "[6/6] Creating public URL..."
echo ""
echo "Choose a tunneling option to expose DIA to the client:"
echo ""
echo "  OPTION A - Cloudflare Tunnel (recommended, stable):"
echo "    curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o cloudflared"
echo "    chmod +x cloudflared"
echo "    ./cloudflared tunnel --url http://localhost:8501"
echo ""
echo "  OPTION B - ngrok (quick, free tier):"
echo "    curl -s https://ngrok-agent.s3.amazonaws.com/ngrok-v3-stable-linux-amd64.tgz | tar xz"
echo "    ./ngrok http 8501"
echo ""
echo "  OPTION C - Direct IP (if VM has public IP):"
echo "    Open port 8501 in your cloud security group/firewall"
echo "    Access at: http://$(curl -s ifconfig.me):8501"
echo ""
echo "============================================"
echo "  DIA v2 is ready!"
echo "============================================"
echo ""
echo "  Local access:"
echo "    → Streamlit UI: http://localhost:8501"
echo "    → API docs:     http://localhost:8000/docs"
echo "    → Health check: http://localhost:8000/health"
echo ""
echo "  Send the public URL to your client POC."
echo "  They click the link → start asking questions → done."
echo ""
echo "  Logs:"
echo "    → API:  docker-compose logs -f api  (or /tmp/dia-api.log)"
echo "    → UI:   docker-compose logs -f ui   (or /tmp/dia-ui.log)"
echo ""
