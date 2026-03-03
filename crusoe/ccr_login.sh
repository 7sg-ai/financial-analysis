#!/bin/bash
set -euo pipefail

CCR_URL="registry.us-east1-a.ccr.crusoecloudcompute.com"
PROJECT_ID="28e7f623-e5a4-436e-90da-dbda189ab509"
EMAIL="nd@7sg.ai"

API_REPO="${CCR_URL}/financial-analysis-api.28e7f623"
STREAMLIT_REPO="${CCR_URL}/financial-analysis-streamlit.28e7f623"

echo "==> Generating CCR token..."
crusoe registry tokens create --expires-at 2026-02-28T23:59:59Z --project-id "$PROJECT_ID" --format json > /tmp/ccr_token.json
TOKEN=$(python3 -c "import json; print(json.load(open('/tmp/ccr_token.json'))['token'])")
rm -f /tmp/ccr_token.json

echo "==> Logging into CCR (API repo)..."
printf '%s' "$TOKEN" | docker login "$API_REPO" -u "$EMAIL" --password-stdin

echo "==> Logging into CCR (Streamlit repo)..."
printf '%s' "$TOKEN" | docker login "$STREAMLIT_REPO" -u "$EMAIL" --password-stdin

echo "==> Tagging images..."
docker tag financial-analysis-api:latest "${API_REPO}/financial-analysis-api:latest"
docker tag financial-analysis-streamlit:latest "${STREAMLIT_REPO}/financial-analysis-streamlit:latest"

echo "==> Pushing API image..."
docker push "${API_REPO}/financial-analysis-api:latest"

echo "==> Pushing Streamlit image..."
docker push "${STREAMLIT_REPO}/financial-analysis-streamlit:latest"

echo "==> Done! Both images pushed to CCR."
