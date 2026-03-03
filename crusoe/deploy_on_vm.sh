#!/bin/bash
set -euo pipefail

VM_IP="160.211.46.250"
SSH="ssh -o StrictHostKeyChecking=no ubuntu@${VM_IP}"
SCP="scp -o StrictHostKeyChecking=no"
PROJECT_ID="28e7f623-e5a4-436e-90da-dbda189ab509"
CCR_URL="registry.us-east1-a.ccr.crusoecloudcompute.com"
EMAIL="nd@7sg.ai"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

echo "==> Generating fresh CCR token..."
crusoe registry tokens create --expires-at 2026-03-06T23:59:59Z --project-id "$PROJECT_ID" --format json > /tmp/ccr_token.json
TOKEN=$(python3 -c "import json; print(json.load(open('/tmp/ccr_token.json'))['token'])")
rm -f /tmp/ccr_token.json

echo "==> Copying docker-compose file to VM..."
$SCP "$SCRIPT_DIR/docker-compose.crusoe.yml" ubuntu@${VM_IP}:~/docker-compose.yml

echo "==> Logging into CCR on VM..."
$SSH "echo '${TOKEN}' | sudo docker login ${CCR_URL} -u ${EMAIL} --password-stdin"

echo "==> Starting services on VM..."
$SSH "sudo docker compose -f ~/docker-compose.yml pull && sudo docker compose -f ~/docker-compose.yml up -d"

echo "==> Waiting for services to start (30s)..."
sleep 30

echo "==> Checking container status..."
$SSH "sudo docker compose -f ~/docker-compose.yml ps"

echo "==> Deploy complete!"
echo "    API:       http://${VM_IP}:8000"
echo "    Streamlit: http://${VM_IP}:8501"
echo "    MinIO:     http://${VM_IP}:9001"
