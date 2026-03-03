#!/bin/bash
set -euo pipefail

PROJECT_ID="28e7f623-e5a4-436e-90da-dbda189ab509"
VPC_ID="6b0a1b42-2f5d-4d70-95d7-eaef53a5406f"

echo "==> Creating firewall rule for ports 8000,8501,9001..."
crusoe networking vpc-firewall-rules create \
  --name allow-financial-analysis-app \
  --direction INGRESS \
  --action ALLOW \
  --protocols TCP \
  --sources "0.0.0.0/0" \
  --destinations "172.27.16.0/20" \
  --destination-ports "8000,8501,9001" \
  --vpc-network-id "$VPC_ID" \
  --project-id "$PROJECT_ID"

echo "==> Firewall rule created!"
