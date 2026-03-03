#!/bin/bash
set -euo pipefail

PROJECT_ID="28e7f623-e5a4-436e-90da-dbda189ab509"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Creating VM: financial-analysis-vm ..."
crusoe compute vms create \
  --name financial-analysis-vm \
  --type c1a.4x \
  --location us-east1-a \
  --image ubuntu22.04 \
  --keyfile ~/.ssh/id_ed25519.pub \
  --startup-script "$SCRIPT_DIR/vm-startup.sh" \
  --project-id "$PROJECT_ID" \
  --format json
