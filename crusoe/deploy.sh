#!/bin/bash
set -euo pipefail

###############################################################################
# Crusoe Cloud deployment script for Financial Analysis application
# Prerequisites: crusoe CLI configured, kubectl, docker, mc (MinIO Client)
###############################################################################

# --- Configuration (edit these) ----------------------------------------------
PROJECT="${CRUSOE_DEFAULT_PROJECT:-}"
LOCATION="${CRUSOE_LOCATION:-us-east1}"
CLUSTER_NAME="${CLUSTER_NAME:-financial-analysis}"
CCR_LOCATION="${CCR_LOCATION:-us-east1}"

API_IMAGE_NAME="financial-analysis-api"
STREAMLIT_IMAGE_NAME="financial-analysis-streamlit"
IMAGE_TAG="${IMAGE_TAG:-latest}"

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-changeme-secure-password}"
LLM_API_KEY="${LLM_API_KEY:-}"
MINIO_BUCKET="financial-data"
SRC_DATA_DIR="../src_data"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
# -----------------------------------------------------------------------------

log() { echo "==> $*"; }
err() { echo "ERROR: $*" >&2; exit 1; }

if ! command -v crusoe &>/dev/null; then err "crusoe CLI not found"; fi
if ! command -v kubectl &>/dev/null; then err "kubectl not found"; fi
if ! command -v docker &>/dev/null; then err "docker not found"; fi

# 1. Create CCR repositories
log "Creating container registry repositories..."
crusoe registry repos create \
  --name "$API_IMAGE_NAME" \
  --location "$CCR_LOCATION" \
  --mode standard 2>/dev/null || log "API repo already exists"

crusoe registry repos create \
  --name "$STREAMLIT_IMAGE_NAME" \
  --location "$CCR_LOCATION" \
  --mode standard 2>/dev/null || log "Streamlit repo already exists"

# Get the CCR base URL
CCR_URL="${CCR_LOCATION}.registry.crusoecloud.com"
API_FULL_IMAGE="${CCR_URL}/${API_IMAGE_NAME}:${IMAGE_TAG}"
STREAMLIT_FULL_IMAGE="${CCR_URL}/${STREAMLIT_IMAGE_NAME}:${IMAGE_TAG}"

# 2. Build and push Docker images
log "Building API image..."
docker build -t "$API_FULL_IMAGE" -f "$REPO_DIR/Dockerfile" "$REPO_DIR"

log "Building Streamlit image..."
docker build -t "$STREAMLIT_FULL_IMAGE" -f "$REPO_DIR/Dockerfile.streamlit" "$REPO_DIR"

log "Logging in to CCR..."
CCR_TOKEN=$(crusoe registry tokens create --expires-in 3600 --format json | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")
echo "$CCR_TOKEN" | docker login "$CCR_URL" --username token --password-stdin

log "Pushing images..."
docker push "$API_FULL_IMAGE"
docker push "$STREAMLIT_FULL_IMAGE"

# 3. Create CMK cluster (if it doesn't exist)
log "Checking for existing CMK cluster..."
if ! crusoe kubernetes clusters list --format json | python3 -c "
import sys, json
clusters = json.load(sys.stdin)
names = [c.get('name','') for c in (clusters if isinstance(clusters, list) else [])]
sys.exit(0 if '$CLUSTER_NAME' in names else 1)
" 2>/dev/null; then
  log "Creating CMK cluster: $CLUSTER_NAME..."
  crusoe kubernetes clusters create \
    --name "$CLUSTER_NAME" \
    --location "$LOCATION" \
    --version latest \
    --node-pool-name cpu-pool \
    --node-pool-instance-type c1a.2x \
    --node-pool-count 3

  log "Waiting for cluster to be ready..."
  sleep 60
else
  log "Cluster $CLUSTER_NAME already exists"
fi

# 4. Get kubeconfig
log "Fetching kubeconfig..."
crusoe kubernetes clusters get-credentials "$CLUSTER_NAME" --location "$LOCATION"

# 5. Create CCR pull secret for Kubernetes
log "Creating CCR pull secret..."
kubectl create namespace financial-analysis 2>/dev/null || true
kubectl -n financial-analysis create secret docker-registry ccr-credentials \
  --docker-server="$CCR_URL" \
  --docker-username=token \
  --docker-password="$CCR_TOKEN" \
  --docker-email=deploy@7sg.ai \
  --dry-run=client -o yaml | kubectl apply -f -

# 6. Update image references in manifests and apply
log "Applying Kubernetes manifests..."
cd "$SCRIPT_DIR"

kubectl apply -f namespace.yaml

# Update secrets with actual values
kubectl -n financial-analysis create secret generic financial-analysis-secrets \
  --from-literal=LLM_API_KEY="$LLM_API_KEY" \
  --from-literal=S3_ACCESS_KEY="$MINIO_ROOT_USER" \
  --from-literal=S3_SECRET_KEY="$MINIO_ROOT_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f configmap.yaml
kubectl apply -f minio-statefulset.yaml

# Wait for MinIO to be ready
log "Waiting for MinIO to be ready..."
kubectl -n financial-analysis rollout status statefulset/minio --timeout=120s

# Apply app deployments with correct image URLs
sed "s|REPLACE_WITH_CCR_IMAGE_URL:latest|${API_FULL_IMAGE}|g" api-deployment.yaml | kubectl apply -f -
sed "s|REPLACE_WITH_CCR_STREAMLIT_IMAGE_URL:latest|${STREAMLIT_FULL_IMAGE}|g" streamlit-deployment.yaml | kubectl apply -f -

kubectl apply -f api-service.yaml
kubectl apply -f streamlit-service.yaml

# 7. Upload data to MinIO
log "Uploading data to MinIO..."
kubectl -n financial-analysis port-forward svc/minio 9000:9000 &
PF_PID=$!
sleep 5

if command -v mc &>/dev/null; then
  mc alias set crusoe-minio http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
  mc mb "crusoe-minio/$MINIO_BUCKET" 2>/dev/null || true
  mc mb "crusoe-minio/$MINIO_BUCKET/nyc-taxi" 2>/dev/null || true
  mc cp "$REPO_DIR"/src_data/*.parquet "crusoe-minio/$MINIO_BUCKET/nyc-taxi/"
  mc cp "$REPO_DIR"/src_data/*.csv "crusoe-minio/$MINIO_BUCKET/nyc-taxi/"
  log "Data uploaded to MinIO"
else
  log "WARNING: mc (MinIO Client) not found. Install it and upload data manually:"
  log "  mc alias set crusoe-minio http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD"
  log "  mc mb crusoe-minio/$MINIO_BUCKET/nyc-taxi"
  log "  mc cp src_data/*.parquet crusoe-minio/$MINIO_BUCKET/nyc-taxi/"
fi

kill $PF_PID 2>/dev/null || true

# 8. Print status
log "Deployment complete! Waiting for services..."
kubectl -n financial-analysis rollout status deployment/financial-analysis-api --timeout=120s
kubectl -n financial-analysis rollout status deployment/financial-analysis-streamlit --timeout=120s

echo ""
echo "=============================================="
echo "  Financial Analysis - Crusoe Cloud Deployment"
echo "=============================================="
echo ""
echo "Services:"
kubectl -n financial-analysis get svc -o wide
echo ""
echo "Pods:"
kubectl -n financial-analysis get pods
echo ""
echo "To get the public IPs (may take a few minutes for LoadBalancer):"
echo "  kubectl -n financial-analysis get svc"
echo ""
echo "To access MinIO console:"
echo "  kubectl -n financial-analysis port-forward svc/minio 9001:9001"
echo "  Then visit http://localhost:9001"
echo ""
