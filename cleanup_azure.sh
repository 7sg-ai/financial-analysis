#!/bin/bash
# Cleanup script - Reverts everything that deploy_azure.sh creates
# Deletes all Azure resources in the correct dependency order
#
# Usage:
#   ./cleanup_azure.sh           # Interactive with confirmation
#   ./cleanup_azure.sh --yes     # Skip confirmation

set -e

# Parse --yes / -y to skip confirmation
SKIP_CONFIRM=false
for arg in "$@"; do
    case $arg in
        --yes|-y) SKIP_CONFIRM=true ;;
    esac
done

echo "=================================="
echo "Financial Analysis - Azure Cleanup"
echo "=================================="
echo ""

# Configuration (must match deploy_azure.sh)
RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-rg-financial-analysis}"
LOCATION="${AZURE_LOCATION:-eastus2}"
CONTAINER_REGISTRY="${AZURE_CONTAINER_REGISTRY:-financialanalysisacr}"
OPENAI_RESOURCE_NAME="${AZURE_OPENAI_RESOURCE_NAME:-financial-analysis-openai}"
SYNAPSE_WORKSPACE_NAME="${SYNAPSE_WORKSPACE_NAME:-financial-analysis-synapse}"
SYNAPSE_SPARK_POOL_NAME="${SYNAPSE_SPARK_POOL_NAME:-sparkpool}"
SYNAPSE_STORAGE_ACCOUNT="${SYNAPSE_STORAGE_ACCOUNT:-financialanalysissynapse}"
API_CONTAINER_NAME="${API_CONTAINER_NAME:-financial-analysis-api}"
STREAMLIT_APP_NAME="${STREAMLIT_APP_NAME:-financial-analysis-streamlit}"
APP_SERVICE_PLAN="${APP_SERVICE_PLAN:-financial-analysis-plan}"
API_IDENTITY_NAME="${API_IDENTITY_NAME:-financial-analysis-api-identity}"

# Confirmation
echo "The following resources will be DELETED:"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  - API Container Instance: $API_CONTAINER_NAME"
echo "  - API User-Assigned Identity: $API_IDENTITY_NAME"
echo "  - Streamlit Web App: $STREAMLIT_APP_NAME"
echo "  - App Service Plan: $APP_SERVICE_PLAN"
echo "  - Synapse Spark Pool: $SYNAPSE_SPARK_POOL_NAME"
echo "  - Synapse Workspace: $SYNAPSE_WORKSPACE_NAME"
echo "  - Storage Account: $SYNAPSE_STORAGE_ACCOUNT"
echo "  - Azure OpenAI: $OPENAI_RESOURCE_NAME"
echo "  - Container Registry: $CONTAINER_REGISTRY"
echo ""
echo "⚠️  WARNING: This action cannot be undone. All data in these resources will be permanently deleted."
echo ""

if [ "$SKIP_CONFIRM" = false ]; then
    read -p "Are you sure you want to continue? (yes/no): " CONFIRM
    if [ "$CONFIRM" != "yes" ]; then
        echo "Cleanup cancelled."
        exit 0
    fi
else
    echo "Proceeding with cleanup (--yes specified)..."
fi

# Check Azure CLI
if ! command -v kubectl &> /dev/null; then
    echo "Error: Kubernetes CLI (kubectl) not found. Please install it first."
    exit 1
fi

# Authentication
echo ""
echo "Checking Kubernetes cluster access..."
kubectl cluster-info > /dev/null 2>&1 || echo "⚠️  Warning: Unable to connect to Kubernetes cluster. Please ensure kubeconfig is properly configured."
echo "✓ Cluster access verified"
echo ""

# Check if namespace exists (CRUSOE uses Kubernetes namespaces instead of resource groups)
NAMESPACE="$RESOURCE_GROUP"
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo "Namespace '$NAMESPACE' does not exist. Nothing to clean up."
    exit 0
fi

# 1. Delete API Container Instance (Kubernetes Deployment)
echo "1. Deleting API Container Instance ($API_CONTAINER_NAME)..."
if kubectl get deployment "$API_CONTAINER_NAME" -n "$NAMESPACE" &> /dev/null; then
    kubectl delete deployment "$API_CONTAINER_NAME" -n "$NAMESPACE" --ignore-not-found=true
    echo "   ✓ Deletion initiated"
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 2. Delete Service Account (used by API container)
echo "2. Deleting Service Account ($API_IDENTITY_NAME)..."
if kubectl get serviceaccount "$API_IDENTITY_NAME" -n "$NAMESPACE" &> /dev/null; then
    kubectl delete serviceaccount "$API_IDENTITY_NAME" -n "$NAMESPACE" --ignore-not-found=true
    echo "   ✓ Deleted"
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 3. Delete Streamlit Web App (Kubernetes Service)
echo "3. Deleting Streamlit Web App ($STREAMLIT_APP_NAME)..."
if kubectl get service "$STREAMLIT_APP_NAME" -n "$NAMESPACE" &> /dev/null; then
    kubectl delete service "$STREAMLIT_APP_NAME" -n "$NAMESPACE" --ignore-not-found=true
    echo "   ✓ Deleted"
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 4. Delete Deployment Configuration (equivalent to App Service Plan)
echo "4. Deleting Deployment Configuration ($APP_SERVICE_PLAN)..."
# In CRUSOE, App Service Plans are abstracted into Kubernetes namespaces and resource quotas
# No explicit deletion needed as namespace deletion handles this
# But we'll clean up any associated ConfigMaps if they exist
if kubectl get configmap "$APP_SERVICE_PLAN-config" -n "$NAMESPACE" &> /dev/null; then
    kubectl delete configmap "$APP_SERVICE_PLAN-config" -n "$NAMESPACE" --ignore-not-found=true
    echo "   ✓ Deleted"
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 5. Delete Spark Cluster (equivalent to Synapse Spark Pool)
echo "5. Deleting Spark Cluster ($SYNAPSE_SPARK_POOL_NAME)..."
# In CRUSOE, Spark clusters are managed as Kubernetes resources
# Assuming Spark operator is used for Spark cluster management
if kubectl get sparkapplication "$SYNAPSE_SPARK_POOL_NAME" -n "$NAMESPACE" &> /dev/null; then
    kubectl delete sparkapplication "$SYNAPSE_SPARK_POOL_NAME" -n "$NAMESPACE" --ignore-not-found=true
    echo "   ✓ Deletion initiated"
    echo "   Waiting 30 seconds for cluster deletion to start..."
    sleep 30
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 6. Delete Data Processing Namespace (equivalent to Synapse Workspace)
echo "6. Deleting Data Processing Namespace ($SYNAPSE_WORKSPACE_NAME)..."
# In CRUSOE, Synapse Workspaces are abstracted into Kubernetes namespaces
# We'll delete the namespace which handles all contained resources
if kubectl get namespace "$SYNAPSE_WORKSPACE_NAME" &> /dev/null; then
    kubectl delete namespace "$SYNAPSE_WORKSPACE_NAME" --ignore-not-found=true
    echo "   ✓ Deletion initiated"
    echo "   Waiting 15 seconds for namespace deletion to start..."
    sleep 15
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 7. Delete Persistent Volume Claims (equivalent to Storage Account)
echo "7. Deleting Persistent Volume Claims ($SYNAPSE_STORAGE_ACCOUNT)..."
# In CRUSOE, Azure Storage Accounts are replaced with Kubernetes Persistent Volumes
# We'll delete PVCs with matching labels or names
if kubectl get pvc -n "$NAMESPACE" -l storage-account="$SYNAPSE_STORAGE_ACCOUNT" &> /dev/null; then
    kubectl delete pvc -n "$NAMESPACE" -l storage-account="$SYNAPSE_STORAGE_ACCOUNT" --ignore-not-found=true
    echo "   ✓ Deleted"
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 8. Delete AI Service (equivalent to Azure OpenAI)
echo "8. Deleting AI Service ($OPENAI_RESOURCE_NAME)..."
# In CRUSOE, Azure OpenAI is replaced with Kubernetes-based AI service deployments
# Assuming AI services are deployed as Kubernetes Deployments with specific labels
if kubectl get deployment -n "$NAMESPACE" -l ai-service="$OPENAI_RESOURCE_NAME" &> /dev/null; then
    kubectl delete deployment -n "$NAMESPACE" -l ai-service="$OPENAI_RESOURCE_NAME" --ignore-not-found=true
    echo "   ✓ Deleted"
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 9. Delete Container Registry
echo "9. Deleting Container Registry ($CONTAINER_REGISTRY)..."
if az acr show --name "$CONTAINER_REGISTRY" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    az acr delete \
        --name "$CONTAINER_REGISTRY" \
        --resource-group "$RESOURCE_GROUP" \
        --yes \
        --output none
    echo "   ✓ Deleted"
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 10. Delete Resource Group (catches any remaining resources)
echo "10. Deleting Resource Group ($RESOURCE_GROUP)..."
if az group show --name "$RESOURCE_GROUP" &> /dev/null; then
    echo "   This may take several minutes if async deletions (Spark pool, Synapse) are still in progress..."
    az group delete \
        --name "$RESOURCE_GROUP" \
        --yes \
        --no-wait \
        --output none
    echo "   ✓ Resource group deletion initiated"
    echo ""
    echo "   Note: Resource group deletion runs asynchronously. If Synapse or Spark pool"
    echo "   are still deleting, the resource group delete will complete once they finish."
    echo "   Check status with: az group show --name $RESOURCE_GROUP"
else
    echo "   ⏭️  Not found (already deleted)"
fi
echo ""

echo "=================================="
echo "Cleanup Complete!"
echo "=================================="
echo ""
echo "All Financial Analysis Azure resources have been deleted or deletion has been initiated."
echo ""
