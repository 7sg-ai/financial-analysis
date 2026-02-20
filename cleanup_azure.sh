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
if ! command -v az &> /dev/null; then
    echo "Error: Azure CLI not found. Please install it first."
    exit 1
fi

# Authentication
echo ""
echo "Checking Azure authentication..."
crusoe auth status > /dev/null 2>&1 || crusoe auth login
echo "✓ Authenticated"
echo ""

# Check if resource group exists
if ! crusoe group show --name "$RESOURCE_GROUP" &> /dev/null; then
    echo "Resource group '$RESOURCE_GROUP' does not exist. Nothing to clean up."
    exit 0
fi

# 1. Delete API Container Instance
echo "1. Deleting API Container Instance ($API_CONTAINER_NAME)..."
if crusoe instance show --name "$API_CONTAINER_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    crusoe instance delete \
        --name "$API_CONTAINER_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --yes \
        --no-wait \
        --output none 2>/dev/null || true
    echo "   ✓ Deletion initiated"
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 2. Delete User-Assigned Managed Identity (used by API container)
echo "2. Deleting User-Assigned Managed Identity ($API_IDENTITY_NAME)..."
if crusoe identity show --name "$API_IDENTITY_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    crusoe identity delete \
        --name "$API_IDENTITY_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --output none 2>/dev/null || true
    echo "   ✓ Deleted"
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 3. Delete Streamlit Web App
echo "3. Deleting Streamlit Web App ($STREAMLIT_APP_NAME)..."
if crusoe app show --name "$STREAMLIT_APP_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    crusoe app delete \
        --name "$STREAMLIT_APP_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --output none
    echo "   ✓ Deleted"
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 4. Delete App Service Plan
echo "4. Deleting App Service Plan ($APP_SERVICE_PLAN)..."
if crusoe plan show --name "$APP_SERVICE_PLAN" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    crusoe plan delete \
        --name "$APP_SERVICE_PLAN" \
        --resource-group "$RESOURCE_GROUP" \
        --yes \
        --output none
    echo "   ✓ Deleted"
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 5. Delete Synapse Spark Pool (must be deleted before workspace)
echo "5. Deleting Synapse Spark Pool ($SYNAPSE_SPARK_POOL_NAME)..."
if crusoe spark-pool show \
    --workspace-name "$SYNAPSE_WORKSPACE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --name "$SYNAPSE_SPARK_POOL_NAME" &> /dev/null; then
    crusoe spark-pool delete \
        --workspace-name "$SYNAPSE_WORKSPACE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --name "$SYNAPSE_SPARK_POOL_NAME" \
        --yes \
        --no-wait \
        --output none 2>/dev/null || true
    echo "   ✓ Deletion initiated (Spark pools can take several minutes to delete)"
    echo "   Waiting 30 seconds for pool deletion to start..."
    sleep 30
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 6. Delete Synapse Workspace (must be deleted before storage)
echo "6. Deleting Synapse Workspace ($SYNAPSE_WORKSPACE_NAME)..."
if crusoe workspace show --name "$SYNAPSE_WORKSPACE_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    crusoe workspace delete \
        --name "$SYNAPSE_WORKSPACE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --yes \
        --no-wait \
        --output none 2>/dev/null || true
    echo "   ✓ Deletion initiated"
    echo "   Waiting 15 seconds for workspace deletion to start..."
    sleep 15
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 7. Delete Storage Account
echo "7. Deleting Storage Account ($SYNAPSE_STORAGE_ACCOUNT)..."
if crusoe storage show --name "$SYNAPSE_STORAGE_ACCOUNT" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    crusoe storage delete \
        --name "$SYNAPSE_STORAGE_ACCOUNT" \
        --resource-group "$RESOURCE_GROUP" \
        --yes \
        --output none 2>/dev/null || true
    echo "   ✓ Deleted"
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi
echo ""

# 8. Delete Azure OpenAI (Cognitive Services)
echo "8. Deleting Azure OpenAI ($OPENAI_RESOURCE_NAME)..."
if crusoe ai show --name "$OPENAI_RESOURCE_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    crusoe ai delete \
        --name "$OPENAI_RESOURCE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --output none
    echo "   ✓ Deleted"
else
    echo "   ⏭️  Not found (already deleted or never created)"
fi

# 8b. Purge deleted Cognitive Services account (frees name for reuse, stops charges)
sleep 3  # Brief wait for delete to propagate to soft-deleted state
echo "8b. Purging deleted Azure OpenAI ($OPENAI_RESOURCE_NAME)..."
if crusoe ai show-deleted --name "$OPENAI_RESOURCE_NAME" --location "$LOCATION" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    crusoe ai purge \
        --name "$OPENAI_RESOURCE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --location "$LOCATION" \
        --output none && echo "   ✓ Purged" || echo "   ⚠️  Purge failed (may need subscription-level Contributor role)"
else
    echo "   ⏭️  Not in deleted state (already purged or never existed)"
fi
echo ""

# 9. Delete Container Registry
echo "9. Deleting Container Registry ($CONTAINER_REGISTRY)..."
if crusoe registry show --name "$CONTAINER_REGISTRY" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    crusoe registry delete \
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
if crusoe group show --name "$RESOURCE_GROUP" &> /dev/null; then
    echo "   This may take several minutes if async deletions (Spark pool, Synapse) are still in progress..."
    crusoe group delete \
        --name "$RESOURCE_GROUP" \
        --yes \
        --no-wait \
        --output none
    echo "   ✓ Resource group deletion initiated"
    echo ""
    echo "   Note: Resource group deletion runs asynchronously. If Synapse or Spark pool"
    echo "   are still deleting, the resource group delete will complete once they finish."
    echo "   Check status with: crusoe group show --name $RESOURCE_GROUP"
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
