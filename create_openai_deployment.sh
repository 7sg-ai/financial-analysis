#!/bin/bash
# Script to create an Azure OpenAI deployment
# This script creates a deployment for the specified model using Azure CLI

set -e

# Configuration
RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-rg-financial-analysis}"
OPENAI_RESOURCE_NAME="${AZURE_OPENAI_RESOURCE_NAME:-financial-analysis-openai}"
DEPLOYMENT_NAME="${AZURE_OPENAI_DEPLOYMENT_NAME:-gpt-5.2-chat}"
MODEL_NAME="${MODEL_NAME:-gpt-5.2-chat}"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=================================="
echo "Create Azure OpenAI Deployment"
echo "=================================="
echo ""
echo "Configuration:"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  OpenAI Resource: $OPENAI_RESOURCE_NAME"
echo "  Deployment Name: $DEPLOYMENT_NAME"
echo "  Model Name: $MODEL_NAME"
echo ""

# Check if Azure CLI is installed
if ! command -v az &> /dev/null; then
    echo -e "${RED}✗ Azure CLI is not installed${NC}"
    echo "Please install Azure CLI: https://docs.microsoft.com/cli/azure/install-azure-cli"
    exit 1
fi

# Check if logged in
if ! az account show &> /dev/null; then
    echo -e "${YELLOW}⚠️  Not logged in to Azure. Logging in...${NC}"
    az login
fi

# Check if OpenAI resource exists
echo "Checking if OpenAI resource exists..."
if ! az cognitiveservices account show --name "$OPENAI_RESOURCE_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    echo -e "${RED}✗ OpenAI resource '$OPENAI_RESOURCE_NAME' not found in resource group '$RESOURCE_GROUP'${NC}"
    echo ""
    echo "Available Cognitive Services resources:"
    az cognitiveservices account list --resource-group "$RESOURCE_GROUP" --query "[].{Name:name,Kind:kind}" -o table 2>/dev/null || echo "None found"
    exit 1
fi

# Check if deployment already exists
echo "Checking if deployment '$DEPLOYMENT_NAME' already exists..."
if az cognitiveservices account deployment show \
    --name "$OPENAI_RESOURCE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --deployment-name "$DEPLOYMENT_NAME" &> /dev/null; then
    echo -e "${GREEN}✓ Deployment '$DEPLOYMENT_NAME' already exists${NC}"
    echo ""
    echo "Deployment details:"
    az cognitiveservices account deployment show \
        --name "$OPENAI_RESOURCE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --deployment-name "$DEPLOYMENT_NAME" \
        --output table
    exit 0
fi

# Create deployment
echo ""
echo "Creating deployment '$DEPLOYMENT_NAME' with model '$MODEL_NAME'..."
echo "This may take several minutes..."

az cognitiveservices account deployment create \
    --name "$OPENAI_RESOURCE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --deployment-name "$DEPLOYMENT_NAME" \
    --model-name "$MODEL_NAME" \
    --model-format "OpenAI" \
    --sku-capacity 10 \
    --sku-name "Standard" \
    --output table

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ Deployment created successfully${NC}"
    echo ""
    echo "You can now use this deployment in your application."
    echo ""
    echo "To verify the deployment:"
    echo "  az cognitiveservices account deployment show --name $OPENAI_RESOURCE_NAME --resource-group $RESOURCE_GROUP --deployment-name $DEPLOYMENT_NAME"
else
    echo ""
    echo -e "${RED}✗ Failed to create deployment${NC}"
    echo ""
    echo "Common issues:"
    echo "  - Model '$MODEL_NAME' may not be available in your region"
    echo "  - You may need to request access to GPT-5.2-chat"
    echo "  - Check quota limits in Azure Portal"
    exit 1
fi
