#!/bin/bash
# Script to update Streamlit web app with the latest API URL
# This script retrieves the API container's FQDN and updates the Streamlit app's API_URL environment variable
#
# Usage:
#   ./update_streamlit_api_url.sh
#   AZURE_RESOURCE_GROUP=my-rg ./update_streamlit_api_url.sh
#   API_CONTAINER_NAME=my-api STREAMLIT_APP_NAME=my-app ./update_streamlit_api_url.sh
#
# Environment Variables:
#   AZURE_RESOURCE_GROUP      - Azure resource group name (default: financial-analysis-rg)
#   API_CONTAINER_NAME        - API container name (default: financial-analysis-api)
#   STREAMLIT_APP_NAME        - Streamlit web app name (default: financial-analysis-streamlit)
#   API_PORT                  - API port (default: 8000)

set -e

# Show help if requested
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    echo "Update Streamlit API URL Script"
    echo ""
    echo "This script updates the Streamlit web app's API_URL environment variable"
    echo "with the current API container's FQDN."
    echo ""
    echo "Usage:"
    echo "  $0 [options]"
    echo ""
    echo "Options:"
    echo "  -h, --help     Show this help message"
    echo ""
    echo "Environment Variables:"
    echo "  AZURE_RESOURCE_GROUP      Azure resource group name (default: financial-analysis-rg)"
    echo "  API_CONTAINER_NAME        API container name (default: financial-analysis-api)"
    echo "  STREAMLIT_APP_NAME        Streamlit web app name (default: financial-analysis-streamlit)"
    echo "  API_PORT                  API port (default: 8000)"
    echo ""
    echo "Examples:"
    echo "  ./update_streamlit_api_url.sh"
    echo "  AZURE_RESOURCE_GROUP=my-rg ./update_streamlit_api_url.sh"
    echo "  API_CONTAINER_NAME=my-api STREAMLIT_APP_NAME=my-app ./update_streamlit_api_url.sh"
    exit 0
fi

# Configuration - can be overridden via environment variables
RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-financial-analysis-rg}"
API_CONTAINER_NAME="${API_CONTAINER_NAME:-financial-analysis-api}"
STREAMLIT_APP_NAME="${STREAMLIT_APP_NAME:-financial-analysis-streamlit}"
API_PORT="${API_PORT:-8000}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=================================="
echo "Update Streamlit API URL"
echo "=================================="
echo ""
echo "Configuration:"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  API Container: $API_CONTAINER_NAME"
echo "  Streamlit App: $STREAMLIT_APP_NAME"
echo "  API Port: $API_PORT"
echo ""

# Check if Azure CLI is installed
if ! command -v az &> /dev/null; then
    echo -e "${RED}✗ Azure CLI is not installed${NC}"
    echo "Please install Azure CLI: https://docs.microsoft.com/cli/azure/install-azure-cli"
    exit 1
fi

# Check if logged in to Azure
if ! az account show &> /dev/null; then
    echo -e "${YELLOW}⚠️  Not logged in to Azure. Logging in...${NC}"
    az login
fi

# Get API container FQDN
echo "Retrieving API container FQDN..."
if ! az container show --name "$API_CONTAINER_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    echo -e "${RED}✗ API container '$API_CONTAINER_NAME' not found in resource group '$RESOURCE_GROUP'${NC}"
    echo ""
    echo "Available containers in resource group:"
    az container list --resource-group "$RESOURCE_GROUP" --query "[].{Name:name,State:containers[0].instanceView.currentState.state}" -o table 2>/dev/null || echo "None found"
    exit 1
fi

API_FQDN=$(az container show \
    --resource-group "$RESOURCE_GROUP" \
    --name "$API_CONTAINER_NAME" \
    --query "ipAddress.fqdn" -o tsv 2>/dev/null)

if [ -z "$API_FQDN" ] || [ "$API_FQDN" = "null" ]; then
    echo -e "${RED}✗ Could not retrieve API container FQDN${NC}"
    echo ""
    echo "Container details:"
    az container show \
        --resource-group "$RESOURCE_GROUP" \
        --name "$API_CONTAINER_NAME" \
        --query "{name:name,state:containers[0].instanceView.currentState.state,ip:ipAddress.ip,fqdn:ipAddress.fqdn}" \
        --output table
    exit 1
fi

API_URL="http://$API_FQDN:$API_PORT"
echo -e "${GREEN}✓ API URL: $API_URL${NC}"
echo ""

# Check if Streamlit web app exists
echo "Checking if Streamlit web app exists..."
if ! az webapp show --name "$STREAMLIT_APP_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    echo -e "${RED}✗ Streamlit web app '$STREAMLIT_APP_NAME' not found in resource group '$RESOURCE_GROUP'${NC}"
    echo ""
    echo "Available web apps in resource group:"
    az webapp list --resource-group "$RESOURCE_GROUP" --query "[].{Name:name,State:state}" -o table 2>/dev/null || echo "None found"
    exit 1
fi

echo -e "${GREEN}✓ Streamlit web app found${NC}"
echo ""

# Get current API_URL setting
echo "Checking current API_URL setting..."
CURRENT_API_URL=$(az webapp config appsettings list \
    --name "$STREAMLIT_APP_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --query "[?name=='API_URL'].value" -o tsv 2>/dev/null || echo "")

if [ -n "$CURRENT_API_URL" ]; then
    echo "  Current API_URL: $CURRENT_API_URL"
else
    echo "  Current API_URL: (not set)"
fi
echo "  New API_URL: $API_URL"
echo ""

# Confirm update
read -p "Update Streamlit app with new API URL? (y/N): " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Update cancelled."
    exit 0
fi

# Update API_URL setting
echo ""
echo "Updating API_URL environment variable..."
UPDATE_OUTPUT=$(az webapp config appsettings set \
    --name "$STREAMLIT_APP_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --settings API_URL="$API_URL" \
    --output json 2>&1)

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ API_URL updated successfully${NC}"
    
    # Verify the update
    VERIFIED_API_URL=$(az webapp config appsettings list \
        --name "$STREAMLIT_APP_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --query "[?name=='API_URL'].value" -o tsv 2>/dev/null)
    
    if [ "$VERIFIED_API_URL" = "$API_URL" ]; then
        echo -e "${GREEN}✓ Verified: API_URL is set to $API_URL${NC}"
    else
        echo -e "${YELLOW}⚠️  Warning: Verification shows API_URL as '$VERIFIED_API_URL'${NC}"
    fi
    
    echo ""
    echo "=================================="
    echo "Update Complete!"
    echo "=================================="
    echo ""
    echo "The Streamlit app will use the new API URL on its next request."
    echo ""
    echo "To restart the app immediately (recommended):"
    echo "  az webapp restart --name $STREAMLIT_APP_NAME --resource-group $RESOURCE_GROUP"
    echo ""
    read -p "Restart Streamlit app now? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo ""
        echo "Restarting Streamlit app..."
        az webapp restart --name "$STREAMLIT_APP_NAME" --resource-group "$RESOURCE_GROUP"
        echo -e "${GREEN}✓ Streamlit app restarted${NC}"
        echo ""
        echo "The app may take a minute to fully restart."
        echo "Check status: az webapp show --name $STREAMLIT_APP_NAME --resource-group $RESOURCE_GROUP --query state -o tsv"
    fi
else
    echo -e "${RED}✗ Failed to update API_URL${NC}"
    echo ""
    echo "Error output:"
    echo "$UPDATE_OUTPUT"
    exit 1
fi
