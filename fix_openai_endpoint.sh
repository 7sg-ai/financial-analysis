#!/bin/bash
# Script to find and fix Azure OpenAI endpoint and deployment configuration
# This script helps identify the correct endpoint and available deployments

set -e

# Configuration
RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-financial-analysis-rg}"
OPENAI_RESOURCE_NAME="${AZURE_OPENAI_RESOURCE_NAME:-financial-analysis-openai}"

echo "=================================="
echo "Azure OpenAI Endpoint & Deployment Fixer"
echo "=================================="
echo ""
echo "This script will help you:"
echo "  1. Find your Azure OpenAI resource endpoint"
echo "  2. List available deployments"
echo "  3. Create a deployment if needed"
echo ""

# Check if Azure CLI is installed
if ! command -v az &> /dev/null; then
    echo "✗ Azure CLI is not installed"
    exit 1
fi

# Check if logged in
if ! az account show &> /dev/null; then
    echo "⚠️  Not logged in to Azure. Logging in..."
    az login
fi

echo "Step 1: Finding Azure OpenAI resource..."
if ! az cognitiveservices account show --name "$OPENAI_RESOURCE_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    echo "✗ Azure OpenAI resource '$OPENAI_RESOURCE_NAME' not found"
    echo ""
    echo "Available Cognitive Services resources:"
    az cognitiveservices account list --resource-group "$RESOURCE_GROUP" --query "[].{Name:name,Kind:kind,Endpoint:properties.endpoint}" -o table 2>/dev/null || echo "None found"
    exit 1
fi

# Get endpoint
ENDPOINT=$(az cognitiveservices account show \
    --name "$OPENAI_RESOURCE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --query properties.endpoint -o tsv 2>/dev/null)

if [ -z "$ENDPOINT" ]; then
    echo "✗ Could not retrieve endpoint"
    exit 1
fi

echo "✓ Found endpoint: $ENDPOINT"
echo ""

# Check if endpoint is OpenAI-specific or generic Cognitive Services
if [[ "$ENDPOINT" == *".openai.azure.com"* ]]; then
    echo "✓ Endpoint format is correct (OpenAI-specific)"
    OPENAI_ENDPOINT="$ENDPOINT"
elif [[ "$ENDPOINT" == *"api.cognitive.microsoft.com"* ]]; then
    echo "⚠️  Warning: Endpoint is generic Cognitive Services endpoint"
    echo "   Azure OpenAI requires a resource-specific endpoint"
    echo ""
    echo "   The endpoint should be in format: https://your-resource-name.openai.azure.com"
    echo "   Current endpoint: $ENDPOINT"
    echo ""
    echo "   Please check your Azure OpenAI resource in the Azure Portal:"
    echo "   https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.CognitiveServices%2Faccounts"
    echo ""
    read -p "Do you have the correct OpenAI endpoint? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        read -p "Enter the correct OpenAI endpoint (e.g., https://your-resource.openai.azure.com): " OPENAI_ENDPOINT
    else
        echo "Please find your Azure OpenAI resource endpoint and run this script again"
        exit 1
    fi
else
    echo "⚠️  Unknown endpoint format: $ENDPOINT"
    OPENAI_ENDPOINT="$ENDPOINT"
fi

# Normalize endpoint (remove trailing slash)
OPENAI_ENDPOINT=$(echo "$OPENAI_ENDPOINT" | sed 's|/$||')

echo ""
echo "Step 2: Checking available deployments..."
echo ""

# Get API key
API_KEY=$(az cognitiveservices account keys list \
    --name "$OPENAI_RESOURCE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --query key1 -o tsv 2>/dev/null)

if [ -z "$API_KEY" ]; then
    echo "✗ Could not retrieve API key"
    exit 1
fi

# Test if the gpt-5.2-chat deployment exists by trying to use it
echo "Testing if 'gpt-5.2-chat' deployment exists..."
TEST_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
    -H "api-key: $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"messages":[{"role":"user","content":"test"}],"max_completion_tokens":5}' \
    "${OPENAI_ENDPOINT}/openai/deployments/gpt-5.2-chat/chat/completions?api-version=2024-12-01-preview" 2>/dev/null)

HTTP_CODE=$(echo "$TEST_RESPONSE" | tail -n1)
TEST_BODY=$(echo "$TEST_RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "200" ]; then
    echo "✓ 'gpt-5.2-chat' deployment exists and is working"
    DEPLOYMENT_EXISTS=true
elif [ "$HTTP_CODE" = "404" ]; then
    # Check if it's a DeploymentNotFound error
    if echo "$TEST_BODY" | grep -q "DeploymentNotFound"; then
        echo "⚠️  'gpt-5.2-chat' deployment not found"
        DEPLOYMENT_EXISTS=false
    else
        echo "⚠️  Got 404, but may be endpoint format issue"
        DEPLOYMENT_EXISTS=false
    fi
else
    echo "⚠️  Unexpected response (HTTP $HTTP_CODE)"
    echo "Response: $TEST_BODY"
    DEPLOYMENT_EXISTS=false
fi

# Try to list deployments as a fallback (some endpoints may not support this)
if [ "$DEPLOYMENT_EXISTS" = false ]; then
    echo ""
    echo "Attempting to list deployments (may not be supported for this endpoint format)..."
    DEPLOYMENTS_RESPONSE=$(curl -s -w "\n%{http_code}" \
        -H "api-key: $API_KEY" \
        "${OPENAI_ENDPOINT}/openai/deployments?api-version=2024-12-01-preview" 2>/dev/null)
    
    LIST_HTTP_CODE=$(echo "$DEPLOYMENTS_RESPONSE" | tail -n1)
    DEPLOYMENTS_BODY=$(echo "$DEPLOYMENTS_RESPONSE" | sed '$d')
    
    if [ "$LIST_HTTP_CODE" = "200" ]; then
        echo "✓ Successfully retrieved deployments list"
        echo ""
        echo "Available deployments:"
        echo "$DEPLOYMENTS_BODY" | python3 -m json.tool 2>/dev/null | grep -A 5 '"id"' || echo "$DEPLOYMENTS_BODY"
        echo ""
        
        # Extract deployment names
        DEPLOYMENT_NAMES=$(echo "$DEPLOYMENTS_BODY" | python3 -c "import sys, json; data=json.load(sys.stdin); print('\n'.join([d['id'] for d in data.get('data', [])]))" 2>/dev/null || echo "")
        
        if [ -n "$DEPLOYMENT_NAMES" ]; then
            echo "Deployment names found:"
            echo "$DEPLOYMENT_NAMES" | while read -r name; do
                echo "  - $name"
            done
            echo ""
        fi
    else
        echo "⚠️  Deployments list endpoint not available (HTTP $LIST_HTTP_CODE)"
        echo "   This is normal for some endpoint formats - we'll test the deployment directly"
    fi
fi

# Offer to create deployment if it doesn't exist
if [ "$DEPLOYMENT_EXISTS" = false ]; then
    echo ""
    read -p "Create 'gpt-5.2-chat' deployment? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo ""
        echo "Creating GPT-5.2-chat deployment (this may take several minutes)..."
        az cognitiveservices account deployment create \
            --name "$OPENAI_RESOURCE_NAME" \
            --resource-group "$RESOURCE_GROUP" \
            --deployment-name "gpt-5.2-chat" \
            --model-name "gpt-5.2-chat" \
            --model-format "OpenAI" \
            --sku-capacity 10 \
            --sku-name "Standard" 2>&1 | tee /tmp/deployment-create.log
        
        if [ $? -eq 0 ]; then
            echo ""
            echo "✓ GPT-5.2-chat deployment created successfully"
            echo "   Note: It may take a few minutes for the deployment to be fully available"
            DEPLOYMENT_EXISTS=true
        else
            echo ""
            echo "✗ Failed to create deployment. Check /tmp/deployment-create.log for details"
        fi
    fi
fi

echo ""
echo "=================================="
echo "Summary"
echo "=================================="
echo ""
echo "Endpoint: $OPENAI_ENDPOINT"
echo "Resource: $OPENAI_RESOURCE_NAME"
echo "Deployment: gpt-5.2-chat"
echo "API Version: 2024-12-01-preview"
echo ""

# Check if containers/web apps exist and offer to update them
API_CONTAINER_NAME="financial-analysis-api"
STREAMLIT_APP_NAME="financial-analysis-streamlit"

UPDATE_CONTAINER=false
UPDATE_STREAMLIT=false

# Check if API container exists
if az container show --name "$API_CONTAINER_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    echo "✓ Found API container: $API_CONTAINER_NAME"
    read -p "Update API container with new endpoint and deployment settings? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        UPDATE_CONTAINER=true
    fi
else
    echo "⚠️  API container '$API_CONTAINER_NAME' not found (will skip)"
fi

# Check if Streamlit web app exists
if az webapp show --name "$STREAMLIT_APP_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    echo "✓ Found Streamlit web app: $STREAMLIT_APP_NAME"
    read -p "Update Streamlit web app with new endpoint and deployment settings? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        UPDATE_STREAMLIT=true
    fi
else
    echo "⚠️  Streamlit web app '$STREAMLIT_APP_NAME' not found (will skip)"
fi

echo ""

# Update API container
if [ "$UPDATE_CONTAINER" = true ]; then
    echo "Updating API container..."
    echo ""
    
    # Get current environment variables
    CURRENT_ENV=$(az container show \
        --name "$API_CONTAINER_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --query "containers[0].environmentVariables" -o json 2>/dev/null || echo "[]")
    
    # Update environment variables
    UPDATE_OUTPUT=$(az container update \
        --resource-group "$RESOURCE_GROUP" \
        --name "$API_CONTAINER_NAME" \
        --set containers[0].environmentVariables[0].name=AZURE_OPENAI_ENDPOINT \
        containers[0].environmentVariables[0].value="$OPENAI_ENDPOINT" \
        containers[0].environmentVariables[1].name=AZURE_OPENAI_DEPLOYMENT_NAME \
        containers[0].environmentVariables[1].value="gpt-5.2-chat" \
        containers[0].environmentVariables[2].name=AZURE_OPENAI_API_VERSION \
        containers[0].environmentVariables[2].value="2024-12-01-preview" \
        --output json 2>&1)
    
    if [ $? -eq 0 ]; then
        echo "✓ API container updated successfully"
        echo ""
        echo "Restarting container to apply changes..."
        az container restart --resource-group "$RESOURCE_GROUP" --name "$API_CONTAINER_NAME" --output none
        
        if [ $? -eq 0 ]; then
            echo "✓ API container restarted"
        else
            echo "⚠️  Failed to restart container (you may need to restart manually)"
        fi
    else
        echo "✗ Failed to update API container"
        echo "Error: $UPDATE_OUTPUT"
    fi
    echo ""
fi

# Update Streamlit web app
if [ "$UPDATE_STREAMLIT" = true ]; then
    echo "Updating Streamlit web app..."
    echo ""
    
    UPDATE_OUTPUT=$(az webapp config appsettings set --name "$STREAMLIT_APP_NAME" --resource-group "$RESOURCE_GROUP" --settings AZURE_OPENAI_ENDPOINT="$OPENAI_ENDPOINT" AZURE_OPENAI_DEPLOYMENT_NAME="gpt-5.2-chat" AZURE_OPENAI_API_VERSION="2024-12-01-preview" --output json 2>&1)
    
    if [ $? -eq 0 ]; then
        echo "✓ Streamlit web app updated successfully"
        echo ""
        echo "Restarting web app to apply changes..."
        az webapp restart --name "$STREAMLIT_APP_NAME" --resource-group "$RESOURCE_GROUP" --output none
        
        if [ $? -eq 0 ]; then
            echo "✓ Streamlit web app restarted"
        else
            echo "⚠️  Failed to restart web app (you may need to restart manually)"
        fi
    else
        echo "✗ Failed to update Streamlit web app"
        echo "Error: $UPDATE_OUTPUT"
    fi
    echo ""
fi

if [ "$UPDATE_CONTAINER" = false ] && [ "$UPDATE_STREAMLIT" = false ]; then
    echo "No updates were performed."
    echo ""
    echo "To update manually, use these commands (copy-paste ready for zsh/macOS):"
    echo ""
    echo "  # For API container:"
    echo "  az container update --resource-group $RESOURCE_GROUP --name $API_CONTAINER_NAME --set containers[0].environmentVariables[0].name=AZURE_OPENAI_ENDPOINT containers[0].environmentVariables[0].value='$OPENAI_ENDPOINT' containers[0].environmentVariables[1].name=AZURE_OPENAI_DEPLOYMENT_NAME containers[0].environmentVariables[1].value='gpt-5.2-chat' containers[0].environmentVariables[2].name=AZURE_OPENAI_API_VERSION containers[0].environmentVariables[2].value='2024-12-01-preview'"
    echo ""
    echo "  az container restart --resource-group $RESOURCE_GROUP --name $API_CONTAINER_NAME"
    echo ""
    echo "  # For Streamlit web app:"
    echo "  az webapp config appsettings set --name $STREAMLIT_APP_NAME --resource-group $RESOURCE_GROUP --settings AZURE_OPENAI_ENDPOINT='$OPENAI_ENDPOINT' AZURE_OPENAI_DEPLOYMENT_NAME='gpt-5.2-chat' AZURE_OPENAI_API_VERSION='2024-12-01-preview'"
    echo ""
    echo "  az webapp restart --resource-group $RESOURCE_GROUP --name $STREAMLIT_APP_NAME"
    echo ""
fi
