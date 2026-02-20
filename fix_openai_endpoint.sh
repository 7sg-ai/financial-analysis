#!/bin/bash
# Script to find and fix AWS Bedrock endpoint and deployment configuration
# This script helps identify the correct endpoint and available models

set -e

# Configuration
AWS_REGION="${AWS_REGION:-us-east-1}"
BEDROCK_MODEL_ID="${BEDROCK_MODEL_ID:-anthropic.claude-3-5-sonnet-20240620-v1:0}"

echo "=================================="
echo "AWS Bedrock Endpoint & Model Fixer"
echo "=================================="
echo ""
echo "This script will help you:"
echo "  1. Find your AWS Bedrock endpoint"
echo "  2. List available models"
echo "  3. Create a model access configuration if needed"
echo ""

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "✗ AWS CLI is not installed"
    exit 1
fi

# Check if logged in
if ! aws sts get-caller-identity &> /dev/null; then
    echo "⚠️  Not logged in to AWS. Logging in..."
    aws configure
fi

echo "Step 1: Finding AWS Bedrock model..."
if ! aws bedrock list-foundation-models --region "$AWS_REGION" --query "modelSummaries[?contains(modelId, '$BEDROCK_MODEL_ID')]" &> /dev/null; then
    echo "✗ AWS Bedrock model '$BEDROCK_MODEL_ID' not found in region '$AWS_REGION'"
    echo ""
    echo "Available Bedrock models:"
    aws bedrock list-foundation-models --region "$AWS_REGION" --query "modelSummaries[].{ModelId:modelId,ModelName:modelName}" -o table 2>/dev/null || echo "None found"
    exit 1
fi

# Get model ARN
MODEL_ARN=$(aws bedrock list-foundation-models --region "$AWS_REGION" --query "modelSummaries[?contains(modelId, '$BEDROCK_MODEL_ID')].modelArn" --output text 2>/dev/null)

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
BEDROCK_ENDPOINT=$(echo "$BEDROCK_ENDPOINT" | sed 's|/$||')

echo ""
echo "Step 2: Checking available models..."
echo ""

# Get AWS credentials
AWS_ACCESS_KEY=$(aws configure get aws_access_key_id --profile default 2>/dev/null)
AWS_SECRET_KEY=$(aws configure get aws_secret_access_key --profile default 2>/dev/null)

if [ -z "$API_KEY" ]; then
    echo "✗ Could not retrieve API key"
    exit 1
fi

# Test if the model is accessible by trying to use it
echo "Testing if '$BEDROCK_MODEL_ID' model is accessible..."
TEST_RESPONSE=$(aws bedrock-runtime invoke-model --model-id "$BEDROCK_MODEL_ID" --body '{"prompt":"\n\nHuman: test\n\nAssistant:","max_tokens_to_sample":5}' --region "$AWS_REGION" 2>/dev/null)

if [ $? -eq 0 ]; then
    echo "✓ '$BEDROCK_MODEL_ID' model is accessible"
    MODEL_EXISTS=true
else
    echo "⚠️  '$BEDROCK_MODEL_ID' model is not accessible"
    MODEL_EXISTS=false
fi

# Try to list models as a fallback
if [ "$MODEL_EXISTS" = false ]; then
    echo ""
    echo "Attempting to list available models..."
    MODELS_RESPONSE=$(aws bedrock list-foundation-models --region "$AWS_REGION" --query "modelSummaries[].{ModelId:modelId,ModelName:modelName}" -o table 2>/dev/null)
    
    if [ $? -eq 0 ]; then
        echo "✓ Successfully retrieved models list"
        echo ""
        echo "Available models:"
        echo "$MODELS_RESPONSE"
        echo ""
    else
        echo "⚠️  Models list endpoint not available"
    fi
fi

# Offer to enable model access if it's not available
if [ "$MODEL_EXISTS" = false ]; then
    echo ""
    read -p "Enable '$BEDROCK_MODEL_ID' model access? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo ""
        echo "Enabling '$BEDROCK_MODEL_ID' model access..."
        aws bedrock create-model-access-configuration \
            --model-id "$BEDROCK_MODEL_ID" \
            --model-access-type "DIRECT" \
            --region "$AWS_REGION" 2>&1 | tee /tmp/model-access-enable.log
        
        if [ $? -eq 0 ]; then
            echo ""
            echo "✓ '$BEDROCK_MODEL_ID' model access enabled successfully"
            MODEL_EXISTS=true
        else
            echo ""
            echo "✗ Failed to enable model access. Check /tmp/model-access-enable.log for details"
        fi
    fi
fi

echo ""
echo "=================================="
echo "Summary"
echo "=================================="
echo ""
echo "Endpoint: $BEDROCK_ENDPOINT"
echo "Model: $BEDROCK_MODEL_ID"
echo "Region: $AWS_REGION"
echo "Model ARN: $MODEL_ARN"
echo ""

# Check if Lambda functions or containers exist and offer to update them
LAMBDA_FUNCTION_NAME="financial-analysis-api"
STREAMLIT_APP_NAME="financial-analysis-streamlit"

UPDATE_LAMBDA=false
UPDATE_STREAMLIT=false

# Check if Lambda function exists
if aws lambda get-function --function-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION" &> /dev/null; then
    echo "✓ Found Lambda function: $LAMBDA_FUNCTION_NAME"
    read -p "Update Lambda function with new model settings? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        UPDATE_LAMBDA=true
    fi
else
    echo "⚠️  Lambda function '$LAMBDA_FUNCTION_NAME' not found in region '$AWS_REGION'"_CONTAINER_NAME' not found (will skip)"
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
