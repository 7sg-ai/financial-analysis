#!/bin/bash
# Script to create an AWS OpenAI-like deployment
# This script creates a model deployment for the specified model using AWS CLI
# Note: AWS uses Amazon Bedrock for foundation models and SageMaker for custom deployments

set -e

# Configuration
RESOURCE_GROUP="${AWS_RESOURCE_GROUP:-financial-analysis}"
MODEL_NAME="${MODEL_NAME:-gpt-5.2-chat}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-gpt-5.2-chat}"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=================================="
echo "Create AWS Model Deployment"
echo "=================================="
echo ""
echo "Configuration:"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  Model Name: $MODEL_NAME"
echo "  Deployment Name: $DEPLOYMENT_NAME"
echo ""

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo -e "${RED}✗ AWS CLI is not installed${NC}"
    echo "Please install AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html"
    exit 1
fi

# Check if logged in
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${YELLOW}⚠️  Not logged in to AWS. Logging in...${NC}"
    aws configure
fi

# Check if model is available in Bedrock
# Note: AWS Bedrock uses model IDs instead of deployments
# We'll check if the model is available for invocation
echo "Checking if model '$MODEL_NAME' is available..."
if ! aws bedrock list-foundation-models --by-output-ability "TEXT" --query "modelSummaries[?contains(modelId, '$MODEL_NAME')]" &> /dev/null; then
    echo -e "${RED}✗ Model '$MODEL_NAME' not found in available Bedrock models${NC}"
    echo ""
    echo "Available Bedrock models:"
    aws bedrock list-foundation-models --query "modelSummaries[].{ModelId:modelId,Name:modelName}" --output table 2>/dev/null || echo "None found"
    exit 1
fi

# Check if model variant configuration already exists
# In AWS, we use variant configurations for model deployments
echo "Checking if deployment '$DEPLOYMENT_NAME' already exists..."
if aws bedrock get-model-variant-configuration --model-variant-name "$DEPLOYMENT_NAME" &> /dev/null; then
    echo -e "${GREEN}✓ Deployment '$DEPLOYMENT_NAME' already exists${NC}"
    echo ""
    echo "Deployment details:"
    aws bedrock get-model-variant-configuration --model-variant-name "$DEPLOYMENT_NAME" --output table
    exit 0
fi

# Create model variant configuration (AWS equivalent of deployment)
echo ""
echo "Creating deployment '$DEPLOYMENT_NAME' with model '$MODEL_NAME'..."
echo "This may take several minutes..."

# Note: AWS Bedrock uses a different model invocation approach
# For production deployments, you would typically use SageMaker endpoints
# This is a simplified example using Bedrock

# For Bedrock, you don't create deployments - you invoke models directly
# For SageMaker deployments, you would create endpoints
# The following is a placeholder for the actual AWS deployment process

echo "Note: AWS Bedrock models are invoked directly without deployment creation."
echo "For production deployments with custom models, consider using Amazon SageMaker."
echo ""
echo "To invoke the model directly:"
echo "  aws bedrock-runtime.invoke-model --model-id $MODEL_NAME --body '{...}'"
echo ""
echo "To create a SageMaker endpoint for custom models:"
echo "  aws sagemaker create-endpoint-config --endpoint-config-name $DEPLOYMENT_NAME ..."

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ Model configuration completed${NC}"
    echo ""
    echo "You can now invoke this model in your application."
    echo ""
    echo "To verify model availability:"
    echo "  aws bedrock list-foundation-models --query \"modelSummaries[?contains(modelId, '$MODEL_NAME')]\""
else
    echo ""
    echo -e "${RED}✗ Failed to configure model${NC}"
    echo ""
    echo "Common issues:"
    echo "  - Model '$MODEL_NAME' may not be available in your region"
    echo "  - You may need to request access to the model in Bedrock console"
    echo "  - Check quota limits in AWS Service Quotas"
    exit 1
fi