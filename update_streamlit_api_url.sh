#!/bin/bash
# Script to update Streamlit web app with the latest API URL
# This script retrieves the API container's FQDN and updates the Streamlit app's API_URL environment variable
#
# Usage:
#   ./update_streamlit_api_url.sh
#   AWS_REGION=us-east-1 ./update_streamlit_api_url.sh
#   API_TASK_NAME=my-api STREAMLIT_APP_NAME=my-app ./update_streamlit_api_url.sh
#
# Environment Variables:
#   AWS_REGION                - AWS region (default: us-east-1)
#   API_TASK_NAME             - API task name (default: financial-analysis-api)
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
    echo "  AWS_REGION                AWS region (default: us-east-1)"
    echo "  API_TASK_NAME             API task name (default: financial-analysis-api)"
    echo "  STREAMLIT_APP_NAME        Streamlit web app name (default: financial-analysis-streamlit)"
    echo "  API_PORT                  API port (default: 8000)"
    echo ""
    echo "Examples:"
    echo "  ./update_streamlit_api_url.sh"
    echo "  AWS_REGION=us-west-2 ./update_streamlit_api_url.sh"
    echo "  API_TASK_NAME=my-api STREAMLIT_APP_NAME=my-app ./update_streamlit_api_url.sh"
    exit 0
fi

# Configuration - can be overridden via environment variables
AWS_REGION="${AWS_REGION:-us-east-1}"
API_TASK_NAME="${API_TASK_NAME:-financial-analysis-api}"
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
echo "  AWS Region: $AWS_REGION"
echo "  API Task: $API_TASK_NAME"
echo "  Streamlit App: $STREAMLIT_APP_NAME"
echo "  API Port: $API_PORT"
echo ""

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo -e "${RED}✗ AWS CLI is not installed${NC}"
    echo "Please install AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html"
    exit 1
fi

# Check if logged in to AWS
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${YELLOW}⚠️  Not logged in to AWS. Configuring AWS CLI...${NC}"
    aws configure
fi

# Get API container FQDN
echo "Retrieving API container FQDN..."
if ! aws ecs describe-task-definition --task-definition "$API_TASK_NAME" --region "$AWS_REGION" &> /dev/null; then
    echo -e "${RED}✗ API task '$API_TASK_NAME' not found in region '$AWS_REGION'${NC}"
    echo ""
    echo "Available tasks in region:"
    aws ecs list-task-definitions --family-prefix "$API_TASK_NAME" --region "$AWS_REGION" --query "taskDefinitionArns[]" --output text 2>/dev/null || echo "None found"
    exit 1
fi

API_FQDN=$(aws ecs describe-task-definition \
    --task-definition "$API_TASK_NAME" \
    --region "$AWS_REGION" \
    --query "taskDefinition.containerDefinitions[0].environment[?name=='FQDN'].value[0]" -o tsv 2>/dev/null)

if [ -z "$API_FQDN" ] || [ "$API_FQDN" = "null" ]; then
    echo -e "${RED}✗ Could not retrieve API container FQDN${NC}"
    echo ""
    echo "Task details:"
    aws ecs describe-task-definition \
        --task-definition "$API_TASK_NAME" \
        --region "$AWS_REGION" \
        --query "{name:taskDefinitionArn,state:containerDefinitions[0].environment[?name=='STATE'].value[0],ip:containerDefinitions[0].environment[?name=='IP'].value[0],fqdn:containerDefinitions[0].environment[?name=='FQDN'].value[0]}" \
        --output table
    exit 1
fi

API_URL="http://$API_FQDN:$API_PORT"
echo -e "${GREEN}✓ API URL: $API_URL${NC}"
echo ""

# Check if Streamlit web app exists
echo "Checking if Streamlit web app exists..."
if ! aws elasticbeanstalk describe-applications --application-name "$STREAMLIT_APP_NAME" --region "$AWS_REGION" &> /dev/null; then
    echo -e "${RED}✗ Streamlit web app '$STREAMLIT_APP_NAME' not found in region '$AWS_REGION'${NC}"
    echo ""
    echo "Available web apps in region:"
    aws elasticbeanstalk describe-applications --region "$AWS_REGION" --query "Applications[].{Name:ApplicationName,State:Status}" -o table 2>/dev/null || echo "None found"
    exit 1
fi

echo -e "${GREEN}✓ Streamlit web app found${NC}"
echo ""

# Get current API_URL setting
echo "Checking current API_URL setting..."
CURRENT_API_URL=$(aws elasticbeanstalk describe-configuration-settings \
    --application-name "$STREAMLIT_APP_NAME" \
    --environment-name "$STREAMLIT_APP_NAME-env" \
    --region "$AWS_REGION" \
    --query "ConfigurationSettings[?OptionName=='API_URL'].OptionValue" -o tsv 2>/dev/null || echo "")

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
UPDATE_OUTPUT=$(aws elasticbeanstalk update-environment \
    --application-name "$STREAMLIT_APP_NAME" \
    --environment-name "$STREAMLIT_APP_NAME-env" \
    --option-settings Namespace=aws:elasticbeanstalk:application:environment,OptionName=API_URL,Value="$API_URL" \
    --region "$AWS_REGION" \
    --output json 2>&1)

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ API_URL updated successfully${NC}"
    
    # Verify the update
    VERIFIED_API_URL=$(aws elasticbeanstalk describe-configuration-settings \
        --application-name "$STREAMLIT_APP_NAME" \
        --environment-name "$STREAMLIT_APP_NAME-env" \
        --region "$AWS_REGION" \
        --query "ConfigurationSettings[?OptionName=='API_URL'].OptionValue" -o tsv 2>/dev/null)
    
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
    echo "  aws elasticbeanstalk restart-environment --environment-name $STREAMLIT_APP_NAME-env --region $AWS_REGION"
    echo ""
    read -p "Restart Streamlit app now? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo ""
        echo "Restarting Streamlit app..."
        aws elasticbeanstalk restart-environment --environment-name "$STREAMLIT_APP_NAME-env" --region "$AWS_REGION"
        echo -e "${GREEN}✓ Streamlit app restarted${NC}"
        echo ""
        echo "The app may take a minute to fully restart."
        echo "Check status: aws elasticbeanstalk describe-environments --application-name $STREAMLIT_APP_NAME --environment-names $STREAMLIT_APP_NAME-env --region $AWS_REGION --query 'Environments[0].Status' -o tsv"
    fi
else
    echo -e "${RED}✗ Failed to update API_URL${NC}"
    echo ""
    echo "Error output:"
    echo "$UPDATE_OUTPUT"
    exit 1
fi