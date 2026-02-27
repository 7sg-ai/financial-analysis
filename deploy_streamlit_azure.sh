#!/bin/bash
# Deployment script for Streamlit app to Azure App Service
set -e

echo "=================================="
echo "Financial Analysis Streamlit - Azure Deployment"
echo "=================================="

# Configuration
RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-rg-financial-analysis}"
LOCATION="${AZURE_LOCATION:-eastus}"
APP_SERVICE_PLAN="${APP_SERVICE_PLAN:-financial-analysis-plan}"
APP_NAME="${APP_NAME:-financial-analysis-streamlit}"
CONTAINER_REGISTRY="${AZURE_CONTAINER_REGISTRY:-financialanalysisacr}"
IMAGE_NAME="financial-analysis-streamlit"
IMAGE_TAG="${IMAGE_TAG:-latest}"

echo ""
echo "Configuration:"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  Location: $LOCATION"
echo "  App Service Plan: $APP_SERVICE_PLAN"
echo "  App Name: $APP_NAME"
echo "  Container Registry: $CONTAINER_REGISTRY"
echo "  Image: $IMAGE_NAME:$IMAGE_TAG"
echo ""

# Check Azure CLI
if ! command -v az &> /dev/null; then
    echo "Error: Azure CLI not found. Please install it first."
    exit 1
fi

# Login check
echo "Checking Azure login..."
az account show > /dev/null 2>&1 || az login

# Create resource group if it doesn't exist
echo "Ensuring resource group exists..."
az group create \
    --name "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --output none

echo "✓ Resource group ready"

# Create container registry if it doesn't exist
echo "Checking container registry..."
if ! az acr show --name "$CONTAINER_REGISTRY" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    echo "Creating container registry..."
    az acr create \
        --name "$CONTAINER_REGISTRY" \
        --resource-group "$RESOURCE_GROUP" \
        --sku Basic \
        --admin-enabled true \
        --output none
    echo "✓ Container registry created"
else
    echo "✓ Container registry exists"
fi

# Build and push Docker image
echo ""
echo "Building Streamlit Docker image..."
docker build -f Dockerfile.streamlit -t "$IMAGE_NAME:$IMAGE_TAG" .

echo "Logging into container registry..."
az acr login --name "$CONTAINER_REGISTRY"

echo "Tagging image..."
FULL_IMAGE_NAME="$CONTAINER_REGISTRY.azurecr.io/$IMAGE_NAME:$IMAGE_TAG"
docker tag "$IMAGE_NAME:$IMAGE_TAG" "$FULL_IMAGE_NAME"

echo "Pushing image to registry..."
docker push "$FULL_IMAGE_NAME"

echo "✓ Image pushed successfully"

# Get registry credentials
echo ""
echo "Retrieving registry credentials..."
ACR_USERNAME=$(az acr credential show --name "$CONTAINER_REGISTRY" --query username -o tsv)
ACR_PASSWORD=$(az acr credential show --name "$CONTAINER_REGISTRY" --query "passwords[0].value" -o tsv)

# Create App Service Plan
echo ""
echo "Creating App Service Plan..."
if ! az appservice plan show --name "$APP_SERVICE_PLAN" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    az appservice plan create \
        --name "$APP_SERVICE_PLAN" \
        --resource-group "$RESOURCE_GROUP" \
        --location "$LOCATION" \
        --is-linux \
        --sku B2 \
        --output none
    echo "✓ App Service Plan created"
else
    echo "✓ App Service Plan exists"
fi

# Check if environment variables are set
if [ -z "$AZURE_OPENAI_ENDPOINT" ] || [ -z "$AZURE_OPENAI_API_KEY" ]; then
    echo "Error: Required environment variables not set"
    echo "Please set:"
    echo "  - AZURE_OPENAI_ENDPOINT"
    echo "  - AZURE_OPENAI_API_KEY"
    exit 1
fi

# Create or update the web app
echo ""
echo "Creating/updating web app..."

if az webapp show --name "$APP_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    echo "Updating existing web app..."
    az webapp config container set \
        --name "$APP_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --docker-custom-image-name "$FULL_IMAGE_NAME" \
        --docker-registry-server-url "https://$CONTAINER_REGISTRY.azurecr.io" \
        --docker-registry-server-user "$ACR_USERNAME" \
        --docker-registry-server-password "$ACR_PASSWORD" \
        --output none
else
    echo "Creating new web app..."
    az webapp create \
        --name "$APP_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --plan "$APP_SERVICE_PLAN" \
        --deployment-container-image-name "$FULL_IMAGE_NAME" \
        --docker-registry-server-url "https://$CONTAINER_REGISTRY.azurecr.io" \
        --docker-registry-server-user "$ACR_USERNAME" \
        --docker-registry-server-password "$ACR_PASSWORD" \
        --output none
fi

# Configure app settings
echo "Configuring app settings..."
az webapp config appsettings set \
    --name "$APP_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --settings \
        AZURE_OPENAI_ENDPOINT="$AZURE_OPENAI_ENDPOINT" \
        AZURE_OPENAI_API_KEY="$AZURE_OPENAI_API_KEY" \
        AZURE_OPENAI_DEPLOYMENT_NAME="${AZURE_OPENAI_DEPLOYMENT_NAME:-gpt-5.2-chat}" \
        SYNAPSE_SPARK_POOL_NAME="$SYNAPSE_SPARK_POOL_NAME" \
        SYNAPSE_WORKSPACE_NAME="$SYNAPSE_WORKSPACE_NAME" \
        AZURE_SUBSCRIPTION_ID="$AZURE_SUBSCRIPTION_ID" \
        AZURE_RESOURCE_GROUP="$AZURE_RESOURCE_GROUP" \
        STREAMLIT_PORT=8501 \
        STREAMLIT_HOST=0.0.0.0 \
        STREAMLIT_THEME=light \
    --output none

# Configure startup command
echo "Setting startup command..."
az webapp config set \
    --name "$APP_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --startup-file "streamlit run streamlit_app.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true --browser.gatherUsageStats=false" \
    --output none

# Get the web app URL
WEBAPP_URL=$(az webapp show --name "$APP_NAME" --resource-group "$RESOURCE_GROUP" --query defaultHostName -o tsv)

echo ""
echo "=================================="
echo "Deployment Complete!"
echo "=================================="
echo ""
echo "🌐 Streamlit Dashboard: https://$WEBAPP_URL"
echo ""
echo "📊 Features:"
echo "  - Natural language queries"
echo "  - Interactive visualizations"
echo "  - Query history"
echo "  - Example questions"
echo ""
echo "🔧 Management Commands:"
echo "  View logs: az webapp log tail --name $APP_NAME --resource-group $RESOURCE_GROUP"
echo "  Restart: az webapp restart --name $APP_NAME --resource-group $RESOURCE_GROUP"
echo "  Delete: az webapp delete --name $APP_NAME --resource-group $RESOURCE_GROUP"
echo ""

# Test the deployment
echo "Testing deployment..."
sleep 30  # Wait for app to start

if curl -f "https://$WEBAPP_URL" > /dev/null 2>&1; then
    echo "✅ Deployment successful! App is responding."
else
    echo "⚠️  App may still be starting up. Check logs if issues persist."
fi

echo ""
echo "🎉 Your Financial Analysis Dashboard is ready!"
echo "Visit: https://$WEBAPP_URL"
