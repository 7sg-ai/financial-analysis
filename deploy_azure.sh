#!/bin/bash
# Deployment script for Azure Synapse / Azure Container Instances
# This is a template - customize for your Azure environment

set -e

echo "=================================="
echo "Financial Analysis API - Azure Deployment"
echo "=================================="
echo ""
echo "Choose deployment type:"
echo "1) API only (FastAPI)"
echo "2) Streamlit UI only"
echo "3) Both API and Streamlit UI"
echo ""
read -p "Enter choice (1-3): " DEPLOYMENT_CHOICE

# Configuration
RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-financial-analysis-rg}"
LOCATION="${AZURE_LOCATION:-eastus}"
CONTAINER_REGISTRY="${AZURE_CONTAINER_REGISTRY:-financialanalysisacr}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

# Set deployment-specific variables
if [ "$DEPLOYMENT_CHOICE" = "1" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    API_IMAGE_NAME="financial-analysis-api"
    API_CONTAINER_NAME="financial-analysis-api"
fi

if [ "$DEPLOYMENT_CHOICE" = "2" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    STREAMLIT_IMAGE_NAME="financial-analysis-streamlit"
    STREAMLIT_APP_NAME="financial-analysis-streamlit"
    APP_SERVICE_PLAN="financial-analysis-plan"
fi

echo ""
echo "Configuration:"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  Location: $LOCATION"
echo "  Container Registry: $CONTAINER_REGISTRY"
echo "  Deployment Choice: $DEPLOYMENT_CHOICE"
if [ "$DEPLOYMENT_CHOICE" = "1" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    echo "  API Image: $API_IMAGE_NAME:$IMAGE_TAG"
fi
if [ "$DEPLOYMENT_CHOICE" = "2" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    echo "  Streamlit Image: $STREAMLIT_IMAGE_NAME:$IMAGE_TAG"
    echo "  App Service Plan: $APP_SERVICE_PLAN"
fi
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

# Build and push Docker images
echo ""
echo "Building Docker images..."

if [ "$DEPLOYMENT_CHOICE" = "1" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    echo "Building API image..."
    docker build -t "$API_IMAGE_NAME:$IMAGE_TAG" .
fi

if [ "$DEPLOYMENT_CHOICE" = "2" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    echo "Building Streamlit image..."
    docker build -f Dockerfile.streamlit -t "$STREAMLIT_IMAGE_NAME:$IMAGE_TAG" .
fi

echo "Logging into container registry..."
az acr login --name "$CONTAINER_REGISTRY"

# Push API image
if [ "$DEPLOYMENT_CHOICE" = "1" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    echo "Tagging and pushing API image..."
    API_FULL_IMAGE_NAME="$CONTAINER_REGISTRY.azurecr.io/$API_IMAGE_NAME:$IMAGE_TAG"
    docker tag "$API_IMAGE_NAME:$IMAGE_TAG" "$API_FULL_IMAGE_NAME"
    docker push "$API_FULL_IMAGE_NAME"
    echo "✓ API image pushed successfully"
fi

# Push Streamlit image
if [ "$DEPLOYMENT_CHOICE" = "2" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    echo "Tagging and pushing Streamlit image..."
    STREAMLIT_FULL_IMAGE_NAME="$CONTAINER_REGISTRY.azurecr.io/$STREAMLIT_IMAGE_NAME:$IMAGE_TAG"
    docker tag "$STREAMLIT_IMAGE_NAME:$IMAGE_TAG" "$STREAMLIT_FULL_IMAGE_NAME"
    docker push "$STREAMLIT_FULL_IMAGE_NAME"
    echo "✓ Streamlit image pushed successfully"
fi

# Get registry credentials
echo ""
echo "Retrieving registry credentials..."
ACR_USERNAME=$(az acr credential show --name "$CONTAINER_REGISTRY" --query username -o tsv)
ACR_PASSWORD=$(az acr credential show --name "$CONTAINER_REGISTRY" --query "passwords[0].value" -o tsv)

# Deploy based on choice
echo ""
echo "Deploying services..."

# Check if environment variables are set
if [ -z "$AZURE_OPENAI_ENDPOINT" ] || [ -z "$AZURE_OPENAI_API_KEY" ]; then
    echo "Error: Required environment variables not set"
    echo "Please set:"
    echo "  - AZURE_OPENAI_ENDPOINT"
    echo "  - AZURE_OPENAI_API_KEY"
    exit 1
fi

# Deploy API to Container Instances
if [ "$DEPLOYMENT_CHOICE" = "1" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    echo "Deploying API to Azure Container Instances..."
    az container create \
        --resource-group "$RESOURCE_GROUP" \
        --name "$API_CONTAINER_NAME" \
        --image "$API_FULL_IMAGE_NAME" \
        --registry-login-server "$CONTAINER_REGISTRY.azurecr.io" \
        --registry-username "$ACR_USERNAME" \
        --registry-password "$ACR_PASSWORD" \
        --dns-name-label "$API_CONTAINER_NAME" \
        --ports 8000 \
        --cpu 4 \
        --memory 8 \
        --environment-variables \
            AZURE_OPENAI_ENDPOINT="$AZURE_OPENAI_ENDPOINT" \
            AZURE_OPENAI_API_KEY="$AZURE_OPENAI_API_KEY" \
            AZURE_OPENAI_DEPLOYMENT_NAME="${AZURE_OPENAI_DEPLOYMENT_NAME:-gpt-4}" \
            USE_LOCAL_SPARK=true \
            API_PORT=8000 \
        --output none
    echo "✓ API container deployed"
fi

# Deploy Streamlit to App Service
if [ "$DEPLOYMENT_CHOICE" = "2" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    echo "Deploying Streamlit to Azure App Service..."
    
    # Create App Service Plan
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
    
    # Create or update web app
    if az webapp show --name "$STREAMLIT_APP_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
        echo "Updating existing Streamlit web app..."
        az webapp config container set \
            --name "$STREAMLIT_APP_NAME" \
            --resource-group "$RESOURCE_GROUP" \
            --docker-custom-image-name "$STREAMLIT_FULL_IMAGE_NAME" \
            --docker-registry-server-url "https://$CONTAINER_REGISTRY.azurecr.io" \
            --docker-registry-server-user "$ACR_USERNAME" \
            --docker-registry-server-password "$ACR_PASSWORD" \
            --output none
    else
        echo "Creating new Streamlit web app..."
        az webapp create \
            --name "$STREAMLIT_APP_NAME" \
            --resource-group "$RESOURCE_GROUP" \
            --plan "$APP_SERVICE_PLAN" \
            --deployment-container-image-name "$STREAMLIT_FULL_IMAGE_NAME" \
            --docker-registry-server-url "https://$CONTAINER_REGISTRY.azurecr.io" \
            --docker-registry-server-user "$ACR_USERNAME" \
            --docker-registry-server-password "$ACR_PASSWORD" \
            --output none
    fi
    
    # Configure app settings
    az webapp config appsettings set \
        --name "$STREAMLIT_APP_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --settings \
            AZURE_OPENAI_ENDPOINT="$AZURE_OPENAI_ENDPOINT" \
            AZURE_OPENAI_API_KEY="$AZURE_OPENAI_API_KEY" \
            AZURE_OPENAI_DEPLOYMENT_NAME="${AZURE_OPENAI_DEPLOYMENT_NAME:-gpt-4}" \
            USE_LOCAL_SPARK=true \
            STREAMLIT_PORT=8501 \
            STREAMLIT_HOST=0.0.0.0 \
        --output none
    
    # Configure startup command
    az webapp config set \
        --name "$STREAMLIT_APP_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --startup-file "streamlit run streamlit_app.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true --browser.gatherUsageStats=false" \
        --output none
    
    echo "✓ Streamlit web app deployed"
fi

# Get deployment URLs
echo ""
echo "=================================="
echo "Deployment Complete!"
echo "=================================="
echo ""

if [ "$DEPLOYMENT_CHOICE" = "1" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    API_FQDN=$(az container show \
        --resource-group "$RESOURCE_GROUP" \
        --name "$API_CONTAINER_NAME" \
        --query ipAddress.fqdn -o tsv)
    echo "🚀 API URL: http://$API_FQDN:8000"
    echo "📚 API Docs: http://$API_FQDN:8000/docs"
    echo ""
fi

if [ "$DEPLOYMENT_CHOICE" = "2" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    STREAMLIT_URL=$(az webapp show --name "$STREAMLIT_APP_NAME" --resource-group "$RESOURCE_GROUP" --query defaultHostName -o tsv)
    echo "🌐 Streamlit Dashboard: https://$STREAMLIT_URL"
    echo ""
fi

echo "🔧 Management Commands:"
if [ "$DEPLOYMENT_CHOICE" = "1" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    echo "  API logs: az container logs --resource-group $RESOURCE_GROUP --name $API_CONTAINER_NAME"
    echo "  Delete API: az container delete --resource-group $RESOURCE_GROUP --name $API_CONTAINER_NAME"
fi
if [ "$DEPLOYMENT_CHOICE" = "2" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    echo "  Streamlit logs: az webapp log tail --name $STREAMLIT_APP_NAME --resource-group $RESOURCE_GROUP"
    echo "  Restart Streamlit: az webapp restart --name $STREAMLIT_APP_NAME --resource-group $RESOURCE_GROUP"
    echo "  Delete Streamlit: az webapp delete --name $STREAMLIT_APP_NAME --resource-group $RESOURCE_GROUP"
fi
echo ""

