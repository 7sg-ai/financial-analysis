#!/bin/bash
# Deployment script for Azure Synapse / Azure Container Instances
# Uses Azure Container Registry Build (ACR Build) - no local Docker required

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
LOCATION="${AZURE_LOCATION:-eastus2}"
CONTAINER_REGISTRY="${AZURE_CONTAINER_REGISTRY:-financialanalysisacr}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

# Git Repository Configuration
# If GIT_REPO is not set, will try to detect from current git remote
GIT_REPO="${GIT_REPO:-}"
GIT_BRANCH="${GIT_BRANCH:-main}"
if [ -z "$GIT_REPO" ] && command -v git &> /dev/null && git rev-parse --git-dir > /dev/null 2>&1; then
    # Try to get remote URL from current git repository
    GIT_REPO=$(git config --get remote.origin.url 2>/dev/null || echo "")
fi

# Azure OpenAI Configuration
OPENAI_RESOURCE_NAME="${AZURE_OPENAI_RESOURCE_NAME:-financial-analysis-openai}"
OPENAI_DEPLOYMENT_NAME="${AZURE_OPENAI_DEPLOYMENT_NAME:-gpt-4}"

# Azure Synapse Configuration
SYNAPSE_WORKSPACE_NAME="${SYNAPSE_WORKSPACE_NAME:-financial-analysis-synapse}"
SYNAPSE_SPARK_POOL_NAME="${SYNAPSE_SPARK_POOL_NAME:-sparkpool}"
SYNAPSE_STORAGE_ACCOUNT="${SYNAPSE_STORAGE_ACCOUNT:-financialanalysissynapse}"
SYNAPSE_FILE_SYSTEM="${SYNAPSE_FILE_SYSTEM:-data}"
SYNAPSE_ADMIN_USER="${SYNAPSE_ADMIN_USER:-sqladmin}"
SYNAPSE_ADMIN_PASSWORD="${SYNAPSE_ADMIN_PASSWORD:-$(openssl rand -base64 32)}"

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
    echo ""
    echo "Installation: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
    exit 1
fi

# Authentication
echo "Checking Azure authentication..."
az account show > /dev/null 2>&1 || az login
echo "✓ Authenticated"

# Create resource group if it doesn't exist
echo "Ensuring resource group exists..."
az group create \
    --name "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --output none

echo "✓ Resource group ready"

# Create Azure OpenAI service if it doesn't exist
echo ""
echo "Checking Azure OpenAI service..."
if ! az cognitiveservices account show --name "$OPENAI_RESOURCE_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    echo "Creating Azure OpenAI service..."
    az cognitiveservices account create \
        --name "$OPENAI_RESOURCE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --location "$LOCATION" \
        --kind OpenAI \
        --sku S0 \
        --output none
    echo "✓ Azure OpenAI service created"
    
    # Get OpenAI endpoint and key
    AZURE_OPENAI_ENDPOINT=$(az cognitiveservices account show \
        --name "$OPENAI_RESOURCE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --query properties.endpoint -o tsv)
    AZURE_OPENAI_API_KEY=$(az cognitiveservices account keys list \
        --name "$OPENAI_RESOURCE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --query key1 -o tsv)
    
    echo "✓ Azure OpenAI endpoint: $AZURE_OPENAI_ENDPOINT"
    
    # Deploy GPT-4 model if not already deployed
    echo "Checking for GPT-4 deployment..."
    if ! az cognitiveservices account deployment show \
        --name "$OPENAI_DEPLOYMENT_NAME" \
        --account-name "$OPENAI_RESOURCE_NAME" \
        --resource-group "$RESOURCE_GROUP" &> /dev/null; then
        echo "Deploying GPT-4 model (this may take several minutes)..."
        az cognitiveservices account deployment create \
            --name "$OPENAI_DEPLOYMENT_NAME" \
            --account-name "$OPENAI_RESOURCE_NAME" \
            --resource-group "$RESOURCE_GROUP" \
            --model-format OpenAI \
            --model-name gpt-4 \
            --model-version "0613" \
            --sku-capacity 10 \
            --sku-name "Standard" \
            --output none
        echo "✓ GPT-4 deployment created"
    else
        echo "✓ GPT-4 deployment already exists"
    fi
else
    echo "✓ Azure OpenAI service exists"
    # Get existing OpenAI endpoint and key
    AZURE_OPENAI_ENDPOINT=$(az cognitiveservices account show \
        --name "$OPENAI_RESOURCE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --query properties.endpoint -o tsv)
    AZURE_OPENAI_API_KEY=$(az cognitiveservices account keys list \
        --name "$OPENAI_RESOURCE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --query key1 -o tsv)
fi

# Create Azure Storage Account for Synapse (if needed)
echo ""
echo "Checking Azure Storage Account for Synapse..."
if ! az storage account show --name "$SYNAPSE_STORAGE_ACCOUNT" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    echo "Creating storage account for Synapse..."
    az storage account create \
        --name "$SYNAPSE_STORAGE_ACCOUNT" \
        --resource-group "$RESOURCE_GROUP" \
        --location "$LOCATION" \
        --sku Standard_LRS \
        --kind StorageV2 \
        --enable-hierarchical-namespace true \
        --output none
    echo "✓ Storage account created"
    
    # Create file system (container) for data
    STORAGE_KEY=$(az storage account keys list \
        --account-name "$SYNAPSE_STORAGE_ACCOUNT" \
        --resource-group "$RESOURCE_GROUP" \
        --query "[0].value" -o tsv)
    
    az storage container create \
        --name "$SYNAPSE_FILE_SYSTEM" \
        --account-name "$SYNAPSE_STORAGE_ACCOUNT" \
        --account-key "$STORAGE_KEY" \
        --output none
    echo "✓ File system created"
else
    echo "✓ Storage account exists"
fi

# Create Azure Synapse Analytics workspace if it doesn't exist
echo ""
echo "Checking Azure Synapse Analytics workspace..."
if ! az synapse workspace show --name "$SYNAPSE_WORKSPACE_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
    echo "Creating Azure Synapse Analytics workspace..."
    
    # Get storage account details
    STORAGE_ACCOUNT_ID=$(az storage account show \
        --name "$SYNAPSE_STORAGE_ACCOUNT" \
        --resource-group "$RESOURCE_GROUP" \
        --query id -o tsv)
    
    # Generate SQL admin password if not set
    if [ -z "$SYNAPSE_ADMIN_PASSWORD" ]; then
        SYNAPSE_ADMIN_PASSWORD=$(openssl rand -base64 32)
    fi
    
    az synapse workspace create \
        --name "$SYNAPSE_WORKSPACE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --storage-account "$SYNAPSE_STORAGE_ACCOUNT" \
        --file-system "$SYNAPSE_FILE_SYSTEM" \
        --sql-admin-login-user "$SYNAPSE_ADMIN_USER" \
        --sql-admin-login-password "$SYNAPSE_ADMIN_PASSWORD" \
        --location "$LOCATION" \
        --repository-type None \
        --output none
    echo "✓ Synapse workspace created"
    
    # Get subscription ID if not set
    if [ -z "$AZURE_SUBSCRIPTION_ID" ]; then
        AZURE_SUBSCRIPTION_ID=$(az account show --query id -o tsv)
    fi
else
    echo "✓ Synapse workspace exists"
    if [ -z "$AZURE_SUBSCRIPTION_ID" ]; then
        AZURE_SUBSCRIPTION_ID=$(az account show --query id -o tsv)
    fi
fi

# Create Synapse Spark pool if it doesn't exist
echo ""
echo "Checking Synapse Spark pool..."
if ! az synapse spark pool show \
    --workspace-name "$SYNAPSE_WORKSPACE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --name "$SYNAPSE_SPARK_POOL_NAME" &> /dev/null; then
    echo "Creating Synapse Spark pool..."
    az synapse spark pool create \
        --workspace-name "$SYNAPSE_WORKSPACE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --name "$SYNAPSE_SPARK_POOL_NAME" \
        --node-count 3 \
        --node-size Small \
        --spark-version 3.3 \
        --output none
    echo "✓ Spark pool created"
else
    echo "✓ Spark pool exists"
fi

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

# Build Docker images using Azure Container Registry Build
# ACR Build builds images in the cloud - no local Docker installation required
# ACR Build uses Azure CLI authentication directly - no need for 'az acr login'
echo ""
echo "Building Docker images using Azure Container Registry Build..."

# Determine build context (Git repo or local directory)
if [ -n "$GIT_REPO" ]; then
    echo "Using Git repository: $GIT_REPO (branch: $GIT_BRANCH)"
    BUILD_CONTEXT="$GIT_REPO#:$GIT_BRANCH"
else
    echo "Using local directory (current folder)"
    BUILD_CONTEXT="."
fi

# ACR Build automatically builds for Linux/AMD64 and pushes to registry
# It uses the current Azure CLI authentication
if [ "$DEPLOYMENT_CHOICE" = "1" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    echo "Building API image using ACR Build (Linux/AMD64)..."
    az acr build \
        --registry "$CONTAINER_REGISTRY" \
        --image "$API_IMAGE_NAME:$IMAGE_TAG" \
        --platform linux/amd64 \
        --file Dockerfile \
        "$BUILD_CONTEXT"
    API_FULL_IMAGE_NAME="$CONTAINER_REGISTRY.azurecr.io/$API_IMAGE_NAME:$IMAGE_TAG"
    echo "✓ API image built and pushed successfully"
fi

if [ "$DEPLOYMENT_CHOICE" = "2" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    echo "Building Streamlit image using ACR Build (Linux/AMD64)..."
    az acr build \
        --registry "$CONTAINER_REGISTRY" \
        --image "$STREAMLIT_IMAGE_NAME:$IMAGE_TAG" \
        --platform linux/amd64 \
        --file Dockerfile.streamlit \
        "$BUILD_CONTEXT"
    STREAMLIT_FULL_IMAGE_NAME="$CONTAINER_REGISTRY.azurecr.io/$STREAMLIT_IMAGE_NAME:$IMAGE_TAG"
    echo "✓ Streamlit image built and pushed successfully"
fi

# Get registry credentials
echo ""
echo "Retrieving registry credentials..."
ACR_USERNAME=$(az acr credential show --name "$CONTAINER_REGISTRY" --query username -o tsv)
ACR_PASSWORD=$(az acr credential show --name "$CONTAINER_REGISTRY" --query "passwords[0].value" -o tsv)

# Deploy based on choice
echo ""
echo "Deploying services..."

# Use environment variables if provided, otherwise use values from created resources
if [ -z "$AZURE_OPENAI_ENDPOINT" ]; then
    AZURE_OPENAI_ENDPOINT=$(az cognitiveservices account show \
        --name "$OPENAI_RESOURCE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --query properties.endpoint -o tsv)
fi

if [ -z "$AZURE_OPENAI_API_KEY" ]; then
    AZURE_OPENAI_API_KEY=$(az cognitiveservices account keys list \
        --name "$OPENAI_RESOURCE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --query key1 -o tsv)
fi

if [ -z "$AZURE_SUBSCRIPTION_ID" ]; then
    AZURE_SUBSCRIPTION_ID=$(az account show --query id -o tsv)
fi

if [ -z "$AZURE_RESOURCE_GROUP" ]; then
    AZURE_RESOURCE_GROUP="$RESOURCE_GROUP"
fi

# Verify all required values are set
if [ -z "$AZURE_OPENAI_ENDPOINT" ] || [ -z "$AZURE_OPENAI_API_KEY" ]; then
    echo "Error: Could not determine Azure OpenAI configuration"
    exit 1
fi

if [ -z "$SYNAPSE_SPARK_POOL_NAME" ] || [ -z "$SYNAPSE_WORKSPACE_NAME" ] || \
   [ -z "$AZURE_SUBSCRIPTION_ID" ] || [ -z "$AZURE_RESOURCE_GROUP" ]; then
    echo "Error: Could not determine Azure Synapse configuration"
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
        --os-type Linux \
        --ports 8000 \
        --cpu 4 \
        --memory 8 \
        --environment-variables \
            AZURE_OPENAI_ENDPOINT="$AZURE_OPENAI_ENDPOINT" \
            AZURE_OPENAI_API_KEY="$AZURE_OPENAI_API_KEY" \
            AZURE_OPENAI_DEPLOYMENT_NAME="${AZURE_OPENAI_DEPLOYMENT_NAME:-gpt-4}" \
            SYNAPSE_SPARK_POOL_NAME="$SYNAPSE_SPARK_POOL_NAME" \
            SYNAPSE_WORKSPACE_NAME="$SYNAPSE_WORKSPACE_NAME" \
            AZURE_SUBSCRIPTION_ID="$AZURE_SUBSCRIPTION_ID" \
            AZURE_RESOURCE_GROUP="$AZURE_RESOURCE_GROUP" \
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
            SYNAPSE_SPARK_POOL_NAME="$SYNAPSE_SPARK_POOL_NAME" \
            SYNAPSE_WORKSPACE_NAME="$SYNAPSE_WORKSPACE_NAME" \
            AZURE_SUBSCRIPTION_ID="$AZURE_SUBSCRIPTION_ID" \
            AZURE_RESOURCE_GROUP="$AZURE_RESOURCE_GROUP" \
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

echo "📋 Deployed Resources Summary:"
echo ""
echo "Azure OpenAI:"
echo "  Resource: $OPENAI_RESOURCE_NAME"
echo "  Endpoint: $AZURE_OPENAI_ENDPOINT"
echo "  Deployment: $OPENAI_DEPLOYMENT_NAME"
echo ""
echo "Azure Synapse Analytics:"
echo "  Workspace: $SYNAPSE_WORKSPACE_NAME"
echo "  Spark Pool: $SYNAPSE_SPARK_POOL_NAME"
echo "  Storage Account: $SYNAPSE_STORAGE_ACCOUNT"
echo "  File System: $SYNAPSE_FILE_SYSTEM"
echo ""
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
echo "📝 Next Steps:"
echo "  1. Upload data to Azure Data Lake Storage: abfss://$SYNAPSE_FILE_SYSTEM@$SYNAPSE_STORAGE_ACCOUNT.dfs.core.windows.net/"
echo "  2. Configure DATA_PATH environment variable to point to your data location"
echo "  3. Test the API endpoints using the URLs above"
echo ""

