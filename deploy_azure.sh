#!/bin/bash
# Deployment script for Azure Synapse / Azure Container Instances
# Default: Builds locally using Docker (Linux/AMD64 platform)
# Set USE_ACR_BUILD=true to use Azure Container Registry Build instead

set -e

echo "=================================="
echo "Financial Analysis API - Azure Deployment"
echo "=================================="
echo ""
echo "Choose deployment type:"
echo "1) API only (FastAPI)"
echo "2) Streamlit UI only"
echo "3) Both API and Streamlit UI"
echo "4) Update images only (rebuild and update existing containers)"
echo ""
read -p "Enter choice (1-4): " DEPLOYMENT_CHOICE

# Configuration
RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-rg-financial-analysis}"
LOCATION="${AZURE_LOCATION:-eastus2}"
CONTAINER_REGISTRY="${AZURE_CONTAINER_REGISTRY:-financialanalysisacr}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

# Build Configuration
# Set USE_ACR_BUILD=true to use Azure Container Registry Build (no Docker required)
# Default: false (build locally using Docker)
USE_ACR_BUILD="${USE_ACR_BUILD:-false}"

# Azure OpenAI Configuration
OPENAI_RESOURCE_NAME="${AZURE_OPENAI_RESOURCE_NAME:-financial-analysis-openai}"
OPENAI_DEPLOYMENT_NAME="${AZURE_OPENAI_DEPLOYMENT_NAME:-gpt-5.2-chat}"

# Azure Synapse Configuration
SYNAPSE_WORKSPACE_NAME="${SYNAPSE_WORKSPACE_NAME:-financial-analysis-synapse}"
SYNAPSE_SPARK_POOL_NAME="${SYNAPSE_SPARK_POOL_NAME:-sparkpool}"
SYNAPSE_STORAGE_ACCOUNT="${SYNAPSE_STORAGE_ACCOUNT:-financialanalysissynapse}"
SYNAPSE_FILE_SYSTEM="${SYNAPSE_FILE_SYSTEM:-data}"
SYNAPSE_ADMIN_USER="${SYNAPSE_ADMIN_USER:-sqladmin}"
SYNAPSE_ADMIN_PASSWORD="${SYNAPSE_ADMIN_PASSWORD:-$(openssl rand -base64 32)}"

# Set deployment-specific variables
if [ "$DEPLOYMENT_CHOICE" = "1" ] || [ "$DEPLOYMENT_CHOICE" = "3" ] || [ "$DEPLOYMENT_CHOICE" = "4" ]; then
    API_IMAGE_NAME="financial-analysis-api"
    API_CONTAINER_NAME="financial-analysis-api"
fi

if [ "$DEPLOYMENT_CHOICE" = "2" ] || [ "$DEPLOYMENT_CHOICE" = "3" ] || [ "$DEPLOYMENT_CHOICE" = "4" ]; then
    STREAMLIT_IMAGE_NAME="financial-analysis-streamlit"
    STREAMLIT_APP_NAME="financial-analysis-streamlit"
    APP_SERVICE_PLAN="financial-analysis-plan"
fi

# For option 4, determine what to update based on what exists
if [ "$DEPLOYMENT_CHOICE" = "4" ]; then
    UPDATE_API=false
    UPDATE_STREAMLIT=false
    
    # Check if API container exists
    if az container show --name "$API_CONTAINER_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
        UPDATE_API=true
        echo "✓ Found API container: $API_CONTAINER_NAME"
    fi
    
    # Check if Streamlit web app exists
    if az webapp show --name "$STREAMLIT_APP_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
        UPDATE_STREAMLIT=true
        echo "✓ Found Streamlit web app: $STREAMLIT_APP_NAME"
    fi
    
    if [ "$UPDATE_API" = false ] && [ "$UPDATE_STREAMLIT" = false ]; then
        echo "✗ No existing containers or web apps found to update."
        echo "  Please use options 1-3 to deploy first."
        exit 1
    fi
    
    # Override deployment choice for image building
    if [ "$UPDATE_API" = true ] && [ "$UPDATE_STREAMLIT" = true ]; then
        IMAGE_BUILD_CHOICE="3"  # Build both
    elif [ "$UPDATE_API" = true ]; then
        IMAGE_BUILD_CHOICE="1"  # Build API only
    else
        IMAGE_BUILD_CHOICE="2"  # Build Streamlit only
    fi
else
    IMAGE_BUILD_CHOICE="$DEPLOYMENT_CHOICE"
fi

echo ""
echo "Configuration:"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  Location: $LOCATION"
echo "  Container Registry: $CONTAINER_REGISTRY"
echo "  Deployment Choice: $DEPLOYMENT_CHOICE"
echo "  Build Method: $([ "$USE_ACR_BUILD" = "true" ] && echo "Azure Container Registry Build" || echo "Local Docker Build (Linux/AMD64)")"
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

# Check Docker (required for local builds)
if ! command -v docker &> /dev/null; then
    echo "Error: Docker not found. Please install Docker first."
    echo ""
    echo "Installation options:"
    echo "  macOS: Install Docker Desktop from https://www.docker.com/products/docker-desktop"
    echo "  Linux: sudo apt-get install docker.io (Ubuntu/Debian) or sudo yum install docker (RHEL/CentOS)"
    echo "  Windows: Install Docker Desktop from https://www.docker.com/products/docker-desktop"
    echo ""
    echo "After installation, make sure Docker is running and try again."
    echo ""
    echo "Alternatively, set USE_ACR_BUILD=true to use Azure Container Registry Build (no Docker required)"
    exit 1
fi

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    echo "Error: Docker is installed but the daemon is not running."
    echo ""
    echo "Please start Docker Desktop (macOS/Windows) or the Docker service (Linux):"
    echo "  macOS/Windows: Open Docker Desktop application"
    echo "  Linux: sudo systemctl start docker"
    echo ""
    echo "Alternatively, set USE_ACR_BUILD=true to use Azure Container Registry Build (no Docker required)"
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

# Skip Azure resource creation for image update only
if [ "$DEPLOYMENT_CHOICE" != "4" ]; then
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
    
    # Deploy GPT-5.2-chat model if not already deployed
    echo "Checking for GPT-5.2-chat deployment..."
    if ! az cognitiveservices account deployment show \
        --name "$OPENAI_DEPLOYMENT_NAME" \
        --account-name "$OPENAI_RESOURCE_NAME" \
        --resource-group "$RESOURCE_GROUP" &> /dev/null; then
        echo "Deploying GPT-5.2-chat model (this may take several minutes)..."
        az cognitiveservices account deployment create \
            --name "$OPENAI_DEPLOYMENT_NAME" \
            --account-name "$OPENAI_RESOURCE_NAME" \
            --resource-group "$RESOURCE_GROUP" \
            --model-format OpenAI \
            --model-name gpt-5.2-chat \
            --sku-capacity 10 \
            --sku-name "Standard" \
            --output none
        echo "✓ GPT-5.2-chat deployment created"
    else
        echo "✓ GPT-5.2-chat deployment already exists"
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
    
    # Create workspace without repository configuration
    # Note: Repository configuration is optional and not needed for this deployment
    # Using --only-show-errors to suppress repository-related warnings
    az synapse workspace create \
        --name "$SYNAPSE_WORKSPACE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --storage-account "$SYNAPSE_STORAGE_ACCOUNT" \
        --file-system "$SYNAPSE_FILE_SYSTEM" \
        --sql-admin-login-user "$SYNAPSE_ADMIN_USER" \
        --sql-admin-login-password "$SYNAPSE_ADMIN_PASSWORD" \
        --location "$LOCATION" \
        --only-show-errors \
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

# Create container registry if it doesn't exist (needed for all options)
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
fi  # End of skip resource creation for option 4

# Build Docker images
echo ""
echo "Ensuring Git LFS files are fetched..."
if command -v git-lfs &> /dev/null; then
    git lfs fetch --all
    git lfs pull
    echo "✓ Git LFS files fetched"
else
    echo "⚠ git-lfs not found. If src_data/*.parquet files are LFS-tracked, they may not be included in the build."
    echo "  Install git-lfs: https://git-lfs.github.com/"
fi
echo ""

if [ "$USE_ACR_BUILD" = "true" ]; then
    echo "Building Docker images using Azure Container Registry Build..."
    echo "Packing and uploading source code (excluding files in .dockerignore)..."
    
    # Function to retry ACR build on failure
    acr_build_with_retry() {
        local registry=$1
        local image=$2
        local platform=$3
        local dockerfile=$4
        local max_retries=3
        local retry_count=0
        
        while [ $retry_count -lt $max_retries ]; do
            if [ $retry_count -gt 0 ]; then
                echo "Retry attempt $retry_count of $max_retries..."
                sleep 5
            fi
            
            if az acr build \
                --registry "$registry" \
                --image "$image" \
                --platform "$platform" \
                --file "$dockerfile" \
                --timeout 3600 \
                .; then
                return 0
            else
                retry_count=$((retry_count + 1))
                if [ $retry_count -ge $max_retries ]; then
                    echo "Error: Build failed after $max_retries attempts"
                    return 1
                fi
            fi
        done
    }
    
    if [ "$IMAGE_BUILD_CHOICE" = "1" ] || [ "$IMAGE_BUILD_CHOICE" = "3" ]; then
        echo "Building API image using ACR Build (Linux/AMD64)..."
        if acr_build_with_retry "$CONTAINER_REGISTRY" "$API_IMAGE_NAME:$IMAGE_TAG" "linux/amd64" "Dockerfile"; then
            API_FULL_IMAGE_NAME="$CONTAINER_REGISTRY.azurecr.io/$API_IMAGE_NAME:$IMAGE_TAG"
            echo "✓ API image built and pushed successfully"
        else
            echo "✗ Failed to build API image after retries"
            exit 1
        fi
    fi
    
    if [ "$IMAGE_BUILD_CHOICE" = "2" ] || [ "$IMAGE_BUILD_CHOICE" = "3" ]; then
        echo "Building Streamlit image using ACR Build (Linux/AMD64)..."
        if acr_build_with_retry "$CONTAINER_REGISTRY" "$STREAMLIT_IMAGE_NAME:$IMAGE_TAG" "linux/amd64" "Dockerfile.streamlit"; then
            STREAMLIT_FULL_IMAGE_NAME="$CONTAINER_REGISTRY.azurecr.io/$STREAMLIT_IMAGE_NAME:$IMAGE_TAG"
            echo "✓ Streamlit image built and pushed successfully"
        else
            echo "✗ Failed to build Streamlit image after retries"
            exit 1
        fi
    fi
else
    echo "Building Docker images locally (Linux/AMD64 platform)..."
    echo "Note: Building for Linux/AMD64 to ensure compatibility with Azure Container Instances"
    
    # Build locally using Docker with Linux/AMD64 platform
    if [ "$IMAGE_BUILD_CHOICE" = "1" ] || [ "$IMAGE_BUILD_CHOICE" = "3" ]; then
        echo "Building API image locally..."
        docker build --platform linux/amd64 -t "$API_IMAGE_NAME:$IMAGE_TAG" .
        echo "✓ API image built locally"
    fi
    
    if [ "$IMAGE_BUILD_CHOICE" = "2" ] || [ "$IMAGE_BUILD_CHOICE" = "3" ]; then
        echo "Building Streamlit image locally..."
        docker build --platform linux/amd64 -f Dockerfile.streamlit -t "$STREAMLIT_IMAGE_NAME:$IMAGE_TAG" .
        echo "✓ Streamlit image built locally"
    fi
    
    echo "Logging into container registry..."
    az acr login --name "$CONTAINER_REGISTRY"
    
    # Push images to registry
    if [ "$IMAGE_BUILD_CHOICE" = "1" ] || [ "$IMAGE_BUILD_CHOICE" = "3" ]; then
        echo "Pushing API image to registry..."
        API_FULL_IMAGE_NAME="$CONTAINER_REGISTRY.azurecr.io/$API_IMAGE_NAME:$IMAGE_TAG"
        docker tag "$API_IMAGE_NAME:$IMAGE_TAG" "$API_FULL_IMAGE_NAME"
        docker push "$API_FULL_IMAGE_NAME"
        echo "✓ API image pushed successfully"
    fi
    
    if [ "$IMAGE_BUILD_CHOICE" = "2" ] || [ "$IMAGE_BUILD_CHOICE" = "3" ]; then
        echo "Pushing Streamlit image to registry..."
        STREAMLIT_FULL_IMAGE_NAME="$CONTAINER_REGISTRY.azurecr.io/$STREAMLIT_IMAGE_NAME:$IMAGE_TAG"
        docker tag "$STREAMLIT_IMAGE_NAME:$IMAGE_TAG" "$STREAMLIT_FULL_IMAGE_NAME"
        docker push "$STREAMLIT_FULL_IMAGE_NAME"
        echo "✓ Streamlit image pushed successfully"
    fi
fi

# Get registry credentials
echo ""
echo "Retrieving registry credentials..."
admin_enabled=$(az acr show --name "$CONTAINER_REGISTRY" --query "adminUserEnabled" -o tsv 2>/dev/null || echo "false")
if [ "$admin_enabled" != "true" ]; then
    echo "ACR admin user is disabled. Enabling admin user to retrieve credentials..."
    az acr update --name "$CONTAINER_REGISTRY" --admin-enabled true --output none
    echo "Waiting for admin user to be enabled..."
    sleep 3
fi

ACR_USERNAME=$(az acr credential show --name "$CONTAINER_REGISTRY" --query username -o tsv 2>/dev/null)
ACR_PASSWORD=$(az acr credential show --name "$CONTAINER_REGISTRY" --query "passwords[0].value" -o tsv 2>/dev/null)

if [ -z "$ACR_USERNAME" ] || [ -z "$ACR_PASSWORD" ]; then
    echo "⚠️  Warning: Failed to retrieve ACR credentials"
    echo "  Username: ${ACR_USERNAME:-empty}"
    echo "  Password: ${ACR_PASSWORD:+***set***}${ACR_PASSWORD:-empty}"
    echo ""
    echo "Trying to enable admin user again..."
    az acr update --name "$CONTAINER_REGISTRY" --admin-enabled true --output none
    sleep 5
    ACR_USERNAME=$(az acr credential show --name "$CONTAINER_REGISTRY" --query username -o tsv 2>/dev/null)
    ACR_PASSWORD=$(az acr credential show --name "$CONTAINER_REGISTRY" --query "passwords[0].value" -o tsv 2>/dev/null)
    
    if [ -z "$ACR_USERNAME" ] || [ -z "$ACR_PASSWORD" ]; then
        echo "✗ Still unable to retrieve credentials. Manual intervention may be required."
        echo "  Run: az acr credential show --name $CONTAINER_REGISTRY"
    else
        echo "✓ Credentials retrieved successfully"
    fi
else
    echo "✓ Registry credentials retrieved"
fi
if [ -z "$ACR_USERNAME" ] || [ -z "$ACR_PASSWORD" ]; then
    echo "✗ Failed to retrieve ACR credentials."
    echo ""
    echo "Manual commands:"
    echo "  az acr update --name $CONTAINER_REGISTRY --admin-enabled true"
    echo "  az acr credential show --name $CONTAINER_REGISTRY"
    echo ""
    echo "Press Enter to continue with the deployment script (you can configure credentials manually later), or Ctrl+C to abort..."
    read -r
fi

# Update existing containers/web apps with new images (option 4)
if [ "$DEPLOYMENT_CHOICE" = "4" ]; then
    echo ""
    echo "=================================="
    echo "Updating Container Images"
    echo "=================================="
    
    # Get registry credentials if not already set
    if [ -z "$ACR_USERNAME" ] || [ -z "$ACR_PASSWORD" ]; then
        echo "Retrieving registry credentials..."
        ACR_USERNAME=$(az acr credential show --name "$CONTAINER_REGISTRY" --query username -o tsv)
        ACR_PASSWORD=$(az acr credential show --name "$CONTAINER_REGISTRY" --query "passwords[0].value" -o tsv)
    fi
    
    # Update API container
    if [ "$UPDATE_API" = true ]; then
        echo ""
        echo "Updating API container with new image..."
        API_FULL_IMAGE_NAME="$CONTAINER_REGISTRY.azurecr.io/$API_IMAGE_NAME:$IMAGE_TAG"
        
        # Update container image
        UPDATE_OUTPUT=$(az container update \
            --resource-group "$RESOURCE_GROUP" \
            --name "$API_CONTAINER_NAME" \
            --image "$API_FULL_IMAGE_NAME" \
            --registry-login-server "$CONTAINER_REGISTRY.azurecr.io" \
            --registry-username "$ACR_USERNAME" \
            --registry-password "$ACR_PASSWORD" \
            --output json 2>&1)
        
        if [ $? -eq 0 ]; then
            echo "✓ API container image updated"
            echo "Restarting container..."
            az container restart --resource-group "$RESOURCE_GROUP" --name "$API_CONTAINER_NAME" --output none
            echo "✓ API container restarted"
        else
            echo "✗ Failed to update API container"
            echo "Error: $UPDATE_OUTPUT"
        fi
    fi
    
    # Update Streamlit web app
    if [ "$UPDATE_STREAMLIT" = true ]; then
        echo ""
        echo "Updating Streamlit web app with new image..."
        # Use the image name that was built (should already be set, but ensure it's correct)
        if [ -z "$STREAMLIT_FULL_IMAGE_NAME" ]; then
            STREAMLIT_FULL_IMAGE_NAME="$CONTAINER_REGISTRY.azurecr.io/$STREAMLIT_IMAGE_NAME:$IMAGE_TAG"
        fi
        
        # Update web app container image
        UPDATE_OUTPUT=$(az webapp config container set \
            --name "$STREAMLIT_APP_NAME" \
            --resource-group "$RESOURCE_GROUP" \
            --docker-custom-image-name "$STREAMLIT_FULL_IMAGE_NAME" \
            --docker-registry-server-url "https://$CONTAINER_REGISTRY.azurecr.io" \
            --docker-registry-server-user "$ACR_USERNAME" \
            --docker-registry-server-password "$ACR_PASSWORD" \
            --output json 2>&1)
        
        if [ $? -eq 0 ]; then
            echo "✓ Streamlit web app image updated"
            echo "Restarting web app..."
            az webapp restart --resource-group "$RESOURCE_GROUP" --name "$STREAMLIT_APP_NAME" --output none
            echo "✓ Streamlit web app restarted"
        else
            echo "✗ Failed to update Streamlit web app"
            echo "Error: $UPDATE_OUTPUT"
        fi
    fi
    
    echo ""
    echo "=================================="
    echo "Image Update Complete!"
    echo "=================================="
    echo ""
    if [ "$UPDATE_API" = true ]; then
        API_FQDN=$(az container show --resource-group "$RESOURCE_GROUP" --name "$API_CONTAINER_NAME" --query ipAddress.fqdn -o tsv 2>/dev/null)
        if [ -n "$API_FQDN" ] && [ "$API_FQDN" != "null" ]; then
            echo "🚀 API URL: http://$API_FQDN:8000"
        fi
    fi
    if [ "$UPDATE_STREAMLIT" = true ]; then
        STREAMLIT_URL=$(az webapp show --name "$STREAMLIT_APP_NAME" --resource-group "$RESOURCE_GROUP" --query defaultHostName -o tsv 2>/dev/null)
        if [ -n "$STREAMLIT_URL" ] && [ "$STREAMLIT_URL" != "null" ]; then
            echo "🌐 Streamlit Dashboard: https://$STREAMLIT_URL"
        fi
    fi
    echo ""
    exit 0
fi

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

# Function to create container with detailed logging and error diagnostics
create_container() {
    local resource_group=$1
    local container_name=$2
    local image=$3
    local registry_server=$4
    local registry_user=$5
    local registry_pass=$6
    local dns_label=$7
    
    # Check if container already exists
    echo "Checking if container '$container_name' already exists..."
    if az container show --resource-group "$resource_group" --name "$container_name" &> /dev/null; then
        echo "✓ Container '$container_name' already exists. Checking status..."
        local container_state=$(az container show --resource-group "$resource_group" --name "$container_name" --query "containers[0].instanceView.currentState.state" -o tsv 2>/dev/null || echo "unknown")
        local container_ip=$(az container show --resource-group "$resource_group" --name "$container_name" --query "ipAddress.ip" -o tsv 2>/dev/null || echo "N/A")
        local container_fqdn=$(az container show --resource-group "$resource_group" --name "$container_name" --query "ipAddress.fqdn" -o tsv 2>/dev/null || echo "N/A")
        
        echo "  Current state: $container_state"
        if [ "$container_ip" != "N/A" ]; then
            echo "  IP Address: $container_ip"
        fi
        if [ "$container_fqdn" != "N/A" ]; then
            echo "  FQDN: $container_fqdn"
        fi
        echo ""
        echo "Skipping container creation. Using existing container."
        return 0
    fi
    
    echo ""
    echo "Attempting to create container with the following configuration:"
    echo "  Resource Group: $resource_group"
    echo "  Container Name: $container_name"
    echo "  Image: $image"
    echo "  Registry: $registry_server"
    echo "  DNS Label: $dns_label"
    echo "  CPU: 4 cores"
    echo "  Memory: 8 GB"
    echo "  OS Type: Linux"
    echo ""
    
    # Build the command for display (single-line format for easy copy-paste)
    local create_cmd="az container create --resource-group $resource_group --name $container_name --image $image --registry-login-server $registry_server --registry-username $registry_user --registry-password '$registry_pass' --dns-name-label $dns_label --os-type Linux --ports 8000 --cpu 4 --memory 8 --environment-variables AZURE_OPENAI_ENDPOINT=\"$AZURE_OPENAI_ENDPOINT\" AZURE_OPENAI_API_KEY=\"$AZURE_OPENAI_API_KEY\" AZURE_OPENAI_DEPLOYMENT_NAME=\"${AZURE_OPENAI_DEPLOYMENT_NAME:-gpt-5.2-chat}\" SYNAPSE_SPARK_POOL_NAME=\"$SYNAPSE_SPARK_POOL_NAME\" SYNAPSE_WORKSPACE_NAME=\"$SYNAPSE_WORKSPACE_NAME\" AZURE_SUBSCRIPTION_ID=\"$AZURE_SUBSCRIPTION_ID\" AZURE_RESOURCE_GROUP=\"$AZURE_RESOURCE_GROUP\" API_PORT=8000"
    
    # Create container with verbose output for debugging
    local create_output=$(az container create \
        --resource-group "$resource_group" \
        --name "$container_name" \
        --image "$image" \
        --registry-login-server "$registry_server" \
        --registry-username "$registry_user" \
        --registry-password "$registry_pass" \
        --dns-name-label "$dns_label" \
        --os-type Linux \
        --ports 8000 \
        --cpu 4 \
        --memory 8 \
        --environment-variables \
            AZURE_OPENAI_ENDPOINT="$AZURE_OPENAI_ENDPOINT" \
            AZURE_OPENAI_API_KEY="$AZURE_OPENAI_API_KEY" \
            AZURE_OPENAI_DEPLOYMENT_NAME="${AZURE_OPENAI_DEPLOYMENT_NAME:-gpt-5.2-chat}" \
            SYNAPSE_SPARK_POOL_NAME="$SYNAPSE_SPARK_POOL_NAME" \
            SYNAPSE_WORKSPACE_NAME="$SYNAPSE_WORKSPACE_NAME" \
            AZURE_SUBSCRIPTION_ID="$AZURE_SUBSCRIPTION_ID" \
            AZURE_RESOURCE_GROUP="$AZURE_RESOURCE_GROUP" \
            API_PORT=8000 \
        --output json 2>&1)
    
    local create_exit_code=$?
    
    # Check for errors in output (Azure CLI may return 0 even with errors)
    local has_error=false
    if echo "$create_output" | grep -qi "ERROR\|error\|InternalServerError" || [ $create_exit_code -ne 0 ]; then
        has_error=true
    fi
    
    # Also check if output contains error JSON structure
    if echo "$create_output" | jq -e '.error' &> /dev/null; then
        has_error=true
    fi
    
    if [ "$has_error" = true ]; then
        echo "✗ Container creation failed"
        echo ""
        echo "Raw output:"
        echo "$create_output"
        echo ""
        echo "Error details:"
        local error_message=$(echo "$create_output" | jq -r '.error.message // .error // "Unknown error"' 2>/dev/null || echo "$create_output")
        local error_code=$(echo "$create_output" | jq -r '.error.code // "Unknown"' 2>/dev/null || echo "Unknown")
        local activity_id=$(echo "$create_output" | jq -r '.error.details[0].trackingId // "N/A"' 2>/dev/null || echo "N/A")
        local correlation_id=$(echo "$create_output" | jq -r '.error.details[0].correlationId // "N/A"' 2>/dev/null || echo "N/A")
        
        echo "  Error Code: $error_code"
        echo "  Error Message: $error_message"
        if [ "$activity_id" != "N/A" ] || [ "$correlation_id" != "N/A" ]; then
            echo "  Activity ID: $activity_id"
            echo "  Correlation ID: $correlation_id"
        fi
        echo ""
        
        # Show diagnostic commands
        echo "=========================================="
        echo "Diagnostic Commands"
        echo "=========================================="
        echo ""
        echo "To deploy the container manually, run:"
        echo ""
        echo "$create_cmd"
        echo ""
        echo "Other diagnostic commands:"
        echo ""
        
        local registry_name="${registry_server%.azurecr.io}"
        local image_repo="${image%%:*}"
        
        echo "1. Check resource group:"
        echo "   az group show --name $resource_group --query '{name:name,location:location}' -o table"
        echo ""
        
        echo "2. Verify image exists in registry:"
        echo "   az acr repository show-tags --name $registry_name --repository $image_repo --output table"
        echo ""
        
        echo "3. List all repositories in registry:"
        echo "   az acr repository list --name $registry_name --output table"
        echo ""
        
        echo "4. Check resource quotas:"
        echo "   az vm list-usage --location $LOCATION --output table"
        echo ""
        
        echo "5. Check if container already exists:"
        echo "   az container show --resource-group $resource_group --name $container_name --query '{name:name,state:containers[0].instanceView.currentState.state}' -o table"
        echo ""
        
        echo "6. Check Azure service status:"
        echo "   Visit: https://status.azure.com/"
        echo ""
        
        if [ "$activity_id" != "N/A" ] || [ "$correlation_id" != "N/A" ]; then
            echo "7. Contact Azure support with:"
            echo "   Activity ID: $activity_id"
            echo "   Correlation ID: $correlation_id"
            echo ""
        fi
        
        echo "=========================================="
        echo ""
        echo "Press Enter to continue with the deployment script (you can deploy the container manually later), or Ctrl+C to abort..."
        read -r
        echo ""
        echo "⚠️  Continuing script execution. Container deployment will be skipped."
        return 2  # Special return code to indicate failure but continue script
    fi
    
    # Verify container was actually created
    echo "✓ Container creation command succeeded"
    sleep 5
    if az container show --resource-group "$resource_group" --name "$container_name" &> /dev/null; then
        echo "✓ Container verified in Azure"
        local container_state=$(az container show --resource-group "$resource_group" --name "$container_name" --query "containers[0].instanceView.currentState.state" -o tsv 2>/dev/null || echo "unknown")
        echo "  Container state: $container_state"
        return 0
    else
        echo "⚠️  Container creation reported success but container not found"
        echo "Output: $create_output"
        echo ""
        echo "=========================================="
        echo "Diagnostic Commands"
        echo "=========================================="
        echo ""
        echo "To deploy the container manually, run:"
        echo ""
        echo "$create_cmd"
        echo ""
        echo "Check container status:"
        echo "   az container show --resource-group $resource_group --name $container_name --output table"
        echo ""
        echo "=========================================="
        echo ""
        echo "Press Enter to continue with the deployment script (you can deploy the container manually later), or Ctrl+C to abort..."
        read -r
        echo ""
        echo "⚠️  Continuing script execution. Container deployment will be skipped."
        return 2  # Special return code to indicate failure but continue script
    fi
}

# Deploy API to Container Instances
if [ "$DEPLOYMENT_CHOICE" = "1" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    echo ""
    echo "=================================="
    echo "Deploying API to Azure Container Instances"
    echo "=================================="
    
    # Pre-deployment checks
    echo ""
    echo "Pre-deployment checks:"
    echo "  - Verifying image exists in registry..."
    if az acr repository show-tags --name "$CONTAINER_REGISTRY" --repository "$API_IMAGE_NAME" --output table &> /dev/null; then
        echo "    ✓ Image found in registry"
        az acr repository show-tags --name "$CONTAINER_REGISTRY" --repository "$API_IMAGE_NAME" --output table | head -5
    else
        echo "    ✗ Image not found in registry!"
        echo "    Available repositories:"
        az acr repository list --name "$CONTAINER_REGISTRY" --output table
        exit 1
    fi
    
    echo "  - Checking resource group quotas..."
    cpu_usage=$(az vm list-usage --location "$LOCATION" --query "[?name.value=='cores'].currentValue" -o tsv 2>/dev/null || echo "unknown")
    cpu_limit=$(az vm list-usage --location "$LOCATION" --query "[?name.value=='cores'].limit" -o tsv 2>/dev/null || echo "unknown")
    if [ "$cpu_usage" != "unknown" ] && [ "$cpu_limit" != "unknown" ]; then
        echo "    CPU usage: $cpu_usage / $cpu_limit cores"
        if [ "$cpu_usage" -gt $((cpu_limit - 4)) ]; then
            echo "    ⚠️  Warning: Low CPU quota remaining"
        fi
    fi
    
    echo ""
    create_container \
        "$RESOURCE_GROUP" \
        "$API_CONTAINER_NAME" \
        "$API_FULL_IMAGE_NAME" \
        "$CONTAINER_REGISTRY.azurecr.io" \
        "$ACR_USERNAME" \
        "$ACR_PASSWORD" \
        "$API_CONTAINER_NAME"
    
    container_result=$?
    
    if [ $container_result -eq 0 ]; then
        echo ""
        echo "✓ API container deployed successfully"
        
        # Get container details
        echo ""
        echo "Container details:"
        az container show \
            --resource-group "$RESOURCE_GROUP" \
            --name "$API_CONTAINER_NAME" \
            --query "{name:name,state:containers[0].instanceView.currentState.state,ip:ipAddress.ip,fqdn:ipAddress.fqdn}" \
            --output table
    elif [ $container_result -eq 2 ]; then
        echo ""
        echo "⚠️  API container deployment was skipped (user chose to continue)"
        echo "You can deploy it manually using the command shown above."
    else
        echo ""
        echo "✗ Failed to deploy API container"
        echo ""
        echo "The diagnostic commands and manual deployment command were shown above."
        echo "Please review the error details and try deploying manually if needed."
        echo ""
        echo "Continuing with remaining deployment steps..."
    fi
fi

# Function to deploy Streamlit web app with error handling
deploy_streamlit_app() {
    local resource_group=$1
    local app_name=$2
    local app_service_plan=$3
    local image_name=$4
    local registry_url=$5
    local registry_user=$6
    local registry_pass=$7
    local location=$8
    
    echo ""
    echo "Attempting to deploy Streamlit web app with the following configuration:"
    echo "  Resource Group: $resource_group"
    echo "  App Name: $app_name"
    echo "  App Service Plan: $app_service_plan"
    echo "  Image: $image_name"
    echo "  Registry: $registry_url"
    echo "  Location: $location"
    echo ""
    
    # Note: App Service Plan is checked/created before calling this function
    # No need to check again here
    
    # Check if web app already exists
    echo ""
    echo "Checking if Streamlit web app '$app_name' already exists..."
    local webapp_exists=false
    local webapp_state="unknown"
    local webapp_url="N/A"
    local health_check_passed=false
    
    if az webapp show --name "$app_name" --resource-group "$resource_group" &> /dev/null; then
        webapp_exists=true
        echo "✓ Streamlit web app '$app_name' already exists. Checking status..."
        webapp_state=$(az webapp show --name "$app_name" --resource-group "$resource_group" --query "state" -o tsv 2>/dev/null || echo "unknown")
        webapp_url=$(az webapp show --name "$app_name" --resource-group "$resource_group" --query "defaultHostName" -o tsv 2>/dev/null || echo "N/A")
        
        echo "  Current state: $webapp_state"
        if [ "$webapp_url" != "N/A" ] && [ "$webapp_url" != "null" ]; then
            echo "  URL: https://$webapp_url"
            echo "  Checking Streamlit health..."
            local health_check_output=$(curl -fsS --max-time 10 "https://$webapp_url/_stcore/health" 2>&1)
            local health_check_exit=$?
            
            if [ $health_check_exit -eq 0 ]; then
                echo "  ✓ Streamlit health check passed. App is healthy."
                health_check_passed=true
            else
                echo "  ⚠️  Streamlit health check failed (exit code: $health_check_exit)"
                echo "  Health check output: ${health_check_output:-timeout or connection refused}"
                echo "  Reconfiguring existing app to fix issues..."
                health_check_passed=false
            fi
        else
            echo "  ⚠️  Streamlit URL not available. Reconfiguring existing app."
            health_check_passed=false
        fi
        
        # If health check passed, we still need to verify container registry is configured
        # Don't skip - continue to verify/configure registry and settings
        if [ "$health_check_passed" = true ]; then
            echo ""
            echo "✓ Streamlit web app is healthy. Verifying configuration..."
        else
            # If we get here, web app exists but health check failed - continue to reconfigure
            echo ""
            echo "Proceeding to reconfigure Streamlit web app..."
        fi
    fi
    
    # Configure registry and settings whether web app is new or existing
    if [ "$webapp_exists" = false ]; then
        # App Service Plan is already verified before calling this function
        # Proceed directly to creating the web app
        
        # Create web app with container image (must specify for Linux container apps)
        echo ""
        echo "Creating Streamlit web app..."
        local create_cmd="az webapp create --name $app_name --resource-group $resource_group --plan $app_service_plan --deployment-container-image-name $image_name"
        
        local webapp_output=$(az webapp create \
            --name "$app_name" \
            --resource-group "$resource_group" \
            --plan "$app_service_plan" \
            --deployment-container-image-name "$image_name" \
            --output json 2>&1)
        
        local webapp_exit_code=$?
        
        # Check for errors (ignore warnings, only check for actual errors)
        local has_error=false
        local is_network_error=false
        
        # Check for network/connection errors
        if echo "$webapp_output" | grep -qi "Connection.*reset\|Connection.*aborted\|Connection.*refused\|timeout"; then
            is_network_error=true
            has_error=true
        fi
        
        # Check if output contains actual error (not just warnings)
        if echo "$webapp_output" | grep -qi "^ERROR:" && ! echo "$webapp_output" | jq -e '.name' &> /dev/null; then
            has_error=true
        fi
        
        # Check for error JSON structure
        if echo "$webapp_output" | jq -e '.error' &> /dev/null; then
            has_error=true
        fi
        
        # Check if webapp was created successfully by looking for the name in JSON
        if echo "$webapp_output" | jq -e '.name' &> /dev/null; then
            local created_name=$(echo "$webapp_output" | jq -r '.name' 2>/dev/null)
            if [ "$created_name" = "$app_name" ]; then
                has_error=false
                is_network_error=false
            fi
        fi
        
        if [ "$has_error" = true ]; then
            if [ "$is_network_error" = true ]; then
                echo "✗ Streamlit web app deployment failed due to network error"
                echo ""
                echo "This appears to be a transient network connection issue."
                echo "The connection to Azure was reset during deployment."
                echo ""
                echo "Recommended actions:"
                echo "  1. Wait a moment and run the deployment script again"
                echo "  2. Check your internet connection"
                echo "  3. Check Azure service status: https://status.azure.com/"
                echo ""
            else
                echo "✗ Streamlit web app deployment failed"
                echo ""
                echo "Raw output:"
                echo "$webapp_output"
                echo ""
                echo "Error details:"
                local error_message=$(echo "$webapp_output" | jq -r '.error.message // .error // "Unknown error"' 2>/dev/null || echo "$webapp_output")
                local error_code=$(echo "$webapp_output" | jq -r '.error.code // "Unknown"' 2>/dev/null || echo "Unknown")
                local activity_id=$(echo "$webapp_output" | jq -r '.error.details[0].trackingId // "N/A"' 2>/dev/null || echo "N/A")
                local correlation_id=$(echo "$webapp_output" | jq -r '.error.details[0].correlationId // "N/A"' 2>/dev/null || echo "N/A")
                
                echo "  Error Code: $error_code"
                echo "  Error Message: $error_message"
                if [ "$activity_id" != "N/A" ] || [ "$correlation_id" != "N/A" ]; then
                    echo "  Activity ID: $activity_id"
                    echo "  Correlation ID: $correlation_id"
                fi
                echo ""
            fi
            
            # Show diagnostic commands
            echo "=========================================="
            echo "Diagnostic Commands"
            echo "=========================================="
            echo ""
            echo "To deploy the Streamlit app manually, run:"
            echo ""
            echo "$create_cmd"
            echo ""
            echo "Then configure registry credentials:"
            echo "  az webapp config container set --name $app_name --resource-group $resource_group --docker-custom-image-name $image_name --docker-registry-server-url $registry_url --docker-registry-server-user $registry_user --docker-registry-server-password '$registry_pass'"
            echo ""
            echo "Other diagnostic commands:"
            echo ""
            
            local registry_name="${registry_url#https://}"
            registry_name="${registry_name%.azurecr.io}"
            
            echo "1. Check resource group:"
            echo "   az group show --name $resource_group --query '{name:name,location:location}' -o table"
            echo ""
        
        echo "2. Verify image exists in registry:"
        echo "   az acr repository show-tags --name $registry_name --repository ${image_name%%:*} --output table"
        echo ""
        
        echo "3. Check App Service Plan:"
        echo "   az appservice plan show --name $app_service_plan --resource-group $resource_group --output table"
        echo ""
        
        echo "4. Check if web app exists:"
        echo "   az webapp show --name $app_name --resource-group $resource_group --query '{name:name,state:state}' -o table"
        echo ""
        
        echo "5. Check Azure service status:"
        echo "   Visit: https://status.azure.com/"
        echo ""
        
        if [ "$activity_id" != "N/A" ] || [ "$correlation_id" != "N/A" ]; then
            echo "6. Contact Azure support with:"
            echo "   Activity ID: $activity_id"
            echo "   Correlation ID: $correlation_id"
            echo ""
        fi
        
        echo "=========================================="
        echo ""
        echo "Press Enter to continue with the deployment script (you can deploy the app manually later), or Ctrl+C to abort..."
        read -r
        echo ""
        echo "⚠️  Continuing script execution. Streamlit deployment will be skipped."
        return 2
    fi
    
    fi  # Close if [ "$webapp_exists" = false ]
    
    if [ "$webapp_exists" = false ]; then
        echo "✓ Streamlit web app created"
    else
        echo "✓ Streamlit web app exists, reconfiguring"
    fi
    
    # Verify credentials are not empty
    if [ -z "$registry_user" ] || [ -z "$registry_pass" ]; then
        echo "✗ Container registry credentials are missing!"
        echo "  Username: ${registry_user:-empty}"
        echo "  Password: ${registry_pass:+***set***}${registry_pass:-empty}"
        echo ""
        echo "Please ensure ACR admin user is enabled and credentials are available."
        echo "Run: az acr update --name ${registry_url#https://} --admin-enabled true"
        return 2
    fi
    
    # Verify image exists in registry
    echo ""
    echo "Verifying image exists in registry..."
    local registry_name="${registry_url#https://}"
    registry_name="${registry_name%.azurecr.io}"
    local image_repo="${image_name%%:*}"
    local image_tag="${image_name##*:}"
    
    if ! az acr repository show-tags --name "$registry_name" --repository "$image_repo" --query "[?contains(@, '$image_tag')]" -o tsv &> /dev/null; then
        echo "⚠️  Warning: Image tag '$image_tag' not found in repository '$image_repo'"
        echo "Available tags:"
        az acr repository show-tags --name "$registry_name" --repository "$image_repo" --output table 2>/dev/null || echo "  (unable to list tags)"
        echo ""
        echo "The image may not have been pushed successfully. Check build logs above."
    else
        echo "✓ Image found in registry"
    fi
    
    # Configure container registry credentials (must be done after webapp creation)
    echo ""
    echo "Configuring container registry credentials..."
    local registry_output=$(az webapp config container set \
        --name "$app_name" \
        --resource-group "$resource_group" \
        --docker-custom-image-name "$image_name" \
        --docker-registry-server-url "$registry_url" \
        --docker-registry-server-user "$registry_user" \
        --docker-registry-server-password "$registry_pass" \
        --output json 2>&1)
    
    local registry_exit_code=$?
    
    if [ $registry_exit_code -ne 0 ]; then
        echo "✗ Failed to configure container registry"
        echo "Error: $registry_output"
        echo ""
        echo "Manual command to configure:"
        echo "  az webapp config container set --name $app_name --resource-group $resource_group --docker-custom-image-name $image_name --docker-registry-server-url $registry_url --docker-registry-server-user $registry_user --docker-registry-server-password '$registry_pass'"
        echo ""
        echo "Verify credentials:"
        echo "  az acr credential show --name $registry_name"
        return 2
    else
        echo "✓ Container registry configured"
        
        # Verify configuration was applied
        echo "Verifying container configuration..."
        local configured_image=$(az webapp config container show \
            --name "$app_name" \
            --resource-group "$resource_group" \
            --query "[?name=='DOCKER_CUSTOM_IMAGE_NAME'].value" -o tsv 2>/dev/null || echo "")
        
        if [ -n "$configured_image" ]; then
            echo "✓ Container image verified: $configured_image"
        else
            echo "⚠️  Warning: Could not verify container image configuration"
        fi
    fi
    
    # Configure app settings
    echo ""
    echo "Configuring app settings..."
    local settings_output=$(az webapp config appsettings set \
        --name "$app_name" \
        --resource-group "$resource_group" \
        --settings \
            API_URL="${API_URL:-http://localhost:8000}" \
            AZURE_OPENAI_ENDPOINT="$AZURE_OPENAI_ENDPOINT" \
            AZURE_OPENAI_API_KEY="$AZURE_OPENAI_API_KEY" \
            AZURE_OPENAI_DEPLOYMENT_NAME="${AZURE_OPENAI_DEPLOYMENT_NAME:-gpt-5.2-chat}" \
            SYNAPSE_SPARK_POOL_NAME="$SYNAPSE_SPARK_POOL_NAME" \
            SYNAPSE_WORKSPACE_NAME="$SYNAPSE_WORKSPACE_NAME" \
            AZURE_SUBSCRIPTION_ID="$AZURE_SUBSCRIPTION_ID" \
            AZURE_RESOURCE_GROUP="$AZURE_RESOURCE_GROUP" \
            STREAMLIT_PORT=8501 \
            STREAMLIT_HOST=0.0.0.0 \
        --output json 2>&1)
    
    if [ $? -ne 0 ]; then
        echo "⚠️  Warning: Failed to configure app settings"
        echo "You can configure them manually later"
    else
        echo "✓ App settings configured"
    fi
    
    # Configure startup command
    echo ""
    echo "Configuring startup command..."
    local startup_output=$(az webapp config set \
        --name "$app_name" \
        --resource-group "$resource_group" \
        --startup-file "streamlit run streamlit_app.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true --browser.gatherUsageStats=false" \
        --output json 2>&1)
    
    if [ $? -ne 0 ]; then
        echo "⚠️  Warning: Failed to configure startup command"
        echo "You can configure it manually later"
    else
        echo "✓ Startup command configured"
    fi
    
    echo ""
    echo "Checking Streamlit health after deployment..."
    local final_webapp_url=$(az webapp show --name "$app_name" --resource-group "$resource_group" --query "defaultHostName" -o tsv 2>/dev/null || echo "N/A")
    if [ -n "$final_webapp_url" ] && [ "$final_webapp_url" != "N/A" ] && [ "$final_webapp_url" != "null" ]; then
        local health_ok=false
        for attempt in 1 2 3; do
            if curl -fsS --max-time 10 "https://$final_webapp_url/_stcore/health" &> /dev/null; then
                health_ok=true
                break
            fi
            echo "  Streamlit health check attempt $attempt failed. Retrying in 10s..."
            sleep 10
        done
        
        if [ "$health_ok" = true ]; then
            echo "✓ Streamlit web app deployed successfully"
            return 0
        else
            echo "⚠️  Streamlit web app is not healthy yet."
            echo "  URL: https://$final_webapp_url"
            echo "  You may need to wait a few minutes or check logs."
            return 2
        fi
    else
        echo "⚠️  Streamlit web app URL not available to verify health."
        return 2
    fi
}

# Deploy Streamlit to App Service
if [ "$DEPLOYMENT_CHOICE" = "2" ] || [ "$DEPLOYMENT_CHOICE" = "3" ]; then
    echo ""
    echo "=================================="
    echo "Deploying Streamlit to Azure App Service"
    echo "=================================="
    
    # Create App Service Plan if it doesn't exist
    echo ""
    echo "Checking App Service Plan..."
    if ! az appservice plan show --name "$APP_SERVICE_PLAN" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
        echo "Creating App Service Plan '$APP_SERVICE_PLAN'..."
        
        plan_output=$(az appservice plan create \
            --name "$APP_SERVICE_PLAN" \
            --resource-group "$RESOURCE_GROUP" \
            --location "$LOCATION" \
            --is-linux \
            --sku B1 \
            --output json 2>&1)
        
        plan_exit_code=$?
        
        if [ $plan_exit_code -eq 0 ]; then
            echo "✓ App Service Plan created"
            echo "Waiting for plan to be ready..."
            sleep 5
        else
            echo "✗ Failed to create App Service Plan"
            echo ""
            echo "Error output:"
            echo "$plan_output"
            echo ""
            echo "=========================================="
            echo "Manual Command to Create App Service Plan"
            echo "=========================================="
            echo ""
            echo "az appservice plan create --name $APP_SERVICE_PLAN --resource-group $RESOURCE_GROUP --location $LOCATION --is-linux --sku B1"
            echo ""
            echo "=========================================="
            echo ""
            echo "Press Enter to continue (you can create the plan manually later), or Ctrl+C to abort..."
            read -r
            echo ""
            echo "Skipping Streamlit deployment..."
        fi
    else
        echo "✓ App Service Plan already exists"
    fi
    
    # Only proceed with deployment if plan exists
    if az appservice plan show --name "$APP_SERVICE_PLAN" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
        # Get API URL for Streamlit to connect to
        API_URL="http://localhost:8000"  # Default
        if [ "$IMAGE_BUILD_CHOICE" = "1" ] || [ "$IMAGE_BUILD_CHOICE" = "3" ]; then
            # Get API container FQDN if API was deployed
            if az container show --name "$API_CONTAINER_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
                API_FQDN=$(az container show --resource-group "$RESOURCE_GROUP" --name "$API_CONTAINER_NAME" --query "ipAddress.fqdn" -o tsv 2>/dev/null)
                if [ -n "$API_FQDN" ] && [ "$API_FQDN" != "null" ]; then
                    API_URL="http://$API_FQDN:8000"
                    echo "✓ API URL for Streamlit: $API_URL"
                fi
            fi
        fi
        
        # Export API_URL for use in deployment function
        export API_URL
        
        echo ""
        echo "Calling deploy_streamlit_app function..."
        deploy_streamlit_app \
            "$RESOURCE_GROUP" \
            "$STREAMLIT_APP_NAME" \
            "$APP_SERVICE_PLAN" \
            "$STREAMLIT_FULL_IMAGE_NAME" \
            "https://$CONTAINER_REGISTRY.azurecr.io" \
            "$ACR_USERNAME" \
            "$ACR_PASSWORD" \
            "$LOCATION"
        
        streamlit_result=$?
        echo "deploy_streamlit_app returned exit code: $streamlit_result"
        
        if [ $streamlit_result -eq 0 ]; then
            echo ""
            echo "✓ Streamlit web app deployed successfully"
        elif [ $streamlit_result -eq 2 ]; then
            echo ""
            echo "⚠️  Streamlit deployment was skipped (user chose to continue)"
            echo "You can deploy it manually using the command shown above."
        else
            echo ""
            echo "✗ Failed to deploy Streamlit web app"
            echo ""
            echo "The diagnostic commands and manual deployment command were shown above."
            echo "Please review the error details and try deploying manually if needed."
            echo ""
            echo "Continuing with remaining deployment steps..."
        fi
    fi
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
    # Check if Streamlit web app exists before trying to get URL
    if az webapp show --name "$STREAMLIT_APP_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
        STREAMLIT_URL=$(az webapp show --name "$STREAMLIT_APP_NAME" --resource-group "$RESOURCE_GROUP" --query defaultHostName -o tsv 2>/dev/null)
        if [ -n "$STREAMLIT_URL" ] && [ "$STREAMLIT_URL" != "null" ]; then
            echo "🌐 Streamlit Dashboard: https://$STREAMLIT_URL"
        else
            echo "⚠️  Streamlit web app deployment was skipped or failed"
        fi
    else
        echo "⚠️  Streamlit web app was not deployed"
    fi
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

