#!/bin/bash
# Deployment script for Streamlit app to CRUSOE
set -e

echo "=================================="
echo "Financial Analysis Streamlit - CRUSOE Deployment"
echo "=================================="

# Configuration
PROJECT_ID="${CRUSOE_PROJECT_ID:-project-financial-analysis}"
LOCATION="${CRUSOE_LOCATION:-us-east-1}"
APP_SERVICE_PLAN="${APP_SERVICE_PLAN:-financial-analysis-plan}"
APP_NAME="${APP_NAME:-financial-analysis-streamlit}"
CONTAINER_REGISTRY="${CRUSOE_CONTAINER_REGISTRY:-financialanalysisacr}"
IMAGE_NAME="financial-analysis-streamlit"
IMAGE_TAG="${IMAGE_TAG:-latest}"

echo ""
echo "Configuration:"
echo "  Project ID: $PROJECT_ID"
echo "  Location: $LOCATION"
echo "  App Service Plan: $APP_SERVICE_PLAN"
echo "  App Name: $APP_NAME"
echo "  Container Registry: $CONTAINER_REGISTRY"
echo "  Image: $IMAGE_NAME:$IMAGE_TAG"
echo ""

# Check CRUSOE CLI
if ! command -v crusoe &> /dev/null; then
    echo "Error: CRUSOE CLI not found. Please install it first."
    exit 1
fi

# Login check
echo "Checking CRUSOE login..."
crusoe account show > /dev/null 2>&1 || crusoe login

# Create project if it doesn't exist
echo "Ensuring project exists..."
crusoe resource-group create \
    --name "$PROJECT_ID" \
    --location "$LOCATION" \
    --output none

echo "✓ Project ready"

# Create container registry if it doesn't exist
echo "Checking container registry..."
if ! crusoe container-registry show --name "$CONTAINER_REGISTRY" --project "$PROJECT_ID" &> /dev/null; then
    echo "Creating container registry..."
    crusoe container-registry create \
        --name "$CONTAINER_REGISTRY" \
        --project "$PROJECT_ID" \
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
crusoe container-registry login --name "$CONTAINER_REGISTRY"

echo "Tagging image..."
FULL_IMAGE_NAME="$CONTAINER_REGISTRY.registry.crusoe.ai/$IMAGE_NAME:$IMAGE_TAG"
docker tag "$IMAGE_NAME:$IMAGE_TAG" "$FULL_IMAGE_NAME"

echo "Pushing image to registry..."
docker push "$FULL_IMAGE_NAME"

echo "✓ Image pushed successfully"

# Get registry credentials
echo ""
echo "Retrieving registry credentials..."
CRUSOE_USERNAME=$(crusoe container-registry credential show --name "$CONTAINER_REGISTRY" --project "$PROJECT_ID" --query username -o tsv)
CRUSOE_PASSWORD=$(crusoe container-registry credential show --name "$CONTAINER_REGISTRY" --project "$PROJECT_ID" --query "passwords[0].value" -o tsv)

# Create App Service Plan
echo ""
echo "Creating App Service Plan..."
if ! crusoe app-service-plan show --name "$APP_SERVICE_PLAN" --project "$PROJECT_ID" &> /dev/null; then
    crusoe app-service-plan create \
        --name "$APP_SERVICE_PLAN" \
        --project "$PROJECT_ID" \
        --location "$LOCATION" \
        --is-linux \
        --sku B2 \
        --output none
    echo "✓ App Service Plan created"
else
    echo "✓ App Service Plan exists"
fi

# Check if environment variables are set
if [ -z "$CRUSOE_AI_ENDPOINT" ] || [ -z "$CRUSOE_AI_API_KEY" ]; then
    echo "Error: Required environment variables not set"
    echo "Please set:"
    echo "  - CRUSOE_AI_ENDPOINT"
    echo "  - CRUSOE_AI_API_KEY"
    exit 1
fi

# Create or update the web app
echo ""
echo "Creating/updating web app..."

if crusoe webapp show --name "$APP_NAME" --project "$PROJECT_ID" &> /dev/null; then
    echo "Updating existing web app..."
    crusoe webapp config container set \
        --name "$APP_NAME" \
        --project "$PROJECT_ID" \
        --docker-custom-image-name "$FULL_IMAGE_NAME" \
        --docker-registry-server-url "https://$CONTAINER_REGISTRY.registry.crusoe.ai" \
        --docker-registry-server-user "$CRUSOE_USERNAME" \
        --docker-registry-server-password "$CRUSOE_PASSWORD" \
        --output none
else
    echo "Creating new web app..."
    crusoe webapp create \
        --name "$APP_NAME" \
        --project "$PROJECT_ID" \
        --plan "$APP_SERVICE_PLAN" \
        --deployment-container-image-name "$FULL_IMAGE_NAME" \
        --docker-registry-server-url "https://$CONTAINER_REGISTRY.registry.crusoe.ai" \
        --docker-registry-server-user "$CRUSOE_USERNAME" \
        --docker-registry-server-password "$CRUSOE_PASSWORD" \
        --output none
fi

# Configure app settings
echo "Configuring app settings..."
crusoe webapp config appsettings set \
    --name "$APP_NAME" \
    --project "$PROJECT_ID" \
    --settings \
        CRUSOE_AI_ENDPOINT="$CRUSOE_AI_ENDPOINT" \
        CRUSOE_AI_API_KEY="$CRUSOE_AI_API_KEY" \
        CRUSOE_AI_MODEL_NAME="${CRUSOE_AI_MODEL_NAME:-gpt-5.2-chat}" \
        CRUSOE_COMPUTE_POOL_NAME="$CRUSOE_COMPUTE_POOL_NAME" \
        CRUSOE_DATA_PROJECT="$CRUSOE_DATA_PROJECT" \
        PROJECT_ID="$PROJECT_ID" \
        STREAMLIT_PORT=8501 \
        STREAMLIT_HOST=0.0.0.0 \
        STREAMLIT_THEME=light \
    --output none

# Configure startup command
echo "Setting startup command..."
crusoe webapp config set \
    --name "$APP_NAME" \
    --project "$PROJECT_ID" \
    --startup-file "streamlit run streamlit_app.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true --browser.gatherUsageStats=false" \
    --output none

# Get the web app URL
WEBAPP_URL=$(crusoe webapp show --name "$APP_NAME" --project "$PROJECT_ID" --query defaultHostname -o tsv)

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
echo "  View logs: crusoe webapp log tail --name $APP_NAME --project $PROJECT_ID"
echo "  Restart: crusoe webapp restart --name $APP_NAME --project $PROJECT_ID"
echo "  Delete: crusoe webapp delete --name $APP_NAME --project $PROJECT_ID"
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
