#!/bin/bash
# Deployment script for Streamlit app to AWS ECS/Fargate
set -e

echo "=================================="
echo "Financial Analysis Streamlit - AWS Deployment"
echo "=================================="

# Configuration
RESOURCE_GROUP="${AWS_RESOURCE_GROUP:-financial-analysis}"
REGION="${AWS_REGION:-us-east-1}"
CLUSTER_NAME="${CLUSTER_NAME:-financial-analysis-cluster}"
SERVICE_NAME="${SERVICE_NAME:-financial-analysis-streamlit}"
ECR_REPOSITORY="${ECR_REPOSITORY:-financial-analysis-streamlit}"
IMAGE_NAME="financial-analysis-streamlit"
IMAGE_TAG="${IMAGE_TAG:-latest}"

# AWS Account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo ""
echo "Configuration:"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  Region: $REGION"
echo "  Cluster: $CLUSTER_NAME"
echo "  Service: $SERVICE_NAME"
echo "  ECR Repository: $ECR_REPOSITORY"
echo "  Image: $IMAGE_NAME:$IMAGE_TAG"
echo "  Account ID: $ACCOUNT_ID"
echo ""

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI not found. Please install it first."
    exit 1
fi

# Login check
echo "Checking AWS login..."
aws sts get-caller-identity > /dev/null 2>&1 || aws sts get-caller-identity

# Create ECR repository if it doesn't exist
echo "Ensuring ECR repository exists..."
if ! aws ecr describe-repositories --repository-names "$ECR_REPOSITORY" --query 'repositories[0].repositoryName' --output text 2>/dev/null | grep -q "$ECR_REPOSITORY"; then
    echo "Creating ECR repository..."
    aws ecr create-repository --repository-name "$ECR_REPOSITORY" --region "$REGION" > /dev/null
    echo "✓ ECR repository created"
else
    echo "✓ ECR repository exists"
fi

# Build and push Docker image
echo ""
echo "Building Streamlit Docker image..."
docker build -f Dockerfile.streamlit -t "$IMAGE_NAME:$IMAGE_TAG" .

echo "Logging into ECR..."
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"

echo "Tagging image..."
FULL_IMAGE_NAME="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$ECR_REPOSITORY:$IMAGE_TAG"
docker tag "$IMAGE_NAME:$IMAGE_TAG" "$FULL_IMAGE_NAME"

echo "Pushing image to registry..."
docker push "$FULL_IMAGE_NAME"

echo "✓ Image pushed successfully"

# Create ECS cluster if it doesn't exist
echo ""
echo "Ensuring ECS cluster exists..."
if ! aws ecs describe-clusters --clusters "$CLUSTER_NAME" --query 'clusters[0].clusterName' --output text 2>/dev/null | grep -q "$CLUSTER_NAME"; then
    echo "Creating ECS cluster..."
    aws ecs create-cluster --cluster-name "$CLUSTER_NAME" --region "$REGION" > /dev/null
    echo "✓ ECS cluster created"
else
    echo "✓ ECS cluster exists"
fi

# Check if environment variables are set
if [ -z "$AWS_OPENAI_ENDPOINT" ] || [ -z "$AWS_OPENAI_API_KEY" ]; then
    echo "Error: Required environment variables not set"
    echo "Please set:"
    echo "  - AWS_OPENAI_ENDPOINT"
    echo "  - AWS_OPENAI_API_KEY"
    exit 1
fi

# Create or update the ECS service
echo ""
echo "Creating/updating ECS service..."

# Define task definition
TASK_DEFINITION="financial-analysis-streamlit-task"

cat > task-definition.json << EOF
{
    "family": "$TASK_DEFINITION",
    "networkMode": "awsvpc",
    "requiresCompatibilities": ["FARGATE"],
    "cpu": "256",
    "memory": "512",
    "executionRoleArn": "arn:aws:iam::$ACCOUNT_ID:role/ecsTaskExecutionRole",
    "containerDefinitions": [
        {
            "name": "$SERVICE_NAME",
            "image": "$FULL_IMAGE_NAME",
            "cpu": 256,
            "memory": 512,
            "essential": true,
            "portMappings": [
                {
                    "containerPort": 8501,
                    "hostPort": 8501,
                    "protocol": "tcp"
                }
            ],
            "environment": [
                {"name": "AWS_OPENAI_ENDPOINT", "value": "$AWS_OPENAI_ENDPOINT"},
                {"name": "AWS_OPENAI_API_KEY", "value": "$AWS_OPENAI_API_KEY"},
                {"name": "AWS_OPENAI_DEPLOYMENT_NAME", "value": "${AWS_OPENAI_DEPLOYMENT_NAME:-gpt-5.2-chat}"},
                {"name": "GLUE_SPARK_POOL_NAME", "value": "$GLUE_SPARK_POOL_NAME"},
                {"name": "GLUE_WORKSPACE_NAME", "value": "$GLUE_WORKSPACE_NAME"},
                {"name": "AWS_ACCOUNT_ID", "value": "$ACCOUNT_ID"},
                {"name": "AWS_RESOURCE_GROUP", "value": "$RESOURCE_GROUP"},
                {"name": "STREAMLIT_PORT", "value": "8501"},
                {"name": "STREAMLIT_HOST", "value": "0.0.0.0"},
                {"name": "STREAMLIT_THEME", "value": "light"}
            ],
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": "/ecs/$SERVICE_NAME",
                    "awslogs-region": "$REGION",
                    "awslogs-stream-prefix": "ecs"
                }
            }
        }
    ]
}
EOF

# Register task definition
aws ecs register-task-definition --cli-input-json "$(cat task-definition.json)" --region "$REGION" > /dev/null
echo "✓ Task definition registered"

# Create service if it doesn't exist
if ! aws ecs describe-services --cluster "$CLUSTER_NAME" --services "$SERVICE_NAME" --query 'services[0].serviceName' --output text 2>/dev/null | grep -q "$SERVICE_NAME"; then
    echo "Creating new ECS service..."
    aws ecs create-service \
        --cluster "$CLUSTER_NAME" \
        --service-name "$SERVICE_NAME" \
        --task-definition "$TASK_DEFINITION" \
        --desired-count 1 \
        --launch-type FARGATE \
        --network-configuration "awsvpcConfiguration={subnets=[${AWS_SUBNETS:-subnet-12345678}],securityGroups=[${AWS_SECURITY_GROUPS:-sg-12345678}],assignPublicIp=ENABLED}" \
        --region "$REGION" > /dev/null
    echo "✓ ECS service created"
else
    echo "Updating existing ECS service..."
    aws ecs update-service \
        --cluster "$CLUSTER_NAME" \
        --service "$SERVICE_NAME" \
        --task-definition "$TASK_DEFINITION" \
        --force-new-deployment \
        --region "$REGION" > /dev/null
    echo "✓ ECS service updated"
fi

# Get the service endpoint
SERVICE_ARN=$(aws ecs describe-services --cluster "$CLUSTER_NAME" --services "$SERVICE_NAME" --query 'services[0].serviceArn' --output text --region "$REGION")
LOAD_BALANCER_DNS=$(aws ecs describe-services --cluster "$CLUSTER_NAME" --services "$SERVICE_NAME" --query 'services[0].loadBalancers[0].targetGroupArn' --output text --region "$REGION" | xargs -I {} aws elbv2 describe-target-groups --target-group-arns {} --query 'TargetGroups[0].LoadBalancerArns[0]' --output text | xargs -I {} aws elbv2 describe-load-balancers --load-balancer-arns {} --query 'LoadBalancers[0].DNSName' --output text)

# Alternative: if no load balancer, use task IP (requires networking setup)
if [ -z "$LOAD_BALANCER_DNS" ]; then
    echo "⚠️  No load balancer configured. Service is accessible within VPC only."
    LOAD_BALANCER_DNS="<VPC-internal-IP>"
fi

echo ""
echo "=================================="
echo "Deployment Complete!"
echo "=================================="
echo ""
echo "🌐 Streamlit Dashboard: https://$LOAD_BALANCER_DNS"
echo ""
echo "📊 Features:"
echo "  - Natural language queries"
echo "  - Interactive visualizations"
echo "  - Query history"
echo "  - Example questions"
echo ""
echo "🔧 Management Commands:"
echo "  View logs: aws logs tail /ecs/$SERVICE_NAME --follow"
echo "  Restart: aws ecs update-service --cluster $CLUSTER_NAME --service $SERVICE_NAME --force-new-deployment --region $REGION"
echo "  Delete: aws ecs delete-service --cluster $CLUSTER_NAME --service $SERVICE_NAME --force --region $REGION"
echo ""

# Test the deployment
echo "Testing deployment..."
sleep 30  # Wait for app to start

if curl -f "https://$LOAD_BALANCER_DNS" > /dev/null 2>&1; then
    echo "✅ Deployment successful! App is responding."
else
    echo "⚠️  App may still be starting up. Check logs if issues persist."
fi

echo ""
echo "🎉 Your Financial Analysis Dashboard is ready!"
echo "Visit: https://$LOAD_BALANCER_DNS"
