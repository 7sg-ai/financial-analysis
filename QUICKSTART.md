# Quick Start Guide - Azure Deployment

Deploy the Financial Analysis API to Azure in 10 minutes.

## Prerequisites

- Azure subscription with appropriate permissions
- Azure OpenAI service deployed
- Azure Container Registry (ACR)
- Azure CLI installed and configured

## Azure Deployment Steps

### 1. Clone the Repository
```bash
git clone https://github.com/7sg-ai/financial-analysis.git
cd financial-analysis
```

### 2. Configure Azure Resources

**Set up Azure Container Registry:**
```bash
# Create resource group
az group create --name financial-analysis-rg --location eastus

# Create container registry
az acr create --resource-group financial-analysis-rg \
  --name financialanalysisacr --sku Basic --admin-enabled true
```

**Deploy Azure OpenAI (if not already done):**
```bash
# Create Azure OpenAI resource
az cognitiveservices account create \
  --name financial-analysis-openai \
  --resource-group financial-analysis-rg \
  --location eastus \
  --kind OpenAI \
  --sku S0
```

### 3. Deploy the Application

**Deploy both API and Streamlit UI:**
```bash
# Make deployment script executable
chmod +x deploy_azure.sh

# Deploy everything
./deploy_azure.sh --deploy-all
```

**Or deploy components separately:**
```bash
# Deploy API only
./deploy_azure.sh --deploy-api

# Deploy Streamlit UI only  
./deploy_azure.sh --deploy-ui
```

### 4. Configure Environment Variables

Set the following in your Azure App Service or Container Instance:

```bash
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-actual-api-key-here
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4
SYNAPSE_SPARK_POOL_NAME=your-spark-pool
SYNAPSE_WORKSPACE_NAME=your-workspace
```

### 5. Download Data in Azure

**SSH into your Azure container or use Azure Cloud Shell:**
```bash
# Download all data (2023-2025)
python3 download_data.py

# Or download specific subsets to manage costs
python3 download_data.py --years 2024 --service-types yellow --months 1 2 3
```

**Data download options:**
- `--years 2023 2024 2025` - Select specific years
- `--service-types yellow green fhv fhvhv` - Choose service types
- `--months 1 2 3` - Select specific months
- `--verbose` - Enable detailed logging

## Accessing the Deployed Application

### Streamlit Dashboard (Recommended)

After deployment, access your dashboard at:
- **URL**: `https://your-app-name.azurewebsites.net`
- **Features**:
  - 🎨 Beautiful web interface
  - 📊 Interactive visualizations  
  - 💡 Example questions
  - 📚 Query history
  - ⚡ Real-time results

### API Endpoints

Access the REST API at:
- **Base URL**: `https://your-api-name.azurecontainer.io`
- **API Documentation**: `https://your-api-name.azurecontainer.io/docs`
- **Health Check**: `https://your-api-name.azurecontainer.io/health`

### Testing the Deployment

**Test API endpoint:**
```bash
curl "https://your-api-name.azurecontainer.io/api/analyze?question=What was the total revenue in 2024?"
```

**Test Streamlit UI:**
Open `https://your-app-name.azurewebsites.net` in your browser

## Using the Application

### 1. Streamlit Dashboard (Recommended)

1. Open `https://your-app-name.azurewebsites.net` in your browser
2. Click **"🚀 Initialize Engine"** in the sidebar
3. Wait for the data to load (1-2 minutes)
4. Type your question in the text area
5. Click **"🔍 Analyze"**
6. View results with charts and explanations

**Example workflow:**
1. Try: "What was the total revenue from yellow taxis in January 2024?"
2. See the generated SQL query
3. View the results table
4. Read the narrative explanation
5. Explore the interactive charts

### 2. API Server (Advanced Users)

**Open API Documentation:**
- **Swagger UI**: `https://your-api-name.azurecontainer.io/docs`
- **ReDoc**: `https://your-api-name.azurecontainer.io/redoc`

**Test with curl:**
```bash
curl -X POST https://your-api-name.azurecontainer.io/api/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What was the total revenue from yellow taxis in January 2024?",
    "include_narrative": true
  }'
```

**Try Example Questions:**
```bash
# Revenue analysis
curl -X POST https://your-api-name.azurecontainer.io/api/analyze \
  -H "Content-Type: application/json" \
  -d '{"question": "What was the average fare for yellow taxis?"}'

# Get as formatted table
curl "https://your-api-name.azurecontainer.io/api/analyze/tabular?question=Show%20top%205%20pickup%20zones&format=grid"
```

## Example Questions to Try

### In Streamlit Dashboard
Click any example question in the sidebar to auto-fill the query box:

### Revenue Analysis
- "What was the total revenue from yellow taxis in January 2024?"
- "Compare revenue between yellow and green taxis"
- "Which pickup zone generated the most revenue?"

### Trip Analysis
- "How many trips were taken in January 2024?"
- "What is the average trip distance?"
- "Which hour of the day has the most trips?"

### Financial Metrics
- "What is the average tip percentage for credit card payments?"
- "Show the breakdown of fare components (base fare, tips, tolls, taxes)"
- "Compare passenger fares to driver pay for HVFHS"

### Location Analysis
- "What are the top 10 pickup locations by trip count?"
- "Which borough has the highest average fare?"
- "Show revenue by service zone"

## Azure Integration

### Using Azure Synapse Spark

The application is designed to work with Azure Synapse Analytics:

1. **Configure Synapse connection** in your environment variables
2. **Upload data** to Azure Data Lake Storage Gen2
3. **Process queries** using Synapse Spark pools
4. **Scale automatically** based on workload

### Data Management in Azure

- **Download data** directly in Azure environment
- **Store parquet files** in Azure Data Lake Storage
- **Process with Spark** using Azure Synapse pools
- **Cache results** for improved performance

## Streamlit Features

The dashboard includes:

- **🎨 Interactive Interface**: Modern, responsive design
- **📊 Visualizations**: Automatic charts and graphs
- **💡 Example Questions**: Pre-built queries to try
- **📚 Query History**: Track and re-run previous queries
- **⚙️ Settings**: Configure analysis options
- **📱 Mobile Friendly**: Works on phones and tablets

## Troubleshooting

### Azure Deployment Issues

**Container fails to start:**
1. Check environment variables are set correctly
2. Verify Azure OpenAI credentials
3. Check container logs in Azure portal
4. Ensure sufficient memory allocation

**Data download fails:**
1. Verify internet connectivity in Azure environment
2. Check available disk space
3. Use `--verbose` flag for detailed logging
4. Try downloading smaller data subsets first

**API not responding:**
1. Check Azure Container Instance health
2. Verify port configuration (8000 for API)
3. Check firewall rules
4. Review application logs

### Azure OpenAI Connection Issues

1. Verify endpoint URL format: `https://your-resource.openai.azure.com/`
2. Ensure API key is valid and not expired
3. Check you have access to the GPT-4 deployment
4. Verify the deployment name matches your configuration

### Spark/Memory Issues

**In Azure Container Instances:**
1. Increase container memory allocation
2. Use smaller data subsets for testing
3. Configure Spark memory settings in environment variables

**Data loading slow:**
1. Use specific year/month filters
2. Enable Spark caching for repeated queries
3. Consider using Azure Synapse for better performance

## Next Steps

### Learn More
- Read the full [README.md](README.md) for detailed documentation
- Explore the API at `https://your-api-name.azurecontainer.io/docs`
- Check Azure portal for monitoring and logs

### Customize
- Modify `schemas.py` to add new data sources
- Extend `api.py` to add custom endpoints
- Adjust Spark settings for your workload
- Configure Azure Synapse for better performance

### Scale and Optimize
- Use Azure Synapse Spark pools for large datasets
- Implement Azure Data Lake Storage for data persistence
- Configure auto-scaling for variable workloads
- Set up monitoring and alerting

## Support

If you encounter issues:

1. **Check Azure logs**: Review container and application logs in Azure portal
2. **Monitor resources**: Check CPU, memory, and storage usage
3. **Test connectivity**: Verify Azure OpenAI and Synapse connections
4. **Review deployment**: Ensure all environment variables are set

## Summary

You now have:
✅ A deployed Financial Analysis API in Azure
✅ LLM-powered natural language queries
✅ Spark-based data processing in the cloud
✅ Scalable Azure architecture
✅ Interactive Streamlit dashboard

**Start analyzing your data in the cloud!** 🚀☁️

