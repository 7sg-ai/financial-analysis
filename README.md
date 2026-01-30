# Financial Analysis API

Interactive financial analysis application using **Azure Synapse Spark** and **Azure OpenAI** to perform natural language queries on NYC taxi and for-hire vehicle trip data.

## Overview

This application provides an intelligent LLM-powered endpoint that:
1. Accepts natural language questions about taxi/rideshare financial data
2. Generates optimized Spark SQL queries using Azure OpenAI GPT-5.2-chat
3. Executes queries on large-scale parquet datasets using Azure Synapse Spark
4. Returns results as either tabular data or narrative explanations

## Features

- 🤖 **Natural Language Queries**: Ask questions in plain English
- ⚡ **Spark-Powered**: Handle large datasets efficiently with Azure Synapse Spark
- 🧠 **LLM Query Generation**: Azure OpenAI GPT-5.2-chat generates optimized SQL
- 📊 **Multiple Output Formats**: JSON, tables, narratives, Markdown, HTML
- 🔍 **Smart Validation**: Automatic query validation and safety checks
- 📈 **Financial Metrics**: Revenue, tips, driver pay, trip analysis
- 🌐 **REST API**: FastAPI-based endpoints with OpenAPI documentation
- 🎨 **Streamlit UI**: Interactive web dashboard with visualizations
- ☁️ **Azure Hosting**: Deploy to Azure App Service or Container Instances
- 🔄 **Scalable Processing**: Azure Synapse Spark pools for enterprise-scale data processing

## Architecture

```
┌─────────────────┐
│  User Question  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│  Azure OpenAI (GPT-5.2-chat)   │  ← Generate Spark SQL Query
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│   Query Validator       │  ← Safety & correctness checks
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  Azure Synapse Spark    │  ← Execute query on data
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  Response Formatter     │  ← Format as table/narrative
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│   LLM Narrator (opt)    │  ← Generate explanation
└────────┬────────────────┘
         │
         ▼
┌─────────────────┐
│  JSON Response  │
└─────────────────┘
```

## Data

The application analyzes NYC Taxi and For-Hire Vehicle trip data:

- **Yellow Taxi**: Traditional yellow taxi trips with fare details
- **Green Taxi**: Boro taxis serving outer boroughs
- **FHV**: For-hire vehicles (non-Uber/Lyft)
- **FHVHV**: High-volume for-hire vehicles (Uber, Lyft, Via)
- **Taxi Zones**: Location lookup table

### Financial Metrics Available
- Fare amounts and total revenue
- Tips, tolls, taxes, and surcharges
- Driver pay vs passenger fares
- Payment methods analysis
- Trip distances and durations
- Location-based revenue

## Quick Start - Azure Deployment

Deploy the Financial Analysis API to Azure in 10 minutes.

### Prerequisites

- Azure subscription with appropriate permissions
- Azure OpenAI service deployed
- Azure Synapse Analytics workspace with Spark pool configured
- Azure Container Registry (ACR) - will be created if needed
- Azure CLI installed and configured
- Docker installed and running (for building images)

### Step 1: Clone the Repository

```bash
git clone https://github.com/7sg-ai/financial-analysis.git
cd financial-analysis
```

### Step 2: Configure Azure Resources

**Set up Azure Container Registry:**
```bash
# Create resource group
az group create --name rg-financial-analysis --location eastus2

# Create container registry
az acr create --resource-group rg-financial-analysis \
  --name financialanalysisacr --sku Basic --admin-enabled true
```

**Deploy Azure OpenAI (if not already done):**
```bash
# Create Azure OpenAI resource
az cognitiveservices account create \
  --name financial-analysis-openai \
  --resource-group rg-financial-analysis \
  --location eastus2 \
  --kind OpenAI \
  --sku S0
```

### Step 3: Deploy the Application

**Make deployment script executable:**
```bash
chmod +x deploy_azure.sh
```

**Deploy both API and Streamlit UI:**
```bash
./deploy_azure.sh
# Choose option 3 for both API and Streamlit UI
```

**Or deploy components separately:**
- The script will prompt you to choose:
  - Option 1: API only (FastAPI)
  - Option 2: Streamlit UI only
  - Option 3: Both API and Streamlit UI

### Step 4: Configure Environment Variables

Set the following in your Azure App Service or Container Instance:

```bash
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-actual-api-key-here
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-5.2-chat
```

**Required Azure Synapse Spark configuration:**
```bash
SYNAPSE_SPARK_POOL_NAME=your-spark-pool
SYNAPSE_WORKSPACE_NAME=your-workspace
AZURE_SUBSCRIPTION_ID=your-subscription-id
AZURE_RESOURCE_GROUP=your-resource-group
```

**Note**: The application requires Azure Synapse Spark for data processing. Ensure your Synapse workspace and Spark pool are configured before deployment.

### Step 5: Download Data in Azure

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

**Using the Streamlit Dashboard:**
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

### API Endpoints

Access the REST API at:
- **Base URL**: `https://your-api-name.azurecontainer.io`
- **API Documentation**: `https://your-api-name.azurecontainer.io/docs`
- **Health Check**: `https://your-api-name.azurecontainer.io/health`

**Test API endpoint:**
```bash
curl "https://your-api-name.azurecontainer.io/api/analyze?question=What was the total revenue in 2024?"
```

**Test with POST request:**
```bash
curl -X POST https://your-api-name.azurecontainer.io/api/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What was the total revenue from yellow taxis in January 2024?",
    "include_narrative": true
  }'
```

## API Endpoints

### POST `/api/analyze`
Analyze data using natural language question

**Request:**
```json
{
  "question": "What was the total revenue from yellow taxis in January 2024?",
  "return_format": "both",
  "include_narrative": true,
  "max_rows": 100
}
```

**Response:**
```json
{
  "question": "What was the total revenue from yellow taxis in January 2024?",
  "query": "SELECT SUM(total_amount) as total_revenue FROM yellow_taxi WHERE month(tpep_pickup_datetime) = 1",
  "results": [
    {"total_revenue": 45123456.78}
  ],
  "result_count": 1,
  "narrative": "In January 2024, yellow taxis generated a total revenue of $45.1 million...",
  "execution_time_ms": 1234,
  "metadata": {
    "query_type": "aggregation",
    "tables_used": ["yellow_taxi"],
    "is_financial": true
  }
}
```

### POST `/api/query`
Execute custom SQL query

**Request:**
```json
{
  "query": "SELECT COUNT(*) as trip_count FROM yellow_taxi",
  "include_narrative": false
}
```

### GET `/api/analyze/tabular`
Get results as formatted table

```bash
curl "https://your-api-name.azurecontainer.io/api/analyze/tabular?question=Show%20top%205%20pickup%20zones&format=grid"
```

### GET `/api/analyze/narrative`
Get narrative explanation only

```bash
curl "https://your-api-name.azurecontainer.io/api/analyze/narrative?question=What were the peak hours for taxi trips?"
```

### GET `/api/suggestions`
Get related question suggestions

```bash
curl "https://your-api-name.azurecontainer.io/api/suggestions?question=What was the total revenue?"
```

### GET `/api/datasets`
Get information about loaded datasets

### GET `/api/history`
View recent query execution history

### GET `/api/docs/examples`
Get example questions

## Example Questions

### Revenue Analysis
- "What was the total revenue from yellow taxis in January 2024?"
- "Compare revenue between yellow and green taxis"
- "Which pickup zone generated the most revenue?"
- "What is the average revenue per trip for HVFHS services?"

### Trip Analysis
- "How many trips were taken in January 2024?"
- "What is the average trip distance?"
- "Which hour of the day has the most trips?"
- "Show daily trip counts for March 2024"

### Financial Metrics
- "What is the average tip percentage for credit card payments?"
- "Show the breakdown of fare components (base fare, tips, tolls, taxes)"
- "Compare passenger fares to driver pay for HVFHS"
- "What are the most profitable routes?"

### Location Analysis
- "What are the top 10 pickup locations by trip count?"
- "Which borough has the highest average fare?"
- "Show revenue by service zone"
- "Compare Manhattan vs outer borough trips"

### Time Series
- "Show revenue trend by month for 2024"
- "Compare weekend vs weekday trip volumes"
- "What is the busiest day of the week?"

## Azure Synapse Spark Integration

The application uses **Azure Synapse Spark** for scalable data processing. Azure Synapse Spark is required for all data processing operations.

### Prerequisites

- Azure Synapse Analytics workspace
- Spark pool configured in Synapse
- Data uploaded to Azure Data Lake Storage Gen2

### Configuration

1. **Set required environment variables:**
```bash
SYNAPSE_SPARK_POOL_NAME=your-spark-pool-name
SYNAPSE_WORKSPACE_NAME=your-workspace-name
AZURE_SUBSCRIPTION_ID=your-subscription-id
AZURE_RESOURCE_GROUP=your-resource-group
```

2. **Configure Azure authentication** (the application uses Azure Identity for authentication)

3. **Upload data to Azure Data Lake Storage Gen2**

4. **Update `DATA_PATH`** to point to your Azure Data Lake Storage path:
```bash
DATA_PATH=abfss://container@account.dfs.core.windows.net/path/to/data
```

### Benefits of Azure Synapse Spark

- ✅ Scalable processing for large datasets (TB+)
- ✅ Auto-scaling Spark pools
- ✅ Integration with Azure Data Lake Storage
- ✅ Better performance for complex queries
- ✅ Cost-effective pay-per-use model

### Implementation

The application includes the `azure-synapse-spark` package. The `create_spark_session` function in `data_loader.py` should be configured to connect to your Synapse Spark pool using the Synapse Spark session API.

## Azure Architecture

### Recommended Azure Setup

1. **Azure Container Registry (ACR)**
   - Store Docker images for API and Streamlit UI
   - Enable admin access for deployment

2. **Azure Container Instances (ACI)**
   - Host the FastAPI backend
   - Configure with appropriate CPU/memory for Spark workloads

3. **Azure App Service**
   - Host the Streamlit frontend
   - Configure custom domain and SSL

4. **Azure Synapse Analytics** (Required)
   - Spark pools for data processing
   - Data lake storage for parquet files
   - Required for all data processing operations

5. **Azure OpenAI Service**
   - GPT-5.2-chat deployment for query generation
   - Configure appropriate rate limits

### Data Storage Strategy

- **Azure Data Lake Storage Gen2**: Store parquet files
- **Azure Synapse**: Process queries using Spark pools
- **Container storage**: Cache frequently accessed data

## Python SDK Example

```python
from analysis_engine import FinancialAnalysisEngine
from config import get_settings

# Initialize
settings = get_settings()
engine = FinancialAnalysisEngine(settings)
engine.initialize_data()

# Ask a question
response = engine.analyze(
    question="What was the average tip amount in January?",
    return_format="both",
    include_narrative=True
)

# Access results
print(f"Query: {response.query}")
print(f"Results: {response.results}")
print(f"Narrative: {response.narrative}")

# Format as table
print(response.to_tabular())

# Format as markdown
print(response.to_markdown())

# Cleanup
engine.shutdown()
```

## Project Structure

```
financial-analysis/
├── src_data/                      # Data files (downloaded in Azure)
│   ├── yellow_tripdata_*.parquet
│   ├── green_tripdata_*.parquet
│   ├── fhv_tripdata_*.parquet
│   ├── fhvhv_tripdata_*.parquet
│   └── taxi_zone_lookup.csv
├── config.py                      # Configuration management
├── schemas.py                     # Data schema definitions
├── data_loader.py                 # Spark data loading
├── llm_query_generator.py         # LLM query generation
├── query_executor.py              # Query execution engine
├── response_formatter.py          # Response formatting
├── analysis_engine.py             # Main orchestration engine
├── api.py                         # FastAPI REST API
├── streamlit_app.py               # Streamlit web UI
├── download_data.py               # Data download script
├── deploy_azure.sh                # Azure deployment script
├── Dockerfile                     # API container image
├── Dockerfile.streamlit           # Streamlit container image
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

## Configuration

All configuration is managed through environment variables or `.env` file:

| Variable | Required | Description |
|----------|----------|-------------|
| `AZURE_OPENAI_ENDPOINT` | Yes | Azure OpenAI service endpoint |
| `AZURE_OPENAI_API_KEY` | Yes | Azure OpenAI API key |
| `AZURE_OPENAI_DEPLOYMENT_NAME` | No | Model deployment name (default: gpt-5.2-chat) |
| `DATA_PATH` | No | Path to data directory (default: ./src_data or Azure Data Lake path) |
| `API_PORT` | No | API server port (default: 8000) |
| `SYNAPSE_SPARK_POOL_NAME` | Yes | Synapse Spark pool name |
| `SYNAPSE_WORKSPACE_NAME` | Yes | Synapse workspace name |
| `AZURE_SUBSCRIPTION_ID` | Yes | Azure subscription ID |
| `AZURE_RESOURCE_GROUP` | Yes | Azure resource group |

## Performance Tuning

### Spark Configuration
Adjust Spark settings in `data_loader.py`:
```python
spark = create_spark_session(
    config_overrides={
        "spark.driver.memory": "8g",
        "spark.executor.memory": "8g",
        "spark.sql.shuffle.partitions": "200"
    }
)
```

### Query Optimization
- Use date filters to reduce data scanned
- Limit result sets appropriately
- Cache frequently accessed data

### LLM Settings
Adjust in `llm_query_generator.py`:
- Temperature: Lower (0.1) for consistent SQL
- Max tokens: Increase for complex queries

## Troubleshooting

### Azure Deployment Issues

**Container fails to start:**
1. Check environment variables are set correctly
2. Verify Azure OpenAI credentials
3. Check container logs in Azure portal
4. Ensure sufficient memory allocation

**Docker not found:**
- Install Docker Desktop from https://www.docker.com/products/docker-desktop
- Make sure Docker is running before executing the deployment script

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
3. Check you have access to the GPT-5.2-chat deployment
4. Verify the deployment name matches your configuration

### Spark/Memory Issues

**In Azure Container Instances:**
1. Increase container memory allocation
2. Use smaller data subsets for testing
3. Configure Spark memory settings in environment variables

**Data loading slow:**
1. Use specific year/month filters
2. Enable Spark caching for repeated queries
3. Scale up your Azure Synapse Spark pool for better performance

### Azure Synapse Spark Connection Issues

1. Verify Synapse workspace and Spark pool exist
2. Check Azure authentication credentials
3. Ensure data path points to Azure Data Lake Storage
4. Verify network connectivity to Synapse workspace
5. Check Spark pool is running and has available capacity

## Security

- ✅ Query validation prevents DROP/DELETE/INSERT operations
- ✅ SQL injection protection via parameterized queries
- ✅ API key authentication (add middleware for production)
- ⚠️ Add authentication/authorization for production use
- ⚠️ Use Azure Key Vault for secrets in production

## Contributing

To extend the application:

1. **Add new data sources**: Update `schemas.py` and `data_loader.py`
2. **Custom formatters**: Extend `response_formatter.py`
3. **Additional LLM features**: Modify `llm_query_generator.py`
4. **New endpoints**: Add to `api.py`
5. **Azure Synapse integration**: Enhance `data_loader.py` to use Synapse Spark sessions

## License

MIT License - see LICENSE file

## Support

For issues or questions:
- Check the `/api/docs/examples` endpoint for query examples
- Review query history via `/api/history`
- Enable DEBUG logging for detailed troubleshooting
- Review Azure portal logs for deployment issues

## Roadmap

- [ ] Add user authentication and authorization
- [ ] Implement query result caching
- [ ] Support for real-time streaming data
- [ ] Advanced visualizations (charts, graphs)
- [ ] Multi-turn conversational interface
- [ ] Export results to Excel/PDF
- [ ] Scheduled report generation
- [ ] Multi-tenant support
- [ ] Enhanced Azure Synapse Spark integration

---

**Built with**: Python, Azure Synapse Spark, Azure OpenAI, FastAPI
