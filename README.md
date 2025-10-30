# Financial Analysis API

Interactive financial analysis application using Azure Synapse Spark and Azure OpenAI to perform natural language queries on NYC taxi and for-hire vehicle trip data.

## Overview

This application provides an intelligent LLM-powered endpoint that:
1. Accepts natural language questions about taxi/rideshare financial data
2. Generates optimized Spark SQL queries using Azure OpenAI
3. Executes queries on large-scale parquet datasets
4. Returns results as either tabular data or narrative explanations

## Features

- 🤖 **Natural Language Queries**: Ask questions in plain English
- ⚡ **Spark-Powered**: Handle large datasets efficiently with PySpark
- 🧠 **LLM Query Generation**: Azure OpenAI GPT-4 generates optimized SQL
- 📊 **Multiple Output Formats**: JSON, tables, narratives, Markdown, HTML
- 🔍 **Smart Validation**: Automatic query validation and safety checks
- 📈 **Financial Metrics**: Revenue, tips, driver pay, trip analysis
- 🌐 **REST API**: FastAPI-based endpoints with OpenAPI documentation
- 🎨 **Streamlit UI**: Interactive web dashboard with visualizations
- ☁️ **Azure Hosting**: Deploy to Azure App Service or Container Instances

## Architecture

```
┌─────────────────┐
│  User Question  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│  Azure OpenAI (GPT-4)   │  ← Generate Spark SQL Query
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
│  (or Local PySpark)     │
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

## Azure Deployment

### Prerequisites

- Azure subscription with appropriate permissions
- Azure OpenAI service deployed
- Azure Container Registry (for containerized deployment)
- Azure App Service or Azure Container Instances

### Quick Deploy to Azure

1. **Deploy the application to Azure**
```bash
# Deploy both API and Streamlit UI
./deploy_azure.sh --deploy-all

# Or deploy components separately
./deploy_azure.sh --deploy-api    # API only
./deploy_azure.sh --deploy-ui     # Streamlit UI only
```

2. **Configure Azure environment variables**
Set the following in your Azure App Service or Container Instance:
```
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-api-key-here
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4
SYNAPSE_SPARK_POOL_NAME=your-spark-pool
SYNAPSE_WORKSPACE_NAME=your-workspace
```

3. **Download data in Azure environment**
```bash
# SSH into your Azure container or use Azure Cloud Shell
python3 download_data.py

# Or download specific data subsets
python3 download_data.py --years 2024 --service-types yellow --months 1 2 3
```

**Note**: Data files are downloaded directly in the Azure environment after deployment. The download script supports flexible data selection to manage storage costs.

## Usage

### Accessing the Application

After deployment to Azure, you can access the application through:

**Streamlit Dashboard (Recommended)**
- **URL**: `https://your-app-name.azurewebsites.net`
- Interactive web interface with visualizations
- Natural language query interface
- Real-time data analysis

**API Endpoints**
- **Base URL**: `https://your-api-name.azurecontainer.io`
- **API Documentation**: `https://your-api-name.azurecontainer.io/docs`
- RESTful API for programmatic access

### Testing the Deployment

Test your deployed application:
```bash
# Test API endpoint
curl "https://your-api-name.azurecontainer.io/api/analyze?question=What was the total revenue in 2024?"

# Test Streamlit UI
# Open https://your-app-name.azurewebsites.net in your browser
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
curl "http://localhost:8000/api/analyze/tabular?question=Show top 5 pickup zones by revenue&format=grid"
```

### GET `/api/analyze/narrative`
Get narrative explanation only

```bash
curl "http://localhost:8000/api/analyze/narrative?question=What were the peak hours for taxi trips?"
```

### GET `/api/suggestions`
Get related question suggestions

```bash
curl "http://localhost:8000/api/suggestions?question=What was the total revenue?"
```

### GET `/api/datasets`
Get information about loaded datasets

### GET `/api/history`
View recent query execution history

### GET `/api/docs/examples`
Get example questions

## Example Questions

### Revenue Analysis
- "What was the total revenue from yellow taxis in 2024?"
- "Compare revenue between yellow and green taxis by month"
- "Which pickup zones generated the most revenue?"
- "What is the average revenue per trip for HVFHS services?"

### Trip Analysis
- "How many trips were taken in January 2024?"
- "What is the average trip distance by taxi type?"
- "Which hour of the day has the most trips?"
- "Show daily trip counts for March 2024"

### Financial Metrics
- "What is the average tip percentage for credit card payments?"
- "Calculate total driver pay vs passenger fares for Uber/Lyft"
- "What are the most profitable routes?"
- "Break down all revenue components (fares, tips, tolls, taxes)"

### Location Analysis
- "Which borough has the highest average fare?"
- "Top 10 pickup locations by trip count"
- "Revenue breakdown by service zone"
- "Compare Manhattan vs outer borough trips"

### Time Series
- "Show revenue trend by month for 2024"
- "Compare weekend vs weekday trip volumes"
- "What is the busiest day of the week?"

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

4. **Azure Synapse Analytics**
   - Spark pools for data processing
   - Data lake storage for parquet files

5. **Azure OpenAI Service**
   - GPT-4 deployment for query generation
   - Configure appropriate rate limits

### Data Storage Strategy

- **Azure Data Lake Storage Gen2**: Store parquet files
- **Azure Synapse**: Process queries using Spark pools
- **Local container storage**: Cache frequently accessed data

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
├── .env.template                  # Environment template
└── README.md                      # This file
```

## Configuration

All configuration is managed through environment variables or `.env` file:

| Variable | Required | Description |
|----------|----------|-------------|
| `AZURE_OPENAI_ENDPOINT` | Yes | Azure OpenAI service endpoint |
| `AZURE_OPENAI_API_KEY` | Yes | Azure OpenAI API key |
| `AZURE_OPENAI_DEPLOYMENT_NAME` | No | Model deployment name (default: gpt-4) |
| `DATA_PATH` | No | Path to data directory (default: ./src_data) |
| `API_PORT` | No | API server port (default: 8000) |
| `USE_LOCAL_SPARK` | No | Use local Spark instead of Synapse (default: true) |

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

### Spark Memory Issues
```python
# Increase driver/executor memory
export SPARK_DRIVER_MEMORY=8g
export SPARK_EXECUTOR_MEMORY=8g
```

### Azure OpenAI Rate Limits
- Implement retry logic (already included via tenacity)
- Use exponential backoff
- Consider using multiple deployments

### Data Loading Slow
- Load specific months only
- Use parquet partition pruning
- Enable Spark caching for repeated queries

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

## License

MIT License - see LICENSE file

## Support

For issues or questions:
- Check the `/api/docs/examples` endpoint for query examples
- Review query history via `/api/history`
- Enable DEBUG logging for detailed troubleshooting

## Roadmap

- [ ] Add user authentication and authorization
- [ ] Implement query result caching
- [ ] Support for real-time streaming data
- [ ] Advanced visualizations (charts, graphs)
- [ ] Multi-turn conversational interface
- [ ] Export results to Excel/PDF
- [ ] Scheduled report generation
- [ ] Multi-tenant support

---

**Built with**: Python, PySpark, Azure OpenAI, FastAPI, Azure Synapse

