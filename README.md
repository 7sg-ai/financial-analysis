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

## Installation

### Prerequisites

- Python 3.9+
- Azure OpenAI access (endpoint and API key)
- 8GB+ RAM recommended for Spark

### Setup

1. **Clone or navigate to the repository**
```bash
cd /Users/neebhatt/code/financial-analysis
```

2. **Create virtual environment**
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure environment**
```bash
# Copy template
cp .env.template .env

# Edit .env and add your Azure credentials
# Required:
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-api-key-here
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4

# Optional (for Azure Synapse deployment):
SYNAPSE_SPARK_POOL_NAME=your-spark-pool
SYNAPSE_WORKSPACE_NAME=your-workspace
```

5. **Download data files**
```bash
# Download NYC taxi data (6.7 GB)
python download_data.py

# Verify data files
ls src_data/*.parquet
# Should see yellow_tripdata_*, green_tripdata_*, fhv_tripdata_*, fhvhv_tripdata_*
```

**Note**: The data files are not included in the Git repository due to their large size (6.7 GB). Run the download script to get the data.

## Usage

### Running Locally

**Option 1: Streamlit Dashboard (Recommended)**
```bash
python run_streamlit.py
```
- **Dashboard**: http://localhost:8501
- Interactive web interface with visualizations

**Option 2: API Server**
```bash
python run_local.py
```
- **API**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs

**Option 3: Direct uvicorn**
```bash
uvicorn api:app --reload
```

### Testing

Run the test script to verify everything works:
```bash
python test_query.py
```

This will execute sample queries and display results.

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

## Deployment to Azure

### Azure Synapse Deployment

1. **Upload code to Synapse workspace**
```bash
# Using Azure CLI
az synapse workspace upload \
  --workspace-name your-workspace \
  --source . \
  --destination /financial-analysis
```

2. **Create Spark pool**
- Node size: Medium (8 vCPU, 64 GB)
- Autoscale: 3-10 nodes
- Spark version: 3.4

3. **Configure environment variables in Synapse**
- Add secrets to Azure Key Vault
- Reference in Synapse pipeline

4. **Deploy API as Azure Function or Container**
```bash
# Using Azure Container Instances
az container create \
  --resource-group your-rg \
  --name financial-analysis-api \
  --image your-registry/financial-analysis:latest \
  --environment-variables \
    AZURE_OPENAI_ENDPOINT=... \
    AZURE_OPENAI_API_KEY=...
```

### Docker Deployment

```dockerfile
# Dockerfile provided in repository
docker build -t financial-analysis .
docker run -p 8000:8000 --env-file .env financial-analysis
```

## Project Structure

```
financial-analysis/
├── src_data/                      # Data files
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
├── run_local.py                   # Local development script
├── test_query.py                  # Testing script
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

