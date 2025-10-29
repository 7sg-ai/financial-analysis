# Quick Start Guide

Get started with the Financial Analysis API in 5 minutes.

## Prerequisites

- Python 3.9 or higher
- Azure OpenAI access (endpoint and API key)
- 8GB RAM minimum

## Installation Steps

### 1. Navigate to Project Directory
```bash
cd /Users/neebhatt/code/financial-analysis
```

### 2. Create Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

This will install:
- PySpark for data processing
- FastAPI for the REST API
- Azure OpenAI SDK
- All other required packages

### 4. Configure Environment

Create a `.env` file with your Azure credentials:

```bash
# Copy the template
cp .env.template .env

# Edit .env and add your credentials
nano .env  # or use your preferred editor
```

**Required settings in `.env`:**
```bash
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-actual-api-key-here
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4  # or your deployment name
```

**How to get these values:**
1. Go to [Azure Portal](https://portal.azure.com)
2. Navigate to your Azure OpenAI resource
3. Under "Keys and Endpoint":
   - Copy the endpoint URL
   - Copy one of the keys
4. Under "Model deployments", note your GPT-4 deployment name

### 5. Verify Data Files

Ensure your data files are in the `src_data` directory:

```bash
ls src_data/*.parquet
```

You should see files like:
- `yellow_tripdata_2024-01.parquet`
- `green_tripdata_2024-01.parquet`
- `fhvhv_tripdata_2024-01.parquet`
- etc.

## Running the Application

### Option 1: Streamlit Dashboard (Recommended)

Start the interactive web dashboard:

```bash
python run_streamlit.py
```

The dashboard will open at `http://localhost:8501`

**Features:**
- 🎨 Beautiful web interface
- 📊 Interactive visualizations
- 💡 Example questions
- 📚 Query history
- ⚡ Real-time results

### Option 2: Quick Test (Verify Setup)

Run a simple test to verify everything works:

```bash
python test_query.py
```

This will:
- Initialize the Spark engine
- Load January 2024 data
- Execute 3 sample queries
- Display results

**Expected output:**
```
Starting Financial Analysis Engine...
Loading data...
Registered view: yellow_taxi (2845234 rows)
...
[Question 1/3]
Q: How many yellow taxi trips were taken in January 2024?

Generated Query:
SELECT COUNT(*) as trip_count FROM yellow_taxi

Results (1 rows):
  trip_count
------------
    2845234

Narrative:
In January 2024, there were 2,845,234 yellow taxi trips...
```

### Option 3: API Server

```bash
python run_local.py
```

The API will start at `http://localhost:8000`

You should see:
```
INFO:     Started server process
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8000
```

## Using the Application

### 1. Streamlit Dashboard (Recommended)

1. Open http://localhost:8501 in your browser
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
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

**Test with curl:**
```bash
curl -X POST http://localhost:8000/api/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What was the total revenue from yellow taxis in January 2024?",
    "include_narrative": true
  }'
```

**Try Example Questions:**
```bash
# Revenue analysis
curl -X POST http://localhost:8000/api/analyze \
  -H "Content-Type: application/json" \
  -d '{"question": "What was the average fare for yellow taxis?"}'

# Get as formatted table
curl "http://localhost:8000/api/analyze/tabular?question=Show%20top%205%20pickup%20zones&format=grid"
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

## Python SDK Usage

You can also use the engine directly in Python:

```python
from analysis_engine import FinancialAnalysisEngine
from config import get_settings

# Initialize
settings = get_settings()
engine = FinancialAnalysisEngine(settings)
engine.initialize_data()

# Ask a question
response = engine.analyze(
    question="What was the total revenue from yellow taxis?",
    return_format="both",
    include_narrative=True
)

# Print results
print("Query:", response.query)
print("\nResults:")
print(response.to_tabular())
print("\nNarrative:")
print(response.narrative)

# Cleanup
engine.shutdown()
```

## Streamlit Features

The dashboard includes:

- **🎨 Interactive Interface**: Modern, responsive design
- **📊 Visualizations**: Automatic charts and graphs
- **💡 Example Questions**: Pre-built queries to try
- **📚 Query History**: Track and re-run previous queries
- **⚙️ Settings**: Configure analysis options
- **📱 Mobile Friendly**: Works on phones and tablets

## Troubleshooting

### "Module not found" errors

Make sure you activated the virtual environment:
```bash
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

Then reinstall dependencies:
```bash
pip install -r requirements.txt
```

### "Azure OpenAI" connection errors

1. Check your `.env` file has correct values
2. Verify the endpoint URL format: `https://your-resource.openai.azure.com/`
3. Ensure your API key is valid
4. Check you have access to the GPT-4 deployment

Test your credentials:
```python
from openai import AzureOpenAI
import os

client = AzureOpenAI(
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version="2024-02-15-preview"
)

# This should not raise an error
print("Connection successful!")
```

### "Java not found" or Spark errors

PySpark requires Java. Install Java 11 or 17:

**macOS:**
```bash
brew install openjdk@17
```

**Ubuntu/Debian:**
```bash
sudo apt-get install openjdk-17-jre-headless
```

**Windows:**
Download from [Oracle](https://www.oracle.com/java/technologies/downloads/) or [Adoptium](https://adoptium.net/)

### Memory errors

If you see "OutOfMemory" errors, increase Spark memory:

```bash
export SPARK_DRIVER_MEMORY=8g
export SPARK_EXECUTOR_MEMORY=8g
```

Or load less data:
```python
# In Python
engine.initialize_data(months=["01"], year="2024")  # Load only January
```

### Port already in use

If port 8000 is busy, change it in `.env`:
```bash
API_PORT=8080
```

## Next Steps

### Learn More
- Read the full [README.md](README.md) for detailed documentation
- Check [USAGE_EXAMPLES.md](USAGE_EXAMPLES.md) for advanced examples
- Explore the API at http://localhost:8000/docs

### Customize
- Modify `schemas.py` to add new data sources
- Extend `api.py` to add custom endpoints
- Adjust Spark settings in `data_loader.py`

### Deploy to Azure
- Follow the Azure deployment guide in README.md
- Use the provided `Dockerfile` for containerization
- Run `./deploy_azure.sh` for automated deployment

## Support

If you encounter issues:

1. **Check logs**: The application logs detailed information
2. **Test individual components**: Use `test_query.py`
3. **Review query history**: Use the `/api/history` endpoint
4. **Enable debug logging**: Set `LOG_LEVEL=DEBUG` in `.env`

## Summary

You now have:
✅ A working Financial Analysis API
✅ LLM-powered natural language queries
✅ Spark-based data processing
✅ Multiple output formats (JSON, tables, narratives)

**Start analyzing your data!** 🚀

