# Usage Examples

Comprehensive examples for using the Financial Analysis API.

## Table of Contents
- [Quick Start](#quick-start)
- [Python SDK Examples](#python-sdk-examples)
- [REST API Examples](#rest-api-examples)
- [Advanced Queries](#advanced-queries)
- [Integration Examples](#integration-examples)

## Quick Start

### 1. Start the API
```bash
python run_local.py
```

### 2. Test with curl
```bash
curl -X POST http://localhost:8000/api/analyze \
  -H "Content-Type: application/json" \
  -d '{"question": "How many yellow taxi trips in January 2024?"}'
```

## Python SDK Examples

### Basic Usage

```python
from analysis_engine import FinancialAnalysisEngine
from config import get_settings

# Initialize
settings = get_settings()
engine = FinancialAnalysisEngine(settings)
engine.initialize_data()

# Simple question
response = engine.analyze("What was the total revenue from yellow taxis?")

print(f"Results: {response.results}")
print(f"Narrative: {response.narrative}")

# Cleanup
engine.shutdown()
```

### Revenue Analysis

```python
# Monthly revenue trend
response = engine.analyze(
    "Show total revenue by month for yellow taxis in 2024",
    return_format="both",
    include_narrative=True
)

# Print as table
print(response.to_tabular(format="grid"))

# Print narrative
print("\n" + response.narrative)
```

### Custom Query Execution

```python
# Execute custom SQL
custom_query = """
SELECT 
    PULocationID,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    SUM(total_amount) as total_revenue
FROM yellow_taxi
WHERE month(tpep_pickup_datetime) = 1
GROUP BY PULocationID
ORDER BY total_revenue DESC
LIMIT 10
"""

response = engine.execute_custom_query(
    query=custom_query,
    include_narrative=True,
    question_context="What are the top 10 pickup locations by revenue?"
)

print(response.to_markdown())
```

### Batch Analysis

```python
questions = [
    "What was the total revenue in January?",
    "What is the average trip distance?",
    "Which payment type is most common?",
    "What are peak hours for trips?"
]

for question in questions:
    print(f"\nQ: {question}")
    response = engine.analyze(question, max_rows=5)
    print(response.to_tabular(format="simple"))
    print(f"Execution time: {response.execution_time_ms}ms")
```

### Export Results

```python
import json
import csv

# Analyze
response = engine.analyze("Show top 10 routes by trip count")

# Export as JSON
with open('results.json', 'w') as f:
    json.dump(response.to_dict(), f, indent=2, default=str)

# Export as CSV
if response.results:
    with open('results.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=response.results[0].keys())
        writer.writeheader()
        writer.writerows(response.results)

# Export as Markdown
with open('results.md', 'w') as f:
    f.write(response.to_markdown())
```

## REST API Examples

### Using curl

#### Basic Query
```bash
curl -X POST http://localhost:8000/api/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What was the average fare for yellow taxis?",
    "return_format": "both",
    "include_narrative": true,
    "max_rows": 10
  }'
```

#### Get Tabular Results
```bash
curl "http://localhost:8000/api/analyze/tabular?question=Show%20top%205%20zones&format=grid"
```

#### Get Narrative Only
```bash
curl "http://localhost:8000/api/analyze/narrative?question=What%20were%20peak%20hours?"
```

#### Execute Custom Query
```bash
curl -X POST http://localhost:8000/api/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT COUNT(*) as total FROM yellow_taxi",
    "include_narrative": false
  }'
```

### Using Python requests

```python
import requests
import json

API_URL = "http://localhost:8000"

# Analyze with natural language
response = requests.post(
    f"{API_URL}/api/analyze",
    json={
        "question": "Compare revenue between yellow and green taxis",
        "return_format": "both",
        "include_narrative": True,
        "max_rows": 100
    }
)

result = response.json()
print(json.dumps(result, indent=2))

# Get suggestions
response = requests.get(
    f"{API_URL}/api/suggestions",
    params={"question": "What was the total revenue?"}
)

suggestions = response.json()
print("Related questions:", suggestions["suggestions"])
```

### Using JavaScript/Node.js

```javascript
const axios = require('axios');

const API_URL = 'http://localhost:8000';

async function analyze(question) {
    const response = await axios.post(`${API_URL}/api/analyze`, {
        question: question,
        return_format: 'both',
        include_narrative: true,
        max_rows: 10
    });
    
    return response.data;
}

// Use it
analyze('What was the total revenue from HVFHS in 2024?')
    .then(result => {
        console.log('Query:', result.query);
        console.log('Results:', result.results);
        console.log('Narrative:', result.narrative);
    });
```

## Advanced Queries

### Financial Analysis

```python
# Tip percentage by payment type
response = engine.analyze("""
    Calculate the average tip percentage for credit card vs cash payments
    for yellow taxis
""")

# Profit margin analysis (HVFHS)
response = engine.analyze("""
    Compare total passenger fares to driver pay for HVFHS services
    and calculate the platform's take rate
""")

# Revenue composition
response = engine.analyze("""
    Break down total revenue into components: base fare, tips, tolls,
    taxes, and surcharges for yellow taxis
""")
```

### Time Series Analysis

```python
# Hourly patterns
response = engine.analyze("""
    Show average revenue per hour of day for yellow taxis,
    comparing weekdays vs weekends
""")

# Monthly trends
response = engine.analyze("""
    Show monthly trip count and revenue trends for all taxi types
    in 2024, ordered by month
""")

# Day of week analysis
response = engine.analyze("""
    Which day of the week has the highest revenue?
    Show revenue by day of week for yellow taxis
""")
```

### Location Analysis

```python
# Top routes
response = engine.analyze("""
    What are the top 10 most profitable routes?
    Show pickup zone, dropoff zone, trip count, and total revenue
""")

# Borough comparison
response = engine.analyze("""
    Compare average fare and trip count across boroughs.
    Join with taxi zones to get borough information
""")

# Airport trips
response = engine.analyze("""
    What percentage of trips involve JFK or LaGuardia airports?
    Calculate revenue from airport trips
""")
```

### Comparative Analysis

```python
# Yellow vs Green
response = engine.analyze("""
    Compare yellow and green taxis:
    - Total trips
    - Total revenue
    - Average fare
    - Average trip distance
""")

# Traditional vs Rideshare
response = engine.analyze("""
    Compare traditional taxis (yellow + green) to HVFHS:
    - Market share by trip count
    - Revenue comparison
    - Average driver earnings
""")
```

## Integration Examples

### Jupyter Notebook

```python
# In Jupyter
%load_ext autoreload
%autoreload 2

from analysis_engine import FinancialAnalysisEngine
from config import get_settings
import pandas as pd

# Initialize
settings = get_settings()
engine = FinancialAnalysisEngine(settings)
engine.initialize_data()

# Analyze and convert to pandas
response = engine.analyze("Show revenue by month")
df = pd.DataFrame(response.results)

# Visualize
import matplotlib.pyplot as plt
df.plot(kind='bar', x='month', y='revenue')
plt.title('Revenue by Month')
plt.show()
```

### Streamlit Dashboard

```python
import streamlit as st
from analysis_engine import FinancialAnalysisEngine
from config import get_settings

# Cache the engine
@st.cache_resource
def get_engine():
    settings = get_settings()
    engine = FinancialAnalysisEngine(settings)
    engine.initialize_data()
    return engine

engine = get_engine()

# UI
st.title("Financial Analysis Dashboard")
question = st.text_input("Ask a question about the data:")

if st.button("Analyze"):
    with st.spinner("Analyzing..."):
        response = engine.analyze(question)
        
        # Show narrative
        if response.narrative:
            st.info(response.narrative)
        
        # Show results as table
        if response.results:
            st.dataframe(response.results)
        
        # Show query
        with st.expander("View SQL Query"):
            st.code(response.query, language="sql")
```

### Flask Web App

```python
from flask import Flask, request, jsonify, render_template
from analysis_engine import FinancialAnalysisEngine
from config import get_settings

app = Flask(__name__)

# Initialize engine
settings = get_settings()
engine = FinancialAnalysisEngine(settings)
engine.initialize_data()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/analyze', methods=['POST'])
def analyze():
    data = request.json
    question = data.get('question')
    
    response = engine.analyze(question)
    
    return jsonify({
        'query': response.query,
        'results': response.results,
        'narrative': response.narrative,
        'table': response.to_tabular(format='html')
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)
```

### Automated Reporting

```python
from analysis_engine import FinancialAnalysisEngine
from config import get_settings
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def generate_daily_report():
    """Generate and send daily financial report"""
    
    engine = FinancialAnalysisEngine(get_settings())
    engine.initialize_data(months=["01"])  # Current month
    
    # Key metrics
    questions = [
        "What was yesterday's total revenue across all taxi types?",
        "Which were the top 5 pickup locations by trip count?",
        "What was the average tip percentage?",
        "Compare yesterday to same day last week"
    ]
    
    report_parts = [
        f"# Daily Financial Report - {datetime.now().strftime('%Y-%m-%d')}",
        ""
    ]
    
    for question in questions:
        response = engine.analyze(question)
        report_parts.append(f"## {question}")
        report_parts.append(response.narrative or "")
        report_parts.append(response.to_markdown())
        report_parts.append("")
    
    report = "\n".join(report_parts)
    
    # Send email (configure SMTP settings)
    # send_email("Daily Report", report)
    
    # Or save to file
    with open(f"report_{datetime.now().strftime('%Y%m%d')}.md", 'w') as f:
        f.write(report)
    
    engine.shutdown()

if __name__ == "__main__":
    generate_daily_report()
```

## Performance Tips

### Load Only Needed Data
```python
# Load specific months
engine.initialize_data(months=["01", "02", "03"], year="2024")
```

### Use Result Limits
```python
# Limit results for faster queries
response = engine.analyze(
    "Show all trips",
    max_rows=1000  # Limit to 1000 rows
)
```

### Cache Frequently Used Data
```python
# The data loader caches loaded datasets automatically
# Reusing the same engine instance is efficient
```

### Optimize Queries
```python
# Provide specific time ranges in questions
engine.analyze("Show revenue for January 2024 only")

# Instead of
engine.analyze("Show all revenue")  # Scans all data
```

## Error Handling

```python
from analysis_engine import FinancialAnalysisEngine

engine = FinancialAnalysisEngine(get_settings())
engine.initialize_data()

try:
    response = engine.analyze("Show me data")
    
    # Check for errors
    if 'error' in response.metadata:
        print(f"Error: {response.metadata['error']}")
    else:
        print(response.to_tabular())
        
except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    engine.shutdown()
```

