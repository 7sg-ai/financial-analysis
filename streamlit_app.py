"""
Streamlit UI for Financial Analysis Application
Provides a web interface for natural language queries on taxi data
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import time
import logging
from typing import Dict, Any, List, Optional
import os
import requests
import boto3
import os

# Configure logging - set to DEBUG for troubleshooting
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Financial Analysis Dashboard",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .query-box {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border: 1px solid #dee2e6;
    }
    .result-section {
        margin-top: 2rem;
        padding: 1rem;
        background-color: #ffffff;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stButton > button {
        background-color: #1f77b4;
        color: white;
        border-radius: 0.5rem;
        border: none;
        padding: 0.5rem 1rem;
        font-weight: 500;
    }
    .stButton > button:hover {
        background-color: #0d5a8a;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
# Always read API_URL from environment variable (can change between restarts)
# This ensures the app uses the latest environment variable value
env_api_url = os.getenv("API_URL", "https://<api-id>.execute-api.<region>.amazonaws.com/prod")
if 'api_url' not in st.session_state or st.session_state.api_url != env_api_url:
    # Update session state if environment variable changed or not set
    st.session_state.api_url = env_api_url if env_api_url.startswith('http') else f"https://{os.getenv('AWS_API_HOST', '<api-id>.execute-api.<region>.amazonaws.com')}/prod"
    logger.info(f"API URL set from environment: {env_api_url}")
    # Reset API availability when URL changes
    if 'api_url' in st.session_state and st.session_state.api_url != env_api_url:
        logger.info(f"API URL changed from {st.session_state.api_url} to {env_api_url}, resetting connection")
        st.session_state.api_available = False

if 'api_available' not in st.session_state:
    st.session_state.api_available = False
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'question_input' not in st.session_state:
    st.session_state.question_input = ""

# Log the current API URL being used
logger.info(f"Using API URL: {st.session_state.api_url} (from env: {env_api_url})")

def check_api_health():
    """Check if the API backend is available"""
    try:
        response = import boto3
session = boto3.Session()
client = session.client('apigatewaymanagementapi', endpoint_url=st.session_state.api_url.replace('https://', '').split('/')[0]) if 'execute-api' in st.session_state.api_url else None
# For non-API Gateway, use requests as-is; for API Gateway, sign with SigV4 or use API key header
        st.session_state.api_available = response.status_code == 200
        return st.session_state.api_available
    except Exception as e:
        st.session_state.api_available = False
        return False

def initialize_api():
    """Initialize connection to the API backend"""
    # Re-read API_URL from environment in case it changed
    env_api_url = os.getenv("API_URL", "http://localhost:8000")
    if st.session_state.api_url != env_api_url:
        logger.info(f"API URL changed, updating from {st.session_state.api_url} to {env_api_url}")
        st.session_state.api_url = env_api_url
        st.session_state.api_available = False
    
    if not st.session_state.api_available:
        logger.info(f"Initializing API connection to: {st.session_state.api_url}")
        with st.spinner("Connecting to Financial Analysis API..."):
            if check_api_health():
                st.success("✅ Connected to API backend successfully!")
            else:
                st.error(f"❌ Failed to connect to API backend at {st.session_state.api_url}")
                st.info("Make sure the FastAPI backend is running and accessible.")
                logger.warning(f"Failed to connect to API at {st.session_state.api_url}")

def call_api(question: str, return_format: str = "both", include_narrative: bool = True) -> Optional[Dict[str, Any]]:
    """Call the analysis API backend"""
    logger.info(f"call_api called with question: {question[:100]}...")
    logger.debug(f"API URL: {st.session_state.api_url}")
    logger.debug(f"Parameters: return_format={return_format}, include_narrative={include_narrative}")
    
    if not st.session_state.api_available:
        logger.info("API not available, initializing...")
        initialize_api()
        if not st.session_state.api_available:
            logger.error("API initialization failed")
            return None
    
    try:
        request_payload = {
            "question": question,
            "return_format": return_format,
            "include_narrative": include_narrative,
            "max_rows": 1000
        }
        logger.debug(f"Sending POST request to {st.session_state.api_url}/api/analyze")
        logger.debug(f"Request payload: {request_payload}")
        
        response = requests.post(
            f"{st.session_state.api_url}/api/analyze",
            json=request_payload,
            timeout=300.0  # 5 minute timeout for long queries
        )
        
        logger.info(f"API response status: {response.status_code}")
        response.raise_for_status()
        
        response_json = response.json()
        logger.info(f"API response received. Response keys: {list(response_json.keys())}")
        logger.debug(f"Response result_count: {response_json.get('result_count', 'N/A')}")
        logger.debug(f"Response results type: {type(response_json.get('results'))}")
        logger.debug(f"Response results length: {len(response_json.get('results', []))}")
        logger.debug(f"Response has narrative: {bool(response_json.get('narrative'))}")
        logger.debug(f"Response metadata: {response_json.get('metadata', {})}")
        
        if response_json.get('results'):
            logger.debug(f"First result sample: {response_json['results'][0] if len(response_json['results']) > 0 else 'N/A'}")
        else:
            logger.warning("API response has empty results!")
            logger.debug(f"Full response structure: {response_json}")
        
        return response_json
    except requests.exceptions.HTTPError as e:
        logger.error(f"API HTTP Error: {str(e)}")
        logger.error(f"Response status: {e.response.status_code if hasattr(e, 'response') else 'N/A'}")
        logger.error(f"Response text: {e.response.text if hasattr(e, 'response') and hasattr(e.response, 'text') else 'N/A'}")
        st.error(f"API Error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error calling API: {str(e)}", exc_info=True)
        st.error(f"Error calling API: {str(e)}")
        return None

def display_results(results: Dict[str, Any]):
    """Display analysis results in various formats"""
    logger.info("display_results called")
    logger.debug(f"Results parameter type: {type(results)}")
    logger.debug(f"Results is None: {results is None}")
    logger.debug(f"Results keys: {list(results.keys()) if results else 'N/A'}")
    logger.debug(f"Results.get('results'): {results.get('results') if results else 'N/A'}")
    logger.debug(f"Results.get('results') type: {type(results.get('results')) if results else 'N/A'}")
    logger.debug(f"Results.get('results') length: {len(results.get('results', [])) if results else 'N/A'}")
    logger.debug(f"Results.get('result_count'): {results.get('result_count') if results else 'N/A'}")
    
    if not results:
        logger.warning("display_results: results parameter is None or empty")
        st.warning("No results to display")
        return
    
    if not results.get('results'):
        logger.warning(f"display_results: results.get('results') is empty. Full results dict: {results}")
        validation_errors = results.get('metadata', {}).get('validation_errors', [])
        is_security_rejection = any(
            'dangerous keyword' in str(e).lower() or 'forbidden operation' in str(e).lower()
            for e in validation_errors
        )
        if is_security_rejection:
            st.error("Query was stopped due to security concerns with the SQL statement")
        else:
            st.warning("No results to display")
        logger.debug(f"Results structure: {json.dumps(results, indent=2, default=str)}")
        return
    
    # Results summary
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Results Count", len(results['results']))
    with col2:
        st.metric("Execution Time", f"{results.get('execution_time_ms', 0)}ms")
    with col3:
        st.metric("Query Type", results.get('metadata', {}).get('query_type', 'Unknown'))
    with col4:
        st.metric("Tables Used", len(results.get('metadata', {}).get('tables_used', [])))
    
    # Display narrative if available
    if results.get('narrative'):
        st.markdown("### 📝 Analysis Summary")
        st.info(results['narrative'])
    
    # Display results as table
    if results['results']:
        st.markdown("### 📊 Results")
        df = pd.DataFrame(results['results'])
        st.dataframe(df, use_container_width=True)
        
        # Create visualizations if data is suitable
        create_visualizations(df, results)
    
    # Show the generated SQL query
    with st.expander("🔍 View Generated SQL Query"):
        st.code(results['query'], language='sql')

def create_visualizations(df: pd.DataFrame, results: Dict[str, Any]):
    """Create visualizations based on the results"""
    if df.empty:
        return
    
    # Determine visualization type based on data
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    
    # If we have numeric data, create charts
    if numeric_cols and len(df) > 1:
        st.markdown("### 📈 Visualizations")
        
        # Bar chart for top values
        if len(df) <= 20:  # Only for reasonable number of rows
            fig = px.bar(
                df.head(10), 
                x=df.columns[0], 
                y=numeric_cols[0] if numeric_cols else df.columns[1],
                title=f"Top 10 {df.columns[0]} by {numeric_cols[0] if numeric_cols else df.columns[1]}"
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        
        # Pie chart for categorical data
        if categorical_cols and numeric_cols and len(df) <= 10:
            fig = px.pie(
                df, 
                names=categorical_cols[0], 
                values=numeric_cols[0],
                title=f"Distribution by {categorical_cols[0]}"
            )
            st.plotly_chart(fig, use_container_width=True)
def main():
    """Main Streamlit application"""
    
    # Header
    st.markdown('<h1 class="main-header">🚕 Financial Analysis Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("**Analyze NYC Taxi & For-Hire Vehicle Data with Natural Language Queries**")
    
    # Sidebar
    with st.sidebar:
        st.markdown("### 🎯 Quick Actions")
        
        # Connect to API button
        if st.button("🚀 Connect to API", type="primary"):
            initialize_api()
        
        # API status
        if st.session_state.api_available:
            st.success("✅ API Connected")
            st.info(f"Backend: {st.session_state.api_url}")
            # Show environment variable value for debugging
            env_api_url = os.getenv("API_URL", "Not set")
            if env_api_url != st.session_state.api_url:
                st.warning(f"⚠️ Env API_URL ({env_api_url}) differs from session ({st.session_state.api_url})")
        else:
            st.warning("⚠️ API Not Connected")
            st.info(f"API URL: {st.session_state.api_url}")
            env_api_url = os.getenv("API_URL", "Not set")
            st.info(f"Environment API_URL: {env_api_url}")
            st.info("Click 'Connect to API' to connect to the backend")
        
        st.markdown("---")
        
        # Example questions
        st.markdown("### 💡 Example Questions")
        example_questions = [
            "What was the total revenue from yellow taxis in January 2024?",
            "Compare revenue between yellow and green taxis",
            "Which pickup zones generated the most revenue?",
            "What is the average tip percentage for credit card payments?",
            "Show the top 10 most profitable routes",
            "What are the peak hours for taxi trips?",
            "Calculate total driver pay vs passenger fares for HVFHS",
            "Which borough has the highest average fare?",
            "Show daily trip counts for March 2024",
            "What percentage of trips involve airports?"
        ]
        
        for i, question in enumerate(example_questions):
            if st.button(f"💬 {question[:50]}...", key=f"example_{i}"):
                st.session_state.question_input = question
                st.rerun()
        
        st.markdown("---")
        
        # Query history
        if st.session_state.query_history:
            st.markdown("### 📚 Recent Queries")
            for i, (question, timestamp) in enumerate(st.session_state.query_history[-5:]):
                if st.button(f"🔄 {question[:30]}...", key=f"history_{i}"):
                    st.session_state.question_input = question
                    st.rerun()
        
        st.markdown("---")
        
        # Settings
        st.markdown("### ⚙️ Settings")
        include_narrative = st.checkbox("Include Narrative", value=True)
        max_rows = st.slider("Max Rows", 10, 1000, 100)
        
        # Data info (from API /api/data-status)
        if st.session_state.api_available:
            st.markdown("### 📊 Data Info")
            try:
                import requests
                r = requests.get(f"{st.session_state.api_url}/api/data-status", timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    for view_name, detail in data.get("views", {}).get("details", {}).items():
                        rc = detail.get("row_count")
                        if rc is not None:
                            st.text(f"{view_name}: {rc:,} rows")
                else:
                    st.text("Data info unavailable")
            except Exception:
                st.text("Data info unavailable")

    # Main content area
    if not st.session_state.api_available:
        st.markdown("""
        <div class="query-box">
            <h3>🚀 Welcome to Financial Analysis Dashboard</h3>
            <p>This application allows you to analyze NYC taxi and for-hire vehicle data using natural language queries.</p>
            <p><strong>To get started:</strong></p>
            <ol>
                <li>Click "Initialize Engine" in the sidebar</li>
                <li>Wait for the engine to load the data</li>
                <li>Start asking questions about the data!</li>
            </ol>
            <p><strong>Available Data:</strong></p>
            <ul>
                <li>Yellow Taxi trips (2024)</li>
                <li>Green Taxi trips (2024)</li>
                <li>For-Hire Vehicle (FHV) trips (2024)</li>
                <li>High-Volume For-Hire Vehicle (FHVHV) trips (2024)</li>
                <li>Taxi zone lookup data</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        # Show sample questions
        st.markdown("### 💡 Sample Questions You Can Ask")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **Revenue Analysis:**
            - What was the total revenue from yellow taxis?
            - Compare revenue between taxi types
            - Which zones generated the most revenue?
            - Show revenue trends by month
            """)
        
        with col2:
            st.markdown("""
            **Trip Analysis:**
            - How many trips were taken each month?
            - What are the peak hours for trips?
            - Which routes are most popular?
            - Compare weekend vs weekday trips
            """)
        
        return

    # Query input section
    st.markdown('<div class="query-box">', unsafe_allow_html=True)
    st.markdown("### 🔍 Ask a Question")
    
    question = st.text_area(
        "Enter your question about the taxi data:",
        height=100,
        placeholder="e.g., What was the total revenue from yellow taxis in January 2024?",
        key="question_input",
    )
    
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        analyze_button = st.button("🔍 Analyze", type="primary")
    with col2:
        clear_button = st.button("🗑️ Clear")
    with col3:
        st.markdown("*Tip: Use the example questions in the sidebar for inspiration*")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Handle clear button
    if clear_button:
        st.session_state.question_input = ""
        st.rerun()
    
    # Handle analyze button
    if analyze_button and question:
        logger.info(f"Analyze button clicked with question: {question}")
        with st.spinner("🤔 Analyzing your question..."):
            start_time = time.time()
            
            # Call the analysis API
            logger.debug("Calling call_api function...")
            results = call_api(
                question=question,
                return_format="both",
                include_narrative=include_narrative
            )
            
            end_time = time.time()
            logger.info(f"API call completed in {end_time - start_time:.2f} seconds")
            logger.debug(f"Results returned from call_api: {results is not None}")
            
            if results:
                logger.info("Results received, adding to history and displaying...")
                # Add to query history
                st.session_state.query_history.append((question, datetime.now()))
                
                # Display results
                st.markdown('<div class="result-section">', unsafe_allow_html=True)
                display_results(results)
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Show performance info
                st.success(f"✅ Analysis completed in {end_time - start_time:.2f} seconds")
            else:
                logger.error("call_api returned None - no results received")
                st.error("❌ Failed to analyze the question. Please try again.")
                st.info("Check the logs for detailed error information.")
    
    # Show recent queries if any
    if st.session_state.query_history:
        st.markdown("### 📚 Recent Queries")
        for i, (q, timestamp) in enumerate(st.session_state.query_history[-3:]):
            with st.expander(f"Query {len(st.session_state.query_history) - i}: {q[:50]}..."):
                st.text(f"Time: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                st.text(f"Question: {q}")
                if st.button(f"Re-run Query {len(st.session_state.query_history) - i}", key=f"rerun_{i}"):
                    st.session_state.question_input = q
                    st.rerun()

if __name__ == "__main__":
    main()