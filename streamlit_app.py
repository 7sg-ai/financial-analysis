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

# Configure logging
logging.basicConfig(level=logging.INFO)
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
if 'analysis_engine' not in st.session_state:
    st.session_state.analysis_engine = None
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'api_available' not in st.session_state:
    st.session_state.api_available = False

def initialize_engine():
    """Initialize the analysis engine"""
    try:
        from analysis_engine import FinancialAnalysisEngine
        from config import get_settings
        
        if st.session_state.analysis_engine is None:
            with st.spinner("Initializing Financial Analysis Engine..."):
                settings = get_settings()
                engine = FinancialAnalysisEngine(settings)
                engine.initialize_data(months=["01", "02", "03"], year="2024")  # Load first 3 months for demo
                st.session_state.analysis_engine = engine
                st.session_state.api_available = True
                st.success("✅ Engine initialized successfully!")
    except Exception as e:
        st.error(f"❌ Failed to initialize engine: {str(e)}")
        st.session_state.api_available = False

def call_api(question: str, return_format: str = "both", include_narrative: bool = True) -> Optional[Dict[str, Any]]:
    """Call the analysis API"""
    if not st.session_state.api_available or st.session_state.analysis_engine is None:
        return None
    
    try:
        response = st.session_state.analysis_engine.analyze(
            question=question,
            return_format=return_format,
            include_narrative=include_narrative,
            max_rows=1000
        )
        return response.to_dict()
    except Exception as e:
        st.error(f"API Error: {str(e)}")
        return None

def display_results(results: Dict[str, Any]):
    """Display analysis results in various formats"""
    if not results or not results.get('results'):
        st.warning("No results to display")
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
        
        # Initialize engine button
        if st.button("🚀 Initialize Engine", type="primary"):
            initialize_engine()
        
        # Engine status
        if st.session_state.api_available:
            st.success("✅ Engine Ready")
        else:
            st.warning("⚠️ Engine Not Initialized")
        
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
                st.session_state.selected_question = question
        
        st.markdown("---")
        
        # Query history
        if st.session_state.query_history:
            st.markdown("### 📚 Recent Queries")
            for i, (question, timestamp) in enumerate(st.session_state.query_history[-5:]):
                if st.button(f"🔄 {question[:30]}...", key=f"history_{i}"):
                    st.session_state.selected_question = question
        
        st.markdown("---")
        
        # Settings
        st.markdown("### ⚙️ Settings")
        include_narrative = st.checkbox("Include Narrative", value=True)
        max_rows = st.slider("Max Rows", 10, 1000, 100)
        
        # Data info
        if st.session_state.api_available:
            st.markdown("### 📊 Data Info")
            try:
                info = st.session_state.analysis_engine.get_dataset_info()
                for dataset, stats in info.items():
                    if 'row_count' in stats:
                        st.text(f"{dataset}: {stats['row_count']:,} rows")
            except:
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
    
    # Pre-fill with selected question
    default_question = ""
    if 'selected_question' in st.session_state:
        default_question = st.session_state.selected_question
        del st.session_state.selected_question
    
    question = st.text_area(
        "Enter your question about the taxi data:",
        value=default_question,
        height=100,
        placeholder="e.g., What was the total revenue from yellow taxis in January 2024?"
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
        st.rerun()
    
    # Handle analyze button
    if analyze_button and question:
        with st.spinner("🤔 Analyzing your question..."):
            start_time = time.time()
            
            # Call the analysis API
            results = call_api(
                question=question,
                return_format="both",
                include_narrative=include_narrative
            )
            
            end_time = time.time()
            
            if results:
                # Add to query history
                st.session_state.query_history.append((question, datetime.now()))
                
                # Display results
                st.markdown('<div class="result-section">', unsafe_allow_html=True)
                display_results(results)
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Show performance info
                st.success(f"✅ Analysis completed in {end_time - start_time:.2f} seconds")
            else:
                st.error("❌ Failed to analyze the question. Please try again.")
    
    # Show recent queries if any
    if st.session_state.query_history:
        st.markdown("### 📚 Recent Queries")
        for i, (q, timestamp) in enumerate(st.session_state.query_history[-3:]):
            with st.expander(f"Query {len(st.session_state.query_history) - i}: {q[:50]}..."):
                st.text(f"Time: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                st.text(f"Question: {q}")
                if st.button(f"Re-run Query {len(st.session_state.query_history) - i}", key=f"rerun_{i}"):
                    st.session_state.selected_question = q
                    st.rerun()

if __name__ == "__main__":
    main()
