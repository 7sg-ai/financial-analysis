# Streamlit UI Guide

Complete guide for using the Financial Analysis Streamlit Dashboard.

## Overview

The Streamlit UI provides an interactive web interface for analyzing NYC taxi and for-hire vehicle data using natural language queries. It features:

- 🎨 **Beautiful Interface**: Modern, responsive design
- 📊 **Interactive Visualizations**: Charts and graphs for data insights
- 🔍 **Query History**: Track and re-run previous queries
- 💡 **Example Questions**: Pre-built queries to get started
- ⚡ **Real-time Results**: Instant analysis with progress indicators

## Getting Started

### 1. Launch the Dashboard

```bash
# Start Streamlit dashboard
python run_streamlit.py

# Or run directly
streamlit run streamlit_app.py
```

The dashboard will open at http://localhost:8501

### 2. Initialize the Engine

1. Click **"🚀 Initialize Engine"** in the sidebar
2. Wait for the data to load (this may take 1-2 minutes)
3. You'll see a success message when ready

### 3. Start Asking Questions

1. Type your question in the text area
2. Click **"🔍 Analyze"** 
3. View results with visualizations and narrative explanations

## Interface Overview

### Main Dashboard

```
┌─────────────────────────────────────────────────────────┐
│  🚕 Financial Analysis Dashboard                       │
│  Analyze NYC Taxi & For-Hire Vehicle Data              │
├─────────────────────────────────────────────────────────┤
│  🔍 Ask a Question                                     │
│  ┌─────────────────────────────────────────────────┐   │
│  │ What was the total revenue from yellow taxis?   │   │
│  └─────────────────────────────────────────────────┘   │
│  [🔍 Analyze] [🗑️ Clear]                             │
├─────────────────────────────────────────────────────────┤
│  📝 Analysis Summary                                   │
│  In January 2024, yellow taxis generated $45.1M...    │
├─────────────────────────────────────────────────────────┤
│  📊 Results                                            │
│  ┌─────────────────────────────────────────────────┐   │
│  │ total_revenue │ 45123456.78                    │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Sidebar Features

- **🚀 Initialize Engine**: Start the analysis engine
- **💡 Example Questions**: Pre-built queries to try
- **📚 Recent Queries**: History of your questions
- **⚙️ Settings**: Configure analysis options
- **📊 Data Info**: View dataset statistics

## Example Questions

### Revenue Analysis
- "What was the total revenue from yellow taxis in January 2024?"
- "Compare revenue between yellow and green taxis by month"
- "Which pickup zones generated the most revenue?"
- "Show revenue trends by month for 2024"

### Trip Analysis
- "How many trips were taken each month in 2024?"
- "What is the average trip distance for each taxi type?"
- "Which hour of the day has the most trips?"
- "Compare weekend vs weekday trip volumes"

### Financial Metrics
- "What is the average tip percentage for credit card payments?"
- "Calculate total driver pay vs passenger fares for HVFHS"
- "Break down revenue into fare, tips, tolls, taxes, and surcharges"
- "What are the most profitable routes?"

### Location Intelligence
- "What are the top 10 pickup locations by trip count?"
- "Which borough has the highest average fare?"
- "Show revenue breakdown by service zone"
- "Compare Manhattan vs outer borough trips"

## Features Explained

### 1. Query Input
- **Large text area** for natural language questions
- **Auto-complete** from example questions
- **Clear button** to reset the input

### 2. Results Display
- **Summary metrics** showing result count, execution time, query type
- **Narrative explanation** in plain English
- **Data table** with sortable columns
- **Interactive visualizations** (charts, graphs)

### 3. Visualizations
The dashboard automatically creates visualizations based on your data:

- **Bar Charts**: For comparing values across categories
- **Pie Charts**: For showing proportions
- **Line Charts**: For time series data
- **Tables**: For detailed data views

### 4. Query History
- **Recent queries** appear in the sidebar
- **Click to re-run** any previous query
- **Timestamp tracking** for each query

### 5. Settings
- **Include Narrative**: Toggle explanatory text
- **Max Rows**: Limit result set size
- **Data Info**: View dataset statistics

## Advanced Usage

### Custom Visualizations

The dashboard automatically creates charts based on your data structure:

```python
# For revenue data
"What was the revenue by month?"
# → Creates bar chart showing monthly revenue

# For location data  
"Show top 10 pickup zones by trip count"
# → Creates horizontal bar chart

# For time series
"Show daily trip counts for March"
# → Creates line chart
```

### Query Tips

1. **Be Specific**: "Revenue in January 2024" vs "Revenue"
2. **Use Time Ranges**: "Last 3 months" or "Q1 2024"
3. **Compare Data**: "Yellow vs green taxis"
4. **Ask for Aggregations**: "Total", "Average", "Top 10"

### Performance Optimization

- **Load specific months**: The demo loads only 3 months for faster startup
- **Use result limits**: Set max rows to avoid large datasets
- **Cache results**: The engine caches loaded data

## Troubleshooting

### Engine Won't Initialize

**Symptoms**: "Engine Not Initialized" message persists

**Solutions**:
1. Check your `.env` file has correct Azure OpenAI credentials
2. Ensure data files exist in `src_data/` directory
3. Check console logs for error messages
4. Try refreshing the page

### Slow Performance

**Symptoms**: Long loading times or timeouts

**Solutions**:
1. Reduce data scope (load fewer months)
2. Set lower max rows limit
3. Use more specific queries
4. Check available memory

### No Results Returned

**Symptoms**: Empty result tables

**Solutions**:
1. Check your question is clear and specific
2. Try example questions from the sidebar
3. Verify data exists for your time range
4. Check the generated SQL query in the expandable section

### Visualization Issues

**Symptoms**: Charts don't appear or look wrong

**Solutions**:
1. Ensure your query returns numeric data for charts
2. Try queries with fewer result rows
3. Check if data has the expected structure

## Keyboard Shortcuts

- **Ctrl+Enter**: Submit query (when focused on text area)
- **Escape**: Clear current query
- **Tab**: Navigate between interface elements

## Mobile Usage

The dashboard is responsive and works on mobile devices:

- **Touch-friendly** interface
- **Responsive charts** that adapt to screen size
- **Sidebar** collapses on small screens
- **Optimized** for tablet and phone use

## Integration with API

The Streamlit UI uses the same analysis engine as the REST API:

- **Same queries** work in both interfaces
- **Consistent results** across platforms
- **Shared configuration** and settings
- **Compatible** with API endpoints

## Customization

### Themes
The dashboard uses a light theme by default. You can customize:

```python
# In streamlit_app.py
st.set_page_config(
    page_title="Your Title",
    page_icon="🚀",  # Your icon
    layout="wide",
    initial_sidebar_state="expanded"
)
```

### Adding New Example Questions

Edit the `example_questions` list in `streamlit_app.py`:

```python
example_questions = [
    "Your custom question here",
    "Another example question",
    # ... existing questions
]
```

### Custom Visualizations

Add new chart types in the `create_visualizations` function:

```python
def create_visualizations(df, results):
    # Add your custom chart logic here
    if condition:
        fig = px.your_chart_type(df, ...)
        st.plotly_chart(fig)
```

## Best Practices

### Query Writing
1. **Start simple**: Begin with basic questions
2. **Be specific**: Include time ranges and filters
3. **Use examples**: Try the provided example questions
4. **Iterate**: Refine queries based on results

### Performance
1. **Limit data scope**: Use specific time ranges
2. **Set row limits**: Avoid extremely large result sets
3. **Cache results**: The engine caches data automatically
4. **Monitor usage**: Check execution times

### Data Exploration
1. **Start broad**: "Show total revenue"
2. **Drill down**: "Revenue by month"
3. **Compare**: "Yellow vs green taxis"
4. **Analyze**: "What drives the differences?"

## Support

If you encounter issues:

1. **Check logs**: Look at the console output
2. **Try examples**: Use the provided example questions
3. **Restart**: Click "Initialize Engine" again
4. **Check data**: Ensure data files are present
5. **Verify credentials**: Confirm Azure OpenAI settings

## Next Steps

After mastering the Streamlit UI:

1. **Explore the API**: Try the REST endpoints
2. **Customize**: Modify the interface for your needs
3. **Deploy**: Use Azure deployment scripts
4. **Integrate**: Connect to other applications
5. **Extend**: Add new data sources or visualizations

---

**Happy Analyzing!** 🚕📊
