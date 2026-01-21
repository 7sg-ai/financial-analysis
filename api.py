"""
FastAPI REST API for Financial Analysis
Provides endpoints for query-based financial analysis
"""
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import logging
import asyncio
from contextlib import asynccontextmanager

from config import get_settings
from analysis_engine import FinancialAnalysisEngine

# Configure logging - set to DEBUG for troubleshooting
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global engine instance
engine: Optional[FinancialAnalysisEngine] = None


async def poll_for_data_files():
    """Background task to periodically check for new data files"""
    global engine
    poll_interval = 300  # Check every 5 minutes
    
    while True:
        try:
            await asyncio.sleep(poll_interval)
            if engine:
                engine.check_and_reload_data()
        except Exception as e:
            logger.error(f"Error in data polling task: {e}")
            # Continue polling even if there's an error
            await asyncio.sleep(poll_interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan"""
    global engine
    
    # Startup
    logger.info("Starting Financial Analysis API...")
    settings = get_settings()
    
    try:
        engine = FinancialAnalysisEngine(settings)
        # Try to initialize data, but don't fail if no files exist yet
        try:
            engine.initialize_data()
            logger.info("Financial Analysis Engine initialized successfully")
        except Exception as e:
            logger.warning(f"Data initialization skipped (no files found): {e}")
            logger.info("Application will start and poll for data files periodically")
        
        # Start background task to poll for new data files
        poll_task = asyncio.create_task(poll_for_data_files())
        logger.info("Started background task to poll for new data files (every 5 minutes)")
    except Exception as e:
        logger.error(f"Failed to initialize engine: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down Financial Analysis API...")
    if engine:
        engine.shutdown()


# Create FastAPI app
settings = get_settings()
app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description="Interactive financial analysis API using Azure Synapse Spark and Azure OpenAI",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request/Response Models
class AnalysisRequest(BaseModel):
    """Request model for analysis endpoint"""
    question: str = Field(..., description="Natural language question about the data")
    return_format: str = Field(
        default="both",
        description="Response format: 'tabular', 'narrative', or 'both'"
    )
    include_narrative: bool = Field(
        default=True,
        description="Whether to include narrative explanation"
    )
    max_rows: Optional[int] = Field(
        default=None,
        description="Maximum number of rows to return"
    )


class CustomQueryRequest(BaseModel):
    """Request model for custom query endpoint"""
    query: str = Field(..., description="SQL query to execute")
    question_context: Optional[str] = Field(
        default=None,
        description="Optional context for narrative generation"
    )
    include_narrative: bool = Field(
        default=False,
        description="Whether to generate narrative"
    )


class AnalysisResult(BaseModel):
    """Response model for analysis results"""
    question: str
    query: str
    results: List[Dict[str, Any]]
    result_count: int
    narrative: Optional[str] = None
    execution_time_ms: Optional[int] = None
    metadata: Dict[str, Any] = {}


# API Endpoints

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "name": settings.api_title,
        "version": settings.api_version,
        "description": "Financial Analysis API",
        "endpoints": {
            "analyze": "/api/analyze - Analyze data using natural language",
            "custom_query": "/api/query - Execute custom SQL query",
            "suggestions": "/api/suggestions - Get query suggestions",
            "datasets": "/api/datasets - Get dataset information",
            "history": "/api/history - Get query history",
            "data_status": "/api/data-status - Get status of data files and loaded views",
            "reload_data": "/api/reload-data - Manually trigger data reload from files",
            "health": "/health - Health check"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    if engine is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    return {
        "status": "healthy",
        "engine_initialized": engine is not None,
        "data_loaded": engine._data_loaded if engine else False
    }


@app.get("/api/data-status")
async def get_data_status():
    """
    Get status of data files and loaded views
    
    Shows what files exist in src_data directory and what Spark SQL views are currently registered.
    """
    if engine is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        import glob
        from pathlib import Path
        
        # Get data path from settings
        data_path = Path(engine.settings.data_path)
        
        # List all parquet files in the data directory
        parquet_files = []
        if data_path.exists():
            for pattern in ["yellow_tripdata_*.parquet", "green_tripdata_*.parquet", 
                          "fhv_tripdata_*.parquet", "fhvhv_tripdata_*.parquet"]:
                files = glob.glob(str(data_path / pattern))
                parquet_files.extend([Path(f).name for f in files])
        
        parquet_files.sort()
        
        # Get registered Spark SQL views
        registered_views = []
        view_details = {}
        
        try:
            # List all temp views
            temp_views = engine.spark.catalog.listTables()
            for view in temp_views:
                if view.tableType == "TEMPORARY":
                    view_name = view.name
                    registered_views.append(view_name)
                    
                    # Get row count for each view
                    try:
                        row_count = engine.spark.sql(f"SELECT COUNT(*) as cnt FROM {view_name}").collect()[0]["cnt"]
                        view_details[view_name] = {
                            "row_count": row_count,
                            "exists": True
                        }
                    except Exception as e:
                        view_details[view_name] = {
                            "row_count": None,
                            "exists": True,
                            "error": str(e)
                        }
        except Exception as e:
            logger.warning(f"Error listing views: {e}")
        
        # Expected views
        expected_views = ["yellow_taxi", "green_taxi", "fhv", "fhvhv"]
        
        return {
            "data_loaded": engine._data_loaded,
            "data_path": str(data_path),
            "files": {
                "total_parquet_files": len(parquet_files),
                "parquet_files": parquet_files,
                "files_by_type": {
                    "yellow": [f for f in parquet_files if f.startswith("yellow")],
                    "green": [f for f in parquet_files if f.startswith("green")],
                    "fhv": [f for f in parquet_files if f.startswith("fhv_tripdata")],
                    "fhvhv": [f for f in parquet_files if f.startswith("fhvhv")]
                }
            },
            "views": {
                "registered": registered_views,
                "expected": expected_views,
                "missing": [v for v in expected_views if v not in registered_views],
                "details": view_details
            }
        }
    except Exception as e:
        logger.error(f"Error getting data status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get data status: {str(e)}")


@app.post("/api/reload-data")
async def reload_data(
    year: str = Query(default="2024", description="Year to load data for"),
    months: Optional[List[str]] = Query(default=None, description="Months to load (e.g., ['01', '02'])")
):
    """
    Manually trigger data reload from files in src_data directory
    
    This endpoint checks for new parquet files and reloads/refreshes the Spark SQL views.
    Useful when files have been added after the initial startup.
    """
    if engine is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        logger.info(f"Manual data reload triggered for year={year}, months={months}")
        success = engine.check_and_reload_data(months=months, year=year)
        
        if success:
            return {
                "status": "success",
                "message": "Data reloaded successfully",
                "data_loaded": engine._data_loaded
            }
        else:
            return {
                "status": "no_change",
                "message": "No new data files found or data already loaded",
                "data_loaded": engine._data_loaded
            }
    except Exception as e:
        logger.error(f"Error reloading data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to reload data: {str(e)}")


@app.post("/api/analyze", response_model=AnalysisResult)
async def analyze(request: AnalysisRequest):
    """
    Analyze data using natural language question
    
    Returns results in JSON format with optional narrative explanation
    """
    if engine is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        logger.info(f"Received analysis request: {request.question}")
        logger.debug(f"Request parameters: return_format={request.return_format}, include_narrative={request.include_narrative}, max_rows={request.max_rows}")
        
        response = engine.analyze(
            question=request.question,
            return_format=request.return_format,
            include_narrative=request.include_narrative,
            max_rows=request.max_rows
        )
        
        # Debug: Log response structure
        response_dict = response.to_dict()
        logger.info(f"Analysis response structure: keys={list(response_dict.keys())}")
        logger.info(f"Response result_count: {response_dict.get('result_count', 'N/A')}")
        logger.info(f"Response results type: {type(response_dict.get('results'))}")
        logger.info(f"Response results length: {len(response_dict.get('results', []))}")
        if response_dict.get('results'):
            logger.debug(f"First result sample: {response_dict['results'][0] if len(response_dict['results']) > 0 else 'N/A'}")
        logger.debug(f"Response metadata: {response_dict.get('metadata', {})}")
        logger.debug(f"Response narrative present: {bool(response_dict.get('narrative'))}")
        
        return response_dict
        
    except Exception as e:
        logger.error(f"Error processing analysis request: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/query")
async def execute_query(request: CustomQueryRequest):
    """
    Execute a custom SQL query
    
    Allows direct SQL execution with optional narrative generation
    """
    if engine is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        logger.info(f"Received custom query request")
        
        response = engine.execute_custom_query(
            query=request.query,
            include_narrative=request.include_narrative,
            question_context=request.question_context
        )
        
        return response.to_dict()
        
    except Exception as e:
        logger.error(f"Error executing custom query: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analyze/tabular")
async def analyze_tabular(
    question: str = Query(..., description="Natural language question"),
    format: str = Query(default="grid", description="Table format (grid, simple, html, etc.)")
):
    """
    Analyze data and return results as formatted table
    
    Returns plain text formatted table
    """
    if engine is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        response = engine.analyze(
            question=question,
            return_format="tabular",
            include_narrative=False
        )
        
        table = response.to_tabular(format=format)
        
        return PlainTextResponse(content=table)
        
    except Exception as e:
        logger.error(f"Error in tabular analysis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analyze/narrative")
async def analyze_narrative(
    question: str = Query(..., description="Natural language question")
):
    """
    Analyze data and return narrative explanation only
    
    Returns plain text narrative
    """
    if engine is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        response = engine.analyze(
            question=question,
            return_format="narrative",
            include_narrative=True
        )
        
        narrative = response.narrative or "No narrative generated"
        
        return PlainTextResponse(content=narrative)
        
    except Exception as e:
        logger.error(f"Error in narrative analysis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analyze/markdown")
async def analyze_markdown(
    question: str = Query(..., description="Natural language question")
):
    """
    Analyze data and return results in Markdown format
    
    Returns Markdown formatted response with narrative and table
    """
    if engine is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        response = engine.analyze(
            question=question,
            return_format="both",
            include_narrative=True
        )
        
        markdown = response.to_markdown()
        
        return PlainTextResponse(content=markdown)
        
    except Exception as e:
        logger.error(f"Error in markdown analysis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/suggestions")
async def get_suggestions(
    question: str = Query(..., description="Question to get suggestions for")
):
    """
    Get related question suggestions based on a question
    """
    if engine is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        suggestions = engine.get_suggestions(question)
        
        return {
            "question": question,
            "suggestions": suggestions
        }
        
    except Exception as e:
        logger.error(f"Error getting suggestions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/datasets")
async def get_datasets():
    """
    Get information about available datasets
    """
    if engine is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        info = engine.get_dataset_info()
        
        return {
            "datasets": info,
            "description": "NYC Taxi and For-Hire Vehicle trip data for 2024"
        }
        
    except Exception as e:
        logger.error(f"Error getting dataset info: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/history")
async def get_history(
    limit: int = Query(default=10, ge=1, le=100, description="Number of history entries")
):
    """
    Get recent query execution history
    """
    if engine is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        history = engine.get_query_history(limit=limit)
        
        return {
            "count": len(history),
            "history": history
        }
        
    except Exception as e:
        logger.error(f"Error getting history: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/docs/examples")
async def get_examples():
    """
    Get example questions that can be asked
    """
    examples = {
        "revenue_analysis": [
            "What was the total revenue from yellow taxis in 2024?",
            "Compare revenue between yellow and green taxis by month",
            "Which taxi zones generated the most revenue?"
        ],
        "trip_analysis": [
            "How many trips were taken in January 2024?",
            "What is the average trip distance for HVFHS services?",
            "Which hour of the day has the most trips?"
        ],
        "financial_metrics": [
            "What is the average tip percentage for credit card payments?",
            "Calculate the total driver pay vs passenger fares for HVFHS",
            "What are the top 10 most profitable routes?"
        ],
        "time_series": [
            "Show daily trip counts for March 2024",
            "What is the revenue trend by month?",
            "Compare weekend vs weekday trip volumes"
        ],
        "location_analysis": [
            "Which borough has the highest average fare?",
            "Top 10 pickup locations by trip count",
            "Revenue breakdown by service zone"
        ]
    }
    
    return examples


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "api:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True,
        log_level=settings.log_level.lower()
    )

