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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
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
        
        response = engine.analyze(
            question=request.question,
            return_format=request.return_format,
            include_narrative=request.include_narrative,
            max_rows=request.max_rows
        )
        
        return response.to_dict()
        
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

