"""
Main analysis engine that orchestrates all components
Integrates query generation, execution, and response formatting
"""
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession
import logging

from data_loader import DataLoader, create_spark_session
from llm_query_generator import QueryGenerator, NarrativeGenerator
from query_executor import QueryExecutor
from response_formatter import AnalysisResponse, ResponseFormatter
from config import Settings

logger = logging.getLogger(__name__)


class FinancialAnalysisEngine:
    """
    Main engine for financial analysis queries
    Coordinates all components to answer user questions
    """
    
    def __init__(
        self,
        settings: Settings,
        spark_session: Optional[SparkSession] = None
    ):
        """
        Initialize FinancialAnalysisEngine
        
        Args:
            settings: Application settings
            spark_session: Optional pre-configured Spark session
        """
        self.settings = settings
        
        # Initialize Spark
        # Spark session connects to Azure Synapse Spark pools remotely
        if spark_session:
            self.spark = spark_session
        else:
            self.spark = create_spark_session(
                app_name="FinancialAnalysis",
                settings=settings
            )
        
        # Initialize components
        self.data_loader = DataLoader(self.spark, settings.data_path)
        self.query_generator = QueryGenerator(
            endpoint=settings.azure_openai_endpoint,
            api_key=settings.azure_openai_api_key,
            deployment_name=settings.azure_openai_deployment_name,
            api_version=settings.azure_openai_api_version
        )
        self.narrative_generator = NarrativeGenerator(
            endpoint=settings.azure_openai_endpoint,
            api_key=settings.azure_openai_api_key,
            deployment_name=settings.azure_openai_deployment_name,
            api_version=settings.azure_openai_api_version
        )
        self.query_executor = QueryExecutor(self.spark, max_result_rows=1000)
        self.response_formatter = ResponseFormatter()
        
        # Load data and register views
        self._data_loaded = False
        
        logger.info("FinancialAnalysisEngine initialized")
    
    def initialize_data(
        self,
        months: Optional[List[str]] = None,
        year: str = "2024"
    ) -> None:
        """
        Load data and register Spark SQL views
        
        Args:
            months: Months to load (None = all)
            year: Year to load data for
        """
        if self._data_loaded:
            logger.info("Data already loaded, skipping initialization")
            return
        
        logger.info("Initializing data and registering views...")
        success = self.data_loader.register_temp_views(months=months, year=year)
        if success:
            self._data_loaded = True
            logger.info("Data initialization complete")
        else:
            logger.warning("No data files found at startup. Application will start but queries may fail until data is available.")
            logger.info("The application will periodically check for new data files.")
    
    def check_and_reload_data(
        self,
        months: Optional[List[str]] = None,
        year: str = "2024"
    ) -> bool:
        """
        Check for new data files and reload if found
        
        Args:
            months: Months to check (None = all)
            year: Year to check data for
            
        Returns:
            True if new data was loaded, False otherwise
        """
        if self._data_loaded:
            # Check if there are new files
            import glob
            from pathlib import Path
            data_path = Path(self.settings.data_path)
            
            patterns = [
                f"yellow_tripdata_{year}-*.parquet",
                f"green_tripdata_{year}-*.parquet",
                f"fhv_tripdata_{year}-*.parquet",
                f"fhvhv_tripdata_{year}-*.parquet"
            ]
            
            has_new_files = any(
                glob.glob(str(data_path / pattern))
                for pattern in patterns
            )
            
            if not has_new_files:
                return False
        
        # Try to reload data
        logger.info("Checking for new data files...")
        success = self.data_loader.register_temp_views(months=months, year=year)
        if success and not self._data_loaded:
            self._data_loaded = True
            logger.info("New data files detected and loaded successfully")
            return True
        elif success:
            logger.info("Data views refreshed with new files")
            return True
        
        return False
    
    def analyze(
        self,
        question: str,
        return_format: str = "both",
        include_narrative: bool = True,
        max_rows: Optional[int] = None
    ) -> AnalysisResponse:
        """
        Perform end-to-end analysis from question to answer
        
        Args:
            question: Natural language question
            return_format: "tabular", "narrative", or "both"
            include_narrative: Whether to generate narrative explanation
            max_rows: Maximum rows to return
            
        Returns:
            AnalysisResponse object with results
        """
        logger.info(f"Starting analysis for question: {question}")
        
        # Ensure data is loaded
        if not self._data_loaded:
            self.initialize_data()
        
        try:
            # Step 1: Generate SQL query
            logger.info("Generating SQL query...")
            query_result = self.query_generator.generate_query(
                user_question=question,
                include_explanation=True,
                max_rows=max_rows
            )
            
            query = query_result.get('query')
            query_explanation = query_result.get('explanation', '')
            
            logger.info(f"Generated query: {query[:200]}...")
            
            # Step 2: Validate query
            logger.info("Validating query...")
            validation = self.query_generator.validate_query(query)
            
            if not validation['is_valid']:
                error_msg = f"Query validation failed: {', '.join(validation['issues'])}"
                logger.error(error_msg)
                return AnalysisResponse(
                    question=question,
                    query=query,
                    results=[],
                    narrative=f"Error: {error_msg}",
                    metadata={
                        'validation_errors': validation['issues'],
                        'query_explanation': query_explanation
                    }
                )
            
            # Log warnings
            if validation['warnings']:
                for warning in validation['warnings']:
                    logger.warning(warning)
            
            # Step 3: Execute query
            logger.info("Executing query...")
            logger.debug(f"Executing SQL query: {query}")
            execution_result = self.query_executor.execute_query(
                query=query,
                max_rows=max_rows
            )
            
            logger.debug(f"Query execution result keys: {list(execution_result.keys())}")
            logger.debug(f"Query execution success: {execution_result.get('success')}")
            logger.debug(f"Query execution error: {execution_result.get('error', 'None')}")
            logger.debug(f"Query execution data type: {type(execution_result.get('data'))}")
            logger.debug(f"Query execution data length: {len(execution_result.get('data', []))}")
            
            if not execution_result['success']:
                error_msg = execution_result.get('error', 'Unknown error')
                logger.error(f"Query execution failed: {error_msg}")
                return AnalysisResponse(
                    question=question,
                    query=query,
                    results=[],
                    narrative=f"Query execution error: {error_msg}",
                    metadata={
                        'error': error_msg,
                        'query_explanation': query_explanation
                    }
                )
            
            results = execution_result['data']
            execution_time = execution_result['execution_time_ms']
            
            logger.info(f"Query executed successfully: {len(results)} rows in {execution_time}ms")
            logger.debug(f"Results type: {type(results)}")
            logger.debug(f"Results is list: {isinstance(results, list)}")
            if results:
                logger.debug(f"First result type: {type(results[0])}")
                logger.debug(f"First result keys: {list(results[0].keys()) if isinstance(results[0], dict) else 'Not a dict'}")
                logger.debug(f"First result sample: {results[0] if len(results) > 0 else 'N/A'}")
            else:
                logger.warning("Query execution returned empty results list!")
            
            # Step 4: Generate narrative (if requested)
            narrative = None
            if include_narrative and return_format in ["narrative", "both"]:
                logger.info("Generating narrative...")
                try:
                    narrative = self.narrative_generator.generate_narrative(
                        user_question=question,
                        query_results=results,
                        query_used=query
                    )
                    logger.info("Narrative generated successfully")
                except Exception as e:
                    logger.error(f"Error generating narrative: {e}")
                    narrative = f"Note: Could not generate narrative explanation. {query_explanation}"
            
            # Step 5: Create response
            logger.debug(f"Creating AnalysisResponse with {len(results)} results")
            logger.debug(f"Results before creating response: {results[:2] if len(results) > 0 else 'Empty'}")
            
            response = AnalysisResponse(
                question=question,
                query=query,
                results=results,
                narrative=narrative,
                execution_time_ms=execution_time,
                metadata={
                    'query_type': query_result.get('query_type'),
                    'tables_used': query_result.get('tables_used'),
                    'is_financial': query_result.get('is_financial'),
                    'query_explanation': query_explanation,
                    'validation_warnings': validation.get('warnings', [])
                }
            )
            
            logger.debug(f"Response created. Response.results type: {type(response.results)}")
            logger.debug(f"Response.results length: {len(response.results)}")
            logger.info("Analysis complete")
            return response
            
        except Exception as e:
            logger.error(f"Error during analysis: {e}", exc_info=True)
            return AnalysisResponse(
                question=question,
                query="",
                results=[],
                narrative=f"An error occurred during analysis: {str(e)}",
                metadata={'error': str(e)}
            )
    
    def execute_custom_query(
        self,
        query: str,
        include_narrative: bool = False,
        question_context: Optional[str] = None
    ) -> AnalysisResponse:
        """
        Execute a custom SQL query directly
        
        Args:
            query: SQL query to execute
            include_narrative: Whether to generate narrative
            question_context: Optional context for narrative generation
            
        Returns:
            AnalysisResponse object
        """
        logger.info("Executing custom query")
        
        # Ensure data is loaded
        if not self._data_loaded:
            self.initialize_data()
        
        try:
            # Validate query
            validation = self.query_generator.validate_query(query)
            
            if not validation['is_valid']:
                error_msg = f"Query validation failed: {', '.join(validation['issues'])}"
                return AnalysisResponse(
                    question=question_context or "Custom query",
                    query=query,
                    results=[],
                    narrative=f"Error: {error_msg}",
                    metadata={'validation_errors': validation['issues']}
                )
            
            # Execute
            execution_result = self.query_executor.execute_query(query)
            
            if not execution_result['success']:
                error_msg = execution_result.get('error', 'Unknown error')
                return AnalysisResponse(
                    question=question_context or "Custom query",
                    query=query,
                    results=[],
                    narrative=f"Execution error: {error_msg}",
                    metadata={'error': error_msg}
                )
            
            results = execution_result['data']
            execution_time = execution_result['execution_time_ms']
            
            # Generate narrative if requested
            narrative = None
            if include_narrative and question_context:
                try:
                    narrative = self.narrative_generator.generate_narrative(
                        user_question=question_context,
                        query_results=results,
                        query_used=query
                    )
                except Exception as e:
                    logger.error(f"Error generating narrative: {e}")
            
            return AnalysisResponse(
                question=question_context or "Custom query",
                query=query,
                results=results,
                narrative=narrative,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            logger.error(f"Error executing custom query: {e}", exc_info=True)
            return AnalysisResponse(
                question=question_context or "Custom query",
                query=query,
                results=[],
                narrative=f"Error: {str(e)}",
                metadata={'error': str(e)}
            )
    
    def get_suggestions(self, question: str) -> List[str]:
        """
        Get related question suggestions
        
        Args:
            question: User's question
            
        Returns:
            List of suggested questions
        """
        try:
            return self.query_generator.suggest_related_queries(question)
        except Exception as e:
            logger.error(f"Error getting suggestions: {e}")
            return []
    
    def get_dataset_info(self) -> Dict[str, Any]:
        """
        Get information about loaded datasets
        
        Returns:
            Dictionary with dataset statistics
        """
        try:
            return self.data_loader.get_dataset_stats()
        except Exception as e:
            logger.error(f"Error getting dataset info: {e}")
            return {'error': str(e)}
    
    def get_query_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent query execution history
        
        Args:
            limit: Maximum number of entries
            
        Returns:
            List of query history entries
        """
        return self.query_executor.get_query_history(limit=limit)
    
    def shutdown(self) -> None:
        """Shutdown the engine and clean up resources"""
        logger.info("Shutting down FinancialAnalysisEngine")
        try:
            self.spark.stop()
            logger.info("Spark session stopped")
        except Exception as e:
            logger.error(f"Error stopping Spark session: {e}")

