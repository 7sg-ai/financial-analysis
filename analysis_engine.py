"""
Main analysis engine that orchestrates all components.
Integrates query generation, execution, and response formatting.
Uses DuckDB for SQL execution (in-process, no remote sessions).
"""
from typing import Dict, Any, Optional, List
import logging

from duckdb_client import create_duckdb_session, DuckDBConnectionError
from data_loader import DataLoader
from llm_query_generator import QueryGenerator, NarrativeGenerator
from query_executor import QueryExecutor
from response_formatter import AnalysisResponse, ResponseFormatter
from config import Settings

logger = logging.getLogger(__name__)


class FinancialAnalysisEngine:
    """
    Main engine for financial analysis queries.
    SQL execution runs in-process via DuckDB.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._session = create_duckdb_session(settings)
        self.data_loader = DataLoader(self._session)
        self.query_generator = QueryGenerator(
            base_url=settings.llm_base_url,
            api_key=settings.llm_api_key,
            model_name=settings.llm_model_name,
            cf_access_client_id=settings.cf_access_client_id,
            cf_access_client_secret=settings.cf_access_client_secret,
        )
        self.narrative_generator = NarrativeGenerator(
            base_url=settings.llm_base_url,
            api_key=settings.llm_api_key,
            model_name=settings.llm_model_name,
            cf_access_client_id=settings.cf_access_client_id,
            cf_access_client_secret=settings.cf_access_client_secret,
        )
        self.query_executor = QueryExecutor(self._session, max_result_rows=1000)
        self.response_formatter = ResponseFormatter()
        self._data_loaded = False
        logger.info("FinancialAnalysisEngine initialized (DuckDB)")

    def initialize_data(
        self,
        months: Optional[List[str]] = None,
        year: Optional[str] = None,
    ) -> None:
        """
        Load data and register SQL views.

        Args:
            months: Months to load (None = all)
            year: Year to load. If None, loads all available years.
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
            logger.warning(
                "No data files found at startup. Application will start "
                "but queries may fail until data is available."
            )

    def check_and_reload_data(
        self,
        months: Optional[List[str]] = None,
        year: Optional[str] = None,
    ) -> bool:
        """Reload data and re-register views."""
        self.data_loader.clear_cache()
        logger.info("Reloading data...")
        success = self.data_loader.register_temp_views(months=months, year=year)
        if success:
            self._data_loaded = True
            logger.info("Data views refreshed")
        return success

    def analyze(
        self,
        question: str,
        return_format: str = "both",
        include_narrative: bool = True,
        max_rows: Optional[int] = None,
    ) -> AnalysisResponse:
        """
        Perform end-to-end analysis from question to answer.

        Args:
            question: Natural language question
            return_format: "tabular", "narrative", or "both"
            include_narrative: Whether to generate narrative explanation
            max_rows: Maximum rows to return

        Returns:
            AnalysisResponse object with results
        """
        logger.info("Starting analysis for question: %s", question)

        if not self._data_loaded:
            self.initialize_data()

        try:
            logger.info("Generating SQL query...")
            query_result = self.query_generator.generate_query(
                user_question=question,
                include_explanation=True,
                max_rows=max_rows,
            )

            query = query_result.get("query")
            query_explanation = query_result.get("explanation", "")
            logger.info("Generated query: %s...", query[:200])

            logger.info("Validating query...")
            validation = self.query_generator.validate_query(query)

            if not validation["is_valid"]:
                error_msg = f"Query validation failed: {', '.join(validation['issues'])}"
                logger.error(error_msg)
                return AnalysisResponse(
                    question=question,
                    query=query,
                    results=[],
                    narrative=f"Error: {error_msg}",
                    metadata={
                        "validation_errors": validation["issues"],
                        "query_explanation": query_explanation,
                    },
                )

            if validation["warnings"]:
                for warning in validation["warnings"]:
                    logger.warning(warning)

            logger.info("Executing query...")
            execution_result = self.query_executor.execute_query(
                query=query, max_rows=max_rows
            )

            if not execution_result["success"]:
                error_msg = execution_result.get("error", "Unknown error")
                logger.error("Query execution failed: %s", error_msg)
                return AnalysisResponse(
                    question=question,
                    query=query,
                    results=[],
                    narrative=f"Query execution error: {error_msg}",
                    metadata={
                        "error": error_msg,
                        "query_explanation": query_explanation,
                    },
                )

            results = execution_result["data"]
            execution_time = execution_result["execution_time_ms"]
            logger.info(
                "Query executed successfully: %d rows in %dms",
                len(results),
                execution_time,
            )

            narrative = None
            if include_narrative and return_format in ["narrative", "both"]:
                logger.info("Generating narrative...")
                try:
                    narrative = self.narrative_generator.generate_narrative(
                        user_question=question,
                        query_results=results,
                        query_used=query,
                    )
                    logger.info("Narrative generated successfully")
                except Exception as e:
                    logger.error("Error generating narrative: %s", e)
                    narrative = f"Note: Could not generate narrative explanation. {query_explanation}"

            response = AnalysisResponse(
                question=question,
                query=query,
                results=results,
                narrative=narrative,
                execution_time_ms=execution_time,
                metadata={
                    "query_type": query_result.get("query_type"),
                    "tables_used": query_result.get("tables_used"),
                    "is_financial": query_result.get("is_financial"),
                    "query_explanation": query_explanation,
                    "validation_warnings": validation.get("warnings", []),
                },
            )

            logger.info("Analysis complete")
            return response

        except Exception as e:
            logger.error("Error during analysis: %s", e, exc_info=True)
            return AnalysisResponse(
                question=question,
                query="",
                results=[],
                narrative=f"An error occurred during analysis: {str(e)}",
                metadata={"error": str(e)},
            )

    def execute_custom_query(
        self,
        query: str,
        include_narrative: bool = False,
        question_context: Optional[str] = None,
    ) -> AnalysisResponse:
        """Execute a custom SQL query directly."""
        logger.info("Executing custom query")

        if not self._data_loaded:
            self.initialize_data()

        try:
            validation = self.query_generator.validate_query(query)

            if not validation["is_valid"]:
                error_msg = f"Query validation failed: {', '.join(validation['issues'])}"
                return AnalysisResponse(
                    question=question_context or "Custom query",
                    query=query,
                    results=[],
                    narrative=f"Error: {error_msg}",
                    metadata={"validation_errors": validation["issues"]},
                )

            execution_result = self.query_executor.execute_query(query)

            if not execution_result["success"]:
                error_msg = execution_result.get("error", "Unknown error")
                return AnalysisResponse(
                    question=question_context or "Custom query",
                    query=query,
                    results=[],
                    narrative=f"Execution error: {error_msg}",
                    metadata={"error": error_msg},
                )

            results = execution_result["data"]
            execution_time = execution_result["execution_time_ms"]

            narrative = None
            if include_narrative and question_context:
                try:
                    narrative = self.narrative_generator.generate_narrative(
                        user_question=question_context,
                        query_results=results,
                        query_used=query,
                    )
                except Exception as e:
                    logger.error("Error generating narrative: %s", e)

            return AnalysisResponse(
                question=question_context or "Custom query",
                query=query,
                results=results,
                narrative=narrative,
                execution_time_ms=execution_time,
            )

        except Exception as e:
            logger.error("Error executing custom query: %s", e, exc_info=True)
            return AnalysisResponse(
                question=question_context or "Custom query",
                query=query,
                results=[],
                narrative=f"Error: {str(e)}",
                metadata={"error": str(e)},
            )

    def get_suggestions(self, question: str) -> List[str]:
        """Get related question suggestions."""
        try:
            return self.query_generator.suggest_related_queries(question)
        except Exception as e:
            logger.error("Error getting suggestions: %s", e)
            return []

    def get_dataset_info(self) -> Dict[str, Any]:
        """Get information about loaded datasets."""
        try:
            return self.data_loader.get_dataset_stats()
        except Exception as e:
            logger.error("Error getting dataset info: %s", e)
            return {"error": str(e)}

    def get_query_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent query execution history."""
        return self.query_executor.get_query_history(limit=limit)

    def shutdown(self) -> None:
        """Shutdown the engine and close the DuckDB session."""
        logger.info("Shutting down FinancialAnalysisEngine")
        try:
            self._session.close()
            logger.info("DuckDB session closed")
        except Exception as e:
            logger.error("Error closing DuckDB session: %s", e)
