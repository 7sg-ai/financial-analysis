"""
Main analysis engine that orchestrates all components
Integrates query generation, execution, and response formatting
Uses Crusoe Cloud EMR Serverless and managed inference.
"""
from typing import Dict, Any, Optional, List
import logging
import boto3
from botocore.exceptions import ClientError
from openai import OpenAI

from data_loader import DataLoader
from response_formatter import AnalysisResponse, ResponseFormatter
from config import Settings
from typing import Optional

logger = logging.getLogger(__name__)


class FinancialAnalysisEngine:
    """
    Main engine for financial analysis queries.
    All Spark execution runs in Crusoe Cloud EMR Serverless.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._emr_client = boto3.client(
            "emr-serverless",
            region_name=settings.aws_region or "us-east-1",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key
        )
        self._openai_client = OpenAI(
            base_url=settings.crusoe_inference_endpoint or "https://inference.api.crusoecloud.com/v1",
            api_key=settings.crusoe_api_key
        )
        self.data_loader = DataLoader()
        self.query_executor = QueryExecutor(self._emr_client, max_result_rows=1000)
        self.response_formatter = ResponseFormatter()
        self._data_loaded = False
        logger.info("FinancialAnalysisEngine initialized (Crusoe Cloud EMR Serverless)")
    
    def initialize_data(
        self,
        months: Optional[List[str]] = None,
        year: Optional[str] = None
    ) -> None:
        """
        Load data and register Spark SQL views
        
        Args:
            months: Months to load (None = all)
            year: Year to load. If None, loads all available years (2023-2025)
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
        year: Optional[str] = None,
    ) -> bool:
        """
        Reload data and re-register views in EMR Serverless.
        With EMR Serverless we always attempt reload when called.
        """
        self.data_loader.clear_cache()
        logger.info("Reloading data in EMR Serverless...")
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
            response = self._openai_client.chat.completions.create(
                model="meta-llama/Meta-Llama-3.1-8B-Instruct",
                messages=[
                    {"role": "system", "content": "You are a SQL expert."},
                    {"role": "user", "content": f"Generate a SQL query for: {question}. Limit to {max_rows} rows."}
                ],
                temperature=0.1
            )
            query_result = {
                "query": response.choices[0].message.content,
                "explanation": "Generated via Llama-3.1-8B-Instruct"
            }
            
            query = query_result.get('query')
            query_explanation = query_result.get('explanation', '')
            
            logger.info(f"Generated query: {query[:200]}...")
            
            # Step 2: Validate query
            logger.info("Validating query...")
            response = self._openai_client.chat.completions.create(
                model="meta-llama/Meta-Llama-3.1-8B-Instruct",
                messages=[
                    {"role": "system", "content": "Validate this SQL query for correctness and safety."},
                    {"role": "user", "content": query}
                ],
                temperature=0.0
            )
            validation = {
                "is_valid": "valid" in response.choices[0].message.content.lower(),
                "issues": [],
                "warnings": [] if "valid" in response.choices[0].message.content.lower() else [response.choices[0].message.content]
            }
            
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
            try:
                response = self._emr_client.start_job_run(
                    applicationId=settings.emr_application_id,
                    executionRoleArn=settings.emr_execution_role_arn,
                    jobDriver={
                        "sparkSubmit": {
                            "entryPoint": "s3://.../query_runner.py",
                            "sparkSubmitParameters": f"--conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true --conf spark.sql.adaptive.skewedJoin.enabled=true --conf spark.sql.adaptive.skewedJoin.minPartitionSize=10485760 --conf spark.sql.adaptive.skewedPartitionFactor=5 --conf spark.sql.adaptive.skewedPartitionThreshold=0.3 --conf spark.sql.adaptive.skewedPartitionMinSize=1048576 --conf spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin=0.8 --conf spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin=0.8 --conf spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin=0.8"
                        }
                    },
                    configurationOverrides={
                        "applicationConfiguration": [
                            {"classification": "spark-defaults", "properties": {"spark.executor.instances": "1", "spark.executor.cores": "2", "spark.executor.memory": "4g", "spark.driver.cores": "1", "spark.driver.memory": "2g"}}
                        ]
                    },
                    tags={"max_rows": str(max_rows)}
                )
                job_run_id = response["jobRunId"]
                # Poll for completion
                execution_result = self._poll_emr_job(job_run_id, max_rows)
            except ClientError as e:
                execution_result = {"error": str(e)}
            
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
                    response = self._openai_client.chat.completions.create(
                        model="meta-llama/Meta-Llama-3.1-8B-Instruct",
                        messages=[
                            {"role": "system", "content": "You are a data analyst explaining SQL results."},
                            {"role": "user", "content": f"Question: {question}\nQuery: {query}\nResults: {results}\nExplain the results in natural language."}
                        ],
                        temperature=0.3
                    )
                    narrative = response.choices[0].message.content
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
            response = self._openai_client.chat.completions.create(
                model="meta-llama/Meta-Llama-3.1-8B-Instruct",
                messages=[
                    {"role": "system", "content": "Validate this SQL query for correctness and safety."},
                    {"role": "user", "content": query}
                ],
                temperature=0.0
            )
            validation = {
                "is_valid": "valid" in response.choices[0].message.content.lower(),
                "issues": [],
                "warnings": [] if "valid" in response.choices[0].message.content.lower() else [response.choices[0].message.content]
            }
            
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
            try:
                response = self._emr_client.start_job_run(
                    applicationId=settings.emr_application_id,
                    executionRoleArn=settings.emr_execution_role_arn,
                    jobDriver={
                        "sparkSubmit": {
                            "entryPoint": "s3://.../query_runner.py",
                            "sparkSubmitParameters": ""
                        }
                    },
                    configurationOverrides={
                        "applicationConfiguration": [
                            {"classification": "spark-defaults", "properties": {"spark.executor.instances": "1", "spark.executor.cores": "2", "spark.executor.memory": "4g", "spark.driver.cores": "1", "spark.driver.memory": "2g"}}
                        ]
                    },
                    tags={}
                )
                job_run_id = response["jobRunId"]
                execution_result = self._poll_emr_job(job_run_id)
            except ClientError as e:
                execution_result = {"error": str(e)}
            
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
                    response = self._openai_client.chat.completions.create(
                        model="meta-llama/Meta-Llama-3.1-8B-Instruct",
                        messages=[
                            {"role": "system", "content": "You are a data analyst explaining SQL results."},
                            {"role": "user", "content": f"Question: {question_context}\nQuery: {query}\nResults: {results}\nExplain the results in natural language."}
                        ],
                        temperature=0.3
                    )
                    narrative = response.choices[0].message.content
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
            response = self._openai_client.chat.completions.create(
                model="meta-llama/Meta-Llama-3.1-8B-Instruct",
                messages=[
                    {"role": "system", "content": "You are a query assistant suggesting related questions."},
                    {"role": "user", "content": f"Based on the question: '{question}', suggest 3 related questions."}
                ],
                temperature=0.5,
                max_tokens=200
            )
            suggestions = [line.strip('- ').strip() for line in response.choices[0].message.content.split('\n') if line.strip().startswith('-')]
            return suggestions
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
        try:
            response = self._emr_client.list_job_runs(
                applicationId=settings.emr_application_id,
                maxResults=limit
            )
            return response.get("jobRuns", [])
        except ClientError as e:
            return []
    
    def shutdown(self) -> None:
        """Shutdown the engine and close EMR Serverless session."""
        logger.info("Shutting down FinancialAnalysisEngine")
        # No explicit session close needed for EMR Serverless
        pass

    def _poll_emr_job(self, job_run_id: str, max_rows: Optional[int] = None) -> Dict[str, Any]:
        """Poll EMR Serverless job run until completion and fetch results."""
        import time
        while True:
            try:
                response = self._emr_client.get_job_run(
                    applicationId=settings.emr_application_id,
                    jobRunId=job_run_id
                )
                state = response['jobRun']['state']
                if state in ['SUCCESS', 'FAILED', 'CANCELLED']:
                    if state == 'SUCCESS':
                        return {
                            'success': True,
                            'data': [],  # Placeholder - actual data retrieval would require additional steps
                            'execution_time_ms': 0  # Placeholder
                        }
                    else:
                        return {
                            'success': False,
                            'error': f"Job failed with state: {state}"
                        }
                time.sleep(5)
            except ClientError as e:
                return {'success': False, 'error': str(e)}