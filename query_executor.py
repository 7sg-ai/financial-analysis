"""
Query execution engine for Spark SQL queries
Handles query execution, error handling, and result processing
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, List, Any, Optional, Tuple
import logging
from datetime import datetime
import traceback

logger = logging.getLogger(__name__)


class QueryExecutionError(Exception):
    """Custom exception for query execution errors"""
    pass


class QueryExecutor:
    """
    Executes Spark SQL queries and manages results
    """
    
    def __init__(self, spark: SparkSession, max_result_rows: int = 1000):
        """
        Initialize QueryExecutor
        
        Args:
            spark: Active SparkSession
            max_result_rows: Maximum number of rows to return
        """
        self.spark = spark
        self.max_result_rows = max_result_rows
        self._query_history = []
        
        logger.info(f"QueryExecutor initialized with max_result_rows={max_result_rows}")
    
    def execute_query(
        self,
        query: str,
        return_dataframe: bool = False,
        max_rows: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute a Spark SQL query
        
        Args:
            query: SQL query to execute
            return_dataframe: If True, include DataFrame in results
            max_rows: Override default max rows limit
            
        Returns:
            Dictionary containing:
                - success: Boolean indicating if query succeeded
                - data: List of dictionaries (row data)
                - columns: List of column names
                - row_count: Number of rows returned
                - execution_time_ms: Query execution time
                - error: Error message if query failed
                - dataframe: Spark DataFrame (if return_dataframe=True)
        """
        start_time = datetime.now()
        result = {
            'success': False,
            'data': [],
            'columns': [],
            'row_count': 0,
            'execution_time_ms': 0,
            'query': query
        }
        
        try:
            logger.info(f"Executing query: {query[:100]}...")
            logger.debug(f"Query parameters: max_rows={max_rows}, return_dataframe={return_dataframe}")
            logger.debug(f"Full query: {query}")
            
            # Execute query
            logger.debug("Calling spark.sql()...")
            df = self.spark.sql(query)
            logger.debug(f"Query executed. DataFrame type: {type(df)}")
            
            # Get column names
            result['columns'] = df.columns
            logger.debug(f"Query returned columns: {result['columns']}")
            
            # Determine row limit
            limit = max_rows if max_rows is not None else self.max_result_rows
            logger.debug(f"Row limit: {limit}")
            
            # Collect results
            logger.debug("Collecting results from DataFrame...")
            rows = df.limit(limit).collect()
            result['row_count'] = len(rows)
            logger.debug(f"Collected {result['row_count']} rows")
            
            # Convert to dictionaries
            logger.debug("Converting rows to dictionaries...")
            result['data'] = [row.asDict() for row in rows]
            logger.debug(f"Converted to {len(result['data'])} dictionaries")
            if result['data']:
                logger.debug(f"First row sample: {result['data'][0]}")
            else:
                logger.warning("Query returned 0 rows!")
            
            # Include DataFrame if requested
            if return_dataframe:
                result['dataframe'] = df
            
            # Calculate execution time
            end_time = datetime.now()
            result['execution_time_ms'] = int((end_time - start_time).total_seconds() * 1000)
            
            result['success'] = True
            
            logger.debug(f"Query execution result structure: success={result['success']}, row_count={result['row_count']}, data_length={len(result['data'])}")
            logger.debug(f"Result keys: {list(result.keys())}")
            
            # Log to history
            self._add_to_history(query, True, result['execution_time_ms'], result['row_count'])
            
            logger.info(
                f"Query executed successfully: {result['row_count']} rows, "
                f"{result['execution_time_ms']}ms"
            )
            
        except Exception as e:
            end_time = datetime.now()
            execution_time = int((end_time - start_time).total_seconds() * 1000)
            
            error_msg = str(e)
            error_trace = traceback.format_exc()
            
            result['error'] = error_msg
            result['error_trace'] = error_trace
            result['execution_time_ms'] = execution_time
            
            # Log to history
            self._add_to_history(query, False, execution_time, 0, error_msg)
            
            logger.error(f"Query execution failed: {error_msg}")
            logger.debug(f"Full traceback: {error_trace}")
        
        return result
    
    def execute_and_analyze(self, query: str) -> Dict[str, Any]:
        """
        Execute query and provide basic statistical analysis
        
        Args:
            query: SQL query to execute
            
        Returns:
            Dictionary with results and analysis
        """
        result = self.execute_query(query, return_dataframe=True)
        
        if not result['success']:
            return result
        
        df = result.get('dataframe')
        if df is None:
            return result
        
        # Add analysis
        analysis = {
            'total_rows': df.count(),
            'columns_info': {},
            'has_nulls': {}
        }
        
        # Analyze each column
        for col_name in df.columns:
            col_type = dict(df.dtypes)[col_name]
            analysis['columns_info'][col_name] = {
                'type': col_type,
                'null_count': df.filter(df[col_name].isNull()).count()
            }
            
            # For numeric columns, add statistics
            if col_type in ['int', 'bigint', 'double', 'float', 'decimal']:
                try:
                    stats = df.select(col_name).summary('min', 'max', 'mean').collect()
                    analysis['columns_info'][col_name]['min'] = stats[0][col_name]
                    analysis['columns_info'][col_name]['max'] = stats[1][col_name]
                    analysis['columns_info'][col_name]['mean'] = stats[2][col_name]
                except:
                    pass
        
        result['analysis'] = analysis
        
        # Remove dataframe from result before returning
        if 'dataframe' in result:
            del result['dataframe']
        
        return result
    
    def validate_and_execute(
        self,
        query: str,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Validate query syntax and optionally execute
        
        Args:
            query: SQL query to validate/execute
            dry_run: If True, only validate without executing
            
        Returns:
            Dictionary with validation results and execution results
        """
        result = {
            'valid': False,
            'validation_errors': [],
            'execution_result': None
        }
        
        # Basic validation
        query_upper = query.upper().strip()
        
        # Check for dangerous operations
        dangerous_ops = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE TABLE', 'INSERT', 'UPDATE']
        for op in dangerous_ops:
            if op in query_upper:
                result['validation_errors'].append(f"Query contains forbidden operation: {op}")
                return result
        
        # Check if it's a SELECT query
        if not query_upper.startswith('SELECT') and not query_upper.startswith('WITH'):
            result['validation_errors'].append("Query must be a SELECT statement or CTE")
            return result
        
        # Try to parse query with Spark
        try:
            # Create a logical plan without executing
            df = self.spark.sql(query)
            df.explain(extended=False)  # This will validate the query
            result['valid'] = True
            logger.info("Query validation successful")
        except Exception as e:
            result['validation_errors'].append(f"Query parsing error: {str(e)}")
            logger.error(f"Query validation failed: {e}")
            return result
        
        # Execute if not dry run
        if not dry_run and result['valid']:
            result['execution_result'] = self.execute_query(query)
        
        return result
    
    def get_query_plan(self, query: str) -> str:
        """
        Get the execution plan for a query
        
        Args:
            query: SQL query
            
        Returns:
            String representation of query execution plan
        """
        try:
            df = self.spark.sql(query)
            # Capture explain output
            plan = df._jdf.queryExecution().toString()
            return plan
        except Exception as e:
            logger.error(f"Error getting query plan: {e}")
            return f"Error: {str(e)}"
    
    def get_query_history(
        self,
        limit: int = 10,
        successful_only: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get recent query execution history
        
        Args:
            limit: Maximum number of history entries to return
            successful_only: If True, only return successful queries
            
        Returns:
            List of query history entries
        """
        history = self._query_history
        
        if successful_only:
            history = [h for h in history if h['success']]
        
        return history[-limit:]
    
    def clear_cache(self) -> None:
        """Clear Spark SQL cache"""
        try:
            self.spark.catalog.clearCache()
            logger.info("Spark cache cleared")
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
    
    def _add_to_history(
        self,
        query: str,
        success: bool,
        execution_time_ms: int,
        row_count: int,
        error: Optional[str] = None
    ) -> None:
        """Add query to execution history"""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'query': query,
            'success': success,
            'execution_time_ms': execution_time_ms,
            'row_count': row_count
        }
        
        if error:
            entry['error'] = error
        
        self._query_history.append(entry)
        
        # Keep history limited to last 100 queries
        if len(self._query_history) > 100:
            self._query_history = self._query_history[-100:]


class BatchQueryExecutor:
    """
    Executes multiple queries in batch
    """
    
    def __init__(self, query_executor: QueryExecutor):
        """
        Initialize BatchQueryExecutor
        
        Args:
            query_executor: QueryExecutor instance to use
        """
        self.executor = query_executor
        logger.info("BatchQueryExecutor initialized")
    
    def execute_batch(
        self,
        queries: List[str],
        stop_on_error: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Execute multiple queries
        
        Args:
            queries: List of SQL queries to execute
            stop_on_error: If True, stop execution on first error
            
        Returns:
            List of execution results
        """
        results = []
        
        logger.info(f"Executing batch of {len(queries)} queries")
        
        for i, query in enumerate(queries):
            logger.info(f"Executing query {i+1}/{len(queries)}")
            
            result = self.executor.execute_query(query)
            results.append(result)
            
            if stop_on_error and not result['success']:
                logger.warning(f"Stopping batch execution due to error in query {i+1}")
                break
        
        successful = sum(1 for r in results if r['success'])
        logger.info(f"Batch execution complete: {successful}/{len(results)} successful")
        
        return results
    
    def execute_with_dependencies(
        self,
        query_specs: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Execute queries with dependencies (e.g., temp table creation)
        
        Args:
            query_specs: List of query specifications, each containing:
                - query: SQL query
                - name: Query identifier
                - depends_on: List of query names this depends on
                - create_temp_view: Name of temp view to create (optional)
                
        Returns:
            Dictionary mapping query names to results
        """
        results = {}
        executed = set()
        
        def can_execute(spec):
            """Check if all dependencies are met"""
            depends_on = spec.get('depends_on', [])
            return all(dep in executed for dep in depends_on)
        
        remaining = query_specs.copy()
        
        while remaining:
            executed_this_round = False
            
            for spec in remaining[:]:
                if can_execute(spec):
                    name = spec['name']
                    query = spec['query']
                    
                    logger.info(f"Executing dependent query: {name}")
                    result = self.executor.execute_query(query, return_dataframe=True)
                    
                    # Create temp view if specified
                    if result['success'] and 'create_temp_view' in spec:
                        view_name = spec['create_temp_view']
                        df = result['dataframe']
                        df.createOrReplaceTempView(view_name)
                        logger.info(f"Created temp view: {view_name}")
                    
                    results[name] = result
                    executed.add(name)
                    remaining.remove(spec)
                    executed_this_round = True
                    
                    # Stop if query failed
                    if not result['success']:
                        logger.error(f"Query '{name}' failed, stopping dependent execution")
                        return results
            
            # Check for circular dependencies
            if not executed_this_round and remaining:
                logger.error("Circular dependency detected in query batch")
                for spec in remaining:
                    results[spec['name']] = {
                        'success': False,
                        'error': 'Circular dependency or missing dependency'
                    }
                break
        
        return results

