"""
Query execution engine for Spark SQL via Crusoe Spark batch API
Executes queries remotely via HTTP; no local Spark.
"""
from typing import Dict, List, Any, Optional, TYPE_CHECKING
import logging
import re
from datetime import datetime
import traceback
import os
import requests

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from synapse_client import SynapseExecutionError


class QueryExecutionError(Exception):
    """Custom exception for query execution errors"""
    pass


class SparkBatchJobError(Exception):
    """Custom exception for Crusoe Spark batch job errors"""
    pass


class QueryExecutor:
    """
    Executes Spark SQL queries via Crusoe Spark batch API.
    """

    def __init__(self, session: "SparkBatchJobConfig", max_result_rows: int = 1000):
        self.session = session
        self.max_result_rows = max_result_rows
        self._query_history: List[Dict[str, Any]] = []
        logger.info(f"QueryExecutor initialized (Crusoe Spark) max_result_rows={max_result_rows}")

    def execute_query(
        self,
        query: str,
        return_dataframe: bool = False,
        max_rows: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Execute SQL via Crusoe Spark batch API and return results.

        Args:
            query: SQL query
            return_dataframe: Ignored (no local DataFrame with Crusoe Spark)
            max_rows: Row limit (default from constructor)

        Returns:
            Dict with success, data, columns, row_count, execution_time_ms, error
        """
        start = datetime.now()
        limit = max_rows if max_rows is not None else self.max_result_rows
        result = {
            "success": False,
            "data": [],
            "columns": [],
            "row_count": 0,
            "execution_time_ms": 0,
            "query": query,
        }

        try:
            logger.info(f"Executing query via Crusoe Spark: {query[:100]}...")
            r = self._execute_batch_statement(query, max_rows=limit)
            result["data"] = r.get("data", [])
            result["columns"] = r.get("columns", [])
            result["row_count"] = r.get("row_count", len(result["data"]))
            result["execution_time_ms"] = int((datetime.now() - start).total_seconds() * 1000)
            result["success"] = True
            self._add_to_history(query, True, result["execution_time_ms"], result["row_count"])
            logger.info(f"Query succeeded: {result['row_count']} rows, {result['execution_time_ms']}ms")
        except SparkBatchJobError as e:
            result["error"] = str(e)
            result["error_trace"] = traceback.format_exc()
            result["execution_time_ms"] = int((datetime.now() - start).total_seconds() * 1000)
            self._add_to_history(query, False, result["execution_time_ms"], 0, str(e))
            logger.error(f"Query failed: {e}")
        except Exception as e:
            result["error"] = str(e)
            result["error_trace"] = traceback.format_exc()
            result["execution_time_ms"] = int((datetime.now() - start).total_seconds() * 1000)
            self._add_to_history(query, False, result["execution_time_ms"], 0, str(e))
            logger.error(f"Query failed: {e}")

        return result

    def _execute_batch_statement(self, query: str, max_rows: int = 1000) -> Dict[str, Any]:
        """Execute SQL via Crusoe Spark batch API using requests.post()."""
        spark_api_url = os.environ.get("CRUSOE_SPARK_API_URL")
        if not spark_api_url:
            raise RuntimeError("CRUSOE_SPARK_API_URL environment variable not set")

        # Submit batch job to Crusoe Spark
        url = f"{spark_api_url}/batches"
        payload = {
            "name": f"query_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "file": "local:///opt/spark/app/query.py",
            "className": "com.crusoe.spark.QueryRunner",
            "args": [query],
            "conf": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true"
            }
        }
        
        # Prepare auth headers if credentials are available
        headers = {"Content-Type": "application/json"}
        spark_api_key = os.environ.get("CRUSOE_SPARK_API_KEY")
        if spark_api_key:
            headers["Authorization"] = f"Bearer {spark_api_key}"

        try:
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            batch_data = response.json()
            
            # Get batch ID
            batch_id = batch_data.get("id")
            if batch_id is None:
                raise RuntimeError("No batch ID returned from Crusoe Spark")
            
            # Poll for batch completion
            import time
            poll_interval = 1  # seconds
            max_wait = 300  # seconds
            elapsed = 0
            while elapsed < max_wait:
                time.sleep(poll_interval)
                poll_url = f"{spark_api_url}/batches/{batch_id}"
                poll_response = requests.get(poll_url, headers=headers, timeout=10)
                poll_response.raise_for_status()
                poll_data = poll_response.json()
                state = poll_data.get("state")
                
                if state == "success":
                    # Extract output from batch result
                    output = poll_data.get("output", {})
                    if output:
                        result_data = output.get("data", {})
                        if isinstance(result_data, dict) and "schema" in result_data and "data" in result_data:
                            # Handle structured response
                            columns = [field["name"] for field in result_data["schema"]["fields"]]
                            rows = result_data["data"]
                            # Convert rows to list of dicts
                            result_rows = []
                            for row in rows:
                                if isinstance(row, list) and len(row) == len(columns):
                                    result_rows.append(dict(zip(columns, row)))
                                else:
                                    # Fallback: treat as single column
                                    result_rows.append({columns[0]: row})
                            return {
                                "data": result_rows[:max_rows],
                                "columns": columns,
                                "row_count": len(result_rows)
                            }
                        elif isinstance(result_data, list):
                            # Direct list of rows
                            return {
                                "data": result_data[:max_rows],
                                "columns": [],
                                "row_count": len(result_data)
                            }
                        else:
                            # Generic fallback
                            return {
                                "data": [str(result_data)][:max_rows],
                                "columns": ["result"],
                                "row_count": 1
                            }
                    else:
                        raise SparkBatchJobError("No output data in successful batch job")
                elif state in ["failed", "killed", "dead"]:
                    error_message = poll_data.get("appInfo", {}).get("error", "Unknown error")
                    raise SparkBatchJobError(f"Batch job failed with state: {state}, error: {error_message}")
                
                elapsed += poll_interval
            
            raise SparkBatchJobError(f"Batch job execution timed out after {max_wait} seconds")
            
        except requests.exceptions.RequestException as e:
            raise SparkBatchJobError(f"HTTP request failed: {str(e)}")

    def execute_and_analyze(self, query: str) -> Dict[str, Any]:
        """Execute query; analysis simplified for Crusoe Spark (no DataFrame)."""
        result = self.execute_query(query)
        if result["success"] and result.get("data"):
            result["analysis"] = {
                "total_rows": result["row_count"],
                "columns_info": {c: {} for c in result["columns"]},
            }
        return result

    def validate_and_execute(
        self,
        query: str,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Validate (basic checks) and optionally execute."""
        result = {"valid": False, "validation_errors": [], "execution_result": None}
        q = query.upper().strip()
        # Use word boundaries so identifiers like "dropoff" or "tpep_dropoff_datetime" are allowed
        forbidden_ops = [
            (r"\bDROP\b", "DROP"),
            (r"\bDELETE\b", "DELETE"),
            (r"\bTRUNCATE\b", "TRUNCATE"),
            (r"\bALTER\b", "ALTER"),
            (r"\bCREATE\s+TABLE\b", "CREATE TABLE"),
            (r"\bINSERT\b", "INSERT"),
            (r"\bUPDATE\b", "UPDATE"),
        ]
        for pattern, op_name in forbidden_ops:
            if re.search(pattern, q):
                result["validation_errors"].append(f"Forbidden operation: {op_name}")
                return result
        if not (q.startswith("SELECT") or q.startswith("WITH")):
            result["validation_errors"].append("Query must be SELECT or CTE")
            return result
        result["valid"] = True
        if not dry_run:
            result["execution_result"] = self.execute_query(query)
        return result

    def get_query_plan(self, query: str) -> str:
        """Get execution plan via EXPLAIN."""
        try:
            r = self._execute_batch_statement(f"EXPLAIN {query}", max_rows=100)
            lines = []
            for row in r.get("data", []):
                lines.append(str(row))
            return "\n".join(lines) if lines else "No plan returned"
        except Exception as e:
            return f"Error: {e}"

    def get_query_history(
        self,
        limit: int = 10,
        successful_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """Return recent query history."""
        h = self._query_history
        if successful_only:
            h = [x for x in h if x.get("success")]
        return h[-limit:]

    def clear_cache(self) -> None:
        """No-op for Crusoe Spark (compatibility)."""
        pass

    def _add_to_history(
        self,
        query: str,
        success: bool,
        execution_time_ms: int,
        row_count: int,
        error: Optional[str] = None,
    ) -> None:
        entry = {
            "timestamp": datetime.now().isoformat(),
            "query": query,
            "success": success,
            "execution_time_ms": execution_time_ms,
            "row_count": row_count,
        }
        if error:
            entry["error"] = error
        self._query_history.append(entry)
        if len(self._query_history) > 100:
            self._query_history = self._query_history[-100:]


class BatchQueryExecutor:
    """Execute multiple queries."""

    def __init__(self, query_executor: QueryExecutor):
        self.executor = query_executor

    def execute_batch(
        self,
        queries: List[str],
        stop_on_error: bool = False,
    ) -> List[Dict[str, Any]]:
        results = []
        for q in queries:
            r = self.executor.execute_query(q)
            results.append(r)
            if stop_on_error and not r.get("success"):
                break
        return results