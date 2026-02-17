"""
Query execution engine for Spark SQL via Azure Synapse (Livy)
Executes queries remotely in Synapse; no local Spark.
"""
from typing import Dict, List, Any, Optional, TYPE_CHECKING
import logging
from datetime import datetime
import traceback

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from synapse_client import SynapseSparkSession


class QueryExecutionError(Exception):
    """Custom exception for query execution errors"""
    pass


class QueryExecutor:
    """
    Executes Spark SQL queries in Azure Synapse via Livy API.
    """

    def __init__(self, session: "SynapseSparkSession", max_result_rows: int = 1000):
        self.session = session
        self.max_result_rows = max_result_rows
        self._query_history: List[Dict[str, Any]] = []
        logger.info(f"QueryExecutor initialized (Synapse) max_result_rows={max_result_rows}")

    def execute_query(
        self,
        query: str,
        return_dataframe: bool = False,
        max_rows: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Execute SQL in Synapse and return results.

        Args:
            query: SQL query
            return_dataframe: Ignored (no local DataFrame with Synapse)
            max_rows: Row limit (default from constructor)

        Returns:
            Dict with success, data, columns, row_count, execution_time_ms, error
        """
        from synapse_client import SynapseExecutionError

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
            logger.info(f"Executing query in Synapse: {query[:100]}...")
            r = self.session.execute_sql(query, max_rows=limit)
            result["data"] = r.get("data", [])
            result["columns"] = r.get("columns", [])
            result["row_count"] = r.get("row_count", len(result["data"]))
            result["execution_time_ms"] = int((datetime.now() - start).total_seconds() * 1000)
            result["success"] = True
            self._add_to_history(query, True, result["execution_time_ms"], result["row_count"])
            logger.info(f"Query succeeded: {result['row_count']} rows, {result['execution_time_ms']}ms")
        except SynapseExecutionError as e:
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

    def execute_and_analyze(self, query: str) -> Dict[str, Any]:
        """Execute query; analysis simplified for Synapse (no DataFrame)."""
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
        for op in ["DROP", "DELETE", "TRUNCATE", "ALTER", "CREATE TABLE", "INSERT", "UPDATE"]:
            if op in q:
                result["validation_errors"].append(f"Forbidden operation: {op}")
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
            r = self.session.execute_sql(f"EXPLAIN {query}", max_rows=100)
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
        """No-op for Synapse (compatibility)."""
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
