"""
Query execution engine for Spark SQL via AWS EMR Serverless (Livy-compatible)
Executes queries remotely in EMR Serverless; no local Spark.
"""
from typing import Dict, List, Any, Optional, TYPE_CHECKING
import logging
import re
import json
from datetime import datetime
import traceback
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from typing import Any


class QueryExecutionError(Exception):
    """Custom exception for query execution errors"""
    pass


class QueryExecutor:
    """
    Executes Spark SQL queries in AWS EMR Serverless via Livy-compatible wrapper.
    """

    def __init__(self, region_name: str = "us-east-1", execution_role_arn: str = os.getenv("EMR_EXECUTION_ROLE_ARN"), max_result_rows: int = 1000):
        self.client = boto3.client("emr-serverless", region_name=region_name)
        self.execution_role_arn = execution_role_arn
        self.max_result_rows = max_result_rows
        self._query_history: List[Dict[str, Any]] = []
        self.application_id = os.getenv("EMR_SERVERLESS_APPLICATION_ID")
        logger.info(f"QueryExecutor initialized (EMR Serverless) max_result_rows={max_result_rows}")

    def execute_query(
        self,
        query: str,
        return_dataframe: bool = False,
        max_rows: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Execute SQL in EMR Serverless and return results.

        Args:
            query: SQL query
            return_dataframe: Ignored (no local DataFrame with EMR Serverless)
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
            logger.info(f"Executing query in EMR Serverless: {query[:100]}...")
            response = self.client.start_job_run(
                applicationId=self.application_id,
                executionRoleArn=self.execution_role_arn,
                jobDriver={
                    "sparkSubmit": {
                        "entryPoint": os.getenv("LIVY_WRAPPER_S3_PATH", "s3://<bucket>/livy-wrapper.py"),
                        "sparkSubmitParameters": f"--conf spark.livy.sql.query={query} --conf spark.livy.sql.maxRows={limit}"
                    }
                },
                configurationOverrides={
                    "applicationConfiguration": [
                        {"classification": "spark-defaults", "properties": {"spark.sql.adaptive.enabled": "true"}}
                    ]
                }
            )
            job_run_id = response["jobRunId"]
            # Poll for completion or use async callback
            r = self._wait_for_job_result(job_run_id, max_rows=limit)
            result["data"] = r.get("data", [])
            result["columns"] = r.get("columns", [])
            result["row_count"] = r.get("row_count", len(result["data"]))
            result["execution_time_ms"] = int((datetime.now() - start).total_seconds() * 1000)
            result["success"] = True
            self._add_to_history(query, True, result["execution_time_ms"], result["row_count"])
            logger.info(f"Query succeeded: {result['row_count']} rows, {result['execution_time_ms']}ms")
        except ClientError as e:
            result["error"] = f"{e.response['Error']['Code']}: {e.response['Error']['Message']}"
            result["error_trace"] = traceback.format_exc()
            result["execution_time_ms"] = int((datetime.now() - start).total_seconds() * 1000)
            self._add_to_history(query, False, result["execution_time_ms"], 0, result["error"])
            logger.error(f"Query failed: {e.response['Error']['Code']}: {e.response['Error']['Message']}")
        except Exception as e:
            result["error"] = str(e)
            result["error_trace"] = traceback.format_exc()
            result["execution_time_ms"] = int((datetime.now() - start).total_seconds() * 1000)
            self._add_to_history(query, False, result["execution_time_ms"], 0, str(e))
            logger.error(f"Query failed: {e}")

        return result

    def execute_and_analyze(self, query: str) -> Dict[str, Any]:
        """Execute query; analysis simplified for EMR Serverless (no DataFrame)."""
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
            response = self.client.start_job_run(
                applicationId=self.application_id,
                executionRoleArn=self.execution_role_arn,
                jobDriver={
                    "sparkSubmit": {
                        "entryPoint": "s3://<bucket>/livy-wrapper.py",
                        "sparkSubmitParameters": f"--conf spark.livy.sql.query=EXPLAIN {query} --conf spark.livy.sql.maxRows=100"
                    }
                },
                configurationOverrides={
                    "applicationConfiguration": [
                        {"classification": "spark-defaults", "properties": {"spark.sql.adaptive.enabled": "true"}}
                    ]
                }
            )
            job_run_id = response["jobRunId"]
            r = self._wait_for_job_result(job_run_id, max_rows=100)
            lines = []
            for row in r.get("data", []):
                lines.append(str(row))
            return "\n".join(lines) if lines else "No plan returned"
        except ClientError as e:
            return f"Error: {e.response['Error']['Code']}: {e.response['Error']['Message']}"
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
        """No-op for EMR Serverless (compatibility)."""
        pass

    def _wait_for_job_result(self, job_run_id: str, max_rows: int) -> Dict[str, Any]:
        """Poll for job completion and fetch results."""
        import time
        
        while True:
            response = self.client.get_job_run(
                applicationId=self.application_id,
                jobRunId=job_run_id
            )
            state = response["jobRun"]["state"]
            if state == "SUCCESS":
                # In a real implementation, results would be fetched from S3 or similar
                # For now, return placeholder structure
                return {
                    "data": [],
                    "columns": [],
                    "row_count": 0
                }
            elif state in ["FAILED", "CANCELLED"]:
                raise RuntimeError(f"Job failed with state: {state}")
            else:
                time.sleep(2)  # Polling interval

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