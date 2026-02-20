"""
AWS EMR Serverless Spark client via boto3.
All Spark execution happens remotely in EMR Serverless; no local PySpark.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

from crusoe import Client as EMRServerlessClient
from botocore.exceptions import ClientError
from crusoe import Client as EMRServerlessClient, EMRServerlessError

logger = logging.getLogger(__name__)

# Polling config for async statement execution
STATEMENT_POLL_INTERVAL_SEC = 2
STATEMENT_MAX_WAIT_SEC = 300
# Session startup can take 5-10+ min for Large pools (cold start).
# None = wait indefinitely (container will not restart; keeps waiting for EMR).
SESSION_STARTUP_TIMEOUT_SEC = None


class EMRServerlessConnectionError(Exception):
    """Raised when connection to AWS EMR Serverless fails"""
    pass


class EMRServerlessExecutionError(Exception):
    """Raised when statement execution fails in EMR Serverless"""
    pass


class SynapseSparkSession:
    """
    Remote Spark session in AWS EMR Serverless via boto3.
    Mimics a subset of PySpark SparkSession for SQL and data loading.
    """

    def __init__(
        self,
        application_id: str,
        region_name: str = "crusoe",
        data_path: str,
        storage_bucket: Optional[str] = None,
        storage_prefix: Optional[str] = "logs",
        execution_role_arn: Optional[str] = None,
    ):
        self._client = EMRServerlessClient(region_name=region_name)
        self.application_id = application_id
        self.region_name = region_name
        self.data_path = data_path.rstrip("/")
        self._session_id: Optional[str] = None
        self._storage_bucket = storage_bucket
        self._storage_prefix = storage_prefix
        self._execution_role_arn = execution_role_arn

    def connect(self) -> None:
        """Create a Spark job run in EMR Serverless."""
        if self._session_id is not None:
            logger.info("EMR Serverless session already active")
            return

        conf: Dict[str, str] = {}
        if self._storage_bucket:
            logger.info(f"Configured Spark for S3: {self._storage_bucket}")

        # Dynamic executor allocation: scale executors based on query complexity.
        # Max 5 executors so driver (2) + 5*2 = 12 vCores fits in 3 Small nodes.
        conf["spark.dynamicAllocation.enabled"] = "true"
        conf["spark.dynamicAllocation.minExecutors"] = "1"
        conf["spark.dynamicAllocation.maxExecutors"] = "5"
        conf["spark.dynamicAllocation.initialExecutors"] = "2"
        # Shuffle tracking allows executor release without external shuffle service
        conf["spark.dynamicAllocation.shuffleTracking.enabled"] = "true"

        # Base config for 3 Small nodes (12 vCores, 96 GB total)
        # executor_count=2 is initial; dynamic allocation adjusts at runtime
        spark_submit_params = (
            "--conf spark.app.name=FinancialAnalysis "
            "--conf spark.executor.instances=2 "
            "--conf spark.executor.cores=2 "
            "--conf spark.executor.memory=12g "
            "--conf spark.driver.cores=2 "
            "--conf spark.driver.memory=4g "
            "--conf spark.sql.adaptive.enabled=true"
        )

        logger.info("Creating EMR Serverless Spark session...")
        try:
            response = self._client.submit_job(
                applicationId=self.application_id,
                executionRoleArn=self._execution_role_arn,
                jobDriver={
                    "sparkSubmit": {
                        "entryPoint": "local:///app/main.py",
                        "sparkSubmitParameters": spark_submit_params
                    }
                },
                config=confConfiguration": {
                            "logUri": f"s3://{self._storage_bucket}/{self._storage_prefix}/"
                        }
                    }
                },
                tags={"project": "FinancialAnalysis"}
            )
            self._session_id = response["jobRunId"]
            logger.info(f"EMR Serverless session created: id={self._session_id}")
        except ClientError as e:
            raise EMRServerlessConnectionError(f"Failed to start job run: {e}")

        # Wait for session to be idle
        self._wait_for_session_idle()

    def _extract_session_error_detail(self, session: Any) -> str:
        """Extract error details from failed session for troubleshooting."""
        details: List[str] = []
        try:
            state_details = session.get("stateDetails", {})
            if state_details:
                details.append(f"stateDetails={json.dumps(state_details)}")
            # Fallback: log serialized session if we have as_dict (Azure SDK models)
            if not details and hasattr(session, "as_dict"):
                d = session.as_dict()
                err_bits = [
                    str(v) for k, v in (d or {}).items()
                    if v and ("error" in str(k).lower() or "exception" in str(k).lower())
                ]
                if err_bits:
                    details.append("; ".join(err_bits[:3]))  # limit length
        except Exception:
            pass
        return ". " + "; ".join(details) if details else ""

    def _wait_for_session_idle(self) -> None:
        """Wait indefinitely for the session to reach idle state. Large pools can take 5-10 min to cold start."""
        start = time.time()
        last_log = 0.0
        while True:
            try:
                response = self._client.get_job_run(
                    applicationId=self.application_id,
                    jobRunId=self._session_id
                )
                state = response["jobRun"].get("state", "UNKNOWN")
            except ClientError as e:
                raise EMRServerlessConnectionError(f"Failed to get job run: {e}")

            state = str(state).lower() if state else "unknown"
            if state in ("idle", "busy"):
                logger.info(f"Session ready, state={state}")
                return
            if state in ("failed", "error", "cancelled", "success"):
                err_detail = self._extract_session_error_detail(response["jobRun"])
                raise EMRServerlessConnectionError(
                    f"Session failed: state={state}{err_detail}"
                )
            # Log every 30s so user sees progress (Large pool cold start can take minutes)
            now = time.time()
            if now - last_log >= 30:
                elapsed = int(now - start)
                logger.info(f"Session state: {state}, waiting... ({elapsed}s elapsed)")
                last_log = now
            else:
                logger.debug(f"Session state: {state}, waiting...")
            time.sleep(STATEMENT_POLL_INTERVAL_SEC)

    def _execute_statement(
        self,
        code: str,
        kind: str = "pyspark",
    ) -> Dict[str, Any]:
        """Execute code and return the statement result."""
        if self._session_id is None:
            raise EMRServerlessConnectionError("Not connected to EMR Serverless")

        # Create spark-submit parameters from code
        spark_submit_params = (
            "--conf spark.app.name=StatementExecution "
            "--conf spark.executor.instances=2 "
            "--conf spark.executor.cores=2 "
            "--conf spark.executor.memory=12g "
            "--conf spark.driver.cores=2 "
            "--conf spark.driver.memory=4g "
            "--conf spark.sql.adaptive.enabled=true"
        )

        try:
            response = self._client.start_job_run(
                applicationId=self.application_id,
                executionRoleArn=self._execution_role_arn,
                jobDriver={
                    "sparkSubmit": {
                        "entryPoint": "local:///app/main.py",
                        "sparkSubmitParameters": spark_submit_params
                    }
                },
                configurationOverrides={
                    "applicationConfiguration": [
                        {"classification": "spark-defaults", "properties": {}}
                    ],
                    "monitoringConfiguration": {
                        "s3MonitoringConfiguration": {
                            "logUri": f"s3://{self._storage_bucket}/{self._storage_prefix}/"
                        }
                    }
                },
                tags={"project": "FinancialAnalysis"}
            )
            stmt_id = response["jobRunId"]
        except ClientError as e:
            raise EMRServerlessExecutionError(f"Failed to start job run: {e}")

        # Poll for completion
        start = time.time()
        while time.time() - start < STATEMENT_MAX_WAIT_SEC:
            try:
                response = self._client.get_job_run(
                    applicationId=self.application_id,
                    jobRunId=stmt_id
                )
                state = response["jobRun"].get("state", "UNKNOWN")
            except ClientError as e:
                raise EMRServerlessExecutionError(f"Failed to get job run: {e}")

            state = str(state).lower() if state else "unknown"

            if state in ("success",):
                # In EMR Serverless, we don't get direct output like Livy.
                # For SQL execution, we'd need to write results to S3 and read back.
                # This is a placeholder; real implementation would read from S3.
                return {"data": [], "columns": [], "row_count": 0}
            if state in ("failed", "error", "cancelled"):
                err_msg = "Unknown error"
                state_details = response["jobRun"].get("stateDetails", {})
                if state_details:
                    err_msg = str(state_details)
                raise EMRServerlessExecutionError(f"Statement failed: {err_msg}")

            time.sleep(STATEMENT_POLL_INTERVAL_SEC)

        raise EMRServerlessExecutionError("Statement timed out")

    def execute_sql(
        self,
        query: str,
        max_rows: int = 1000,
    ) -> Dict[str, Any]:
        """
        Execute SQL and return result as dict with keys: data, columns, row_count.
        """
        # In EMR Serverless, we'd typically write results to S3 and read them back.
        # This is a simplified placeholder implementation.
        escaped = query.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
        code = f'''
df = spark.sql("""{escaped}""")
rows = df.limit({max_rows}).collect()
import json
result = {{"columns": df.columns, "data": [row.asDict() for row in rows]}}
print(json.dumps(result))
'''
        output = self._execute_statement(code, kind="pyspark")

        # Placeholder: In real implementation, read from S3
        return {"data": [], "columns": [], "row_count": 0}

    def load_parquet_and_create_view(
        self,
        pattern: str,
        view_name: str,
        months: Optional[List[str]] = None,
    ) -> bool:
        """
        Load parquet files matching pattern and create a temp view.
        Reads files one-by-one to avoid mergeSchema failures (double vs bigint).
        Casts location ID columns to bigint to unify INT32/INT64/double.
        """
        base_dir = self.data_path.rstrip("/")
        # Import fnmatch for glob-style pattern matching
        # Columns that often vary as int/bigint/double across NYC TLC files
        cast_cols = '["PULocationID", "DOLocationID", "PUlocationID", "DOlocationID", "SR_Flag", "VendorID", "RatecodeID", "passenger_count", "payment_type", "trip_type"]'
        # List files: try mssparkutils first (Synapse); fallback to Hadoop GlobStatus
        list_files = '''
try:
    files = [f.path for f in mssparkutils.fs.ls(base_dir) if fnmatch.fnmatch(f.name, pattern)]
except NameError:
    Path = spark._jvm.org.apache.hadoop.fs.Path
    FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
    conf = spark._jvm.org.apache.hadoop.conf.Configuration()
    glob_path = base_dir + "/" + pattern
    fs = FileSystem.get(Path(glob_path).toUri(), conf)
    statuses = fs.globStatus(Path(glob_path))
    files = [str(s.getPath().toString()) for s in (statuses or [])]
'''
        if months:
            month_filter = ",".join(f'"{m}"' for m in months)
            code = f'''
import fnmatch
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
base_dir = "{base_dir}"
pattern = "{pattern}"
{list_files}
if not files:
    raise Exception("No files matching " + pattern + " in " + base_dir)
dfs = []
cast_cols = {cast_cols}
for fp in files:
    d = spark.read.parquet(fp)
    for c in cast_cols:
        if c in d.columns:
            d = d.withColumn(c, d[c].cast("double").cast("bigint"))
    dfs.append(d)
df = dfs[0]
for d in dfs[1:]:
    df = df.unionByName(d, allowMissingColumns=True)
from pyspark.sql.functions import month
date_col = [c for c in df.columns if "pickup" in c.lower()][0]
month_strs = [{month_filter}]
df = df.filter(month(df[date_col]).cast("string").isin(month_strs))
df.createOrReplaceTempView("{view_name}")
print("OK")
'''
        else:
            code = f'''
import fnmatch
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
base_dir = "{base_dir}"
pattern = "{pattern}"
{list_files}
if not files:
    raise Exception("No files matching " + pattern + " in " + base_dir)
dfs = []
cast_cols = {cast_cols}
for fp in files:
    d = spark.read.parquet(fp)
    for c in cast_cols:
        if c in d.columns:
            d = d.withColumn(c, d[c].cast("double").cast("bigint"))
    dfs.append(d)
df = dfs[0]
for d in dfs[1:]:
    df = df.unionByName(d, allowMissingColumns=True)
df.createOrReplaceTempView("{view_name}")
print("OK")
'''
        try:
            self._execute_statement(code, kind="pyspark")
            return True
        except EMRServerlessExecutionError as e:
            logger.warning(f"Failed to load {pattern} into {view_name}: {e}")
            return False

    def load_csv_and_create_view(
        self,
        rel_path: str,
        view_name: str,
    ) -> bool:
        """Load CSV and create temp view."""
        path = f"{self.data_path}/{rel_path}".replace('"', '\\"')
        code = f'''
df = spark.read.csv("{path}", header=True, inferSchema=True)
df.createOrReplaceTempView("{view_name}")
print("OK")
'''
        try:
            self._execute_statement(code, kind="pyspark")
            return True
        except EMRServerlessExecutionError as e:
            logger.warning(f"Failed to load {rel_path} into {view_name}: {e}")
            return False

    def close(self) -> None:
        """Terminate the Spark job run."""
        if self._session_id is not None:
            try:
                self._client.stop_job_run(
                    applicationId=self.application_id,
                    jobRunId=self._session_id
                )
                logger.info("EMR Serverless session closed")
            except Exception as e:
                logger.warning(f"Error closing session: {e}")
            self._session_id = None


def create_synapse_session(settings: Any) -> SynapseSparkSession:
    """
    Create and connect an EMR Serverless Spark session.
    Requires: emr_application_id, aws_region, data_path.
    """
    application_id = getattr(settings, "emr_application_id", None)
    region_name = getattr(settings, "aws_region", None)
    data_path = getattr(settings, "data_path", "./src_data")

    if not application_id or not region_name:
        raise EMRServerlessConnectionError(
            "emr_application_id and aws_region are required"
        )
    if not str(data_path).startswith("s3://"):
        raise EMRServerlessConnectionError(
            "DATA_PATH must be an s3:// path when using EMR Serverless. "
            f"Got: {data_path}"
        )

    storage_bucket = getattr(settings, "aws_s3_bucket", None)
    storage_prefix = getattr(settings, "aws_s3_prefix", "logs")
    execution_role_arn = getattr(settings, "emr_execution_role_arn", None)

    session = SynapseSparkSession(
        application_id=application_id,
        region_name=region_name,
        data_path=data_path,
        storage_bucket=storage_bucket,
        storage_prefix=storage_prefix,
        execution_role_arn=execution_role_arn,
    )
    session.connect()
    return session