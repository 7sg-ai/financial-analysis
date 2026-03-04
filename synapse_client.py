"""
Crusoe Spark batch client via Crusoe Spark Batch API.
All Spark execution happens remotely in Crusoe; no local PySpark.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests



logger = logging.getLogger(__name__)

# Polling config for async statement execution
STATEMENT_POLL_INTERVAL_SEC = 2
STATEMENT_MAX_WAIT_SEC = 300
# Batch startup can take 5-10+ min for large jobs (cold start).
# None = wait indefinitely (container will not restart; keeps waiting for Crusoe).
BATCH_STARTUP_TIMEOUT_SEC = None

# Crusoe Spark Batch API endpoint from environment
CRUSOE_SPARK_API_URL = os.environ.get('CRUSOE_SPARK_API_URL', 'http://localhost:8998')


class SparkBatchJobError(Exception):
    """Raised when Crusoe Spark batch job fails"""
    pass


class SynapseSparkSession:
    """
    Remote Spark batch job in Crusoe via Spark Batch API.
    Mimics a subset of PySpark SparkSession for SQL and data loading.
    """

    def __init__(
        self,
        credential: Any,
        endpoint: str,
        spark_pool_name: str,
        data_path: str,
        storage_account: Optional[str] = None,
        storage_key: Optional[str] = None,
    ):
        self._batch_id: Optional[str] = None
        self._storage_account = storage_account
        self._storage_key = storage_key
        self._spark_api_url = CRUSOE_SPARK_API_URL
        self._batch_data: Optional[Dict[str, Any]] = None
        self._credential = credential
        # Use CRUSOE environment variables for storage config
        self._storage_bucket = os.environ.get('CRUSOE_STORAGE_BUCKET')
        self._file_system = os.environ.get('CRUSOE_FILE_SYSTEM')
        self._data_path = os.environ.get('CRUSOE_DATA_PATH', './src_data')
        # Use CRUSOE environment variables for credentials
        self._storage_access_key = os.environ.get('CRUSOE_STORAGE_ACCESS_KEY')
        self._client_id = os.environ.get('CRUSOE_CLIENT_ID')
        self._tenant_id = os.environ.get('CRUSOE_TENANT_ID')
        self._client_secret = os.environ.get('CRUSOE_CLIENT_SECRET')
        self._resource = os.environ.get('CRUSOE_RESOURCE', 'https://storage.azure.com/.default')

    def connect(self) -> None:
        """Create a Crusoe Spark batch job."""
        if self._batch_id is not None:
            logger.info("Crusoe Spark batch job already active")
            return

        # Use CRUSOE S3-compatible storage config
        storage_bucket = self._storage_bucket
        storage_account = os.environ.get('CRUSOE_STORAGE_ACCOUNT')
        storage_key = self._storage_access_key
        if storage_account and storage_key and storage_bucket:
            # Configure S3-compatible storage for Spark via Hadoop config
            conf = {
                "spark.hadoop.fs.s3a.access.key": storage_account,
                "spark.hadoop.fs.s3a.secret.key": storage_key,
                "spark.hadoop.fs.s3a.endpoint": os.environ.get('CRUSOE_S3_ENDPOINT', 'https://s3.amazonaws.com'),
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
            }
            logger.info(f"Configured Spark for S3-compatible storage: {storage_bucket}")
        else:
            logger.warning("CRUSOE_STORAGE_ACCOUNT, CRUSOE_STORAGE_ACCESS_KEY, and CRUSOE_STORAGE_BUCKET must be set for S3-compatible storage")

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
        payload = {
            "name": "FinancialAnalysis",
            "conf": conf,
            "executorCount": 2,
            "executorMemory": "12g",
            "executorCores": 2,
            "driverMemory": "4g",
            "driverCores": 2,
            "kind": "pyspark"
        }

        logger.info("Creating Crusoe Spark batch job...")
        resp = requests.post(
            f"{self._spark_api_url}/batches",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        resp.raise_for_status()
        batch_data = resp.json()
        self._batch_id = batch_data.get("id")
        self._batch_data = batch_data
        logger.info(f"Crusoe Spark batch job created: id={self._batch_id}")

        # Wait for batch to be idle
        self._wait_for_batch_job_idle()

    def _extract_batch_error_detail(self, batch_data: Dict[str, Any]) -> str:
        """Extract error details from failed batch job for troubleshooting."""
        details: List[str] = []
        try:
            app_id = batch_data.get("appId")
            if app_id:
                details.append(
                    f"appId={app_id} (Crusoe Spark UI: Monitor → Apache Spark applications)"
                )
            app_info = batch_data.get("appInfo", {})
            if app_info:
                driver = app_info.get("driverLogUrl") or app_info.get("log")
                if driver:
                    details.append(f"driverLogUrl={driver}")
            state = batch_data.get("state")
            if state and state.lower() in ("error", "dead", "killed"):
                err_msg = batch_data.get("log", [])
                if err_msg:
                    details.append(f"exception={'; '.join(err_msg[-3:])}")
        except Exception:
            pass
        return ". " + "; ".join(details) if details else ""

    def _wait_for_batch_idle(self) -> None:
        """Wait indefinitely for the batch job to reach idle state. Large jobs can take 5-10 min to cold start."""
        start = time.time()
        last_log = 0.0
        while True:
            resp = requests.get(
                f"{self._spark_api_url}/batches/{self._batch_id}",
                headers={"Content-Type": "application/json"}
            )
            resp.raise_for_status()
            batch_data = resp.json()
            state = batch_data.get("state", "").lower()
            if state in ("idle", "busy"):
                logger.info(f"Batch job ready, state={state}")
                return
            if state in ("dead", "error", "killed", "success"):
                err_detail = self._extract_batch_error_detail(batch_data)
                raise SparkBatchJobError(
                    f"Batch job failed: state={state}{err_detail}"
                )
            # Log every 30s so user sees progress (Large job cold start can take minutes)
            now = time.time()
            if now - last_log >= 30:
                elapsed = int(now - start)
                logger.info(f"Batch job state: {state}, waiting... ({elapsed}s elapsed)")
                last_log = now
            else:
                logger.debug(f"Batch job state: {state}, waiting...")
            time.sleep(STATEMENT_POLL_INTERVAL_SEC)

    def _execute_statement(
        self,
        code: str,
        kind: str = "pyspark",
    ) -> Dict[str, Any]:
        """Execute code and return the statement result."""
        # Use CRUSOE Spark Batch API directly (no Livy sessions)
        payload = {
            "code": code,
            "kind": kind
        }
        resp = requests.post(
            f"{self._spark_api_url}/batches/{self._batch_id}/statements",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        resp.raise_for_status()
        stmt_data = resp.json()
        stmt_id = stmt_data.get("id")

        # Poll for completion
        start = time.time()
        while time.time() - start < STATEMENT_MAX_WAIT_SEC:
            resp = requests.get(
                f"{self._spark_api_url}/batches/{self._batch_id}/statements/{stmt_id}",
                headers={"Content-Type": "application/json"}
            )
            resp.raise_for_status()
            stmt_status = resp.json()
            state = stmt_status.get("state", "unknown").lower()

            if state in ("available", "success"):
                output = stmt_status.get("output", {})
                if output:
                    return output
                return {}
            if state in ("error", "cancelled", "dead"):
                output = stmt_status.get("output", {})
                err_msg = "Unknown error"
                if output:
                    data = output.get("data", {})
                    if isinstance(data, dict):
                        err_msg = str(data.get("text/plain", data))
                    else:
                        err_msg = str(data)
                raise SparkBatchJobError(f"Statement failed: {err_msg}")

            time.sleep(STATEMENT_POLL_INTERVAL_SEC)

        raise SparkBatchJobError("Statement timed out")

    def execute_spark_sql(
        self,
        query: str,
        max_rows: int = 1000,
    ) -> Dict[str, Any]:
        """
        Execute SQL and return result as dict with keys: data, columns, row_count.
        """
        # Escape query for Python string: backslashes and triple-quotes
        escaped = query.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
        code = f'''
df = spark.sql("""{escaped}""")
rows = df.limit({max_rows}).collect()
import json
result = {{"columns": df.columns, "data": [row.asDict() for row in rows]}}
print(json.dumps(result))
'''
        output = self._execute_batch_statement(code, kind="pyspark")

        # Extract text from Crusoe Spark Batch statement output
        if isinstance(output, dict):
            data = output.get("data", {})
            if isinstance(data, dict):
                text = data.get("text/plain") or data.get("application/json")
            else:
                text = str(data)
        else:
            text = str(output)

        if not text:
            return {"data": [], "columns": [], "row_count": 0}

        try:
            parsed = json.loads(text.strip())
            if isinstance(parsed, dict):
                return {
                    "data": parsed.get("data", []),
                    "columns": parsed.get("columns", []),
                    "row_count": len(parsed.get("data", [])),
                }
            return {"data": [], "columns": [], "row_count": 0}
        except json.JSONDecodeError:
            logger.warning(f"Could not parse statement output as JSON: {text[:200]}")
            return {"data": [], "columns": [], "row_count": 0}

    def load_s3_parquet_and_create_view(
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
            self._execute_batch_statement(code, kind="pyspark")
            return True
        except SparkBatchJobError as e:
            logger.warning(f"Failed to load {pattern} into {view_name}: {e}")
            return False

    def load_s3_csv_and_create_view(
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
            self._execute_batch_statement(code, kind="pyspark")
            return True
        except SparkBatchJobError as e:
            logger.warning(f"Failed to load {rel_path} into {view_name}: {e}")
            return False

    def terminate(self) -> None:
        """Terminate the Crusoe Spark batch job."""
        if self._batch_id is not None:
            try:
                resp = requests.delete(
                    f"{self._spark_api_url}/batches/{self._batch_id}",
                    headers={"Content-Type": "application/json"}
                )
                resp.raise_for_status()
                logger.info("Crusoe Spark batch job terminated")
            except Exception as e:
                logger.warning(f"Error terminating batch job: {e}")
            self._batch_id = None


def create_spark_batch_job(settings: Any) -> SynapseSparkSession:
    """
    Create and connect a Crusoe Spark batch job.
    Requires: synapse_workspace_name, synapse_spark_pool_name, data_path.
    """
    workspace = getattr(settings, "synapse_workspace_name", None)
    pool = getattr(settings, "synapse_spark_pool_name", None)
    data_path = getattr(settings, "data_path", "./src_data")

    if not workspace or not pool:
        raise SparkBatchJobError(
            "CRUSOE_WORKSPACE and CRUSOE_SPARK_POOL are required"
        )
    # CRUSOE uses S3-compatible storage; no abfss:// requirement
    # Use CRUSOE environment variables for storage config
    storage_account = os.environ.get('CRUSOE_STORAGE_ACCESS_KEY')
    storage_key = os.environ.get('CRUSOE_STORAGE_SECRET_KEY')
    storage_bucket = os.environ.get('CRUSOE_STORAGE_BUCKET')

    session = SynapseSparkSession(
        credential=None,
        endpoint="",
        spark_pool_name=pool,
        data_path=data_path,
        storage_account=storage_account,
        storage_key=storage_key,
    )
    # Set additional CRUSOE-specific attributes on session
    session._storage_access_key = storage_key
    session._client_id = os.environ.get('CRUSOE_CLIENT_ID')
    session._tenant_id = os.environ.get('CRUSOE_TENANT_ID')
    session._client_secret = os.environ.get('CRUSOE_CLIENT_SECRET')
    session._resource = os.environ.get('CRUSOE_RESOURCE', 'https://storage.azure.com/.default')
    session.connect()
    return session
"""