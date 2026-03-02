"""
Azure Synapse Spark client via Livy REST API.
All Spark execution happens remotely in Synapse; no local PySpark.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests
from azure.identity import ClientSecretCredential


logger = logging.getLogger(__name__)

# Polling config for async statement execution
STATEMENT_POLL_INTERVAL_SEC = 2
STATEMENT_MAX_WAIT_SEC = 300
# Session startup can take 5-10+ min for Large pools (cold start).
# None = wait indefinitely (container will not restart; keeps waiting for Synapse).
SESSION_STARTUP_TIMEOUT_SEC = None

# Livy endpoint from environment
LIVY_ENDPOINT = os.environ.get('LIVY_ENDPOINT', 'http://localhost:8998')
# TODO: Crusoe has no managed Synapse equivalent. Use self-hosted Spark Thrift Server or EMR Serverless.
# Note: Synapse Spark (Livy) is replaced by hybrid_synapse_livy strategy using requests to Livy-compatible endpoint.


class SynapseConnectionError(Exception):
    """Raised when connection to Azure Synapse fails"""
    pass


class SynapseExecutionError(Exception):
    """Raised when statement execution fails in Synapse"""
    pass


class SynapseSparkSession:
    """
    Remote Spark session in Azure Synapse via Livy API.
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
        self._session_id: Optional[int] = None
        self._storage_account = storage_account
        self._storage_key = storage_key
        self._livy_endpoint = LIVY_ENDPOINT
        self._session_data: Optional[Dict[str, Any]] = None
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
        # Ensure required Azure AD token env vars are present (hybrid access)
        self._tenant_id = os.environ.get('CRUSOE_TENANT_ID')
        self._client_secret = os.environ.get('CRUSOE_CLIENT_SECRET')
        self._resource = os.environ.get('CRUSOE_RESOURCE', 'https://storage.azure.com/.default')

    def connect(self) -> None:
        """Create a Livy PySpark session in Synapse."""
        if self._session_id is not None:
            logger.info("Synapse session already active")
            return

        # Acquire Azure AD token using ClientSecretCredential
        if self._client_id and self._tenant_id and self._client_secret:
            credential = ClientSecretCredential(
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret
            )
            token = credential.get_token(self._resource)
            auth_header = {"Authorization": f"Bearer {token.token}"}
        else:
            auth_header = {}

        conf: Dict[str, str] = {}
        # Use CRUSOE S3-compatible storage config
        storage_bucket = self._storage_bucket
        storage_account = os.environ.get('CRUSOE_STORAGE_ACCOUNT')
        storage_key = self._storage_access_key
        if storage_account and storage_key and storage_bucket:
            # Configure S3-compatible storage for Spark via Hadoop config
            conf["spark.hadoop.fs.s3a.access.key"] = storage_account
            conf["spark.hadoop.fs.s3a.secret.key"] = storage_key
            conf["spark.hadoop.fs.s3a.endpoint"] = f"{storage_bucket}.s3.amazonaws.com"
            conf["spark.hadoop.fs.s3a.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"
            logger.info(f"Configured Spark for S3-compatible storage: {storage_bucket}")
        else:
            logger.warning("CRUSOE_STORAGE_ACCOUNT, CRUSOE_STORAGE_ACCESS_KEY, and CRUSOE_STORAGE_BUCKET must be set for S3-compatible storage")

        # Dynamic executor allocation: scale executors based on query complexity.
        # Requires pool to have --enable-dynamic-exec (see deploy_azure.sh).
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

        logger.info("Creating Synapse Spark session...")
        resp = requests.post(
            f"{self._livy_endpoint}/sessions",
            json=payload,
            headers={"Content-Type": "application/json", **auth_header}
        )
        resp.raise_for_status()
        session_data = resp.json()
        self._session_id = session_data.get("id")
        self._session_data = session_data
        logger.info(f"Synapse session created: id={self._session_id}")

        # Wait for session to be idle
        self._wait_for_session_idle()

    def _extract_session_error_detail(self, session_data: Dict[str, Any]) -> str:
        """Extract error details from failed session for troubleshooting."""
        details: List[str] = []
        try:
            app_id = session_data.get("appId")
            if app_id:
                details.append(
                    f"appId={app_id} (Synapse Studio: Monitor → Apache Spark applications)"
                )
            app_info = session_data.get("appInfo", {})
            if app_info:
                driver = app_info.get("driverLogUrl") or app_info.get("log")
                if driver:
                    details.append(f"driverLogUrl={driver}")
            state = session_data.get("state")
            if state and state.lower() in ("error", "dead", "killed"):
                err_msg = session_data.get("log", [])
                if err_msg:
                    details.append(f"exception={'; '.join(err_msg[-3:])}")
        except Exception:
            pass
        return ". " + "; ".join(details) if details else ""

    def _wait_for_session_idle(self) -> None:
        """Wait indefinitely for the session to reach idle state. Large pools can take 5-10 min to cold start."""
        start = time.time()
        last_log = 0.0
        while True:
            # Use CRUSOE credentials for auth (if available)
            auth_header = {}
            if self._client_id and self._tenant_id and self._client_secret:
                credential = ClientSecretCredential(
                    tenant_id=self._tenant_id,
                    client_id=self._client_id,
                    client_secret=self._client_secret
                )
                token = credential.get_token(self._resource)
                auth_header = {"Authorization": f"Bearer {token.token}"}
            auth_header["Content-Type"] = "application/json"

            resp = requests.get(
                f"{self._livy_endpoint}/sessions/{self._session_id}",
                headers=auth_header
            )
            resp.raise_for_status()
            session_data = resp.json()
            state = session_data.get("state", "").lower()
            if state in ("idle", "busy"):
                logger.info(f"Session ready, state={state}")
                return
            if state in ("dead", "error", "killed", "success"):
                err_detail = self._extract_session_error_detail(session_data)
                raise SynapseConnectionError(
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
            raise SynapseConnectionError("Not connected to Synapse")

        # Use CRUSOE credentials for auth (if available)
        auth_header = {}
        if self._client_id and self._tenant_id and self._client_secret:
            credential = ClientSecretCredential(
                tenant_id=self._tenant_id,
                client_id=self._client_id,
                client_secret=self._client_secret
            )
            token = credential.get_token(self._resource)
            auth_header = {"Authorization": f"Bearer {token.token}"}
        auth_header["Content-Type"] = "application/json"

        payload = {
            "code": code,
            "kind": kind
        }
        resp = requests.post(
            f"{self._livy_endpoint}/sessions/{self._session_id}/statements",
            json=payload,
            headers=auth_header
        )
        resp.raise_for_status()
        stmt_data = resp.json()
        stmt_id = stmt_data.get("id")

        # Poll for completion
        start = time.time()
        while time.time() - start < STATEMENT_MAX_WAIT_SEC:
            resp = requests.get(
                f"{self._livy_endpoint}/sessions/{self._session_id}/statements/{stmt_id}",
                headers=auth_header
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
                raise SynapseExecutionError(f"Statement failed: {err_msg}")

            time.sleep(STATEMENT_POLL_INTERVAL_SEC)

        raise SynapseExecutionError("Statement timed out")

    def execute_sql(
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
        output = self._execute_statement(code, kind="pyspark")

        # Extract text from Livy output
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
        except SynapseExecutionError as e:
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
        except SynapseExecutionError as e:
            logger.warning(f"Failed to load {rel_path} into {view_name}: {e}")
            return False

    def close(self) -> None:
        """Terminate the Livy session."""
        if self._session_id is not None:
            try:
                # Use CRUSOE credentials for auth (if available)
                auth_header = {}
                if self._client_id and self._tenant_id and self._client_secret:
                    credential = ClientSecretCredential(
                        tenant_id=self._tenant_id,
                        client_id=self._client_id,
                        client_secret=self._client_secret
                    )
                    token = credential.get_token(self._resource)
                    auth_header = {"Authorization": f"Bearer {token.token}"}
                auth_header["Content-Type"] = "application/json"

                resp = requests.delete(
                    f"{self._livy_endpoint}/sessions/{self._session_id}",
                    headers=auth_header
                )
                resp.raise_for_status()
                logger.info("Synapse session closed")
            except Exception as e:
                logger.warning(f"Error closing session: {e}")
            self._session_id = None


def create_synapse_session(settings: Any) -> SynapseSparkSession:
    """
    Create and connect a Synapse Spark session.
    Requires: synapse_workspace_name, synapse_spark_pool_name, data_path.
    """
    workspace = getattr(settings, "synapse_workspace_name", None)
    pool = getattr(settings, "synapse_spark_pool_name", None)
    data_path = getattr(settings, "data_path", "./src_data")

    if not workspace or not pool:
        raise SynapseConnectionError(
            "CRUSOE_WORKSPACE and CRUSOE_SPARK_POOL are required"
        )
    # CRUSOE uses S3-compatible storage; no abfss:// requirement
    # Use CRUSOE environment variables for storage config
    storage_account = os.environ.get('CRUSOE_STORAGE_ACCOUNT')
    storage_key = os.environ.get('CRUSOE_STORAGE_ACCESS_KEY')
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
