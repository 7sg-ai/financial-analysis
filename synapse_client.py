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

from azure.identity import ManagedIdentityCredential
from azure.synapse.spark import SparkClient
from azure.synapse.spark.models import (
    LivyStatementStates,
    LivyStates,
    SparkSessionOptions,
    SparkStatementOptions,
)

logger = logging.getLogger(__name__)

# Polling config for async statement execution
STATEMENT_POLL_INTERVAL_SEC = 2
STATEMENT_MAX_WAIT_SEC = 300
# Session startup can take 5-10+ min for Large pools (cold start).
# None = wait indefinitely (container will not restart; keeps waiting for Synapse).
SESSION_STARTUP_TIMEOUT_SEC = None


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
        self._client = SparkClient(
            credential=credential,
            endpoint=endpoint,
            spark_pool_name=spark_pool_name,
        )
        self.endpoint = endpoint
        self.spark_pool_name = spark_pool_name
        self.data_path = data_path.rstrip("/")
        self._session_id: Optional[int] = None
        self._storage_account = storage_account
        self._storage_key = storage_key

    def connect(self) -> None:
        """Create a Livy PySpark session in Synapse."""
        if self._session_id is not None:
            logger.info("Synapse session already active")
            return

        conf: Dict[str, str] = {}
        if self._storage_account and self._storage_key:
            key = f"fs.azure.account.key.{self._storage_account}.dfs.core.windows.net"
            conf[key] = self._storage_key
            logger.info(f"Configured Spark for Azure Storage: {self._storage_account}")

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
        opts = SparkSessionOptions(
            name="FinancialAnalysis",
            configuration=conf,
            executor_count=2,
            executor_memory="12g",
            executor_cores=2,
            driver_memory="4g",
            driver_cores=2,
        )

        logger.info("Creating Synapse Spark session...")
        session = self._client.spark_session.create_spark_session(
            opts, detailed=True
        )
        self._session_id = session.id
        logger.info(f"Synapse session created: id={self._session_id}")

        # Wait for session to be idle
        self._wait_for_session_idle()

    def _extract_session_error_detail(self, session: Any) -> str:
        """Extract error details from failed session for troubleshooting."""
        details: List[str] = []
        try:
            app_id = getattr(session, "app_id", None)
            app_info = getattr(session, "app_info", None)
            if app_info and not app_id:
                app_id = getattr(app_info, "id", None)
            if app_id:
                details.append(
                    f"appId={app_id} (Synapse Studio: Monitor → Apache Spark applications)"
                )
            if app_info:
                driver = getattr(app_info, "driver_log_url", None) or getattr(
                    app_info, "driverLogUrl", None
                )
                if driver:
                    details.append(f"driverLogUrl={driver}")
            livy = getattr(session, "livy_info", None)
            if livy:
                ex = getattr(livy, "exception", None) or getattr(
                    livy, "exception_message", None
                )
                if ex:
                    details.append(f"exception={ex}")
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
            session = self._client.spark_session.get_spark_session(
                self._session_id, detailed=True
            )
            state = getattr(session, "state", None)
            if state is None and hasattr(session, "livy_info"):
                li = session.livy_info
                state = getattr(li, "current_state", None) if li else None
            state = str(state).lower() if state else "unknown"
            if state in ("idle", "busy"):
                logger.info(f"Session ready, state={state}")
                return
            if state in ("dead", "error", "killed", "success"):
                err_detail = self._extract_session_error_detail(session)
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

        opts = SparkStatementOptions(code=code, kind=kind)
        stmt = self._client.spark_session.create_spark_statement(
            self._session_id, opts
        )
        stmt_id = stmt.id

        # Poll for completion
        start = time.time()
        while time.time() - start < STATEMENT_MAX_WAIT_SEC:
            stmt = self._client.spark_session.get_spark_statement(
                self._session_id, stmt_id
            )
            state = getattr(stmt, "livy_info", None)
            if state:
                state = getattr(state, "current_state", None) or str(state)
            else:
                state = getattr(stmt, "state", "unknown")

            if state in (
                LivyStatementStates.AVAILABLE,
                "available",
            ):
                output = getattr(stmt, "output", None)
                if output:
                    return output
                return {}
            if state in (
                LivyStatementStates.ERROR,
                LivyStatementStates.CANCELLED,
                "error",
                "cancelled",
            ):
                output = getattr(stmt, "output", None)
                err_msg = "Unknown error"
                if output:
                    data = getattr(output, "data", None) or {}
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

        data = getattr(output, "data", None) or {}
        if isinstance(data, dict):
            text = data.get("text/plain") or data.get("application/json")
        else:
            text = str(data)
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
                self._client.spark_session.cancel_spark_session(self._session_id)
                logger.info("Synapse session closed")
            except Exception as e:
                logger.warning(f"Error closing session: {e}")
            self._session_id = None
        if hasattr(self._client, "close"):
            self._client.close()


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
            "SYNAPSE_WORKSPACE_NAME and SYNAPSE_SPARK_POOL_NAME are required"
        )
    if not str(data_path).startswith("abfss://"):
        raise SynapseConnectionError(
            "DATA_PATH must be an abfss:// path when using Azure Synapse. "
            f"Got: {data_path}"
        )

    endpoint = f"https://{workspace}.dev.azuresynapse.net"
    # Use managed identity only (no service principal). AZURE_CLIENT_ID selects user-assigned identity when set.
    client_id = os.environ.get("AZURE_CLIENT_ID")
    credential = ManagedIdentityCredential(client_id=client_id)
    storage_account = getattr(settings, "azure_storage_account_name", None)
    storage_key = getattr(settings, "azure_storage_account_key", None)

    session = SynapseSparkSession(
        credential=credential,
        endpoint=endpoint,
        spark_pool_name=pool,
        data_path=data_path,
        storage_account=storage_account,
        storage_key=storage_key,
    )
    session.connect()
    return session
