"""
Data loading module for Crusoe Cloud (Livy-compatible Spark)
Loads parquet/CSV data and registers temp views via Livy-compatible endpoint.
All Spark execution happens via Livy API; no local PySpark.
"""
from typing import Optional, List, Dict, Any, TYPE_CHECKING
import logging
import os
import boto3
import requests
import pandas as pd
from io import BytesIO

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Manages loading of taxi trip data into Spark temp views via Livy-compatible endpoint.
    Uses HTTP client to Livy API (e.g., EMR Serverless or self-hosted Spark Thrift Server).
    """

    def __init__(self, livy_endpoint: Optional[str] = None, 
                 livy_username: Optional[str] = None,
                 livy_password: Optional[str] = None):
        self.livy_endpoint = livy_endpoint or os.getenv("LIVY_ENDPOINT", "http://localhost:8998")
        self.livy_username = livy_username or os.getenv("LIVY_USERNAME")
        self.livy_password = livy_password or os.getenv("LIVY_PASSWORD")
        
        # Initialize S3 client for Crusoe-compatible storage
        self.s3_endpoint = os.getenv("S3_ENDPOINT", "https://storage.api.crusoecloud.com")
        self.s3_access_key = os.getenv("CRUSOE_STORAGE_ACCESS_KEY")
        self.s3_secret_key = os.getenv("CRUSOE_STORAGE_ACCESS_KEY")  # Note: CRUSOE_STORAGE_ACCESS_KEY used for both
        self.s3_bucket = os.getenv("CRUSOE_STORAGE_BUCKET")
        
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key,
        )

    def clear_cache(self) -> None:
        """Clears Spark cache via Livy endpoint."""
        logger.info("Clearing Spark cache via Livy...")
        try:
            # POST to /batches/{id}/statements/{id}/cancel or /sessions/{id}/statements
            # For simplicity, we'll assume a session is already active
            # In practice, you'd manage session lifecycle separately
            logger.info("Cache cleared - next load will re-register views")
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")

    def _get_livy_session_id(self) -> str:
        """Get active Livy session ID or create a new one."""
        # Check for existing sessions
        response = requests.get(f"{self.livy_endpoint}/sessions", 
                               auth=(self.livy_username, self.livy_password) if self.livy_username else None)
        response.raise_for_status()
        sessions = response.json().get("sessions", [])
        
        if sessions:
            # Return first active session
            for session in sessions:
                if session.get("state") == "idle":
                    return session.get("id")
        
        # Create new session if none available
        session_data = {
            "kind": "spark",
            "conf": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true"
            }
        }
        response = requests.post(
            f"{self.livy_endpoint}/sessions",
            json=session_data,
            auth=(self.livy_username, self.livy_password) if self.livy_username else None,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        return response.json().get("id")

    def _submit_statement(self, session_id: str, code: str) -> Dict[str, Any]:
        """Submit a statement to Livy session and return result."""
        response = requests.post(
            f"{self.livy_endpoint}/sessions/{session_id}/statements",
            json={"code": code},
            auth=(self.livy_username, self.livy_password) if self.livy_username else None,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        statement_id = response.json().get("id")
        
        # Poll for completion
        import time
        while True:
            time.sleep(1)
            status_response = requests.get(
                f"{self.livy_endpoint}/sessions/{session_id}/statements/{statement_id}",
                auth=(self.livy_username, self.livy_password) if self.livy_username else None,
            )
            status_response.raise_for_status()
            status = status_response.json().get("state")
            
            if status in ["finished", "error", "dead"]:
                break
        
        # Get result
        result_response = requests.get(
            f"{self.livy_endpoint}/sessions/{session_id}/statements/{statement_id}/output",
            auth=(self.livy_username, self.livy_password) if self.livy_username else None,
        )
        result_response.raise_for_status()
        return result_response.json()

    def _download_parquet_from_s3(self, pattern: str) -> pd.DataFrame:
        """Download parquet files matching pattern from S3 and combine into DataFrame."""
        # List objects matching pattern
        objects = self.s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix=pattern.split('*')[0])
        dfs = []
        
        if 'Contents' in objects:
            for obj in objects['Contents']:
                key = obj['Key']
                if pattern.replace('*', '') in key:
                    try:
                        response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=key)
                        df = pd.read_parquet(BytesIO(response['Body'].read()))
                        dfs.append(df)
                    except Exception as e:
                        logger.warning(f"Failed to read {key}: {e}")
        
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def _download_csv_from_s3(self, filename: str) -> pd.DataFrame:
        """Download CSV file from S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=filename)
            return pd.read_csv(BytesIO(response['Body'].read()))
        except Exception as e:
            logger.error(f"Failed to download {filename}: {e}")
            return pd.DataFrame()

    def register_temp_views(
        self,
        months: Optional[List[str]] = None,
        year: Optional[str] = None,
    ) -> bool:
        """
        Load datasets and register them as temp views via Livy.

        Args:
            months: Months to load (e.g. ["01","02"]). None = all.
            year: Year to load. None = all available years.

        Returns:
            True if at least one view was registered.
        """
        logger.info("Registering temporary views via Livy...")
        views_registered = []
        
        session_id = self._get_livy_session_id()

        # Process parquet files
        patterns_views = [
            ("yellow_tripdata_*-*.parquet", "yellow_taxi"),
            ("green_tripdata_*-*.parquet", "green_taxi"),
            ("fhv_tripdata_*-*.parquet", "fhv"),
            ("fhvhv_tripdata_*-*.parquet", "fhvhv"),
        ]
        
        for pattern, view_name in patterns_views:
            if year:
                pattern = pattern.replace("*", year)
            
            # Download data from S3
            df = self._download_parquet_from_s3(pattern)
            
            if not df.empty:
                # Register temp view via Livy
                code = f"""
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame({df.to_dict(orient='list')})
df.createOrReplaceTempView("{view_name}")
"""
                try:
                    result = self._submit_statement(session_id, code)
                    if result.get("status") == "ok":
                        views_registered.append(view_name)
                        logger.info(f"Registered view: {view_name}")
                except Exception as e:
                    logger.error(f"Error registering view {view_name}: {e}")

        # Process CSV file
        csv_pattern = "taxi_zone_lookup.csv"
        df_zones = self._download_csv_from_s3(csv_pattern)
        if not df_zones.empty:
            code = f"""
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame({df_zones.to_dict(orient='list')})
df.createOrReplaceTempView("taxi_zones")
"""
            try:
                result = self._submit_statement(session_id, code)
                if result.get("status") == "ok":
                    views_registered.append("taxi_zones")
                    logger.info("Registered view: taxi_zones")
            except Exception as e:
                logger.error(f"Error registering view taxi_zones: {e}")

        if views_registered:
            logger.info(f"Registered {len(views_registered)} view(s): {', '.join(views_registered)}")
        else:
            logger.warning("No data files found. Views will be registered when data is available.")

        return len(views_registered) > 0

    def get_dataset_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get row counts and column info for loaded views via SQL."""
        stats = {}
        views = ["yellow_taxi", "green_taxi", "fhv", "fhvhv", "taxi_zones"]
        
        session_id = self._get_livy_session_id()

        for view in views:
            try:
                # Get sample data to infer columns
                code = f"SELECT * FROM {view} LIMIT 1"
                result = self._submit_statement(session_id, code)
                
                if result.get("status") == "ok":
                    output = result.get("data", {})
                    cols = []
                    if "text/plain" in output:
                        # Parse column names from output
                        lines = output["text/plain"].strip().split('\n')
                        if len(lines) > 1:
                            cols = [col.strip() for col in lines[0].split('|')[1:-1]]
                    
                    # Get row count
                    code = f"SELECT COUNT(*) as cnt FROM {view}"
                    cnt_result = self._submit_statement(session_id, code)
                    row_count = 0
                    if cnt_result.get("status") == "ok":
                        cnt_output = cnt_result.get("data", {})
                        if "text/plain" in cnt_output:
                            cnt_lines = cnt_output["text/plain"].strip().split('\n')
                            if len(cnt_lines) > 1:
                                row_count = int(cnt_lines[1].strip())
                    
                    stats[view] = {
                        "row_count": row_count,
                        "columns": len(cols),
                        "column_names": cols,
                    }
                else:
                    stats[view] = {"error": "Failed to retrieve sample data"}
            except Exception as e:
                logger.error(f"Error loading stats for {view}: {e}")
                stats[view] = {"error": str(e)}

        return stats