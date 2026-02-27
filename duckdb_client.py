"""
DuckDB query engine — replaces Azure Synapse Spark (Livy).
All SQL execution happens in-process; no remote sessions.
Reads parquet from local disk or S3-compatible storage (MinIO).
"""
from __future__ import annotations

import glob as globmod
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

logger = logging.getLogger(__name__)


class DuckDBConnectionError(Exception):
    """Raised when DuckDB initialisation fails"""


class DuckDBExecutionError(Exception):
    """Raised when a SQL statement fails in DuckDB"""


class DuckDBSession:
    """
    In-process DuckDB session that mirrors the SynapseSparkSession interface.
    Supports local filesystem paths and S3 (MinIO) paths.
    """

    def __init__(
        self,
        data_path: str,
        s3_endpoint: Optional[str] = None,
        s3_access_key: Optional[str] = None,
        s3_secret_key: Optional[str] = None,
        s3_region: str = "us-east-1",
        s3_use_ssl: bool = False,
    ):
        self.data_path = data_path.rstrip("/")
        self._s3_endpoint = s3_endpoint
        self._s3_access_key = s3_access_key
        self._s3_secret_key = s3_secret_key
        self._s3_region = s3_region
        self._s3_use_ssl = s3_use_ssl
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    def connect(self) -> None:
        """Create an in-memory DuckDB database and configure S3 if needed."""
        if self._conn is not None:
            logger.info("DuckDB session already active")
            return

        self._conn = duckdb.connect(database=":memory:")

        if self._is_s3_path():
            self._conn.install_extension("httpfs")
            self._conn.load_extension("httpfs")
            if self._s3_endpoint:
                endpoint = self._s3_endpoint.replace("http://", "").replace("https://", "")
                self._conn.execute(f"SET s3_endpoint='{endpoint}'")
                self._conn.execute(f"SET s3_use_ssl={'true' if self._s3_use_ssl else 'false'}")
                self._conn.execute("SET s3_url_style='path'")
            if self._s3_access_key:
                self._conn.execute(f"SET s3_access_key_id='{self._s3_access_key}'")
            if self._s3_secret_key:
                self._conn.execute(f"SET s3_secret='{self._s3_secret_key}'")
            if self._s3_region:
                self._conn.execute(f"SET s3_region='{self._s3_region}'")
            logger.info("DuckDB configured for S3 storage: %s", self._s3_endpoint)

        logger.info("DuckDB session ready (data_path=%s)", self.data_path)

    def _is_s3_path(self) -> bool:
        return self.data_path.startswith("s3://") or self._s3_endpoint is not None

    def execute_sql(self, query: str, max_rows: int = 1000) -> Dict[str, Any]:
        """
        Execute SQL and return result as dict with keys: data, columns, row_count.
        Same return shape as SynapseSparkSession.execute_sql().
        """
        if self._conn is None:
            raise DuckDBConnectionError("Not connected to DuckDB")

        try:
            result = self._conn.execute(query)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchmany(max_rows)
            data = [dict(zip(columns, row)) for row in rows]
            return {"data": data, "columns": columns, "row_count": len(data)}
        except duckdb.Error as exc:
            raise DuckDBExecutionError(f"DuckDB query failed: {exc}") from exc

    def load_parquet_and_create_view(
        self,
        pattern: str,
        view_name: str,
        months: Optional[List[str]] = None,
    ) -> bool:
        """
        Load parquet files matching pattern and create a view.
        Type-casts location ID columns to BIGINT for consistency.
        """
        if self._conn is None:
            raise DuckDBConnectionError("Not connected to DuckDB")

        parquet_glob = f"{self.data_path}/{pattern}"

        if not self._is_s3_path():
            matched = globmod.glob(parquet_glob)
            if not matched:
                logger.warning("No files matching %s in %s", pattern, self.data_path)
                return False

        cast_cols = [
            "PULocationID", "DOLocationID", "PUlocationID", "DOlocationID",
            "SR_Flag", "VendorID", "RatecodeID", "passenger_count",
            "payment_type", "trip_type",
        ]

        try:
            read_expr = (
                f"read_parquet('{parquet_glob}', "
                f"union_by_name=true, hive_partitioning=false)"
            )

            raw_cols = [
                desc[0]
                for desc in self._conn.execute(
                    f"SELECT * FROM {read_expr} LIMIT 0"
                ).description
            ]

            select_parts = []
            for col in raw_cols:
                if col in cast_cols:
                    select_parts.append(f'CAST("{col}" AS BIGINT) AS "{col}"')
                else:
                    select_parts.append(f'"{col}"')

            select_clause = ", ".join(select_parts)
            where_clause = ""

            if months:
                date_col = next(
                    (c for c in raw_cols if "pickup" in c.lower()), None
                )
                if date_col:
                    where_clause = (
                        f' WHERE CAST(month("{date_col}") AS VARCHAR) '
                        f"IN ({', '.join(repr(m) for m in months)})"
                    )

            self._conn.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS "
                f"SELECT {select_clause} FROM {read_expr}{where_clause}"
            )
            return True

        except duckdb.Error as exc:
            logger.warning("Failed to load %s into %s: %s", pattern, view_name, exc)
            return False

    def load_csv_and_create_view(self, rel_path: str, view_name: str) -> bool:
        """Load CSV and create a view."""
        if self._conn is None:
            raise DuckDBConnectionError("Not connected to DuckDB")

        path = f"{self.data_path}/{rel_path}"
        try:
            self._conn.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS "
                f"SELECT * FROM read_csv_auto('{path}', header=true)"
            )
            return True
        except duckdb.Error as exc:
            logger.warning("Failed to load %s into %s: %s", rel_path, view_name, exc)
            return False

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._conn is not None:
            try:
                self._conn.close()
                logger.info("DuckDB session closed")
            except Exception as exc:
                logger.warning("Error closing DuckDB: %s", exc)
            self._conn = None


def create_duckdb_session(settings: Any) -> DuckDBSession:
    """
    Create and connect a DuckDB session.
    Reads data_path and optional S3 settings from the Settings object.
    """
    data_path = getattr(settings, "data_path", "./src_data")

    session = DuckDBSession(
        data_path=data_path,
        s3_endpoint=getattr(settings, "s3_endpoint", None),
        s3_access_key=getattr(settings, "s3_access_key", None),
        s3_secret_key=getattr(settings, "s3_secret_key", None),
        s3_region=getattr(settings, "s3_region", "us-east-1"),
        s3_use_ssl=getattr(settings, "s3_use_ssl", False),
    )
    session.connect()
    return session
