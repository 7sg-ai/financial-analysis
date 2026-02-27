"""
Data loading module for DuckDB
Loads parquet/CSV data and registers views in DuckDB (in-process).
"""
from typing import Optional, List, Dict, Any, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from duckdb_client import DuckDBSession

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Manages loading of taxi trip data into DuckDB views.
    """

    def __init__(self, session: "DuckDBSession"):
        self.session = session

    def clear_cache(self) -> None:
        """No-op kept for API compatibility."""
        logger.info("DataLoader cache cleared - next load will re-register views")

    def register_temp_views(
        self,
        months: Optional[List[str]] = None,
        year: Optional[str] = None,
    ) -> bool:
        """
        Load datasets and register them as views in DuckDB.

        Args:
            months: Months to load (e.g. ["01","02"]). None = all.
            year: Year to load. None = all available years.

        Returns:
            True if at least one view was registered.
        """
        logger.info("Registering views in DuckDB...")
        views_registered = []

        pattern = "yellow_tripdata_*-*.parquet" if not year else f"yellow_tripdata_{year}-*.parquet"
        if self.session.load_parquet_and_create_view(pattern, "yellow_taxi", months):
            views_registered.append("yellow_taxi")
            logger.info("Registered view: yellow_taxi")

        pattern = "green_tripdata_*-*.parquet" if not year else f"green_tripdata_{year}-*.parquet"
        if self.session.load_parquet_and_create_view(pattern, "green_taxi", months):
            views_registered.append("green_taxi")
            logger.info("Registered view: green_taxi")

        pattern = "fhv_tripdata_*-*.parquet" if not year else f"fhv_tripdata_{year}-*.parquet"
        if self.session.load_parquet_and_create_view(pattern, "fhv", months):
            views_registered.append("fhv")
            logger.info("Registered view: fhv")

        pattern = "fhvhv_tripdata_*-*.parquet" if not year else f"fhvhv_tripdata_{year}-*.parquet"
        if self.session.load_parquet_and_create_view(pattern, "fhvhv", months):
            views_registered.append("fhvhv")
            logger.info("Registered view: fhvhv")

        if self.session.load_csv_and_create_view("taxi_zone_lookup.csv", "taxi_zones"):
            views_registered.append("taxi_zones")
            logger.info("Registered view: taxi_zones")

        if views_registered:
            logger.info("Registered %d view(s): %s", len(views_registered), ", ".join(views_registered))
        else:
            logger.warning("No data files found. Views will be registered when data is available.")

        return len(views_registered) > 0

    def get_dataset_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get row counts and column info for loaded views."""
        stats = {}
        views = ["yellow_taxi", "green_taxi", "fhv", "fhvhv", "taxi_zones"]

        for view in views:
            try:
                r = self.session.execute_sql(f"SELECT * FROM {view} LIMIT 1", max_rows=1)
                cols = r.get("columns", [])
                cnt = self.session.execute_sql(f"SELECT COUNT(*) as cnt FROM {view}", max_rows=1)
                rows = cnt.get("data", [])
                row_count = int(rows[0].get("cnt", 0)) if rows else 0
                stats[view] = {
                    "row_count": row_count,
                    "columns": len(cols),
                    "column_names": cols,
                }
            except Exception as e:
                logger.error("Error loading stats for %s: %s", view, e)
                stats[view] = {"error": str(e)}

        return stats
