"""
Data loading module for Spark-based financial analysis
Handles loading parquet files and CSV data into Spark DataFrames
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from typing import Optional, List, Dict, Union
import os
import glob
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Manages loading of taxi trip data into Spark DataFrames
    """
    
    def __init__(self, spark: SparkSession, data_path: str = "./src_data"):
        """
        Initialize DataLoader
        
        Args:
            spark: Active SparkSession
            data_path: Path to data directory
        """
        self.spark = spark
        self.data_path = Path(data_path)
        self._loaded_datasets = {}
        
    def load_yellow_taxi(
        self, 
        months: Optional[List[str]] = None,
        year: str = "2024"
    ) -> DataFrame:
        """
        Load yellow taxi trip data
        
        Args:
            months: List of months to load (e.g., ["01", "02"]). If None, loads all
            year: Year to load data for
            
        Returns:
            Spark DataFrame with yellow taxi data
        """
        pattern = f"yellow_tripdata_{year}-*.parquet"
        return self._load_parquet_files(pattern, months, "yellow_taxi")
    
    def load_green_taxi(
        self,
        months: Optional[List[str]] = None,
        year: str = "2024"
    ) -> DataFrame:
        """
        Load green taxi trip data
        
        Args:
            months: List of months to load. If None, loads all
            year: Year to load data for
            
        Returns:
            Spark DataFrame with green taxi data
        """
        pattern = f"green_tripdata_{year}-*.parquet"
        return self._load_parquet_files(pattern, months, "green_taxi")
    
    def load_fhv(
        self,
        months: Optional[List[str]] = None,
        year: str = "2024"
    ) -> DataFrame:
        """
        Load FHV (For-Hire Vehicle) trip data
        
        Args:
            months: List of months to load. If None, loads all
            year: Year to load data for
            
        Returns:
            Spark DataFrame with FHV data
        """
        pattern = f"fhv_tripdata_{year}-*.parquet"
        return self._load_parquet_files(pattern, months, "fhv")
    
    def load_fhvhv(
        self,
        months: Optional[List[str]] = None,
        year: str = "2024"
    ) -> DataFrame:
        """
        Load FHVHV (High Volume For-Hire Vehicle) trip data
        
        Args:
            months: List of months to load. If None, loads all
            year: Year to load data for
            
        Returns:
            Spark DataFrame with FHVHV data
        """
        pattern = f"fhvhv_tripdata_{year}-*.parquet"
        return self._load_parquet_files(pattern, months, "fhvhv")
    
    def load_taxi_zones(self) -> DataFrame:
        """
        Load taxi zone lookup data
        
        Returns:
            Spark DataFrame with zone lookup information
        """
        cache_key = "taxi_zones"
        if cache_key in self._loaded_datasets:
            logger.info("Returning cached taxi zones data")
            return self._loaded_datasets[cache_key]
        
        zone_file = self.data_path / "taxi_zone_lookup.csv"
        if not zone_file.exists():
            raise FileNotFoundError(f"Zone lookup file not found: {zone_file}")
        
        logger.info(f"Loading taxi zones from {zone_file}")
        df = self.spark.read.csv(
            str(zone_file),
            header=True,
            inferSchema=True
        )
        
        self._loaded_datasets[cache_key] = df
        return df
    
    def load_all_taxi_data(
        self,
        months: Optional[List[str]] = None,
        year: str = "2024",
        include_fhv: bool = True,
        include_fhvhv: bool = True
    ) -> Dict[str, DataFrame]:
        """
        Load all available taxi datasets
        
        Args:
            months: List of months to load. If None, loads all
            year: Year to load data for
            include_fhv: Whether to include FHV data
            include_fhvhv: Whether to include FHVHV data
            
        Returns:
            Dictionary mapping dataset names to DataFrames
        """
        datasets = {}
        
        logger.info("Loading all taxi datasets...")
        datasets['yellow_taxi'] = self.load_yellow_taxi(months, year)
        datasets['green_taxi'] = self.load_green_taxi(months, year)
        
        if include_fhv:
            datasets['fhv'] = self.load_fhv(months, year)
        
        if include_fhvhv:
            datasets['fhvhv'] = self.load_fhvhv(months, year)
        
        datasets['taxi_zones'] = self.load_taxi_zones()
        
        return datasets
    
    def register_temp_views(
        self,
        months: Optional[List[str]] = None,
        year: str = "2024"
    ) -> None:
        """
        Load all datasets and register them as temporary SQL views
        
        Args:
            months: List of months to load
            year: Year to load data for
        """
        logger.info("Registering temporary views for all datasets...")
        
        # Load and register yellow taxi
        df_yellow = self.load_yellow_taxi(months, year)
        df_yellow.createOrReplaceTempView("yellow_taxi")
        logger.info(f"Registered view: yellow_taxi ({df_yellow.count()} rows)")
        
        # Load and register green taxi
        df_green = self.load_green_taxi(months, year)
        df_green.createOrReplaceTempView("green_taxi")
        logger.info(f"Registered view: green_taxi ({df_green.count()} rows)")
        
        # Load and register FHV
        df_fhv = self.load_fhv(months, year)
        df_fhv.createOrReplaceTempView("fhv")
        logger.info(f"Registered view: fhv ({df_fhv.count()} rows)")
        
        # Load and register FHVHV
        df_fhvhv = self.load_fhvhv(months, year)
        df_fhvhv.createOrReplaceTempView("fhvhv")
        logger.info(f"Registered view: fhvhv ({df_fhvhv.count()} rows)")
        
        # Load and register zones
        df_zones = self.load_taxi_zones()
        df_zones.createOrReplaceTempView("taxi_zones")
        logger.info(f"Registered view: taxi_zones ({df_zones.count()} rows)")
        
        logger.info("All temporary views registered successfully")
    
    def _load_parquet_files(
        self,
        pattern: str,
        months: Optional[List[str]] = None,
        cache_key: Optional[str] = None
    ) -> DataFrame:
        """
        Internal method to load parquet files matching a pattern
        
        Args:
            pattern: File pattern to match
            months: Specific months to load
            cache_key: Key for caching loaded dataset
            
        Returns:
            Combined Spark DataFrame
        """
        # Check cache
        if cache_key and cache_key in self._loaded_datasets:
            logger.info(f"Returning cached dataset: {cache_key}")
            return self._loaded_datasets[cache_key]
        
        # Find matching files
        all_files = glob.glob(str(self.data_path / pattern))
        
        if not all_files:
            raise FileNotFoundError(f"No files found matching pattern: {pattern}")
        
        # Filter by months if specified
        if months:
            filtered_files = []
            for file_path in all_files:
                filename = os.path.basename(file_path)
                for month in months:
                    if f"-{month}.parquet" in filename:
                        filtered_files.append(file_path)
                        break
            files_to_load = filtered_files
        else:
            files_to_load = all_files
        
        if not files_to_load:
            raise FileNotFoundError(
                f"No files found for months {months} with pattern: {pattern}"
            )
        
        logger.info(f"Loading {len(files_to_load)} parquet file(s): {pattern}")
        
        # Load all files into a single DataFrame
        df = self.spark.read.parquet(*files_to_load)
        
        # Cache if requested
        if cache_key:
            self._loaded_datasets[cache_key] = df
        
        logger.info(f"Loaded {df.count()} rows from {len(files_to_load)} file(s)")
        return df
    
    def get_dataset_stats(self) -> Dict[str, Dict[str, any]]:
        """
        Get statistics about available datasets
        
        Returns:
            Dictionary with dataset statistics
        """
        stats = {}
        
        try:
            df_yellow = self.load_yellow_taxi()
            stats['yellow_taxi'] = {
                'row_count': df_yellow.count(),
                'columns': len(df_yellow.columns),
                'column_names': df_yellow.columns
            }
        except Exception as e:
            logger.error(f"Error loading yellow taxi stats: {e}")
            stats['yellow_taxi'] = {'error': str(e)}
        
        try:
            df_green = self.load_green_taxi()
            stats['green_taxi'] = {
                'row_count': df_green.count(),
                'columns': len(df_green.columns),
                'column_names': df_green.columns
            }
        except Exception as e:
            logger.error(f"Error loading green taxi stats: {e}")
            stats['green_taxi'] = {'error': str(e)}
        
        try:
            df_fhvhv = self.load_fhvhv()
            stats['fhvhv'] = {
                'row_count': df_fhvhv.count(),
                'columns': len(df_fhvhv.columns),
                'column_names': df_fhvhv.columns
            }
        except Exception as e:
            logger.error(f"Error loading fhvhv stats: {e}")
            stats['fhvhv'] = {'error': str(e)}
        
        try:
            df_zones = self.load_taxi_zones()
            stats['taxi_zones'] = {
                'row_count': df_zones.count(),
                'columns': len(df_zones.columns),
                'column_names': df_zones.columns
            }
        except Exception as e:
            logger.error(f"Error loading zones stats: {e}")
            stats['taxi_zones'] = {'error': str(e)}
        
        return stats


def create_spark_session(
    app_name: str = "FinancialAnalysis",
    master: str = "local[*]",
    config_overrides: Optional[Dict[str, str]] = None
) -> SparkSession:
    """
    Create and configure a Spark session
    
    Args:
        app_name: Name for the Spark application
        master: Spark master URL
        config_overrides: Additional Spark configuration options
        
    Returns:
        Configured SparkSession
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g")
    
    # Apply config overrides
    if config_overrides:
        for key, value in config_overrides.items():
            builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Spark session created: {app_name}")
    return spark

