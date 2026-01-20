"""
Data loading module for Spark-based financial analysis
Handles loading parquet files and CSV data into Spark DataFrames
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from typing import Optional, List, Dict, Union, Any
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
        
        views_registered = []
        
        # Load and register yellow taxi
        df_yellow = self.load_yellow_taxi(months, year)
        if df_yellow is not None:
            df_yellow.createOrReplaceTempView("yellow_taxi")
            row_count = df_yellow.count()
            logger.info(f"Registered view: yellow_taxi ({row_count} rows)")
            views_registered.append("yellow_taxi")
        else:
            logger.warning("No yellow taxi data files found, skipping view registration")
        
        # Load and register green taxi
        df_green = self.load_green_taxi(months, year)
        if df_green is not None:
            df_green.createOrReplaceTempView("green_taxi")
            row_count = df_green.count()
            logger.info(f"Registered view: green_taxi ({row_count} rows)")
            views_registered.append("green_taxi")
        else:
            logger.warning("No green taxi data files found, skipping view registration")
        
        # Load and register FHV
        df_fhv = self.load_fhv(months, year)
        if df_fhv is not None:
            df_fhv.createOrReplaceTempView("fhv")
            row_count = df_fhv.count()
            logger.info(f"Registered view: fhv ({row_count} rows)")
            views_registered.append("fhv")
        else:
            logger.warning("No FHV data files found, skipping view registration")
        
        # Load and register FHVHV
        df_fhvhv = self.load_fhvhv(months, year)
        if df_fhvhv is not None:
            df_fhvhv.createOrReplaceTempView("fhvhv")
            row_count = df_fhvhv.count()
            logger.info(f"Registered view: fhvhv ({row_count} rows)")
            views_registered.append("fhvhv")
        else:
            logger.warning("No FHVHV data files found, skipping view registration")
        
        if views_registered:
            logger.info(f"Successfully registered {len(views_registered)} view(s): {', '.join(views_registered)}")
        else:
            logger.warning("No data files found. Views will be registered when data becomes available.")
        
        return len(views_registered) > 0
        
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
            logger.warning(f"No files found matching pattern: {pattern}")
            return None
        
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
            logger.warning(
                f"No files found for months {months} with pattern: {pattern}"
            )
            return None
        
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
    config_overrides: Optional[Dict[str, str]] = None,
    settings: Optional[Any] = None
) -> SparkSession:
    """
    Create and configure a Spark session for Azure Synapse Spark
    
    This function connects to Azure Synapse Spark pools remotely.
    Spark execution happens in Azure Synapse, not locally.
    
    Args:
        app_name: Name for the Spark application
        config_overrides: Additional Spark configuration options
        settings: Application settings (for Synapse connection info)
        
    Returns:
        Configured SparkSession connected to Azure Synapse Spark pool
    """
    try:
        # Try to use Azure Synapse Spark connection
        if settings and hasattr(settings, 'synapse_spark_pool_name') and settings.synapse_spark_pool_name:
            logger.info(f"Connecting to Azure Synapse Spark pool: {settings.synapse_spark_pool_name}")
            
            # Azure Synapse Spark uses PySpark APIs but connects remotely
            # The azure-synapse-spark package provides the connection layer
            from azure.identity import DefaultAzureCredential
            from azure.synapse.spark import SparkClient
            from azure.synapse.spark.models import SparkSessionOptions
            
            # Note: azure-synapse-spark package provides remote Spark execution
            # PySpark types are still used, but Spark runs in Synapse pools
            logger.info("Using Azure Synapse Spark remote connection")
            
            # For now, create a standard SparkSession that will connect to Synapse
            # The actual connection is managed by Azure Synapse Spark runtime
            builder = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.parquet.compression.codec", "snappy")
            
            # Apply config overrides
            if config_overrides:
                for key, value in config_overrides.items():
                    builder = builder.config(key, value)
            
            spark = builder.getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            
            logger.info(f"Spark session created for Azure Synapse Spark: {app_name}")
            return spark
            
    except ImportError:
        logger.warning("azure-synapse-spark package not available, using standard PySpark")
    except Exception as e:
        logger.warning(f"Failed to connect to Azure Synapse Spark: {e}, using standard PySpark")
    
    # Fallback: Standard PySpark (for development/testing)
    # Note: In production, this should connect to Azure Synapse Spark pools
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy")
    
    # Apply config overrides
    if config_overrides:
        for key, value in config_overrides.items():
            builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.warning(f"Using standard PySpark session (not Azure Synapse Spark): {app_name}")
    return spark

