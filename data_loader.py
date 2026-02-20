"""
Data loading module for AWS EMR Serverless
Loads parquet/CSV data and registers temp views in EMR Serverless via Spark jobs
All Spark execution happens in EMR Serverless; no local PySpark.
"""
from typing import Optional, List, Dict, Any, TYPE_CHECKING
import logging

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Manages loading of taxi trip data into EMR Serverless temp views.
    Uses EMR Serverless boto3 client - no local Spark.
    """

    def __init__(self, aws_region: str, emr_application_id: str, execution_role_arn: str):
        import boto3
        from botocore.exceptions import ClientError
        
        self.emr_client = boto3.client('emr-serverless', region_name=aws_region)
        self.emr_application_id = emr_application_id
        self.execution_role_arn = execution_role_arn

    def clear_cache(self) -> None:
        """No-op for EMR Serverless (views are server-side); kept for API compatibility."""
        logger.info("DataLoader cache cleared - next load will re-register views")

    def register_temp_views(
        self,
        months: Optional[List[str]] = None,
        year: Optional[str] = None,
    ) -> bool:
        """
        Load datasets and register them as temp views in EMR Serverless.

        Args:
            months: Months to load (e.g. ["01","02"]). None = all.
            year: Year to load. None = all available years.

        Returns:
            True if at least one view was registered.
        """
        logger.info("Registering temporary views in EMR Serverless...")
        views_registered = []

        pattern = "yellow_tripdata_*-*.parquet" if not year else f"yellow_tripdata_{year}-*.parquet"
        try:
            response = self.emr_client.start_job_run(
                applicationId=self.emr_application_id,
                executionRoleArn=self.execution_role_arn,
                jobDriver={
                    'sparkSubmit': {
                        'entryPoint': 's3://<bucket>/scripts/load_parquet.py',
                        'sparkSubmitParameters': f'--conf spark.app.name=load_yellow_taxi --conf spark.sql.sources.partitionOverwriteMode=dynamic s3://<bucket>/data/{pattern} yellow_taxi {months}'
                    }
                },
                configurationOverrides={
                    'applicationConfiguration': {
                        'spark': {
                            'configurations': [
                                {'classification': 'spark-defaults', 'properties': {'spark.sql.adaptive.enabled': 'true'}}
                            ]
                        }
                    }
                }
            )
            job_run_id = response['jobRunId']
            success = True
        except Exception as e:
            success = False
            logger.error(f"Failed to load yellow_taxi: {e}")
        
        if success:
            views_registered.append("yellow_taxi")
            logger.info("Registered view: yellow_taxi")

        pattern = "green_tripdata_*-*.parquet" if not year else f"green_tripdata_{year}-*.parquet"
        try:
            response = self.emr_client.start_job_run(
                applicationId=self.emr_application_id,
                executionRoleArn=self.execution_role_arn,
                jobDriver={
                    'sparkSubmit': {
                        'entryPoint': 's3://<bucket>/scripts/load_parquet.py',
                        'sparkSubmitParameters': f'--conf spark.app.name=load_green_taxi --conf spark.sql.sources.partitionOverwriteMode=dynamic s3://<bucket>/data/{pattern} green_taxi {months}'
                    }
                },
                configurationOverrides={
                    'applicationConfiguration': {
                        'spark': {
                            'configurations': [
                                {'classification': 'spark-defaults', 'properties': {'spark.sql.adaptive.enabled': 'true'}}
                            ]
                        }
                    }
                }
            )
            job_run_id = response['jobRunId']
            success = True
        except Exception as e:
            success = False
            logger.error(f"Failed to load green_taxi: {e}")
        
        if success:
            views_registered.append("green_taxi")
            logger.info("Registered view: green_taxi")

        pattern = "fhv_tripdata_*-*.parquet" if not year else f"fhv_tripdata_{year}-*.parquet"
        try:
            response = self.emr_client.start_job_run(
                applicationId=self.emr_application_id,
                executionRoleArn=self.execution_role_arn,
                jobDriver={
                    'sparkSubmit': {
                        'entryPoint': 's3://<bucket>/scripts/load_parquet.py',
                        'sparkSubmitParameters': f'--conf spark.app.name=load_fhv --conf spark.sql.sources.partitionOverwriteMode=dynamic s3://<bucket>/data/{pattern} fhv {months}'
                    }
                },
                configurationOverrides={
                    'applicationConfiguration': {
                        'spark': {
                            'configurations': [
                                {'classification': 'spark-defaults', 'properties': {'spark.sql.adaptive.enabled': 'true'}}
                            ]
                        }
                    }
                }
            )
            job_run_id = response['jobRunId']
            success = True
        except Exception as e:
            success = False
            logger.error(f"Failed to load fhv: {e}")
        
        if success:
            views_registered.append("fhv")
            logger.info("Registered view: fhv")

        pattern = "fhvhv_tripdata_*-*.parquet" if not year else f"fhvhv_tripdata_{year}-*.parquet"
        try:
            response = self.emr_client.start_job_run(
                applicationId=self.emr_application_id,
                executionRoleArn=self.execution_role_arn,
                jobDriver={
                    'sparkSubmit': {
                        'entryPoint': 's3://<bucket>/scripts/load_parquet.py',
                        'sparkSubmitParameters': f'--conf spark.app.name=load_fhvhv --conf spark.sql.sources.partitionOverwriteMode=dynamic s3://<bucket>/data/{pattern} fhvhv {months}'
                    }
                },
                configurationOverrides={
                    'applicationConfiguration': {
                        'spark': {
                            'configurations': [
                                {'classification': 'spark-defaults', 'properties': {'spark.sql.adaptive.enabled': 'true'}}
                            ]
                        }
                    }
                }
            )
            job_run_id = response['jobRunId']
            success = True
        except Exception as e:
            success = False
            logger.error(f"Failed to load fhvhv: {e}")
        
        if success:
            views_registered.append("fhvhv")
            logger.info("Registered view: fhvhv")

        try:
            response = self.emr_client.start_job_run(
                applicationId=self.emr_application_id,
                executionRoleArn=self.execution_role_arn,
                jobDriver={
                    'sparkSubmit': {
                        'entryPoint': 's3://<bucket>/scripts/load_csv.py',
                        'sparkSubmitParameters': f'--conf spark.app.name=load_taxi_zones s3://<bucket>/data/taxi_zone_lookup.csv taxi_zones'
                    }
                },
                configurationOverrides={
                    'applicationConfiguration': {
                        'spark': {
                            'configurations': [
                                {'classification': 'spark-defaults', 'properties': {'spark.sql.adaptive.enabled': 'true'}}
                            ]
                        }
                    }
                }
            )
            job_run_id = response['jobRunId']
            success = True
        except Exception as e:
            success = False
            logger.error(f"Failed to load taxi_zones: {e}")
        
        if success:
            views_registered.append("taxi_zones")
            logger.info("Registered view: taxi_zones")

        if views_registered:
            logger.info(f"Registered {len(views_registered)} view(s): {', '.join(views_registered)}")
        else:
            logger.warning("No data files found. Views will be registered when data is available.")

        return len(views_registered) > 0

    def get_dataset_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get row counts and column info for loaded views via SQL."""
        stats = {}
        views = ["yellow_taxi", "green_taxi", "fhv", "fhvhv", "taxi_zones"]

        for view in views:
            try:
                # Query sample data
                try:
                    response = self.emr_client.start_job_run(
                        applicationId=self.emr_application_id,
                        executionRoleArn=self.execution_role_arn,
                        jobDriver={
                            'sparkSubmit': {
                                'entryPoint': 's3://<bucket>/scripts/query_sample.py',
                                'sparkSubmitParameters': f'--conf spark.app.name=query_sample_{view} --conf spark.sql.adaptive.enabled=true s3://<bucket>/scripts/query_sample.py {view}'
                            }
                        },
                        configurationOverrides={
                            'applicationConfiguration': {
                                'spark': {
                                    'configurations': [
                                        {'classification': 'spark-defaults', 'properties': {'spark.sql.adaptive.enabled': 'true'}}
                                    ]
                                }
                            }
                        }
                    )
                    job_run_id = response['jobRunId']
                    sample_success = True
                except Exception as e:
                    sample_success = False
                    logger.error(f"Failed to run sample query for {view}: {e}")
                
                # Count rows
                try:
                    response = self.emr_client.start_job_run(
                        applicationId=self.emr_application_id,
                        executionRoleArn=self.execution_role_arn,
                        jobDriver={
                            'sparkSubmit': {
                                'entryPoint': 's3://<bucket>/scripts/count_rows.py',
                                'sparkSubmitParameters': f'--conf spark.app.name=count_rows_{view} --conf spark.sql.adaptive.enabled=true s3://<bucket>/scripts/count_rows.py {view}'
                            }
                        },
                        configurationOverrides={
                            'applicationConfiguration': {
                                'spark': {
                                    'configurations': [
                                        {'classification': 'spark-defaults', 'properties': {'spark.sql.adaptive.enabled': 'true'}}
                                    ]
                                }
                            }
                        }
                    )
                    job_run_id = response['jobRunId']
                    count_success = True
                except Exception as e:
                    count_success = False
                    logger.error(f"Failed to run count query for {view}: {e}")
                
                # For now, return placeholder stats since we can't synchronously fetch results
                stats[view] = {
                    "row_count": -1,  # Placeholder - actual count would come from job output
                    "columns": -1,    # Placeholder - actual column info would come from job output
                    "column_names": [],  # Placeholder
                }
            except Exception as e:
                logger.error(f"Error loading stats for {view}: {e}")
                stats[view] = {"error": str(e)}

        return stats