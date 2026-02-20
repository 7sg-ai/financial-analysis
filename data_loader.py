"""
Data loading module for AWS EMR Serverless
Loads parquet/CSV data and registers temp views in EMR Serverless via SparkSubmit jobs
All Spark execution happens in AWS EMR Serverless; no local PySpark.
"""
from typing import Optional, List, Dict, Any, TYPE_CHECKING
import logging
import boto3

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Manages loading of taxi trip data into EMR Serverless temp views.
    Uses EMR Serverless SparkSubmit jobs - no local Spark.
    """

    def __init__(self, region_name: str = "us-east-1", execution_role_arn: Optional[str] = None):
        self.emr_client = boto3.client("emr-serverless", region_name=region_name)
        self.execution_role_arn = execution_role_arn or self._get_default_execution_role()

    def _get_default_execution_role(self) -> str:
        """Get default execution role ARN from environment (e.g., EC2 instance profile or Lambda role)."""
        # This is a placeholder implementation. In production, use boto3 to retrieve the role
        # attached to the current environment (e.g., via IAM instance profile or Lambda context).
        session = boto3.Session()
        iam_client = session.client('iam')
        try:
            role_name = session.resource('iam').Role('EMRServerlessExecutionRole').name
            return session.resource('iam').Role(role_name).arn
        except Exception:
            raise RuntimeError(
                "Default execution role ARN not found. Ensure IAM role 'EMRServerlessExecutionRole' exists or set EXECUTION_ROLE_ARN environment variable."
            )

    def _get_application_id(self) -> str:
        """Get EMR Serverless application ID. In production, this should be retrieved from config or environment."""
        # This is a placeholder implementation. In production, retrieve from environment variable or config.
        import os
        app_id = os.getenv('EMR_APPLICATION_ID')
        if not app_id:
            raise ValueError("EMR_APPLICATION_ID environment variable must be set.")
        return app_id

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
        job_run_id = self.emr_client.start_job_run(
            applicationId=self._get_application_id(),
            executionRoleArn=self.execution_role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": "s3://{}/scripts/load_parquet.py'.format(os.getenv('S3_BUCKET', 'data'))",
                    "sparkSubmitParameters": f"--conf spark.app.name=load_yellow_taxi --jars s3://<bucket>/jars/parquet.jar --pattern {pattern} --view yellow_taxi --months {','.join(months) if months else ''}"
                }
            },
            configurationOverrides={
                "applicationConfiguration": {
                    "spark-defaults": {
                        "spark.sql.adaptive.enabled": "true"
                    }
                }
            }
        )
        if job_run_id:
            views_registered.append("yellow_taxi")
            logger.info("Registered view: yellow_taxi")

        pattern = "green_tripdata_*-*.parquet" if not year else f"green_tripdata_{year}-*.parquet"
        job_run_id = self.emr_client.start_job_run(
            applicationId=self._get_application_id(),
            executionRoleArn=self.execution_role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": "s3://<bucket>/scripts/load_parquet.py",
                    "sparkSubmitParameters": f"--conf spark.app.name=load_green_taxi --jars s3://{os.getenv('S3_BUCKET', 'data')}/jars/parquet.jar --pattern {pattern} --view green_taxi --months {','.join(months) if months else ''}"ttern} --view green_taxi --months {','.join(months) if months else ''}"
                }
            },
            configurationOverrides={
                "applicationConfiguration": {
                    "spark-defaults": {
                        "spark.sql.adaptive.enabled": "true"
                    }
                }
            }
        )
        if job_run_id:
            views_registered.append("green_taxi")
            logger.info("Registered view: green_taxi")

        pattern = "fhv_tripdata_*-*.parquet" if not year else f"fhv_tripdata_{year}-*.parquet"
        job_run_id = self.emr_client.start_job_run(
            applicationId=self._get_application_id(),
            executionRoleArn=self.execution_role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": "s3://<bucket>/scripts/load_parquet.py",
                    "sparkSubmitParameters": f"--conf spark.app.name=load_fhv --jars s3://<bucket>/jars/parquet.jar --pattern {pattern} --view fhv --months {','.join(months) if months else ''}"
                }
            },
            configurationOverrides={
                "applicationConfiguration": {
                    "spark-defaults": {
                        "spark.sql.adaptive.enabled": "true"
                    }
                }
            }
        )
        if job_run_id:
            views_registered.append("fhv")
            logger.info("Registered view: fhv")

        pattern = "fhvhv_tripdata_*-*.parquet" if not year else f"fhvhv_tripdata_{year}-*.parquet"
        job_run_id = self.emr_client.start_job_run(
            applicationId=self._get_application_id(),
            executionRoleArn=self.execution_role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": "s3://<bucket>/scripts/load_parquet.py",
                    "sparkSubmitParameters": f"--conf spark.app.name=load_fhvhv --jars s3://<bucket>/jars/parquet.jar --pattern {pattern} --view fhvhv --months {','.join(months) if months else ''}"
                }
            },
            configurationOverrides={
                "applicationConfiguration": {
                    "spark-defaults": {
                        "spark.sql.adaptive.enabled": "true"
                    }
                }
            }
        )
        if job_run_id:
            views_registered.append("fhvhv")
            logger.info("Registered view: fhvhv")

        job_run_id = self.emr_client.start_job_run(
            applicationId=self._get_application_id(),
            executionRoleArn=self.execution_role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": "s3://<bucket>/scripts/load_csv.py",
                    "sparkSubmitParameters": f"--conf spark.app.name=load_taxi_zones --jars s3://<bucket>/jars/csv.jar --file taxi_zone_lookup.csv --view taxi_zones"
                }
            },
            configurationOverrides={
                "applicationConfiguration": {
                    "spark-defaults": {
                        "spark.sql.adaptive.enabled": "true"
                    }
                }
            }
        )
        if job_run_id:
            views_registered.append("taxi_zones")
            logger.info("Registered view: taxi_zones")

        if views_registered:
            logger.info(f"Registered {len(views_registered)} view(s): {', '.join(views_registered)}")
        else:
            logger.warning("No data files found. Views will be registered when data is available.")

        return len(views_registered) > 0

    def get_dataset_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get row counts and column info for loaded views via SparkSubmit jobs."""
        stats = {}
        views = ["yellow_taxi", "green_taxi", "fhv", "fhvhv", "taxi_zones"]

        for view in views:
            try:
                # Schema extraction job
                schema_job_id = self.emr_client.start_job_run(
                    applicationId=self._get_application_id(),
                    executionRoleArn=self.execution_role_arn,
                    jobDriver={
                        "sparkSubmit": {
                            "entryPoint": "s3://<bucket>/scripts/query_schema.py",
                            "sparkSubmitParameters": f"--conf spark.app.name=query_schema --view {view} --limit 1"
                        }
                    },
                    configurationOverrides={
                        "applicationConfiguration": {
                            "spark-defaults": {
                                "spark.sql.adaptive.enabled": "true"
                            }
                        }
                    }
                )
                
                # Row count job
                count_job_id = self.emr_client.start_job_run(
                    applicationId=self._get_application_id(),
                    executionRoleArn=self.execution_role_arn,
                    jobDriver={
                        "sparkSubmit": {
                            "entryPoint": "s3://<bucket>/scripts/count_rows.py",
                            "sparkSubmitParameters": f"--conf spark.app.name=count_rows --view {view}"
                        }
                    },
                    configurationOverrides={
                        "applicationConfiguration": {
                            "spark-defaults": {
                                "spark.sql.adaptive.enabled": "true"
                            }
                        }
                    }
                )
                
                # In production, you would wait for job completion and retrieve results from S3 or logs
                # For now, placeholder values
                stats[view] = {
                    "row_count": 0,
                    "columns": 0,
                    "column_names": [],
                }
            except Exception as e:
                logger.error(f"Error loading stats for {view}: {e}")
                stats[view] = {"error": str(e)}

        return stats