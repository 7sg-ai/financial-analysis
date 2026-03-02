"""
Configuration module for Financial Analysis Application
"""
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
import os


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Crusoe Managed Inference (OpenAI-compatible) Configuration
    crusoe_inference_url: str = Field(..., env="CRUSOE_INFERENCE_URL")
    crusoe_api_key: str = Field(..., env="CRUSOE_API_KEY")
    crusoe_deployment_name: str = Field(default="gpt-5.2-chat", env="CRUSOE_DEPLOYMENT_NAME")
    crusoe_api_version: str = Field(default="2024-12-01-preview", env="CRUSOE_API_VERSION")
    
    # Crusoe Synapse Configuration (Required)
    crusoe_spark_pool: str = Field(..., env="CRUSOE_SPARK_POOL")
    crusoe_workspace: str = Field(..., env="CRUSOE_WORKSPACE")
    crusoe_subscription_id: str = Field(..., env="CRUSOE_SUBSCRIPTION_ID")
    crusoe_resource_group: str = Field(..., env="CRUSOE_RESOURCE_GROUP")
    
    # Crusoe Storage Configuration
    crusoe_storage_account: Optional[str] = Field(default=None, env="CRUSOE_STORAGE_ACCOUNT")
    crusoe_storage_access_key: Optional[str] = Field(default=None, env="CRUSOE_STORAGE_ACCESS_KEY")
    crusoe_storage_bucket: Optional[str] = Field(default=None, env="CRUSOE_STORAGE_BUCKET")
    
    # Crusoe S3 Configuration
    crusoe_s3_endpoint: Optional[str] = Field(default=None, env="CRUSOE_S3_ENDPOINT")
    crusoe_s3_access_key: Optional[str] = Field(default=None, env="CRUSOE_S3_ACCESS_KEY")
    crusoe_s3_secret_key: Optional[str] = Field(default=None, env="CRUSOE_S3_SECRET_KEY")
    crusoe_s3_bucket: Optional[str] = Field(default=None, env="CRUSOE_S3_BUCKET")
    
    # Crusoe Livy Configuration
    crusoe_livy_endpoint: Optional[str] = Field(default=None, env="CRUSOE_LIVY_ENDPOINT")
    crusoe_livy_username: Optional[str] = Field(default=None, env="CRUSOE_LIVY_USERNAME")
    crusoe_livy_password: Optional[str] = Field(default=None, env="CRUSOE_LIVY_PASSWORD")
    
    # Crusoe Azure AD Token Configuration (for hybrid Azure AD tokens)
    crusoe_client_id: Optional[str] = Field(default=None, env="CRUSOE_CLIENT_ID")
    crusoe_tenant_id: Optional[str] = Field(default=None, env="CRUSOE_TENANT_ID")
    crusoe_client_secret: Optional[str] = Field(default=None, env="CRUSOE_CLIENT_SECRET")
    crusoe_resource: Optional[str] = Field(default=None, env="CRUSOE_RESOURCE")
    
    # Application Configuration
    data_path: str = Field(default="./src_data", env="DATA_PATH")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    max_query_retries: int = Field(default=3, env="MAX_QUERY_RETRIES")
    query_timeout_seconds: int = Field(default=300, env="QUERY_TIMEOUT_SECONDS")
    
    # API Configuration
    api_host: str = Field(default="0.0.0.0", env="API_HOST")
    api_port: int = Field(default=8000, env="API_PORT")
    api_title: str = Field(default="Financial Analysis API", env="API_TITLE")
    api_version: str = Field(default="1.0.0", env="API_VERSION")
    
    # Streamlit Configuration
    streamlit_port: int = Field(default=8501, env="STREAMLIT_PORT")
    streamlit_host: str = Field(default="0.0.0.0", env="STREAMLIT_HOST")
    streamlit_theme: str = Field(default="light", env="STREAMLIT_THEME")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


def get_settings() -> Settings:
    """Get application settings instance"""
    return Settings()