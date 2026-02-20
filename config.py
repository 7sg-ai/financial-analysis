"""
Configuration module for Financial Analysis Application
"""
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
import os


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Crusoe Managed Inference Configuration
    crusoe_inference_endpoint: str = Field(default="https://inference.api.crusoecloud.com/v1", env="CRUSOE_INFERENCE_ENDPOINT")
    crusoe_api_key: str = Field(..., env="CRUSOE_API_KEY")
    crusoe_model_id: str = Field(default="meta-llama/Meta-Llama-3.1-8B-Instruct", env="CRUSOE_MODEL_ID")
    
    # EMR Serverless Configuration (Required)
    emr_application_id: str = Field(..., env="EMR_APPLICATION_ID")
    aws_account_id: str = Field(..., env="AWS_ACCOUNT_ID")
    
    # Persistent Disk Configuration (replaces Azure Storage)
    persistent_disk_path: Optional[str] = Field(default="/crusoe/data", env="PERSISTENT_DISK_PATH")
    
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