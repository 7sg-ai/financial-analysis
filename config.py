"""
Configuration module for Financial Analysis Application
"""
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
import os


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # AWS Bedrock Configuration
    aws_region: str = Field(..., env="AWS_REGION")
    aws_access_key_id: Optional[str] = Field(default=None, env="AWS_ACCESS_KEY_ID")
    bedrock_model_id: str = Field(default="meta.llama3-2-3b-instruct-v1:0", env="BEDROCK_MODEL_ID")
    
    # AWS EMR Serverless Configuration (Required)
    emr_application_id: str = Field(..., env="EMR_APPLICATION_ID")
    aws_account_id: str = Field(..., env="AWS_ACCOUNT_ID")
    
    # AWS S3 Configuration
    s3_bucket_name: Optional[str] = Field(default=None, env="S3_BUCKET_NAME")
    
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