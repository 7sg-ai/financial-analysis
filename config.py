"""
Configuration module for Financial Analysis Application
"""
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
import os


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Azure OpenAI Configuration
    azure_openai_endpoint: str = Field(..., env="AZURE_OPENAI_ENDPOINT")
    azure_openai_api_key: str = Field(..., env="AZURE_OPENAI_API_KEY")
    azure_openai_deployment_name: str = Field(default="gpt-4", env="AZURE_OPENAI_DEPLOYMENT_NAME")
    azure_openai_api_version: str = Field(default="2024-02-15-preview", env="AZURE_OPENAI_API_VERSION")
    
    # Azure Synapse Configuration (Required)
    synapse_spark_pool_name: str = Field(..., env="SYNAPSE_SPARK_POOL_NAME")
    synapse_workspace_name: str = Field(..., env="SYNAPSE_WORKSPACE_NAME")
    azure_subscription_id: str = Field(..., env="AZURE_SUBSCRIPTION_ID")
    azure_resource_group: str = Field(..., env="AZURE_RESOURCE_GROUP")
    
    # Azure Storage Configuration
    azure_storage_account_name: Optional[str] = Field(default=None, env="AZURE_STORAGE_ACCOUNT_NAME")
    azure_storage_account_key: Optional[str] = Field(default=None, env="AZURE_STORAGE_ACCOUNT_KEY")
    azure_storage_container: Optional[str] = Field(default=None, env="AZURE_STORAGE_CONTAINER")
    
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

