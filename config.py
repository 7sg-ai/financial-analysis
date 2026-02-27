"""
Configuration module for Financial Analysis Application
Crusoe Cloud deployment (DuckDB + self-hosted LLM + MinIO)
"""
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # LLM Configuration (OpenAI-compatible endpoint)
    llm_base_url: str = Field(..., env="LLM_BASE_URL")
    llm_api_key: str = Field(default="", env="LLM_API_KEY")
    llm_model_name: str = Field(default="Qwen/Qwen3-Coder-Next-FP8", env="LLM_MODEL_NAME")

    # Cloudflare Access (optional, for endpoints behind CF Zero Trust)
    cf_access_client_id: Optional[str] = Field(default="4eff9112d37e488616e3e38a50e5a4b2.access", env="CF_ACCESS_CLIENT_ID")
    cf_access_client_secret: Optional[str] = Field(default="c69e39dbe5c38dcdb9f51d89cb33188943bae728042588ee8def81b5c1b0f8f2", env="CF_ACCESS_CLIENT_SECRET")

    # S3-compatible Storage (MinIO)
    s3_endpoint: Optional[str] = Field(default=None, env="S3_ENDPOINT")
    s3_access_key: Optional[str] = Field(default=None, env="S3_ACCESS_KEY")
    s3_secret_key: Optional[str] = Field(default=None, env="S3_SECRET_KEY")
    s3_bucket: Optional[str] = Field(default=None, env="S3_BUCKET")
    s3_region: str = Field(default="us-east-1", env="S3_REGION")
    s3_use_ssl: bool = Field(default=False, env="S3_USE_SSL")

    # Langfuse Observability
    langfuse_secret_key: Optional[str] = Field(default="sk-lf-f708968e-5023-41f6-a4b4-da6b9c6f01c8", env="LANGFUSE_SECRET_KEY")
    langfuse_public_key: Optional[str] = Field(default="pk-lf-b3bc8526-3a13-44ad-be08-78de2f1f89ea", env="LANGFUSE_PUBLIC_KEY")
    langfuse_host: Optional[str] = Field(default="https://langfuse.7sg.ai", env="LANGFUSE_HOST")

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
