#!/usr/bin/env python3
"""
Local development script to run the Financial Analysis API
"""
import os
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_environment():
    """Check if required environment variables are set"""
    required_vars = [
        'AZURE_OPENAI_ENDPOINT',
        'AZURE_OPENAI_API_KEY'
    ]
    
    missing = []
    for var in required_vars:
        if not os.getenv(var):
            missing.append(var)
    
    if missing:
        logger.error("Missing required environment variables:")
        for var in missing:
            logger.error(f"  - {var}")
        logger.error("\nPlease set these variables or create a .env file")
        logger.error("You can copy .env.template to .env and fill in the values")
        return False
    
    return True


def check_data_directory():
    """Check if data directory exists and has data"""
    data_path = project_root / "src_data"
    
    if not data_path.exists():
        logger.error(f"Data directory not found: {data_path}")
        return False
    
    # Check for at least one parquet file
    parquet_files = list(data_path.glob("*.parquet"))
    if not parquet_files:
        logger.warning("No parquet files found in src_data directory")
        logger.warning("The application may not work without data files")
    else:
        logger.info(f"Found {len(parquet_files)} parquet data files")
    
    return True


def main():
    """Main entry point"""
    logger.info("Starting Financial Analysis API (Local Mode)")
    logger.info("=" * 60)
    
    # Load environment variables from .env if it exists
    env_file = project_root / ".env"
    if env_file.exists():
        logger.info(f"Loading environment from {env_file}")
        from dotenv import load_dotenv
        load_dotenv(env_file)
    else:
        logger.warning(".env file not found - using environment variables")
    
    # Check environment
    if not check_environment():
        sys.exit(1)
    
    # Check data
    if not check_data_directory():
        logger.error("Data directory check failed")
        sys.exit(1)
    
    # Set local mode
    os.environ['USE_LOCAL_SPARK'] = 'true'
    
    # Import and run API
    try:
        logger.info("Initializing API server...")
        import uvicorn
        from config import get_settings
        
        settings = get_settings()
        
        logger.info(f"Starting server at http://{settings.api_host}:{settings.api_port}")
        logger.info("API Documentation: http://localhost:8000/docs")
        logger.info("=" * 60)
        
        uvicorn.run(
            "api:app",
            host=settings.api_host,
            port=settings.api_port,
            reload=True,
            log_level=settings.log_level.lower()
        )
        
    except Exception as e:
        logger.error(f"Failed to start API server: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

