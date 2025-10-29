#!/usr/bin/env python3
"""
Data download script for Financial Analysis Application
Downloads NYC taxi and for-hire vehicle data from the official source
"""
import os
import requests
import zipfile
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Data URLs (these are example URLs - replace with actual NYC TLC data URLs)
DATA_URLS = {
    "yellow_2024": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{month:02d}.parquet",
    "green_2024": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-{month:02d}.parquet", 
    "fhv_2024": "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2024-{month:02d}.parquet",
    "fhvhv_2024": "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-{month:02d}.parquet",
    "taxi_zones": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
}

def download_file(url: str, filepath: Path) -> bool:
    """Download a file from URL to filepath"""
    try:
        logger.info(f"Downloading {filepath.name}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"✅ Downloaded {filepath.name}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to download {filepath.name}: {e}")
        return False

def download_parquet_data():
    """Download all parquet data files for 2024"""
    data_dir = Path("src_data")
    data_dir.mkdir(exist_ok=True)
    
    success_count = 0
    total_count = 0
    
    # Download monthly data for each service type
    for service_type, base_url in DATA_URLS.items():
        if service_type == "taxi_zones":
            continue
            
        for month in range(1, 13):
            url = base_url.format(month=month)
            filename = f"{service_type}_2024-{month:02d}.parquet"
            filepath = data_dir / filename
            
            if filepath.exists():
                logger.info(f"⏭️  Skipping {filename} (already exists)")
                continue
                
            total_count += 1
            if download_file(url, filepath):
                success_count += 1
    
    return success_count, total_count

def download_taxi_zones():
    """Download and extract taxi zones data"""
    data_dir = Path("src_data")
    data_dir.mkdir(exist_ok=True)
    
    zip_path = data_dir / "taxi_zones.zip"
    csv_path = data_dir / "taxi_zone_lookup.csv"
    
    if csv_path.exists():
        logger.info("⏭️  Skipping taxi zones (already exists)")
        return True
    
    try:
        logger.info("Downloading taxi zones...")
        response = requests.get(DATA_URLS["taxi_zones"])
        response.raise_for_status()
        
        with open(zip_path, 'wb') as f:
            f.write(response.content)
        
        # Extract the CSV file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(data_dir)
        
        # Clean up zip file
        zip_path.unlink()
        
        logger.info("✅ Downloaded taxi zones")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to download taxi zones: {e}")
        return False

def main():
    """Main download function"""
    logger.info("🚕 Starting NYC Taxi Data Download")
    logger.info("=" * 50)
    
    # Download parquet data
    logger.info("📊 Downloading parquet data files...")
    success, total = download_parquet_data()
    
    # Download taxi zones
    logger.info("\n🗺️  Downloading taxi zones...")
    zones_success = download_taxi_zones()
    
    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("📋 Download Summary:")
    logger.info(f"  Parquet files: {success}/{total} successful")
    logger.info(f"  Taxi zones: {'✅' if zones_success else '❌'}")
    
    if success == total and zones_success:
        logger.info("\n🎉 All data downloaded successfully!")
        logger.info("You can now run the application with: python run_streamlit.py")
    else:
        logger.warning("\n⚠️  Some downloads failed. Check the logs above.")
        logger.info("You can retry by running this script again.")

if __name__ == "__main__":
    main()
