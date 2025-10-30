#!/usr/bin/env python3
"""
Data download script for Financial Analysis Application
Downloads NYC taxi and for-hire vehicle data from the official source (2023-2025)

Usage Examples:
    # Download all data (2023-2025, all service types, all months)
    python download_data.py
    
    # Download only 2024 data
    python download_data.py --years 2024
    
    # Download only yellow and green taxis
    python download_data.py --service-types yellow green
    
    # Download only January and February
    python download_data.py --months 1 2
    
    # Download specific combination
    python download_data.py --years 2024 --service-types yellow --months 1 2 3
    
    # Verbose output
    python download_data.py --verbose
    
    # Show help
    python download_data.py --help

Data Sources:
    - Yellow Taxi: https://d37ci6vzurychx.cloudfront.net/trip-data/
    - Green Taxi: https://d37ci6vzurychx.cloudfront.net/trip-data/
    - FHV: https://d37ci6vzurychx.cloudfront.net/trip-data/
    - FHVHV: https://d37ci6vzurychx.cloudfront.net/trip-data/
    - Taxi Zones: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip

Estimated Sizes:
    - 2023: ~8-10 GB
    - 2024: ~8-10 GB  
    - 2025: ~8-10 GB (partial year)
    - Total: ~20-30 GB for all years
"""
import os
import requests
import zipfile
from pathlib import Path
import logging
import argparse
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Data URLs for NYC TLC data (2023-2025)
DATA_URLS = {
    "yellow": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet",
    "green": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet", 
    "fhv": "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{month:02d}.parquet",
    "fhvhv": "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month:02d}.parquet",
    "taxi_zones": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
}

# Years and months to download
YEARS = [2023, 2024, 2025]
MONTHS = list(range(1, 13))  # 1-12

def download_file(url: str, filepath: Path, retries: int = 3) -> bool:
    """Download a file from URL to filepath with retry logic"""
    for attempt in range(retries):
        try:
            logger.info(f"Downloading {filepath.name}... (attempt {attempt + 1}/{retries})")
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Get file size for logging
            file_size = filepath.stat().st_size / (1024 * 1024)  # MB
            logger.info(f"✅ Downloaded {filepath.name} ({file_size:.1f} MB)")
            return True
        except Exception as e:
            logger.warning(f"⚠️  Attempt {attempt + 1} failed for {filepath.name}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"❌ Failed to download {filepath.name} after {retries} attempts")
                return False

def download_parquet_data(service_types=None, years=None, months=None, skip_existing=True):
    """Download all parquet data files for specified years and service types"""
    data_dir = Path("src_data")
    data_dir.mkdir(exist_ok=True)
    
    if service_types is None:
        service_types = ["yellow", "green", "fhv", "fhvhv"]
    if years is None:
        years = YEARS
    if months is None:
        months = MONTHS
    
    success_count = 0
    total_count = 0
    
    # Download monthly data for each service type and year
    for service_type in service_types:
        if service_type not in DATA_URLS:
            logger.warning(f"Unknown service type: {service_type}")
            continue
            
        base_url = DATA_URLS[service_type]
        
        for year in years:
            for month in months:
                url = base_url.format(year=year, month=month)
                filename = f"{service_type}_tripdata_{year}-{month:02d}.parquet"
                filepath = data_dir / filename
                
                if skip_existing and filepath.exists():
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
    parser = argparse.ArgumentParser(description="Download NYC Taxi Data (2023-2025)")
    parser.add_argument("--years", nargs="+", type=int, default=YEARS, 
                       help="Years to download (default: 2023 2024 2025)")
    parser.add_argument("--service-types", nargs="+", 
                       choices=["yellow", "green", "fhv", "fhvhv"], 
                       default=["yellow", "green", "fhv", "fhvhv"],
                       help="Service types to download")
    parser.add_argument("--months", nargs="+", type=int, default=MONTHS,
                       help="Months to download (1-12, default: all)")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                       help="Skip files that already exist")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Update variables based on arguments
    years = sorted(args.years)
    months = sorted(args.months)
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("🚕 Starting NYC Taxi Data Download")
    logger.info("=" * 60)
    logger.info(f"📅 Years: {', '.join(map(str, years))}")
    logger.info(f"📊 Service Types: {', '.join(args.service_types).title()}")
    logger.info(f"📈 Total Files: {len(years) * len(months) * len(args.service_types)} parquet files")
    logger.info(f"💾 Skip existing: {args.skip_existing}")
    logger.info("=" * 60)
    
    # Download parquet data
    logger.info("📊 Downloading parquet data files...")
    success, total = download_parquet_data(
        service_types=args.service_types, 
        years=years, 
        months=months, 
        skip_existing=args.skip_existing
    )
    
    # Download taxi zones
    logger.info("\n🗺️  Downloading taxi zones...")
    zones_success = download_taxi_zones()
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("📋 Download Summary:")
    logger.info(f"  Parquet files: {success}/{total} successful")
    logger.info(f"  Taxi zones: {'✅' if zones_success else '❌'}")
    logger.info(f"  Data period: {min(years)}-{max(years)}")
    logger.info(f"  Estimated size: ~{len(years) * len(months) * 2:.0f} GB total")
    
    if success == total and zones_success:
        logger.info("\n🎉 All data downloaded successfully!")
        logger.info("You can now run the application with: python run_streamlit.py")
        logger.info("Note: This is a large dataset. Consider using specific months for testing.")
    else:
        logger.warning("\n⚠️  Some downloads failed. Check the logs above.")
        logger.info("You can retry by running this script again.")
        logger.info("Note: Some months may not be available yet (future dates).")

if __name__ == "__main__":
    main()
