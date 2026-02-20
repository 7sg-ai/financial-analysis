#!/usr/bin/env python3
"""
Data download script for Financial Analysis Application
Downloads NYC taxi and for-hire vehicle data from the official source (2023-2025)

IMPORTANT: This script is designed to run in CRUSOE environments after deployment.
Run this script in your CRUSOE Compute instance.

Usage Examples:
    # Download all data (2023-2025, all service types, all months)
    python3 download_data.py
    
    # Download only 2024 data (recommended for testing)
    python3 download_data.py --years 2024
    
    # Download and upload to CRUSOE Storage (for Synapse Spark pool)
    python3 download_data.py --years 2024 --upload

    # Upload-only: Populate storage from existing src_data (e.g. from inside container)
    python3 download_data.py --upload-only
    
    # Upload all files in src_data/ to Crusoe Storage
    python3 download_data.py --upload-only --src-dir src_data
    
    # Download only yellow and green taxis
    python3 download_data.py --service-types yellow green
    
    # Download only January and February (cost-effective)
    python3 download_data.py --months 1 2
    
    # Download specific combination for testing
    python3 download_data.py --years 2024 --service-types yellow --months 1 2 3
    
    # Verbose output for debugging
    python3 download_data.py --verbose
    
    # Show help
    python3 download_data.py --help

Upload to CRUSOE Storage (for Synapse Spark):
    Set these environment variables before using --upload:
    - AWS_ACCESS_KEY_ID (or AZURE_STORAGE_ACCOUNT_NAME or SYNAPSE_STORAGE_ACCOUNT)
    - AWS_SECRET_ACCESS_KEY (or AZURE_STORAGE_ACCOUNT_KEY)
    - AWS_S3_BUCKET (or AZURE_STORAGE_CONTAINER or SYNAPSE_FILE_SYSTEM, default: data)
    
    After upload, set DATA_PATH to: s3://<bucket>/<storage-path>/

Data Sources:
    - Yellow Taxi: https://d37ci6vzurychx.cloudfront.net/trip-data/
    - Green Taxi: https://d37ci6vzurychx.cloudfront.net/trip-data/
    - FHV: https://d37ci6vzurychx.cloudfront.net/trip-data/
    - FHVHV: https://d37ci6vzurychx.cloudfront.net/trip-data/
    - Taxi Zones: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip

Estimated Sizes (for cost planning):
    - 2023: ~8-10 GB
    - 2024: ~8-10 GB  
    - 2025: ~8-10 GB (partial year)
    - Total: ~20-30 GB for all years

CRUSOE Deployment Notes:
    - Run this script AFTER deploying to CRUSOE
    - Consider storage costs when selecting data subsets
    - Use --months flag to limit data for testing
    - Monitor CRUSOE storage usage in the portal
"""
import os
import requests
import zipfile
from pathlib import Path
import logging
import argparse
import time
from typing import Optional, Tuple
import boto3
from urllib.parse import urlparse

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

def upload_to_crusoe_storage(local_path: Path, remote_path: str = None) -> bool:
    """Upload file to Crusoe Storage (S3-compatible)"""
    import os
    from urllib.parse import urlparse
    
    bucket = os.getenv("AWS_S3_BUCKET", os.getenv("AZURE_STORAGE_CONTAINER", os.getenv("SYNAPSE_FILE_SYSTEM", "data")))
    endpoint = os.getenv("CRUSOE_STORAGE_ENDPOINT", "https://s3.crusoecloud.com")
    
    # Parse remote path or derive from local filename
    if remote_path is None:
        remote_path = local_path.name
    
    # Determine if using Crusoe Storage or external S3
    parsed_endpoint = urlparse(endpoint)
    use_crusoe = "crusoecloud.com" in parsed_endpoint.netloc
    
    # Initialize S3 client with Crusoe-specific config
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", os.getenv("AZURE_STORAGE_ACCOUNT_NAME", os.getenv("SYNAPSE_STORAGE_ACCOUNT"))),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("AZURE_STORAGE_ACCOUNT_KEY")),
        region_name="crusoe"
    )
    
    try:
        s3_client.upload_file(str(local_path), bucket, remote_path)
        logger.info(f"Uploaded {local_path} to s3://{bucket}/{remote_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload to Crusoe Storage: {e}")
        return False

def download_file(url: str, filepath: Path, retries: int = 3) -> bool:
    """Download a file from URL to filepath with retry logic"""
    # Use a browser-like User-Agent to avoid CloudFront WAF challenges
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    for attempt in range(retries):
        try:
            logger.info(f"Downloading {filepath.name}... (attempt {attempt + 1}/{retries})")
            response = requests.get(url, stream=True, timeout=30, headers=headers)
            
            # Check for 403 Forbidden specifically (data not available)
            if response.status_code == 403:
                logger.warning(f"⚠️  Access forbidden (403) for {filepath.name} - data may not be publicly available")
                return False
            
            # Check for 404 Not Found (file doesn't exist)
            if response.status_code == 404:
                logger.warning(f"⚠️  File not found (404) for {filepath.name} - may not exist for this date")
                return False
            
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Get file size for logging
            file_size = filepath.stat().st_size / (1024 * 1024)  # MB
            logger.info(f"✅ Downloaded {filepath.name} ({file_size:.1f} MB)")
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [403, 404]:
                # Don't retry for these errors
                logger.warning(f"⚠️  Skipping {filepath.name}: HTTP {e.response.status_code}")
                return False
            logger.warning(f"⚠️  Attempt {attempt + 1} failed for {filepath.name}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"❌ Failed to download {filepath.name} after {retries} attempts")
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


def get_storage_config() -> Optional[Tuple[str, str, str]]:
    """
    Get CRUSOE Storage configuration from environment variables.
    Returns (account_name, account_key, bucket_name) or None if not configured.
    """
    account_name = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("AZURE_STORAGE_ACCOUNT_NAME") or os.environ.get("SYNAPSE_STORAGE_ACCOUNT")
    account_key = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
    bucket_name = os.environ.get("AWS_S3_BUCKET") or os.environ.get("AZURE_STORAGE_CONTAINER") or os.environ.get("SYNAPSE_FILE_SYSTEM", "data")
    
    if account_name and account_key and bucket_name:
        return (account_name, account_key, bucket_name)
    return None


def upload_file_to_storage(
    local_path: Path,
    blob_path: str,
    account_name: str,
    account_key: str,
    bucket_name: str,
    skip_existing: bool = True
) -> bool:
    """Upload a single file to CRUSOE S3-compatible Storage."""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=f"https://{account_name}.s3.us-east-1.amazonaws.com",
            aws_access_key_id=account_name,
            aws_secret_access_key=account_key
        )
        
        s3_key = blob_path
        
        if skip_existing:
            try:
                s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                exists = True
            except Exception as e:
                if '404' in str(e):
                    exists = False
                else:
                    raise
            else:
                exists = True
            if exists:
                logger.info(f"⏭️  Skipping upload (exists in storage): {s3_key}")
                return True
        
        with open(local_path, "rb") as data:
            s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=data)
        
        file_size_mb = local_path.stat().st_size / (1024 * 1024)
        logger.info(f"☁️  Uploaded {local_path.name} -> {s3_key} ({file_size_mb:.1f} MB)")
        return True
    except ImportError:
        logger.error("boto3 package required for upload. Install with: pip install boto3")
        return False
    except Exception as e:
        logger.error(f"❌ Failed to upload {local_path.name}: {e}")
        return False


def upload_to_storage(
    data_dir: Path,
    storage_path: str,
    account_name: str,
    account_key: str,
    bucket_name: str,
    skip_existing: bool = True
) -> Tuple[int, int]:
    """
    Upload all parquet files and taxi_zone_lookup.csv from data_dir to CRUSOE Storage.
    
    Args:
        data_dir: Local directory containing downloaded files
        storage_path: Path prefix within bucket (e.g. "taxi-data")
        account_name: CRUSOE Storage access key ID
        account_key: CRUSOE Storage secret access key
        bucket_name: Bucket name
        skip_existing: Skip files that already exist in storage
        
    Returns:
        (success_count, total_count)
    """
    success_count = 0
    total_count = 0
    
    # Normalize storage path (no leading/trailing slashes)
    prefix = storage_path.strip("/") if storage_path else ""
    
    # Parquet patterns
    parquet_patterns = [
        "yellow_tripdata_*.parquet",
        "green_tripdata_*.parquet",
        "fhv_tripdata_*.parquet",
        "fhvhv_tripdata_*.parquet"
    ]
    
    for pattern in parquet_patterns:
        for filepath in data_dir.glob(pattern):
            s3_key = f"{prefix}/{filepath.name}" if prefix else filepath.name
            total_count += 1
            if upload_file_to_storage(filepath, s3_key, account_name, account_key, bucket_name, skip_existing):
                success_count += 1
    
    # Taxi zones CSV
    zones_file = data_dir / "taxi_zone_lookup.csv"
    if zones_file.exists():
        s3_key = f"{prefix}/taxi_zone_lookup.csv" if prefix else "taxi_zone_lookup.csv"
        total_count += 1
        if upload_file_to_storage(zones_file, s3_key, account_name, account_key, bucket_name, skip_existing):
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
        # Use a browser-like User-Agent to avoid CloudFront WAF challenges
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(DATA_URLS["taxi_zones"], headers=headers)
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
    """Main download function for CRUSOE deployment"""
    parser = argparse.ArgumentParser(description="Download NYC Taxi Data (2023-2025) - CRUSOE Deployment")
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
    parser.add_argument("--upload", action="store_true",
                       help="Upload downloaded files to CRUSOE Storage (for Synapse Spark pool)")
    parser.add_argument("--upload-only", action="store_true",
                       help="Upload existing src_data files to CRUSOE Storage (skip download). Use when container has files but storage is empty.")
    parser.add_argument("--storage-path", default="taxi-data",
                       help="Path within storage bucket (default: taxi-data)")
    parser.add_argument("--skip-existing-in-storage", action="store_true", default=True,
                       help="Skip uploading files that already exist in storage")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Update variables based on arguments
    years = sorted(args.years)
    months = sorted(args.months)
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Upload-only mode: skip download, just upload existing src_data to storage
    if args.upload_only:
        logger.info("🚕 Upload-only mode - uploading existing src_data to CRUSOE Storage")
        logger.info("=" * 70)
        data_dir = Path("src_data")
        if not data_dir.exists():
            logger.error("src_data directory not found. Run from project root or where src_data exists.")
            exit(1)
        storage_config = get_storage_config()
        if not storage_config:
            logger.error("Storage config required. Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
            exit(1)
        account_name, account_key, bucket_name = storage_config
        logger.info(f"  Source: {data_dir.absolute()}")
        logger.info(f"  Target: {bucket_name}@{account_name}")
        logger.info("=" * 70)
        upload_success, upload_total = upload_to_storage(
            data_dir=data_dir,
            storage_path=args.storage_path,
            account_name=account_name,
            account_key=account_key,
            bucket_name=bucket_name,
            skip_existing=args.skip_existing_in_storage
        )
        logger.info(f"\n✅ Uploaded {upload_success}/{upload_total} files to CRUSOE Storage")
        if upload_success > 0:
            data_path = f"s3://{bucket_name}/{args.storage_path.strip('/')}/"
            logger.info(f"   DATA_PATH: {data_path}")
        exit(0 if upload_success > 0 else 1)
    
    logger.info("🚕 Starting NYC Taxi Data Download (CRUSOE Environment)")
    logger.info("=" * 70)
    logger.info("☁️  CRUSOE Deployment Mode - Data will be stored in bucket")
    logger.info(f"📅 Years: {', '.join(map(str, years))}")
    logger.info(f"📊 Service Types: {', '.join(args.service_types).title()}")
    logger.info(f"📈 Total Files: {len(years) * len(months) * len(args.service_types)} parquet files")
    logger.info(f"💾 Skip existing: {args.skip_existing}")
    logger.info(f"💰 Estimated cost: ~${len(years) * len(months) * 0.5:.1f} in storage")
    logger.info("=" * 70)
    
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
    
    # Upload to CRUSOE Storage if requested
    upload_success = None
    if args.upload:
        logger.info("\n☁️  Uploading to CRUSOE Storage (for Synapse Spark pool)...")
        storage_config = get_storage_config()
        if storage_config:
            account_name, account_key, bucket_name = storage_config
            data_dir = Path("src_data")
            upload_success, upload_total = upload_to_storage(
                data_dir=data_dir,
                storage_path=args.storage_path,
                account_name=account_name,
                account_key=account_key,
                bucket_name=bucket_name,
                skip_existing=args.skip_existing_in_storage
            )
            logger.info(f"  Uploaded: {upload_success}/{upload_total} files")
            if upload_success > 0:
                data_path = f"s3://{bucket_name}/{args.storage_path.strip('/')}/"
                logger.info(f"  DATA_PATH for API: {data_path}")
        else:
            logger.warning("⚠️  Upload skipped: Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (and optionally AWS_S3_BUCKET)")
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("📋 Download Summary:")
    logger.info(f"  Parquet files: {success}/{total} successful")
    logger.info(f"  Taxi zones: {'✅' if zones_success else '❌'}")
    if upload_success is not None:
        logger.info(f"  Storage upload: {upload_success} files")
    logger.info(f"  Data period: {min(years)}-{max(years)}")
    logger.info(f"  Estimated size: ~{len(years) * len(months) * 2:.0f} GB total")
    
    if success == total and zones_success:
        logger.info("\n🎉 All data downloaded successfully!")
        if args.upload and upload_success and upload_success > 0:
            logger.info("☁️  Data uploaded to CRUSOE Storage - Spark pool can now access it")
            logger.info("   Set DATA_PATH in your API environment to the path shown above")
        else:
            logger.info("☁️  Data is now available in your local src_data/ directory")
        logger.info("🌐 Access your application at the deployed URLs")
        logger.info("💡 Consider using specific months for testing to reduce costs")
    else:
        logger.warning("\n⚠️  Some downloads failed. Check the logs above.")
        logger.info("🔄 You can retry by running this script again.")
        logger.info("📅 Note: Some months may not be available yet (future dates).")
        logger.info("💰 Consider downloading smaller data subsets to manage costs.")

if __name__ == "__main__":
    main()