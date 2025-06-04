#!/usr/bin/env python
"""
AROME Data Processing Pipeline - All Files

This script processes multiple AROME data files in batch, with support for:
- Multiple pressure levels (500-900 hPa)
- Multiple hours (5-21 UTC)
- Multiple forecast days
- Parallel processing
- Incremental GitHub releases

Usage: python process_all.py --output-dir /path/to/output --parallel 2
"""

import os
import argparse
import logging
import time
import concurrent.futures
from datetime import datetime, timezone, timedelta
import glob
import sys
import tempfile
import shutil
import subprocess
import json

# Add script directory to path to allow imports to work in GitHub Actions
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import our clean modules
from fetch import fetch_vertical_velocity_tiff
from warp import warp_geotiff
from geotiff2mbtiles import geotiff_to_mbtiles

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('process-all')

# GitHub Release Management Functions
def create_github_release(tag_name, title):
    """Create a GitHub release immediately (published, not draft)"""
    try:
        cmd = [
            'gh', 'release', 'create', tag_name,
            '--title', title,
            '--notes', ''  # Empty notes as requested
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info(f"✅ Created GitHub release: {tag_name}")
        return True
    except subprocess.CalledProcessError as e:
        if "already exists" in e.stderr:
            logger.info(f"📋 Release {tag_name} already exists, continuing...")
            return True
        else:
            logger.error(f"❌ Failed to create release {tag_name}: {e.stderr}")
            return False
    except Exception as e:
        logger.error(f"❌ Error creating release: {e}")
        return False

def check_file_exists_in_release(tag_name, filename):
    """Check if a file already exists in the GitHub release"""
    try:
        cmd = ['gh', 'release', 'view', tag_name, '--json', 'assets']
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        release_data = json.loads(result.stdout)
        
        existing_files = [asset['name'] for asset in release_data.get('assets', [])]
        exists = filename in existing_files
        
        if exists:
            logger.info(f"⏭️  File {filename} already exists in release, skipping...")
        
        return exists
    except subprocess.CalledProcessError as e:
        logger.warning(f"⚠️  Could not check release assets: {e.stderr}")
        return False
    except Exception as e:
        logger.warning(f"⚠️  Error checking release assets: {e}")
        return False

def upload_file_to_release(tag_name, file_path):
    """Upload a single file to the GitHub release"""
    try:
        cmd = ['gh', 'release', 'upload', tag_name, file_path]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        filename = os.path.basename(file_path)
        logger.info(f"📤 Uploaded {filename} to release {tag_name}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed to upload {file_path}: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"❌ Error uploading file: {e}")
        return False

def process_single_file(hour, pressure, output_dir, min_zoom=4, max_zoom=8, skip_existing=True, force=False, forecast_days=0, release_tag=None):
    """
    Process a single AROME data file for a specific hour and pressure level
    
    Parameters:
    -----------
    hour : int
        Hour of the day (7-21)
    pressure : int
        Pressure level in hPa (500-900)
    output_dir : str
        Directory to save output files
    min_zoom : int
        Minimum zoom level for MBTiles
    max_zoom : int
        Maximum zoom level for MBTiles
    skip_existing : bool
        Whether to skip existing files
    force : bool
        Force regeneration of all files
    forecast_days : int
        Number of days ahead to forecast (0 for today, 1 for tomorrow)
    release_tag : str, optional
        GitHub release tag to upload to (if provided)
    
    Returns:
    --------
    bool
        True if successful, False otherwise
    """
    # Format hour as two digits with leading zero
    hour_str = f"{hour:02d}"
    
    # Get today's date in UTC (source date)
    source_date = datetime.now(timezone.utc)
    source_date_str = source_date.strftime("%Y-%m-%d")
    
    # Calculate target date (today + forecast_days)
    target_date = source_date + timedelta(days=forecast_days)
    target_date_str = target_date.strftime("%Y-%m-%d")
    
    # Construct time values
    time_value = f"{target_date_str}T{hour_str}:00:00Z"
    ref_time_value = f"{source_date_str}T00:00:00Z"
    
    # Define file paths - include both source date and target date in filename
    pressure_str = str(pressure)
    file_base = f"arome_vv_{source_date_str}_{target_date_str}_{hour_str}_{pressure_str}"
    tiff_path = os.path.join(output_dir, f"{file_base}.tiff")
    warped_tiff_path = os.path.join(output_dir, f"{file_base}_mercator.tiff")
    mbtiles_path = os.path.join(output_dir, f"{file_base}.mbtiles")
    mbtiles_filename = f"{file_base}.mbtiles"
    
    # Check if file already exists in GitHub release (skip processing entirely)
    if release_tag and check_file_exists_in_release(release_tag, mbtiles_filename):
        return True  # Skip processing, file already exists in release
    
    def cleanup_intermediate_file(file_path, description):
        """Helper function to safely delete intermediate files"""
        try:
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                os.remove(file_path)
                logger.info(f"🗑️  Cleaned up {description}: {file_path} ({file_size / 1024 / 1024:.1f} MB freed)")
        except Exception as e:
            logger.warning(f"Failed to cleanup {description} {file_path}: {e}")
    
    try:
        # Step 1: Fetch TIFF
        if os.path.exists(tiff_path) and os.path.getsize(tiff_path) > 1000000 and skip_existing and not force:
            fetch_success = True
        else:
            fetch_success = fetch_vertical_velocity_tiff(
                time_value=time_value,
                ref_time_value=ref_time_value,
                pressure_value=pressure_str,
                output_file=tiff_path
            )
        
        if not fetch_success:
            logger.error(f"Failed to fetch TIFF data for target date {target_date_str}, hour {hour_str}:00, pressure {pressure} hPa")
            cleanup_intermediate_file(tiff_path, "failed TIFF")
            return False
        
        # Step 2: Warp TIFF to Web Mercator
        if os.path.exists(warped_tiff_path) and os.path.getsize(warped_tiff_path) > 1000000 and skip_existing and not force:
            warp_success = True
        else:
            warp_success = warp_geotiff(tiff_path, warped_tiff_path)
        
        if not warp_success:
            logger.error(f"Failed to warp TIFF data for target date {target_date_str}, hour {hour_str}:00, pressure {pressure} hPa")
            cleanup_intermediate_file(tiff_path, "original TIFF")
            cleanup_intermediate_file(warped_tiff_path, "failed warped TIFF")
            return False
        
        # Clean up original TIFF after successful warping (we only need the warped version)
        cleanup_intermediate_file(tiff_path, "original TIFF")
        
        # Step 3: Convert to MBTiles
        if os.path.exists(mbtiles_path) and os.path.getsize(mbtiles_path) > 1000000 and skip_existing and not force:
            mbtiles_success = True
        else:
            try:
                geotiff_to_mbtiles(warped_tiff_path, mbtiles_path, min_zoom=min_zoom, max_zoom=max_zoom)
                mbtiles_success = True
            except Exception as e:
                logger.error(f"Error creating MBTiles: {str(e)}")
                mbtiles_success = False
        
        if not mbtiles_success:
            logger.error(f"Failed to create MBTiles for target date {target_date_str}, hour {hour_str}:00, pressure {pressure} hPa")
            cleanup_intermediate_file(warped_tiff_path, "warped TIFF")
            cleanup_intermediate_file(mbtiles_path, "failed MBTiles")
            return False
        
        # Clean up warped TIFF after successful MBTiles creation (we only need the final MBTiles)
        cleanup_intermediate_file(warped_tiff_path, "warped TIFF")
        
        # Step 4: Upload to GitHub release if specified
        if release_tag and os.path.exists(mbtiles_path):
            upload_success = upload_file_to_release(release_tag, mbtiles_path)
            if not upload_success:
                logger.warning(f"⚠️  Failed to upload {mbtiles_filename} to release, but processing was successful")
        
        logger.info(f"✅ Successfully processed: Target date {target_date_str}, Hour {hour_str}:00, Pressure {pressure} hPa")
        return True
    
    except Exception as e:
        logger.error(f"Error processing target date {target_date_str}, hour {hour_str}:00, pressure {pressure} hPa: {str(e)}")
        # Clean up any intermediate files on error
        cleanup_intermediate_file(tiff_path, "original TIFF")
        cleanup_intermediate_file(warped_tiff_path, "warped TIFF")
        return False

def cleanup_temp_files():
    """Delete temporary log and information files created during processing"""
    try:
        # Find and remove geotiff2mbtiles.log
        log_files = glob.glob("*.log")
        for log_file in log_files:
            if log_file != "arome_process_all.log":  # Keep the main log
                if os.path.exists(log_file):
                    os.remove(log_file)
                    logger.info(f"Removed temporary log file: {log_file}")
        
        # Find and remove any bounds.txt files in output directories
        bounds_files = glob.glob("**/bounds.txt", recursive=True)
        for bounds_file in bounds_files:
            if os.path.exists(bounds_file):
                os.remove(bounds_file)
                logger.info(f"Removed bounds file: {bounds_file}")
                
        # Find and remove README.txt and viewer.html files
        info_files = []
        info_files.extend(glob.glob("**/README.txt", recursive=True))
        info_files.extend(glob.glob("**/viewer.html", recursive=True))
        
        for info_file in info_files:
            if os.path.exists(info_file):
                os.remove(info_file)
                logger.debug(f"Removed information file: {info_file}")
    except Exception as e:
        logger.error(f"Error cleaning up temporary files: {str(e)}")

def process_all(output_dir, min_zoom=4, max_zoom=8, parallel=2, skip_existing=True, force=False, forecast_days=[0, 1], release_tag=None):
    """
    Process all AROME data files for hours 7-21 and pressure levels 500-900
    
    Parameters:
    -----------
    output_dir : str
        Directory to save output files
    min_zoom : int
        Minimum zoom level for MBTiles
    max_zoom : int
        Maximum zoom level for MBTiles
    parallel : int
        Number of parallel processes to use (0 for sequential)
    skip_existing : bool
        Whether to skip existing files
    force : bool
        Force regeneration of all files
    forecast_days : list
        Days to forecast (0 for today, 1 for tomorrow)
    release_tag : str, optional
        GitHub release tag to create and upload to
    
    Returns:
    --------
    tuple
        (total_files, successful_files)
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create GitHub release if specified
    if release_tag:
        source_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        release_title = f"AROME Data {source_date}"
        if not create_github_release(release_tag, release_title):
            logger.warning("⚠️  Failed to create GitHub release, continuing without release uploads")
            release_tag = None  # Disable release uploads
    
    # Define ranges
    hours = range(5, 20)  # 5 to 21 inclusive
    pressure_levels = [500, 600, 700, 800, 900]
    
    # Create tasks list for each forecast day
    tasks = []
    for day in forecast_days:
        tasks.extend([(hour, pressure, day) for hour in hours for pressure in pressure_levels])
    
    # Calculate total files
    total_files = len(tasks)
    
    logger.info(f"=== Starting batch processing of {total_files} AROME data files ===")
    logger.info(f"Processing forecast days: {', '.join([str(d) for d in forecast_days])}")
    if release_tag:
        logger.info(f"📦 Files will be uploaded to GitHub release: {release_tag}")
    
    # Start timing
    start_time = time.time()
    
    successful_files = 0
    
    # Process files
    if parallel > 0:
        # Parallel processing
        with concurrent.futures.ProcessPoolExecutor(max_workers=parallel) as executor:
            futures = {
                executor.submit(
                    process_single_file, 
                    hour, 
                    pressure, 
                    output_dir, 
                    min_zoom, 
                    max_zoom, 
                    skip_existing,
                    force,
                    day,
                    release_tag
                ): (hour, pressure, day) for hour, pressure, day in tasks
            }
            
            for future in concurrent.futures.as_completed(futures):
                hour, pressure, day = futures[future]
                try:
                    if future.result():
                        successful_files += 1
                except Exception as e:
                    source_date = datetime.now(timezone.utc)
                    target_date = source_date + timedelta(days=day)
                    target_date_str = target_date.strftime("%Y-%m-%d")
                    logger.error(f"Error processing target date {target_date_str}, hour {hour:02d}:00, pressure {pressure} hPa: {str(e)}")
    else:
        # Sequential processing
        for hour, pressure, day in tasks:
            if process_single_file(hour, pressure, output_dir, min_zoom, max_zoom, skip_existing, force, day, release_tag):
                successful_files += 1
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    
    # Clean up temporary files
    cleanup_temp_files()
    
    # Report results
    logger.info(f"=== Batch processing completed ===")
    logger.info(f"Total files: {total_files}")
    logger.info(f"Successfully processed: {successful_files}")
    logger.info(f"Failed: {total_files - successful_files}")
    if release_tag:
        logger.info(f"📦 Files uploaded to release: {release_tag}")
    logger.debug(f"Total elapsed time: {elapsed_time:.2f} seconds")
    
    return total_files, successful_files

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="AROME Data Processing Pipeline - Batch Processor",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument("--output-dir", default="./output",
                        help="Directory to save output files")
    parser.add_argument("--min-zoom", type=int, default=4,
                        help="Minimum zoom level for MBTiles")
    parser.add_argument("--max-zoom", type=int, default=8,
                        help="Maximum zoom level for MBTiles")
    parser.add_argument("--parallel", type=int, default=2,
                        help="Number of parallel processes to use (0 for sequential)")
    parser.add_argument("--force", action="store_true",
                        help="Force regeneration of existing files")
    parser.add_argument("--no-skip", action="store_true",
                        help="Don't skip existing files")
    parser.add_argument("--forecast-days", type=int, nargs="+", default=[0, 1],
                        help="Days to forecast (0 for today, 1 for tomorrow, etc.)")
    parser.add_argument("--release-tag", type=str,
                        help="GitHub release tag to create and upload files to")
    
    args = parser.parse_args()
    
    # Run the batch processor
    total, successful = process_all(
        output_dir=args.output_dir,
        min_zoom=args.min_zoom,
        max_zoom=args.max_zoom,
        parallel=args.parallel,
        skip_existing=not args.no_skip,
        force=args.force,
        forecast_days=args.forecast_days,
        release_tag=args.release_tag
    )
    
    # Exit with appropriate code
    if successful == total:
        logger.info("All files processed successfully!")
        exit(0)
    else:
        logger.error(f"Some files failed to process ({total - successful} of {total})")
        exit(1) 