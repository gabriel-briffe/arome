#!/usr/bin/env python
"""
AROME Data Processing Pipeline

This script coordinates the entire pipeline:
1. Fetch vertical velocity TIFF from AROME API
2. Warp to Web Mercator (EPSG:3857)
3. Convert to MBTiles for visualization

Usage: python test_process.py --pressure 850 --hour 12 --output-dir /path/to/output
"""

import os
import argparse
import logging
import time
from datetime import datetime, timezone
import glob
import sys

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
logger = logging.getLogger('process-pipeline')

def process_arome_data(pressure, hour, output_dir, min_zoom=4, max_zoom=8, skip_existing=True):
    """
    Run the full AROME data processing pipeline
    
    Parameters:
    -----------
    pressure : str
        Pressure level in hPa (e.g., "850")
    hour : str
        Hour of the day (e.g., "12")
    output_dir : str
        Directory to save output files
    min_zoom : int
        Minimum zoom level for MBTiles (default: 4)
    max_zoom : int
        Maximum zoom level for MBTiles (default: 8)
    skip_existing : bool
        Whether to skip existing files (default: True)
    
    Returns:
    --------
    bool
        True if the pipeline completed successfully, False otherwise
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get today's date in UTC
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Construct time values
    time_value = f"{today}T{hour}:00:00Z"
    ref_time_value = f"{today}T00:00:00Z"
    
    # Define file paths
    tiff_path = os.path.join(output_dir, f"arome_vv_{today}_{hour}_{pressure}.tiff")
    warped_tiff_path = os.path.join(output_dir, f"arome_vv_{today}_{hour}_{pressure}_mercator.tiff")
    mbtiles_path = os.path.join(output_dir, f"arome_vv_{today}_{hour}_{pressure}.mbtiles")
    
    # Start timing
    start_time = time.time()
    
    logger.info("=== Starting AROME Data Processing Pipeline ===")
    logger.info(f"Processing: {pressure} hPa, Hour: {hour}")
    
    # Step 1: Fetch TIFF
    if os.path.exists(tiff_path) and os.path.getsize(tiff_path) > 1000000 and skip_existing:
        logger.info(f"Using existing TIFF file: {tiff_path}")
        fetch_success = True
    else:
        fetch_success = fetch_vertical_velocity_tiff(
            time_value=time_value,
            ref_time_value=ref_time_value,
            pressure_value=pressure,
            output_file=tiff_path
        )
    
    if not fetch_success:
        logger.error("Failed to fetch TIFF data. Aborting pipeline.")
        return False
    
    # Step 2: Warp TIFF to Web Mercator
    if os.path.exists(warped_tiff_path) and os.path.getsize(warped_tiff_path) > 1000000 and skip_existing:
        logger.info(f"Using existing warped TIFF file: {warped_tiff_path}")
        warp_success = True
    else:
        logger.info("Warping TIFF to Web Mercator...")
        warp_success = warp_geotiff(tiff_path, warped_tiff_path)
    
    if not warp_success:
        logger.error("Failed to warp TIFF data. Aborting pipeline.")
        return False
    
    # Step 3: Convert to MBTiles
    if os.path.exists(mbtiles_path) and os.path.getsize(mbtiles_path) > 1000000 and skip_existing:
        logger.info(f"Using existing MBTiles file: {mbtiles_path}")
        mbtiles_success = True
    else:
        logger.info("Converting to MBTiles...")
        try:
            geotiff_to_mbtiles(warped_tiff_path, mbtiles_path, min_zoom=min_zoom, max_zoom=max_zoom)
            mbtiles_success = True
        except Exception as e:
            logger.error(f"Error creating MBTiles: {str(e)}")
            mbtiles_success = False
    
    if not mbtiles_success:
        logger.error("Failed to create MBTiles. Aborting pipeline.")
        return False
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    
    # Pipeline success
    logger.info("=== Pipeline Completed Successfully ===")
    logger.info(f"Elapsed Time: {elapsed_time:.2f} seconds")
    logger.info(f"Output: {mbtiles_path}")
    
    # Clean up any temporary files
    cleanup_temp_files()
    
    return True

def cleanup_temp_files():
    """Remove any temporary files created during processing"""
    try:
        # Find and remove log files
        log_files = glob.glob("*.log")
        for log_file in log_files:
            if os.path.exists(log_file):
                os.remove(log_file)
                logger.debug(f"Removed temporary log file: {log_file}")
        
        # Find and remove generated HTML, README, and bounds files
        temp_patterns = ["**/bounds.txt", "**/README.txt", "**/viewer.html"]
        for pattern in temp_patterns:
            files = glob.glob(pattern, recursive=True)
            for file_path in files:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.debug(f"Removed temporary file: {file_path}")
    except Exception as e:
        logger.error(f"Error cleaning up temp files: {e}")

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="AROME Data Processing Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument("--pressure", default="850",
                        help="Pressure level in hPa")
    parser.add_argument("--hour", default="12",
                        help="Hour of the day (00-23)")
    parser.add_argument("--output-dir", default="./output",
                        help="Directory to save output files")
    parser.add_argument("--min-zoom", type=int, default=4,
                        help="Minimum zoom level for MBTiles")
    parser.add_argument("--max-zoom", type=int, default=8,
                        help="Maximum zoom level for MBTiles")
    parser.add_argument("--force", action="store_true",
                        help="Force regeneration of existing files")
    
    args = parser.parse_args()
    
    # Run the pipeline
    success = process_arome_data(
        pressure=args.pressure,
        hour=args.hour,
        output_dir=args.output_dir,
        min_zoom=args.min_zoom,
        max_zoom=args.max_zoom,
        skip_existing=not args.force
    )
    
    if success:
        exit(0)
    else:
        logger.error("Pipeline encountered errors. Check the logs for details.")
        exit(1) 