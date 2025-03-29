#!/usr/bin/env python
"""
AROME Data Processing Pipeline - Batch Processor

This script processes multiple AROME data files by iterating through:
- Hours: 07:00 to 21:00 (hourly)
- Pressure levels: 500, 600, 700, 800, 900 hPa

It fetches, warps, and converts each file to MBTiles format.
"""

import os
import argparse
import logging
import time
import concurrent.futures
from datetime import datetime
import glob
import sys

# Add script directory to path to allow imports to work in GitHub Actions
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import our modules
from fetch import fetch_vertical_velocity_tiff
from warp import warp_geotiff
from geotiff2mbtiles import geotiff_to_mbtiles

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("arome_process_all.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('process-all')

def process_single_file(hour, pressure, output_dir, min_zoom=4, max_zoom=10, skip_existing=True, force=False):
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
    
    Returns:
    --------
    bool
        True if successful, False otherwise
    """
    # Format hour as two digits with leading zero
    hour_str = f"{hour:02d}"
    
    # Get today's date
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Construct time values
    time_value = f"{today}T{hour_str}:00:00Z"
    ref_time_value = f"{today}T00:00:00Z"
    
    # Define file paths
    pressure_str = str(pressure)
    file_base = f"arome_vv_{today}_{hour_str}_{pressure_str}"
    tiff_path = os.path.join(output_dir, f"{file_base}.tiff")
    warped_tiff_path = os.path.join(output_dir, f"{file_base}_mercator.tiff")
    mbtiles_path = os.path.join(output_dir, f"{file_base}.mbtiles")
    
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
            logger.error(f"Failed to fetch TIFF data for hour {hour_str}:00, pressure {pressure} hPa")
            return False
        
        # Step 2: Warp TIFF to Web Mercator
        if os.path.exists(warped_tiff_path) and os.path.getsize(warped_tiff_path) > 1000000 and skip_existing and not force:
            warp_success = True
        else:
            warp_success = warp_geotiff(tiff_path, warped_tiff_path)
        
        if not warp_success:
            logger.error(f"Failed to warp TIFF data for hour {hour_str}:00, pressure {pressure} hPa")
            return False
        
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
            logger.error(f"Failed to create MBTiles for hour {hour_str}:00, pressure {pressure} hPa")
            return False
        
        logger.info(f"✅ Successfully processed: Hour {hour_str}:00, Pressure {pressure} hPa")
        return True
    
    except Exception as e:
        logger.error(f"Error processing hour {hour_str}:00, pressure {pressure} hPa: {str(e)}")
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
                logger.info(f"Removed information file: {info_file}")
    except Exception as e:
        logger.error(f"Error cleaning up temporary files: {str(e)}")

def process_all(output_dir, min_zoom=4, max_zoom=10, parallel=2, skip_existing=True, force=False):
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
    
    Returns:
    --------
    tuple
        (total_files, successful_files)
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Define ranges
    hours = range(7, 22)  # 7 to 21 inclusive
    pressure_levels = [500, 600, 700, 800, 900]
    
    # Calculate total files
    total_files = len(hours) * len(pressure_levels)
    
    logger.info(f"=== Starting batch processing of {total_files} AROME data files ===")
    
    # Start timing
    start_time = time.time()
    
    # Create tasks list
    tasks = [(hour, pressure) for hour in hours for pressure in pressure_levels]
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
                    force
                ): (hour, pressure) for hour, pressure in tasks
            }
            
            for future in concurrent.futures.as_completed(futures):
                hour, pressure = futures[future]
                try:
                    if future.result():
                        successful_files += 1
                except Exception as e:
                    logger.error(f"Error processing hour {hour:02d}:00, pressure {pressure} hPa: {str(e)}")
    else:
        # Sequential processing
        for hour, pressure in tasks:
            if process_single_file(hour, pressure, output_dir, min_zoom, max_zoom, skip_existing, force):
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
    logger.info(f"Total elapsed time: {elapsed_time:.2f} seconds")
    
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
    parser.add_argument("--max-zoom", type=int, default=10,
                        help="Maximum zoom level for MBTiles")
    parser.add_argument("--parallel", type=int, default=2,
                        help="Number of parallel processes to use (0 for sequential)")
    parser.add_argument("--force", action="store_true",
                        help="Force regeneration of existing files")
    parser.add_argument("--no-skip", action="store_true",
                        help="Don't skip existing files")
    
    args = parser.parse_args()
    
    # Run the batch processor
    total, successful = process_all(
        output_dir=args.output_dir,
        min_zoom=args.min_zoom,
        max_zoom=args.max_zoom,
        parallel=args.parallel,
        skip_existing=not args.no_skip,
        force=args.force
    )
    
    # Exit with appropriate code
    if successful == total:
        logger.info("All files processed successfully!")
        exit(0)
    else:
        logger.error(f"Some files failed to process ({total - successful} of {total})")
        exit(1) 