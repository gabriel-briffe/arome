#!/usr/bin/env python
"""
GitHub Actions Runner Script for AROME Wind Components Data Download

This script downloads U and V wind component data using the same mechanism
as the vertical velocity processing, but saves the raw TIFF files.

Usage:
  python run_wind_components_github.py --output-dir /path/to/output --log-level INFO
"""

import os
import sys
import argparse
import logging
import traceback
from datetime import datetime, timezone, timedelta

# Add parent directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

def setup_logging(log_level="INFO"):
    """Configure logging based on the specified log level"""
    # Convert string to logging level
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()  # Only log to console in GitHub Actions
        ]
    )

    # Create a logger for this script
    logger = logging.getLogger('wind-components-runner')
    logger.setLevel(numeric_level)

    # Set all other loggers to the same level as specified by the user
    logging.getLogger('tiff-fetcher').setLevel(numeric_level)

    return logger

def is_valid_tiff(file_path):
    """Check if a file is a valid TIFF (not an XML error response)"""
    try:
        # Check file size (>1MB suggests valid data)
        if os.path.getsize(file_path) < 1000000:  # Less than 1MB
            return False

        # Check if file starts with TIFF header (II or MM)
        with open(file_path, 'rb') as f:
            header = f.read(4)
            if header not in [b'II*\x00', b'MM\x00*']:  # Little-endian or big-endian TIFF
                return False

        # Try basic rasterio check (if available)
        try:
            import rasterio
            import numpy as np
            with rasterio.open(file_path) as src:
                # Check if it has expected dimensions
                if src.width < 100 or src.height < 100:
                    return False
                # Quick check for data
                data = src.read(1, window=((0, 1), (0, 1)))  # Read just one pixel
                if data.size == 0:
                    return False
        except ImportError:
            # If rasterio not available, just check file header and size
            pass
        except Exception:
            return False

        return True
    except Exception as e:
        logger.warning(f"Error validating TIFF {file_path}: {e}")
        return False

def upload_to_github_release(file_path, release_tag):
    """Upload a single file to GitHub release"""
    try:
        import subprocess

        filename = os.path.basename(file_path)

        # Check if release exists, create if not
        check_cmd = ["gh", "release", "view", release_tag]
        result = subprocess.run(check_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            # Release doesn't exist, create it
            logger.info(f"Creating GitHub release: {release_tag}")
            create_cmd = [
                "gh", "release", "create", release_tag,
                "--title", f"AROME Data {release_tag.replace('arome-', '')}",
                "--notes", "Daily AROME weather data including vertical velocity and wind components"
            ]
            create_result = subprocess.run(create_cmd, capture_output=True, text=True)
            if create_result.returncode != 0:
                logger.error(f"Failed to create release: {create_result.stderr}")
                return False

        # Upload the file
        upload_cmd = ["gh", "release", "upload", release_tag, file_path]
        upload_result = subprocess.run(upload_cmd, capture_output=True, text=True)

        if upload_result.returncode == 0:
            return True
        else:
            logger.error(f"Failed to upload {filename}: {upload_result.stderr}")
            return False

    except Exception as e:
        logger.error(f"Error uploading {file_path} to GitHub release: {e}")
        return False

def download_wind_components(output_dir, forecast_days=[0, 1], log_level="INFO", release_tag=None):
    """Download U and V wind component data for the specified forecast days"""
    # Setup logging
    logger = setup_logging(log_level)

    try:
        # Print environment information
        logger.debug(f"Current directory: {os.getcwd()}")
        logger.debug(f"Script directory: {current_dir}")
        logger.debug(f"Python path: {sys.path}")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Processing forecast days: {forecast_days}")
        if release_tag:
            logger.info(f"GitHub release tag: {release_tag}")

        # Import the fetch function
        from fetch import fetch_wind_component_tiff

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Define pressure levels and hours (same as vertical velocity)
        pressure_levels = [500, 600, 700, 800, 900]
        hours = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]  # 5-21 UTC

        total_files = len(forecast_days) * len(pressure_levels) * len(hours) * 2  # *2 for U and V
        successful_files = 0

        logger.info(f"=== Starting wind components download: {total_files} files ===")

        for day_offset in forecast_days:
            # Calculate the target date
            base_date = datetime.now(timezone.utc)
            if day_offset == 0:
                # Today: use current date at 00:00:00Z as reference
                source_date = base_date.replace(hour=0, minute=0, second=0, microsecond=0)
                target_date = source_date
            else:
                # Future days: use today at 00:00:00Z as reference, target is future date
                source_date = base_date.replace(hour=0, minute=0, second=0, microsecond=0)
                target_date = source_date + timedelta(days=day_offset)

            source_date_str = source_date.strftime("%Y-%m-%d")
            target_date_str = target_date.strftime("%Y-%m-%d")

            logger.info(f"Processing day {day_offset}: source={source_date_str}, target={target_date_str}")

            for hour in hours:
                for pressure in pressure_levels:
                    # Construct time values (same as vertical velocity)
                    time_value = f"{target_date_str}T{hour:02d}:00:00Z"
                    ref_time_value = f"{source_date_str}T00:00:00Z"

                    # Process U component: Download → Validate → Upload
                    u_filename = f"arome_u_{source_date_str}_{target_date_str}_{hour:02d}_{pressure}.tiff"
                    u_filepath = os.path.join(output_dir, u_filename)

                    logger.info(f"🔄 Processing U component: {u_filename}")

                    success_u = False
                    if fetch_wind_component_tiff(
                        component_type='U',
                        time_value=time_value,
                        ref_time_value=ref_time_value,
                        pressure_value=str(pressure),
                        output_file=u_filepath
                    ):
                        if is_valid_tiff(u_filepath):
                            logger.info(f"✅ Downloaded and validated U component: {u_filename}")
                            # Upload immediately to GitHub release
                            if upload_to_github_release(u_filepath, release_tag):
                                logger.info(f"📤 Successfully uploaded U component to release")
                                success_u = True
                                successful_files += 1
                            else:
                                logger.error(f"❌ Failed to upload U component to release")
                        else:
                            logger.warning(f"❌ Downloaded U component is not valid: {u_filename}")
                    else:
                        logger.error(f"❌ Failed to download U component: {u_filename}")

                    if not success_u and os.path.exists(u_filepath):
                        os.remove(u_filepath)  # Clean up failed files

                    # Process V component: Download → Validate → Upload
                    v_filename = f"arome_v_{source_date_str}_{target_date_str}_{hour:02d}_{pressure}.tiff"
                    v_filepath = os.path.join(output_dir, v_filename)

                    logger.info(f"🔄 Processing V component: {v_filename}")

                    success_v = False
                    if fetch_wind_component_tiff(
                        component_type='V',
                        time_value=time_value,
                        ref_time_value=ref_time_value,
                        pressure_value=str(pressure),
                        output_file=v_filepath
                    ):
                        if is_valid_tiff(v_filepath):
                            logger.info(f"✅ Downloaded and validated V component: {v_filename}")
                            # Upload immediately to GitHub release
                            if upload_to_github_release(v_filepath, release_tag):
                                logger.info(f"📤 Successfully uploaded V component to release")
                                success_v = True
                                successful_files += 1
                            else:
                                logger.error(f"❌ Failed to upload V component to release")
                        else:
                            logger.warning(f"❌ Downloaded V component is not valid: {v_filename}")
                    else:
                        logger.error(f"❌ Failed to download V component: {v_filename}")

                    if not success_v and os.path.exists(v_filepath):
                        os.remove(v_filepath)  # Clean up failed files

        # Report results
        logger.info(f"=== Wind components download completed ===")
        logger.info(f"Total files attempted: {total_files}")
        logger.info(f"Successfully downloaded, validated, and uploaded: {successful_files}")
        logger.info(f"Failed: {total_files - successful_files}")
        logger.info(f"Files uploaded to GitHub release: {release_tag}")

        if successful_files == total_files:
            return True
        elif successful_files > 0:
            logger.warning("Some files failed to download, but continuing with partial success")
            return True
        else:
            return False

    except Exception as e:
        logger.error(f"Error downloading wind components: {e}")
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="GitHub Actions Runner for AROME Wind Components Download",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("--output-dir", default="./output",
                        help="Directory to save output files")
    parser.add_argument("--forecast-days", type=int, nargs="+", default=[0, 1],
                        help="Days to forecast (0 for today, 1 for tomorrow, etc.)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level")
    parser.add_argument("--release-tag", type=str,
                        help="GitHub release tag to create and upload files to")

    args = parser.parse_args()

    # Run the download
    success = download_wind_components(
        output_dir=args.output_dir,
        forecast_days=args.forecast_days,
        log_level=args.log_level,
        release_tag=args.release_tag
    )

    # Exit with appropriate code
    if success:
        sys.exit(0)
    else:
        sys.exit(1)
