#!/usr/bin/env python
"""
GitHub Actions Runner Script for AROME Wind Components Data Download

This script downloads U and V wind component data using the same mechanism
as the vertical velocity processing, splits each component into 4 geographic
regions (South, MiddleWest, MiddleEast, North), and uploads the split files
to GitHub releases with regional naming convention.

The script checks each individual split file before uploading and skips files
that already exist in the release, allowing for resumable uploads.

Regions:
- South: 37.5°N to 41°N (full longitude)
- MiddleWest: 41°N to 48.5°N, west of 4°E
- MiddleEast: 41°N to 48.5°N, east of 4°E
- North: 48.5°N to 55.4°N (full longitude)

Usage:
  python run_wind_components_github.py --output-dir /path/to/output --log-level INFO
"""

import os
import sys
import argparse
import logging
import traceback
import json
import subprocess
from datetime import datetime, timezone, timedelta
import rasterio
from rasterio.windows import from_bounds
import numpy as np

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
            print(f"Creating GitHub release: {release_tag}")
            create_cmd = [
                "gh", "release", "create", release_tag,
                "--title", f"AROME Data {release_tag.replace('arome-', '')}",
                "--notes", "Daily AROME weather data including vertical velocity and wind components"
            ]
            create_result = subprocess.run(create_cmd, capture_output=True, text=True)
            if create_result.returncode != 0:
                print(f"Failed to create release: {create_result.stderr}")
                return False

        # Upload the file
        upload_cmd = ["gh", "release", "upload", release_tag, file_path]
        upload_result = subprocess.run(upload_cmd, capture_output=True, text=True)

        if upload_result.returncode == 0:
            return True
        else:
            print(f"Failed to upload {filename}: {upload_result.stderr}")
            return False

    except Exception as e:
        print(f"Error uploading {file_path} to GitHub release: {e}")
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
            print(f"⏭️  File {filename} already exists in release, skipping...")

        return exists
    except subprocess.CalledProcessError as e:
        print(f"⚠️  Could not check release assets: {e.stderr}")
        return False
    except Exception as e:
        print(f"⚠️  Error checking release assets: {e}")
        return False

def split_wind_component_tiff(input_file, output_dir, source_date_str, target_date_str, hour, pressure, component_type):
    """
    Split a wind component TIFF into 4 regions and save with new naming convention

    Parameters:
    ----------
    input_file : str
        Path to input TIFF file
    output_dir : str
        Directory to save split files
    source_date_str, target_date_str : str
        Date strings for naming
    hour : int
        Forecast hour
    pressure : int
        Pressure level
    component_type : str
        'U' or 'V' component

    Returns:
    -------
    list : Paths to the 4 split files created
    """

    split_files = []

    with rasterio.open(input_file) as src:
        # Get source bounds
        bounds = src.bounds
        left, bottom, right, top = bounds

        # Define the regions with names matching the desired output
        regions = [
            {
                'name': 'South',
                'bounds': (left, bottom, right, 41.0),
                'description': f'{bottom:.1f}°N to 41.0°N'
            },
            {
                'name': 'MiddleWest',
                'bounds': (left, 41.0, 4.0, 48.5),
                'description': f'41.0°N to 48.5°N, {left:.1f}°E to 4.0°E'
            },
            {
                'name': 'MiddleEast',
                'bounds': (4.0, 41.0, right, 48.5),
                'description': f'41.0°N to 48.5°N, 4.0°E to {right:.1f}°E'
            },
            {
                'name': 'North',
                'bounds': (left, 48.5, right, top),
                'description': f'48.5°N to {top:.1f}°N'
            }
        ]

        for region in regions:
            name = region['name']
            region_bounds = region['bounds']
            description = region['description']

            region_left, region_bottom, region_right, region_top = region_bounds

            # Skip if region has no height or width
            if region_top <= region_bottom or region_right <= region_left:
                continue

            try:
                # Create window for this region
                window = rasterio.windows.from_bounds(
                    region_left, region_bottom, region_right, region_top,
                    src.transform
                )

                # Read the data
                region_data = src.read(window=window)

                # Skip if no data
                if region_data.size == 0:
                    continue

                # Create new transform for this region
                region_transform = rasterio.transform.from_bounds(
                    region_left, region_bottom, region_right, region_top,
                    region_data.shape[2], region_data.shape[1]  # width, height
                )

                # Create output filename with new naming convention
                output_filename = f"arome_{component_type.lower()}_{name}_{source_date_str}_{target_date_str}_{hour:02d}_{pressure}.tiff"
                output_path = os.path.join(output_dir, output_filename)

                # Create profile for output
                region_profile = src.profile.copy()
                region_profile.update({
                    'height': region_data.shape[1],
                    'width': region_data.shape[2],
                    'transform': region_transform
                })

                # Write the region
                with rasterio.open(output_path, 'w', **region_profile) as dst:
                    dst.write(region_data)

                split_files.append(output_path)

            except Exception as e:
                print(f"❌ Error processing {name} region: {e}")

    return split_files

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

        # Each component is split into 4 regions, so total files = forecast_days * pressures * hours * components * regions
        # Note: actual uploaded files may be less due to existing files being skipped
        total_files = len(forecast_days) * len(pressure_levels) * len(hours) * 2 * 4  # *2 for U/V, *4 for regions
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

                    # Process U component: Download → Validate → Split → Check each split → Upload missing splits
                    u_base_filename = f"arome_u_{source_date_str}_{target_date_str}_{hour:02d}_{pressure}.tiff"
                    u_filepath = os.path.join(output_dir, u_base_filename)

                    logger.info(f"🔄 Processing U component: {u_base_filename}")

                    success_u = False
                    uploaded_u_splits = 0

                    # Check if raw file already exists locally (skip download if it does)
                    if not os.path.exists(u_filepath):
                        if not fetch_wind_component_tiff(
                            component_type='U',
                            time_value=time_value,
                            ref_time_value=ref_time_value,
                            pressure_value=str(pressure),
                            output_file=u_filepath
                        ):
                            logger.error(f"❌ Failed to download U component: {u_base_filename}")
                        elif not is_valid_tiff(u_filepath):
                            logger.warning(f"❌ Downloaded U component is not valid: {u_base_filename}")
                        else:
                            logger.info(f"✅ Downloaded and validated U component: {u_base_filename}")
                    else:
                        logger.info(f"⏭️  U component already exists locally: {u_base_filename}")

                    # If we have a valid U file, split it
                    if os.path.exists(u_filepath) and is_valid_tiff(u_filepath):
                        split_files = split_wind_component_tiff(
                            u_filepath, output_dir, source_date_str, target_date_str, hour, pressure, 'U'
                        )

                        if len(split_files) == 4:
                            logger.info(f"✅ Split U component into 4 regions")

                            # Check and upload each split file individually
                            for split_file in split_files:
                                split_filename = os.path.basename(split_file)

                                if release_tag and check_file_exists_in_release(release_tag, split_filename):
                                    logger.info(f"⏭️  U split already exists in release: {split_filename}")
                                    uploaded_u_splits += 1  # Count as successful even if skipped
                                elif upload_to_github_release(split_file, release_tag):
                                    logger.info(f"📤 Successfully uploaded U split: {split_filename}")
                                    uploaded_u_splits += 1
                                else:
                                    logger.error(f"❌ Failed to upload U split: {split_filename}")

                            if uploaded_u_splits == 4:
                                success_u = True
                                successful_files += 4

                            # Clean up split files after upload attempts
                            for split_file in split_files:
                                try:
                                    os.remove(split_file)
                                except OSError:
                                    pass  # Ignore cleanup errors

                        else:
                            logger.error(f"❌ Failed to split U component into 4 regions (got {len(split_files)})")

                    # Clean up original file after splitting
                    if os.path.exists(u_filepath):
                        try:
                            os.remove(u_filepath)
                        except OSError:
                            pass  # Ignore cleanup errors

                    # Process V component: Download → Validate → Split → Check each split → Upload missing splits
                    v_base_filename = f"arome_v_{source_date_str}_{target_date_str}_{hour:02d}_{pressure}.tiff"
                    v_filepath = os.path.join(output_dir, v_base_filename)

                    logger.info(f"🔄 Processing V component: {v_base_filename}")

                    success_v = False
                    uploaded_v_splits = 0

                    # Check if raw file already exists locally (skip download if it does)
                    if not os.path.exists(v_filepath):
                        if not fetch_wind_component_tiff(
                            component_type='V',
                            time_value=time_value,
                            ref_time_value=ref_time_value,
                            pressure_value=str(pressure),
                            output_file=v_filepath
                        ):
                            logger.error(f"❌ Failed to download V component: {v_base_filename}")
                        elif not is_valid_tiff(v_filepath):
                            logger.warning(f"❌ Downloaded V component is not valid: {v_base_filename}")
                        else:
                            logger.info(f"✅ Downloaded and validated V component: {v_base_filename}")
                    else:
                        logger.info(f"⏭️  V component already exists locally: {v_base_filename}")

                    # If we have a valid V file, split it
                    if os.path.exists(v_filepath) and is_valid_tiff(v_filepath):
                        split_files = split_wind_component_tiff(
                            v_filepath, output_dir, source_date_str, target_date_str, hour, pressure, 'V'
                        )

                        if len(split_files) == 4:
                            logger.info(f"✅ Split V component into 4 regions")

                            # Check and upload each split file individually
                            for split_file in split_files:
                                split_filename = os.path.basename(split_file)

                                if release_tag and check_file_exists_in_release(release_tag, split_filename):
                                    logger.info(f"⏭️  V split already exists in release: {split_filename}")
                                    uploaded_v_splits += 1  # Count as successful even if skipped
                                elif upload_to_github_release(split_file, release_tag):
                                    logger.info(f"📤 Successfully uploaded V split: {split_filename}")
                                    uploaded_v_splits += 1
                                else:
                                    logger.error(f"❌ Failed to upload V split: {split_filename}")

                            if uploaded_v_splits == 4:
                                success_v = True
                                successful_files += 4

                            # Clean up split files after upload attempts
                            for split_file in split_files:
                                try:
                                    os.remove(split_file)
                                except OSError:
                                    pass  # Ignore cleanup errors

                        else:
                            logger.error(f"❌ Failed to split V component into 4 regions (got {len(split_files)})")

                    # Clean up original file after splitting
                    if os.path.exists(v_filepath):
                        try:
                            os.remove(v_filepath)
                        except OSError:
                            pass  # Ignore cleanup errors

        # Report results
        logger.info(f"=== Wind components download completed ===")
        logger.info(f"Total split files that could be processed: {total_files}")
        logger.info(f"Successfully processed and available in release: {successful_files}")
        logger.info(f"Files may have been skipped if they already existed in release: {release_tag}")
        logger.info(f"Note: Each original component is split into 4 regional files")

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
        description="GitHub Actions Runner for AROME Wind Components Download (with regional splitting)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("--output-dir", default="./output",
                        help="Directory to save output files")
    parser.add_argument("--forecast-days", type=int, nargs="+", default=[0],
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
