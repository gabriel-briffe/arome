#!/usr/bin/env python
"""
GitHub Actions Runner Script for AROME Data Processing

This script is designed to run in GitHub Actions and handles path issues
that might arise when running in that environment.

Usage: 
  python run_in_github.py --output-dir /path/to/output
"""

import os
import sys
import argparse
import logging
import traceback
from datetime import datetime

# Add parent directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Only log to console in GitHub Actions
    ]
)
logger = logging.getLogger('github-runner')

def run_pipeline(output_dir, min_zoom=4, max_zoom=8, parallel=1, skip_existing=True, force=False, forecast_days=[0, 1]):
    """Run the data processing pipeline with extra error handling for GitHub Actions"""
    try:
        # Print environment information
        logger.info(f"Current directory: {os.getcwd()}")
        logger.info(f"Script directory: {current_dir}")
        logger.info(f"Python path: {sys.path}")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Processing forecast days: {forecast_days}")
        
        # Import process_all here to ensure paths are set up correctly
        from process_all import process_all
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Run the main processing function
        total, successful = process_all(
            output_dir=output_dir,
            min_zoom=min_zoom,
            max_zoom=max_zoom,
            parallel=parallel,
            skip_existing=skip_existing,
            force=force,
            forecast_days=forecast_days
        )
        
        # Report results
        logger.info(f"Completed processing {successful} of {total} files")
        
        if successful == total:
            return True
        else:
            return False
            
    except ImportError as e:
        logger.error(f"Import error: {e}")
        logger.error("This is likely due to a path configuration issue in GitHub Actions.")
        logger.error(f"Current sys.path: {sys.path}")
        logger.error(traceback.format_exc())
        return False
    except Exception as e:
        logger.error(f"Error running pipeline: {e}")
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="GitHub Actions Runner for AROME Data Processing",
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
    
    args = parser.parse_args()
    
    # Run the pipeline
    success = run_pipeline(
        output_dir=args.output_dir,
        min_zoom=args.min_zoom,
        max_zoom=args.max_zoom,
        parallel=args.parallel,
        skip_existing=not args.no_skip,
        force=args.force,
        forecast_days=args.forecast_days
    )
    
    # Exit with appropriate code
    if success:
        sys.exit(0)
    else:
        sys.exit(1) 