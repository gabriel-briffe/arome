#!/usr/bin/env python
"""
GitHub Release MBTiles Fetcher (Python Version)

This script demonstrates how to fetch MBTiles files from GitHub Releases
using Python requests.

Usage:
  python fetch_latest_release.py [--date YYYY-MM-DD] [--output-dir ./output]
"""

import os
import sys
import argparse
import requests
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
REPO_OWNER = 'gabriel-briffe'
REPO_NAME = 'arome'
API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/releases"

def fetch_latest_release():
    """Fetch the latest release data from GitHub"""
    try:
        response = requests.get(f"{API_URL}/latest")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching latest release: {e}")
        raise

def fetch_release_by_date(date):
    """
    Get a specific release by date
    
    Parameters:
    -----------
    date : str
        Date in YYYY-MM-DD format
    
    Returns:
    --------
    dict
        The release data
    """
    try:
        # First get all releases
        response = requests.get(API_URL)
        response.raise_for_status()
        releases = response.json()
        
        # Find the release with the matching tag
        tag_name = f"arome-{date}"
        release = next((r for r in releases if r.get('tag_name') == tag_name), None)
        
        if not release:
            raise ValueError(f"No release found for date: {date}")
        
        return release
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching release for date {date}: {e}")
        raise
    except ValueError as e:
        logger.error(str(e))
        raise

def get_mbtiles_from_release(release_data):
    """
    Get all MBTiles files from a release
    
    Parameters:
    -----------
    release_data : dict
        The release data from GitHub API
    
    Returns:
    --------
    list
        List of dictionaries with name, url, size, and created_at for each MBTiles file
    """
    return [
        {
            'name': asset['name'],
            'url': asset['browser_download_url'],
            'size': asset['size'],
            'created_at': asset['created_at']
        }
        for asset in release_data['assets']
        if asset['name'].endswith('.mbtiles')
    ]

def get_mbtiles_for_pressure(mbtiles, pressure):
    """
    Get MBTiles file info for a specific pressure level
    
    Parameters:
    -----------
    mbtiles : list
        List of MBTiles file dictionaries
    pressure : int
        Pressure level (e.g., 850)
    
    Returns:
    --------
    list
        Filtered list of MBTiles for the specified pressure
    """
    pressure_str = str(pressure)
    return [file for file in mbtiles if f"_{pressure_str}." in file['name']]

def get_mbtiles_for_hour(mbtiles, hour):
    """
    Get MBTiles file info for a specific hour
    
    Parameters:
    -----------
    mbtiles : list
        List of MBTiles file dictionaries
    hour : int
        Hour (0-23)
    
    Returns:
    --------
    list
        Filtered list of MBTiles for the specified hour
    """
    hour_str = f"{hour:02d}"
    return [file for file in mbtiles if f"_{hour_str}_" in file['name']]

def download_file(url, output_path):
    """
    Download a file from a URL to the specified path
    
    Parameters:
    -----------
    url : str
        URL of the file to download
    output_path : str
        Path to save the file to
    
    Returns:
    --------
    bool
        True if successful, False otherwise
    """
    try:
        logger.info(f"Downloading {url} to {output_path}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Successfully downloaded to {output_path}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading file: {e}")
        return False
    except IOError as e:
        logger.error(f"IO Error: {e}")
        return False

def load_arome_data_for_date(date=None, output_dir=None):
    """
    Load AROME data for a specific date or the latest available
    
    Parameters:
    -----------
    date : str, optional
        Date in YYYY-MM-DD format, or None for latest
    output_dir : str, optional
        Directory to download files to, or None to just get info
    
    Returns:
    --------
    dict
        Dictionary with release data and MBTiles information
    """
    try:
        # Get release data (latest or for specific date)
        if date:
            release_data = fetch_release_by_date(date)
        else:
            release_data = fetch_latest_release()
            # Extract date from tag name
            date = release_data['tag_name'].replace('arome-', '')
        
        logger.info(f"Loaded release: {release_data['name']}")
        
        # Get all MBTiles from the release
        all_mbtiles = get_mbtiles_from_release(release_data)
        logger.info(f"Found {len(all_mbtiles)} MBTiles files")
        
        if output_dir:
            # Create output directory structure
            date_output_dir = os.path.join(output_dir, date)
            os.makedirs(date_output_dir, exist_ok=True)
            
            # Download all files
            for mbtile in all_mbtiles:
                output_path = os.path.join(date_output_dir, mbtile['name'])
                download_file(mbtile['url'], output_path)
        
        # Return all the data
        return {
            'release_data': release_data,
            'date': date,
            'all_mbtiles': all_mbtiles
        }
    except Exception as e:
        logger.error(f"Failed to load AROME data: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch MBTiles from GitHub Releases"
    )
    parser.add_argument("--date", help="Date in YYYY-MM-DD format, or 'latest' for most recent")
    parser.add_argument("--output-dir", default="./downloaded_mbtiles", 
                        help="Directory to save downloaded files")
    
    args = parser.parse_args()
    
    try:
        # Use today's date if not specified
        if not args.date:
            args.date = "latest"
            
        if args.date.lower() == "latest":
            data = load_arome_data_for_date(output_dir=args.output_dir)
        else:
            data = load_arome_data_for_date(date=args.date, output_dir=args.output_dir)
        
        logger.info(f"Successfully processed release from {data['date']}")
        logger.info(f"Files downloaded to {args.output_dir}/{data['date']}/")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    
    sys.exit(0) 