#!/usr/bin/env python
"""
AROME Vertical Velocity TIFF Data Fetcher

This script fetches vertical velocity data in TIFF format from the AROME API
using the WCS (Web Coverage Service) endpoint.
"""

import os
import requests
import logging
from datetime import datetime, timezone
import numpy as np
import rasterio
import time
import random

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('tiff-fetcher')

# API key
api_key = "eyJ4NXQiOiJZV0kxTTJZNE1qWTNOemsyTkRZeU5XTTRPV014TXpjek1UVmhNbU14T1RSa09ETXlOVEE0Tnc9PSIsImtpZCI6ImdhdGV3YXlfY2VydGlmaWNhdGVfYWxpYXMiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJnYWJyaWVsLmJyaWZmZUBjYXJib24uc3VwZXIiLCJhcHBsaWNhdGlvbiI6eyJvd25lciI6ImdhYnJpZWwuYnJpZmZlIiwidGllclF1b3RhVHlwZSI6bnVsbCwidGllciI6IlVubGltaXRlZCIsIm5hbWUiOiJEZWZhdWx0QXBwbGljYXRpb24iLCJpZCI6MjYxNDEsInV1aWQiOiI1MzE0NDAwNS02MzA2LTQ3YTgtYWQ1Mi03Zjg0NTY1YjE3NTcifSwiaXNzIjoiaHR0cHM6XC9cL3BvcnRhaWwtYXBpLm1ldGVvZnJhbmNlLmZyOjQ0M1wvb2F1dGgyXC90b2tlbiIsInRpZXJJbmZvIjp7IjUwUGVyTWluIjp7InRpZXJRdW90YVR5cGUiOiJyZXF1ZXN0Q291bnQiLCJncmFwaFFMTWF4Q29tcGxleGl0eSI6MCwiZ3JhcGhRTE1heERlcHRoIjowLCJzdG9wT25RdW90YVJlYWNoIjp0cnVlLCJzcGlrZUFycmVzdExpbWl0IjowLCJzcGlrZUFycmVzdFVuaXQiOiJzZWMifX0sImtleXR5cGUiOiJQUk9EVUNUSU9OIiwic3Vic2NyaWJlZEFQSXMiOlt7InN1YnNjcmliZXJUZW5hbnREb21haW4iOiJjYXJib24uc3VwZXIiLCJuYW1lIjoiQVJPTUUiLCJjb250ZXh0IjoiXC9wdWJsaWNcL2Fyb21lXC8xLjAiLCJwdWJsaXNoZXIiOiJhZG1pbl9tZiIsInZlcnNpb24iOiIxLjAiLCJzdWJzY3JpcHRpb25UaWVyIjoiNTBQZXJNaW4ifV0sImV4cCI6MTgzNzg1NjI5NywidG9rZW5fdHlwZSI6ImFwaUtleSIsImlhdCI6MTc0MzI0ODI5NywianRpIjoiY2JjMzNiYjQtNzNiOS00MmI2LTg3ZWQtN2MzY2M3MjdjNmYyIn0=.WfxOzmhfnsRoPARr3a2ra3VRdzz9TmcGwCVPYdEuI5Bba_q_Ox-z_8x4JkedFj_UQl83UhzJlbyH58lKTO9JYT2MMSKqrTmcVEYhk3E65ihVQBTCLXit-bbZobHkbfEErqyiF-sRet-lGs-pqZBjlu2w0kuIpgivQHc0tcKbk_9FS9T5bgoZmtXKUQkI7E9_O599qiQkNSuUD2_WJas65BhDbBiIS-BTc6NndrUzUh-Vqd1dh78qNOROcdTn4BAx8zA8rRQbs5v1AXGYXznv1P84hpBCdTbIhnKl_I4cWazabtFmWly8382c79U6ZtBOdwN1ZAqW9wZzZJu1yHXyzg=="

# Headers
headers = {
    "accept": "*/*",
    "apikey": api_key
}

# Base URLs for the AROME API
base_url = "https://public-api.meteofrance.fr/public/arome/1.0/wcs/MF-NWP-HIGHRES-AROME-0025-FRANCE-WCS"
coverage_url = f"{base_url}/GetCoverage"

# Default geographic bounds (full AROME France domain extent)
# Covers France and surrounding areas including parts of neighboring countries
DEFAULT_LAT_MIN = "37.5"    # Southern boundary of AROME domain
DEFAULT_LAT_MAX = "55.4"    # Northern boundary of AROME domain
DEFAULT_LONG_MIN = "-12.0"  # Western boundary of AROME domain (12°W)
DEFAULT_LONG_MAX = "16.0"   # Eastern boundary of AROME domain (16°E)

def fetch_vertical_velocity_tiff(
    time_value,
    ref_time_value,
    pressure_value,
    lat_min=DEFAULT_LAT_MIN,
    lat_max=DEFAULT_LAT_MAX,
    long_min=DEFAULT_LONG_MIN,
    long_max=DEFAULT_LONG_MAX,
    output_file=None,
    max_retries=6,
    initial_backoff=2,
    max_backoff=120
):
    """
    Fetch vertical velocity data from the AROME API in TIFF format
    
    Parameters:
    -----------
    time_value : str
        The forecast time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    ref_time_value : str
        The reference time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    pressure_value : str
        The pressure level in hPa (e.g., "600")
    lat_min, lat_max : str
        Latitude bounds
    long_min, long_max : str
        Longitude bounds
    output_file : str, optional
        The output file name. If None, a default name will be generated.
    max_retries : int
        Maximum number of retry attempts for rate-limited requests
    initial_backoff : int
        Initial backoff time in seconds
    max_backoff : int
        Maximum backoff time in seconds
    
    Returns:
    --------
    bool
        True if successful, False otherwise
    """
    # Set default output filename if not provided
    if output_file is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_file = f"vertical_velocity_{timestamp}.tiff"
    
    # Build the coverageId from the layer name and reference time
    coverage_id = f"VERTICAL_VELOCITY_GEOMETRIC__ISOBARIC_SURFACE___{ref_time_value}"
    
    # Construct the URL with all parameters
    url = (
        f"{coverage_url}"
        f"?SERVICE=WCS"
        f"&VERSION=2.0.1"
        f"&REQUEST=GetCoverage"
        f"&format=image/tiff"
        f"&coverageId={coverage_id}"
        f"&subset=time({time_value})"
        f"&subset=lat({lat_min},{lat_max})"
        f"&subset=long({long_min},{long_max})"
        f"&subset=pressure({pressure_value})"
    )
    
    logger.info(f"Requesting data from: {url}")
    
    # Implement exponential backoff retry
    retry_count = 0
    backoff = initial_backoff
    
    while retry_count <= max_retries:
        try:
            logger.info(f"Fetching data for {time_value} at {pressure_value}hPa")
            response = requests.get(url, headers=headers, timeout=60)
            
            # Handle rate limiting (HTTP 429)
            if response.status_code == 429:
                retry_count += 1
                if retry_count > max_retries:
                    logger.error(f"Rate limit exceeded after {max_retries} retries. Giving up.")
                    return False
                
                # Get retry-after header if available, otherwise use exponential backoff
                retry_after = response.headers.get('Retry-After')
                if retry_after and retry_after.isdigit():
                    wait_time = int(retry_after)
                else:
                    # Add jitter to avoid thundering herd
                    jitter = random.uniform(0, 0.1 * backoff)
                    wait_time = backoff + jitter
                    # Double the backoff for next time, but don't exceed max
                    backoff = min(backoff * 2, max_backoff)
                
                logger.warning(f"Rate limited (429). Retrying in {wait_time:.1f} seconds (attempt {retry_count}/{max_retries})")
                time.sleep(wait_time)
                continue
            
            # Check response status
            if response.status_code == 200:
                with open(output_file, "wb") as f:
                    f.write(response.content)
                logger.info(f"Successfully saved TIFF to {output_file}")
                
                # Now open with rasterio and replace nodata with 0
                try:
                    with rasterio.open(output_file, 'r+') as src:
                        # Read the data
                        data = src.read(1)
                        
                        # Replace nodata values with 0
                        if src.nodata is not None:
                            data = np.where(data == src.nodata, 0, data)
                        
                        # Also replace any NaN values with 0
                        data = np.nan_to_num(data, nan=0.0)
                        
                        # Write the modified data back
                        src.write(data, 1)
                        
                    logger.debug(f"Replaced nodata values with 0 in {output_file}")
                except Exception as e:
                    logger.warning(f"Could not process nodata values in the TIFF: {e}")
                    # Continue anyway since we already have the raw file
                
                return True
            else:
                logger.error(f"API Error {response.status_code}: {response.text}")
                # Don't retry for non-429 errors
                return False
        except requests.exceptions.RequestException as e:
            # For connection errors, retry with backoff if it might be a temporary issue
            if isinstance(e, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
                retry_count += 1
                if retry_count > max_retries:
                    logger.error(f"Request failed after {max_retries} retries: {e}")
                    return False
                
                # Add jitter to avoid thundering herd
                jitter = random.uniform(0, 0.1 * backoff)
                wait_time = backoff + jitter
                # Double the backoff for next time, but don't exceed max
                backoff = min(backoff * 2, max_backoff)
                
                logger.warning(f"Connection error. Retrying in {wait_time:.1f} seconds (attempt {retry_count}/{max_retries}): {e}")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"Request failed: {e}")
                return False
    
    # If we get here, we've exhausted retries
    logger.error(f"Failed to fetch data after {max_retries} retries.")
    return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Fetch vertical velocity data in TIFF format from the AROME API',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Required arguments
    parser.add_argument('--time', required=True,
                      help='Forecast time (YYYY-MM-DDTHH:MM:SSZ)')
    parser.add_argument('--ref-time', required=True,
                      help='Reference time (YYYY-MM-DDTHH:MM:SSZ)')
    parser.add_argument('--pressure', required=True,
                      help='Pressure level in hPa')
    
    # Optional arguments
    parser.add_argument('--lat-min', default=DEFAULT_LAT_MIN,
                      help='Minimum latitude')
    parser.add_argument('--lat-max', default=DEFAULT_LAT_MAX,
                      help='Maximum latitude')
    parser.add_argument('--long-min', default=DEFAULT_LONG_MIN,
                      help='Minimum longitude')
    parser.add_argument('--long-max', default=DEFAULT_LONG_MAX,
                      help='Maximum longitude')
    parser.add_argument('--output', '-o',
                      help='Output file name (default: auto-generated)')
    
    args = parser.parse_args()
    
    # Fetch the data
    success = fetch_vertical_velocity_tiff(
        args.time,
        args.ref_time,
        args.pressure,
        args.lat_min,
        args.lat_max,
        args.long_min,
        args.long_max,
        args.output
    )
    
    if success:
        print("Successfully fetched vertical velocity data")
    else:
        print("Failed to fetch data")
        exit(1) 