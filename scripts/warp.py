#!/usr/bin/env python
"""
warp.py - Reproject GeoTIFF from EPSG:4326 to Web Mercator (EPSG:3857) without GDAL

This script uses pyproj and scipy.interpolate to warp a GeoTIFF file from 
geographic coordinates (WGS84/EPSG:4326) to Web Mercator (EPSG:3857).

Usage:
    python warp.py input.tiff output.tiff

The script will preserve the original data values and metadata where possible
while transforming the geographic coordinate system.
"""

import os
import sys
import argparse
import logging
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from pyproj import Transformer
from scipy.interpolate import griddata

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('warp')

# Constants for Web Mercator
WEB_MERCATOR_BOUNDS = (-20037508.34, -20048966.10, 20037508.34, 20048966.10)

def lat_lon_to_web_mercator(lat, lon):
    """
    Convert latitude and longitude to Web Mercator coordinates
    
    Parameters:
    -----------
    lat : float or numpy.ndarray
        Latitude in degrees
    lon : float or numpy.ndarray
        Longitude in degrees
        
    Returns:
    --------
    tuple
        (x, y) coordinates in Web Mercator (EPSG:3857)
    """
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    x, y = transformer.transform(lon, lat)
    return x, y

def web_mercator_to_lat_lon(x, y):
    """
    Convert Web Mercator coordinates to latitude and longitude
    
    Parameters:
    -----------
    x : float or numpy.ndarray
        X coordinate in Web Mercator
    y : float or numpy.ndarray
        Y coordinate in Web Mercator
        
    Returns:
    --------
    tuple
        (lat, lon) coordinates in WGS84 (EPSG:4326)
    """
    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(x, y)
    return lat, lon

def reproject_array(data, src_bounds, src_width, src_height, dst_bounds, dst_width, dst_height):
    """
    Reproject a 2D numpy array from one coordinate system to another
    
    Parameters:
    -----------
    data : numpy.ndarray
        2D array of data values
    src_bounds : tuple
        (left, bottom, right, top) in source CRS
    src_width, src_height : int
        Dimensions of source array
    dst_bounds : tuple
        (left, bottom, right, top) in target CRS
    dst_width, dst_height : int
        Dimensions of target array
        
    Returns:
    --------
    numpy.ndarray
        Reprojected 2D array of data values
    """
    # Create source coordinate grid (in lat/lon)
    src_left, src_bottom, src_right, src_top = src_bounds
    src_x_step = (src_right - src_left) / (src_width - 1)
    src_y_step = (src_top - src_bottom) / (src_height - 1)
    
    src_x = np.arange(src_left, src_right + src_x_step/2, src_x_step)[:src_width]
    src_y = np.arange(src_top, src_bottom - src_y_step/2, -src_y_step)[:src_height]
    
    src_lons, src_lats = np.meshgrid(src_x, src_y)
    
    # Create target coordinate grid (in Web Mercator)
    dst_left, dst_bottom, dst_right, dst_top = dst_bounds
    dst_x_step = (dst_right - dst_left) / (dst_width - 1)
    dst_y_step = (dst_top - dst_bottom) / (dst_height - 1)
    
    dst_x = np.arange(dst_left, dst_right + dst_x_step/2, dst_x_step)[:dst_width]
    dst_y = np.arange(dst_top, dst_bottom - dst_y_step/2, -dst_y_step)[:dst_height]
    
    dst_xs, dst_ys = np.meshgrid(dst_x, dst_y)
    
    # Convert target grid to source CRS for interpolation
    dst_lats, dst_lons = web_mercator_to_lat_lon(dst_xs, dst_ys)
    
    # Flatten everything for griddata
    src_points = np.column_stack((src_lats.flatten(), src_lons.flatten()))
    dst_points = np.column_stack((dst_lats.flatten(), dst_lons.flatten()))
    
    logger.debug("Performing interpolation...")
    
    # Interpolate the data values using cubic interpolation for higher quality
    dst_data = griddata(
        src_points, 
        data.flatten(), 
        dst_points, 
        method='cubic', 
        fill_value=0.0
    )
    
    # Reshape to target grid size
    dst_data = dst_data.reshape((dst_height, dst_width))
    
    return dst_data

def warp_geotiff(input_file, output_file, resolution=None):
    """
    Warp a GeoTIFF from EPSG:4326 to EPSG:3857
    
    Parameters:
    -----------
    input_file : str
        Path to input GeoTIFF in WGS84/EPSG:4326
    output_file : str
        Path to output GeoTIFF in Web Mercator/EPSG:3857
    resolution : float, optional
        Target resolution in meters per pixel (if None, will maintain similar resolution)
    
    Returns:
    --------
    bool
        True if successful, False otherwise
    """
    try:
        # Open the input file
        with rasterio.open(input_file) as src:
            # Check if already in Web Mercator
            if src.crs.to_epsg() == 3857:
                logger.warning("Source file is already in Web Mercator projection")
                return False
            
            # Read the data
            data = src.read(1)
            
            # Get the source bounds
            src_bounds = (src.bounds.left, src.bounds.bottom, src.bounds.right, src.bounds.top)
            
            # Calculate the approximate pixel area in degrees
            pixel_height_deg = (src.bounds.top - src.bounds.bottom) / src.height
            pixel_width_deg = (src.bounds.right - src.bounds.left) / src.width
            
            # Transform the source bounds to Web Mercator
            src_top_x, src_top_y = lat_lon_to_web_mercator(src.bounds.top, src.bounds.left)
            src_bottom_x, src_bottom_y = lat_lon_to_web_mercator(src.bounds.bottom, src.bounds.right)
            
            dst_bounds = (src_top_x, src_bottom_y, src_bottom_x, src_top_y)
            
            # Hardcode the resolution to 500 meters per pixel
            target_resolution = 500
            
            # Calculate the target grid dimensions
            dst_width = int(abs(dst_bounds[2] - dst_bounds[0]) / target_resolution)
            dst_height = int(abs(dst_bounds[3] - dst_bounds[1]) / target_resolution)
            
            # Perform the reprojection
            dst_data = reproject_array(
                data, 
                src_bounds, 
                src.width, 
                src.height, 
                dst_bounds, 
                dst_width, 
                dst_height
            )
            
            # Get the transform for the output file
            dst_transform = from_bounds(
                dst_bounds[0], dst_bounds[1], dst_bounds[2], dst_bounds[3], 
                dst_width, dst_height
            )
            
            # Create the output profile
            dst_profile = src.profile.copy()
            dst_profile.update({
                'driver': 'GTiff',
                'height': dst_height,
                'width': dst_width,
                'crs': 'EPSG:3857',
                'transform': dst_transform,
            })
            
            # Write the output file
            logger.debug(f"Writing output file: {output_file}")
            with rasterio.open(output_file, 'w', **dst_profile) as dst:
                dst.write(dst_data, 1)
            
            logger.info("Warping completed successfully")
            return True
            
    except Exception as e:
        logger.error(f"Error warping GeoTIFF: {str(e)}")
        return False

def main():
    """Command line interface for warping GeoTIFFs"""
    parser = argparse.ArgumentParser(
        description='Warp a GeoTIFF from EPSG:4326 to Web Mercator (EPSG:3857)'
    )
    parser.add_argument('input_file', help='Input GeoTIFF file (in EPSG:4326)')
    parser.add_argument('output_file', help='Output GeoTIFF file (in EPSG:3857)')
    parser.add_argument(
        '--resolution', 
        type=float, 
        help='Target resolution in meters per pixel (default: auto)'
    )
    
    args = parser.parse_args()
    
    success = warp_geotiff(args.input_file, args.output_file, args.resolution)
    
    if success:
        print(f"Successfully warped {args.input_file} to {args.output_file}")
        sys.exit(0)
    else:
        print(f"Failed to warp {args.input_file}")
        sys.exit(1)

if __name__ == "__main__":
    main() 