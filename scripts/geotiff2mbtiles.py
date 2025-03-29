import rasterio
import mercantile
import numpy as np
from PIL import Image
import sqlite3
import os
import io
import logging
from rasterio.warp import reproject, Resampling

# Configure logging - remove file handler
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def apply_color_gradient(data, min_val=-3, max_val=3, transparent_range=(-1, 1)):
    """
    Apply a custom color gradient with transparency:
    - Negative: Dark Blue (0, 0, 139) at -3 to Turquoise (64, 224, 208) at -1
    - Positive: Yellow (255, 255, 0) at 1 to Dark Red (139, 0, 0) at 3
    - Transparent between -1 and +1
    """
    # Handle NaN values
    data_clean = np.nan_to_num(data, nan=0.0)
    
    # Initialize RGBA arrays, default alpha = 0 (transparent)
    rgba = np.zeros((4, data.shape[0], data.shape[1]), dtype=np.uint8)
    
    # Define transparency mask: opaque outside -1 to +1
    transparent_min, transparent_max = transparent_range
    opaque_mask = (data_clean < transparent_min) | (data_clean > transparent_max)
    
    if np.any(opaque_mask):
        # Negative range: -3 (dark blue) to -1 (turquoise)
        neg_mask = (data_clean < transparent_min) & (data_clean >= min_val)
        if np.any(neg_mask):
            neg_normalized = (data_clean - min_val) / (transparent_min - min_val)  # 0 at -3, 1 at -1
            neg_normalized = np.clip(neg_normalized, 0, 1)
            rgba[0, neg_mask] = (neg_normalized[neg_mask] * (64 - 0) + 0).astype(np.uint8)  # R
            rgba[1, neg_mask] = (neg_normalized[neg_mask] * (224 - 0) + 0).astype(np.uint8)  # G
            rgba[2, neg_mask] = (neg_normalized[neg_mask] * (208 - 139) + 139).astype(np.uint8)  # B
            rgba[3, neg_mask] = 255  # A (opaque)
        
        # Positive range: 1 (yellow) to 3 (dark red)
        pos_mask = (data_clean > transparent_max) & (data_clean <= max_val)
        if np.any(pos_mask):
            pos_normalized = (data_clean - transparent_max) / (max_val - transparent_max)  # 0 at 1, 1 at 3
            pos_normalized = np.clip(pos_normalized, 0, 1)
            rgba[0, pos_mask] = ((1 - pos_normalized[pos_mask]) * (255 - 139) + 139).astype(np.uint8)  # R
            rgba[1, pos_mask] = ((1 - pos_normalized[pos_mask]) * (255 - 0)).astype(np.uint8)  # G
            rgba[2, pos_mask] = 0  # B
            rgba[3, pos_mask] = 255  # A (opaque)
        
        # Handle extremes beyond min_val and max_val
        extreme_neg_mask = data_clean < min_val
        rgba[0, extreme_neg_mask] = 0    # Dark Blue
        rgba[1, extreme_neg_mask] = 0
        rgba[2, extreme_neg_mask] = 139
        rgba[3, extreme_neg_mask] = 255
        
        extreme_pos_mask = data_clean > max_val
        rgba[0, extreme_pos_mask] = 139  # Dark Red
        rgba[1, extreme_pos_mask] = 0
        rgba[2, extreme_pos_mask] = 0
        rgba[3, extreme_pos_mask] = 255
    else:
        logger.warning("No opaque pixels found; all data falls within transparent range.")
    
    return rgba

def geotiff_to_mbtiles(geotiff_path, mbtiles_path, min_zoom=0, max_zoom=14):
    logger.info(f"Starting conversion: {geotiff_path} to {mbtiles_path}")
    
    try:
        with rasterio.open(geotiff_path) as src:
            bounds = src.bounds
            data = src.read(1)  # Read first band (vertical speed)
            
            # Apply color gradient once
            colored_data = apply_color_gradient(data, min_val=-3, max_val=3, transparent_range=(-1, 1))
            src_transform = src.transform
            src_crs = src.crs
            
            # Create MBTiles database
            conn = sqlite3.connect(mbtiles_path)
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS metadata (name TEXT, value TEXT)")
            cursor.execute("CREATE TABLE IF NOT EXISTS tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS tile_index ON tiles (zoom_level, tile_column, tile_row)")
            
            # Set metadata
            metadata = [
                ("name", os.path.basename(geotiff_path)),
                ("format", "png"),
                ("bounds", f"{bounds.left},{bounds.bottom},{bounds.right},{bounds.top}"),
                ("minzoom", str(min_zoom)),
                ("maxzoom", str(max_zoom)),
                ("type", "baselayer")
            ]
            cursor.executemany("INSERT OR REPLACE INTO metadata VALUES (?, ?)", metadata)
            
            # Generate tiles hierarchically, only for non-empty regions
            tile_count = 0
            skipped_count = 0
            active_tiles = {}
            
            # Start with zoom 0 tiles
            current_zoom = min_zoom
            active_tiles[current_zoom] = list(mercantile.tiles(bounds.left, bounds.bottom, bounds.right, bounds.top, zooms=[current_zoom]))
            
            while current_zoom <= max_zoom:
                next_level_tiles = []
                non_empty_count = 0
                
                for tile in active_tiles[current_zoom]:
                    tile_bounds = mercantile.bounds(tile)
                    transform = rasterio.transform.from_bounds(
                        tile_bounds.west, tile_bounds.south, tile_bounds.east, tile_bounds.north,
                        256, 256
                    )
                    
                    # Reproject pre-colored data
                    tile_data = np.zeros((4, 256, 256), dtype=np.uint8)
                    reproject(
                        source=colored_data,
                        destination=tile_data,
                        src_transform=src_transform,
                        src_crs=src_crs,
                        dst_transform=transform,
                        dst_crs='EPSG:4326',
                        resampling=Resampling.bilinear
                    )
                    
                    # Check if tile has opaque pixels
                    if np.any(tile_data[3] > 0):  # Alpha channel
                        # Convert to image
                        img_array = np.moveaxis(tile_data, 0, -1)
                        img = Image.fromarray(img_array, mode='RGBA')
                        
                        # Save as PNG blob
                        png_buffer = io.BytesIO()
                        img.save(png_buffer, format="PNG")
                        img_blob = png_buffer.getvalue()
                        png_buffer.close()
                        
                        tms_y = (1 << current_zoom) - tile.y - 1
                        cursor.execute(
                            "INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                            (current_zoom, tile.x, tms_y, sqlite3.Binary(img_blob))
                        )
                        tile_count += 1
                        non_empty_count += 1
                        
                        # Add children to next level
                        if current_zoom < max_zoom:
                            children = mercantile.children(tile)
                            next_level_tiles.extend(children)
                    else:
                        skipped_count += 1
                
                # Only log each zoom level once
                if current_zoom % 2 == 0 or current_zoom == max_zoom:
                    logger.info(f"Zoom {current_zoom}: Processed {non_empty_count} non-empty tiles")
                
                # Move to next zoom level
                current_zoom += 1
                if current_zoom <= max_zoom and next_level_tiles:
                    active_tiles[current_zoom] = next_level_tiles
                else:
                    break
            
            conn.commit()
            conn.close()
            logger.info(f"MBTiles file created with {tile_count} non-empty tiles: {mbtiles_path}")
    except Exception as e:
        logger.error(f"Error during conversion: {str(e)}")
        raise

if __name__ == "__main__":
    # Example usage
    input_geotiff = "test_output/test_2025-03-29_12_850_mercator.tiff"
    output_mbtiles = "test_output/test.mbtiles"
    geotiff_to_mbtiles(input_geotiff, output_mbtiles, min_zoom=0, max_zoom=8)