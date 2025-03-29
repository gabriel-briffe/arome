import rasterio
import mercantile
import numpy as np
from PIL import Image
import sqlite3
import os
import io
import logging
from rasterio.warp import reproject, Resampling
from pyproj import Transformer

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
    # Log data statistics
    data_min = np.min(data)
    data_max = np.max(data)
    data_mean = np.mean(data)
    data_std = np.std(data)
    data_nonzero = np.count_nonzero(data)
    logger.info(f"Data statistics: Min={data_min:.4f}, Max={data_max:.4f}, Mean={data_mean:.4f}, StdDev={data_std:.4f}")
    logger.info(f"Non-zero values: {data_nonzero} / {data.size} ({data_nonzero/data.size*100:.2f}%)")
    
    # Handle NaN values
    data_clean = np.nan_to_num(data, nan=0.0)
    
    # Initialize RGBA arrays, default alpha = 0 (transparent)
    rgba = np.zeros((4, data.shape[0], data.shape[1]), dtype=np.uint8)
    
    # Define transparency mask: opaque outside transparent_range
    transparent_min, transparent_max = transparent_range
    logger.info(f"Using transparent range: [{transparent_min}, {transparent_max}]")
    
    opaque_mask = (data_clean < transparent_min) | (data_clean > transparent_max)
    opaque_count = np.count_nonzero(opaque_mask)
    logger.info(f"Opaque pixels: {opaque_count} / {data.size} ({opaque_count/data.size*100:.2f}%)")
    
    if np.any(opaque_mask):
        # Negative range: min_val (dark blue) to transparent_min (turquoise)
        neg_mask = (data_clean < transparent_min) & (data_clean >= min_val)
        neg_count = np.count_nonzero(neg_mask)
        logger.info(f"Negative values outside transparent range: {neg_count} / {data.size} ({neg_count/data.size*100:.2f}%)")
        
        if np.any(neg_mask):
            neg_normalized = (data_clean - min_val) / (transparent_min - min_val)  # 0 at min_val, 1 at transparent_min
            neg_normalized = np.clip(neg_normalized, 0, 1)
            rgba[0, neg_mask] = (neg_normalized[neg_mask] * (64 - 0) + 0).astype(np.uint8)  # R
            rgba[1, neg_mask] = (neg_normalized[neg_mask] * (224 - 0) + 0).astype(np.uint8)  # G
            rgba[2, neg_mask] = (neg_normalized[neg_mask] * (208 - 139) + 139).astype(np.uint8)  # B
            rgba[3, neg_mask] = 255  # A (opaque)
        
        # Positive range: transparent_max (yellow) to max_val (dark red)
        pos_mask = (data_clean > transparent_max) & (data_clean <= max_val)
        pos_count = np.count_nonzero(pos_mask)
        logger.info(f"Positive values outside transparent range: {pos_count} / {data.size} ({pos_count/data.size*100:.2f}%)")
        
        if np.any(pos_mask):
            pos_normalized = (data_clean - transparent_max) / (max_val - transparent_max)  # 0 at transparent_max, 1 at max_val
            pos_normalized = np.clip(pos_normalized, 0, 1)
            rgba[0, pos_mask] = ((1 - pos_normalized[pos_mask]) * (255 - 139) + 139).astype(np.uint8)  # R
            rgba[1, pos_mask] = ((1 - pos_normalized[pos_mask]) * (255 - 0)).astype(np.uint8)  # G
            rgba[2, pos_mask] = 0  # B
            rgba[3, pos_mask] = 255  # A (opaque)
        
        # Handle extremes beyond min_val and max_val
        extreme_neg_mask = data_clean < min_val
        extreme_neg_count = np.count_nonzero(extreme_neg_mask)
        if extreme_neg_count > 0:
            logger.info(f"Extreme negative values (< {min_val}): {extreme_neg_count}")
            rgba[0, extreme_neg_mask] = 0    # Dark Blue
            rgba[1, extreme_neg_mask] = 0
            rgba[2, extreme_neg_mask] = 139
            rgba[3, extreme_neg_mask] = 255
        
        extreme_pos_mask = data_clean > max_val
        extreme_pos_count = np.count_nonzero(extreme_pos_mask)
        if extreme_pos_count > 0:
            logger.info(f"Extreme positive values (> {max_val}): {extreme_pos_count}")
            rgba[0, extreme_pos_mask] = 139  # Dark Red
            rgba[1, extreme_pos_mask] = 0
            rgba[2, extreme_pos_mask] = 0
            rgba[3, extreme_pos_mask] = 255
        
        # Check if any pixels were made opaque
        final_opaque = np.count_nonzero(rgba[3] > 0)
        logger.info(f"Final opaque pixels in RGBA: {final_opaque} / {data.size} ({final_opaque/data.size*100:.2f}%)")
    else:
        logger.warning("No opaque pixels found; all data falls within transparent range.")
    
    return rgba

def geotiff_to_mbtiles(geotiff_path, mbtiles_path, min_zoom=0, max_zoom=14):
    logger.info(f"Starting conversion: {geotiff_path} to {mbtiles_path}")
    
    try:
        with rasterio.open(geotiff_path) as src:
            bounds = src.bounds
            logger.info(f"GeoTIFF bounds: {bounds}")
            data = src.read(1)  # Read first band (vertical speed)
            logger.info(f"GeoTIFF shape: {data.shape}")
            
            # Apply color gradient once
            logger.info("Applying color gradient with transparent range (-1, 1)")
            colored_data = apply_color_gradient(data, min_val=-3, max_val=3, transparent_range=(-1, 1))
            src_transform = src.transform
            src_crs = src.crs
            logger.info(f"Source CRS: {src_crs}")
            
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
            
            # Convert Web Mercator bounds to lon/lat for mercantile.tiles
            transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
            
            # Transform bounds from Web Mercator to WGS84
            west, south = transformer.transform(bounds.left, bounds.bottom)
            east, north = transformer.transform(bounds.right, bounds.top)
            
            logger.info(f"WGS84 bounds: west={west}, south={south}, east={east}, north={north}")
            
            # Now get tiles using WGS84 coordinates
            tiles_at_zoom = list(mercantile.tiles(west, south, east, north, zooms=[current_zoom]))
            logger.info(f"Zoom {current_zoom}: Processing {len(tiles_at_zoom)} tiles")
            active_tiles[current_zoom] = tiles_at_zoom
            
            while current_zoom <= max_zoom:
                next_level_tiles = []
                non_empty_count = 0
                
                for tile_idx, tile in enumerate(active_tiles[current_zoom]):
                    if tile_idx < 5 or tile_idx % 50 == 0:  # Log for a few tiles and then periodically
                        logger.info(f"Processing zoom {current_zoom}, tile {tile_idx+1}/{len(active_tiles[current_zoom])}: {tile}")
                    
                    tile_bounds = mercantile.bounds(tile)
                    transform = rasterio.transform.from_bounds(
                        tile_bounds.west, tile_bounds.south, tile_bounds.east, tile_bounds.north,
                        256, 256
                    )
                    
                    # Reproject pre-colored data
                    tile_data = np.zeros((4, 256, 256), dtype=np.uint8)
                    try:
                        reproject(
                            source=colored_data,
                            destination=tile_data,
                            src_transform=src_transform,
                            src_crs=src_crs,
                            dst_transform=transform,
                            dst_crs='EPSG:4326',
                            resampling=Resampling.nearest  # Try nearest neighbor instead of bilinear
                        )
                        
                        # Debug: Force some opacity for testing if no opacity found
                        if tile_idx < 3 and not np.any(tile_data[3] > 0):
                            logger.info(f"Debug: Original tile has no opacity, checking if source data has values in this region")
                            # Try to identify if source data has values in this region - convert tile bounds to source CRS
                            src_bounds = mercantile.xy_bounds(tile)
                            logger.info(f"Tile mercator bounds: {src_bounds}")
                            
                            # Check if these bounds intersect with our data
                            bounds_overlap = (
                                src_bounds.left < bounds.right and
                                src_bounds.right > bounds.left and
                                src_bounds.bottom < bounds.top and
                                src_bounds.top > bounds.bottom
                            )
                            logger.info(f"Bounds overlap: {bounds_overlap}")
                            
                            if bounds_overlap:
                                # Force some opacity for the first few debugging tiles
                                logger.info(f"Debug: Forcing opacity in center of tile {tile_idx} for testing")
                                tile_data[0, 128, 128] = 255  # Red
                                tile_data[3, 128, 128] = 255  # Alpha (opaque)
                    except Exception as e:
                        logger.error(f"Error reprojecting tile {tile_idx}: {e}")
                    
                    # Check if tile has opaque pixels
                    has_opaque = np.any(tile_data[3] > 0)
                    if tile_idx < 5:  # Log detailed info for first few tiles
                        opaque_count = np.count_nonzero(tile_data[3] > 0)
                        logger.info(f"Tile {tile_idx} opacity check: {opaque_count} opaque pixels ({opaque_count/(256*256)*100:.2f}%), has_opaque={has_opaque}")
                    
                    if has_opaque:  # Alpha channel
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
                
                # Log each zoom level
                logger.info(f"Zoom {current_zoom}: Processed {non_empty_count} non-empty tiles, skipped {skipped_count}")
                
                # Move to next zoom level
                current_zoom += 1
                if current_zoom <= max_zoom and next_level_tiles:
                    active_tiles[current_zoom] = next_level_tiles
                    logger.info(f"Zoom {current_zoom}: Will process {len(next_level_tiles)} tiles")
                else:
                    logger.info(f"No more tiles to process at higher zoom levels, stopping at zoom {current_zoom-1}")
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