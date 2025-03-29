# AROME Vertical Velocity Data Processing Pipeline

This repository contains scripts to process AROME vertical velocity data for weather forecasting and analysis. The pipeline fetches data from the Météo-France AROME API, processes it, and converts it to MBTiles format for easy visualization in QGIS and other GIS applications.

## Features

- Fetch vertical velocity data from the AROME API for various pressure levels and hours
- Warp GeoTIFF data to Web Mercator projection (EPSG:3857)
- Convert to MBTiles format with custom styling for visualization
- Support for batch processing multiple hours and pressure levels
- Parallel processing for improved performance
- Automated daily execution via GitHub Actions

## Quick Start

### Manual Execution

To run the processing pipeline manually:

```bash
# Process data for a specific hour and pressure level
python scripts/test_process.py --pressure 850 --hour 12 --output-dir ./output

# Process all hours (07:00-21:00) and pressure levels (500-900 hPa)
python scripts/process_all.py --output-dir ./output --parallel 2
```

### Options

- `--output-dir` - Directory to save output files (default: ./output)
- `--min-zoom` - Minimum zoom level for MBTiles (default: 4)
- `--max-zoom` - Maximum zoom level for MBTiles (default: 10)
- `--parallel` - Number of parallel processes to use (0 for sequential, default: 2)
- `--force` - Force regeneration of existing files
- `--no-skip` - Don't skip existing files

## Setup

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Ensure you have the necessary API credentials (API key is included in the scripts)

## Automated Processing with GitHub Actions

This repository includes a GitHub Actions workflow that automatically runs the data processing pipeline every day at 1:30 AM UTC. The workflow:

1. Checks out the repository
2. Sets up Python
3. Installs dependencies
4. Creates the output directory
5. Runs the processing script
6. Uploads the generated MBTiles files as artifacts

### Manual Trigger

You can also manually trigger the workflow from the GitHub Actions tab in the repository by clicking "Run workflow" on the "Daily AROME Data Processing" workflow.

### Accessing the Processed Data

After the workflow runs, you can download the MBTiles files from the "Artifacts" section of the completed workflow run. The files are stored for 3 days.

## Data Usage

### Using MBTiles in QGIS

1. Install the 'QuickMapServices' plugin in QGIS if not already installed
2. Go to Web > QuickMapServices > Settings > More services > Add
3. Browse to the MBTiles file from the output directory
4. The MBTiles layer will appear in the QuickMapServices menu

## Structure

- `scripts/process_all.py` - Main script for batch processing
- `scripts/test_process.py` - Script for testing a single hour/pressure level
- `scripts/fetch.py` - Module for fetching data from the AROME API
- `scripts/warp.py` - Module for warping GeoTIFF to Web Mercator
- `scripts/geotiff2mbtiles.py` - Module for converting GeoTIFF to MBTiles
- `.github/workflows/daily-data-processing.yml` - GitHub Actions workflow 