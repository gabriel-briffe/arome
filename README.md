# AROME Weather Data Processing

This repository processes AROME (Application of Research to Operations at MEsoscale) weather model data from Météo-France.

## Overview

The repository automatically downloads and processes high-resolution weather data from the AROME numerical weather prediction model, focusing on vertical velocity and wind components.

## Data Sources

- **Provider**: Météo-France AROME model
- **Resolution**: 0.025° (~2.5km) horizontal resolution
- **Coverage**: France and surrounding areas
- **Forecast Range**: 0-51 hours
- **Update Frequency**: Daily

## Daily Data Processing

The repository includes GitHub Actions workflows that automatically process AROME data daily:

### Vertical Velocity Processing
- **Workflow**: `.github/workflows/daily-data-processing.yml`
- **Schedule**: Runs at 1:30 AM UTC daily
- **Process**: Downloads vertical velocity data, processes it, and creates MBTiles
- **Release**: Creates GitHub releases with processed data

### Wind Components Download
- **Workflow**: `.github/workflows/daily-wind-components.yml`
- **Schedule**: Runs at 3:15 AM UTC daily
- **Process**: Downloads raw U and V wind component TIFF files for today only and splits them into 4 geographic regions
- **Release**: Adds split regional TIFF files to the same daily release as vertical velocity data
- **Regions**: South (37.5-41°N), MiddleWest (41-48.5°N west of 4°E), MiddleEast (41-48.5°N east of 4°E), North (48.5-55.4°N)
- **Resumable**: Checks each split file individually and skips uploads if files already exist
- **Validation**: Only uploads files that are valid TIFFs (>1MB, proper format)

## File Naming Convention

### Processed Files (MBTiles)
```
arome_vv_{source_date}_{target_date}_{hour}_{pressure}.mbtiles
```

### Raw Wind Component Files (TIFF)
```
# Regional split files (4 regions per component):
arome_u_South_{source_date}_{target_date}_{hour}_{pressure}.tiff      # South region (37.5-41°N)
arome_u_MiddleWest_{source_date}_{target_date}_{hour}_{pressure}.tiff # Middle West (41-48.5°N, <4°E)
arome_u_MiddleEast_{source_date}_{target_date}_{hour}_{pressure}.tiff # Middle East (41-48.5°N, >4°E)
arome_u_North_{source_date}_{target_date}_{hour}_{pressure}.tiff      # North region (48.5-55.4°N)

arome_v_South_{source_date}_{target_date}_{hour}_{pressure}.tiff      # V component, South region
arome_v_MiddleWest_{source_date}_{target_date}_{hour}_{pressure}.tiff # V component, Middle West
arome_v_MiddleEast_{source_date}_{target_date}_{hour}_{pressure}.tiff # V component, Middle East
arome_v_North_{source_date}_{target_date}_{hour}_{pressure}.tiff      # V component, North region
```

Where:
- `source_date`: Reference/analysis date (YYYY-MM-DD)
- `target_date`: Forecast target date (YYYY-MM-DD)
- `hour`: Forecast hour (05-21 UTC)
- `pressure`: Pressure level (500, 600, 700, 800, 900 hPa)

## Data Parameters

- **Pressure Levels**: 500, 600, 700, 800, 900 hPa
- **Forecast Hours**: 05:00 to 21:00 UTC
- **Forecast Days**: Today (day 0) and tomorrow (day 1)
- **Total Files per Day**: ~340 processed files + ~340 raw wind component files

## API Access

Data is accessed via Météo-France's AROME WCS (Web Coverage Service) API:
- **Base URL**: `https://public-api.meteofrance.fr/public/arome/1.0/wcs/`
- **Authentication**: API key required
- **Rate Limits**: Implemented with exponential backoff

## Dependencies

- Python 3.12+
- GDAL/Rasterio
- PyProj
- Requests
- NumPy
- Pillow
- SciPy

## Local Development

See `devtest/README.md` for development tools and test data.

## License

Open Licence 1.0 (etalab.gouv.fr)
