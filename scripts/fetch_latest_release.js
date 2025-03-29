/**
 * GitHub Release MBTiles Fetcher
 * 
 * This script demonstrates how to fetch MBTiles files from GitHub Releases
 * in a web application.
 */

// Configuration
const REPO_OWNER = 'gabriel-briffe';
const REPO_NAME = 'arome';
const API_URL = `https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/releases`;

/**
 * Fetch the latest release data from GitHub
 * @returns {Promise<Object>} The release data
 */
async function fetchLatestRelease() {
  try {
    const response = await fetch(`${API_URL}/latest`);
    if (!response.ok) {
      throw new Error(`Failed to fetch latest release: ${response.status} ${response.statusText}`);
    }
    return await response.json();
  } catch (error) {
    console.error('Error fetching latest release:', error);
    throw error;
  }
}

/**
 * Get a specific release by date
 * @param {string} date - Date in YYYY-MM-DD format
 * @returns {Promise<Object>} The release data
 */
async function fetchReleaseByDate(date) {
  try {
    // First get all releases
    const response = await fetch(API_URL);
    if (!response.ok) {
      throw new Error(`Failed to fetch releases: ${response.status} ${response.statusText}`);
    }
    
    const releases = await response.json();
    
    // Find the release with the matching tag
    const tagName = `arome-${date}`;
    const release = releases.find(r => r.tag_name === tagName);
    
    if (!release) {
      throw new Error(`No release found for date: ${date}`);
    }
    
    return release;
  } catch (error) {
    console.error(`Error fetching release for date ${date}:`, error);
    throw error;
  }
}

/**
 * Get all MBTiles files from a release
 * @param {Object} releaseData - The release data from GitHub API
 * @returns {Array<Object>} Array of file objects with name and url
 */
function getMBTilesFromRelease(releaseData) {
  return releaseData.assets
    .filter(asset => asset.name.endsWith('.mbtiles'))
    .map(asset => ({
      name: asset.name,
      url: asset.browser_download_url,
      size: asset.size,
      created_at: asset.created_at
    }));
}

/**
 * Get MBTiles file URLs for a specific pressure level
 * @param {Array<Object>} mbtiles - Array of MBTiles file objects
 * @param {number} pressure - Pressure level (e.g., 850)
 * @returns {Array<Object>} Filtered array of MBTiles for the specified pressure
 */
function getMBTilesForPressure(mbtiles, pressure) {
  const pressureStr = pressure.toString();
  return mbtiles.filter(file => file.name.includes(`_${pressureStr}.mbtiles`));
}

/**
 * Get MBTiles file URLs for a specific hour
 * @param {Array<Object>} mbtiles - Array of MBTiles file objects
 * @param {number} hour - Hour (0-23)
 * @returns {Array<Object>} Filtered array of MBTiles for the specified hour
 */
function getMBTilesForHour(mbtiles, hour) {
  const hourStr = hour.toString().padStart(2, '0');
  return mbtiles.filter(file => file.name.includes(`_${hourStr}_`));
}

/**
 * Example usage in a web application
 */
async function loadAROMEDataForDate(date = null) {
  try {
    // Get release data (latest or for specific date)
    const releaseData = date 
      ? await fetchReleaseByDate(date)
      : await fetchLatestRelease();
    
    console.log(`Loaded release: ${releaseData.name}`);
    
    // Get all MBTiles from the release
    const allMBTiles = getMBTilesFromRelease(releaseData);
    console.log(`Found ${allMBTiles.length} MBTiles files`);
    
    // Example: Filter for a specific pressure level and hour
    const pressure850MBTiles = getMBTilesForPressure(allMBTiles, 850);
    const hour12MBTiles = getMBTilesForHour(allMBTiles, 12);
    
    console.log(`Files for 850 hPa: ${pressure850MBTiles.length}`);
    console.log(`Files for 12:00: ${hour12MBTiles.length}`);
    
    // Return all the data
    return {
      releaseData,
      allMBTiles,
      pressure850MBTiles,
      hour12MBTiles
    };
  } catch (error) {
    console.error('Failed to load AROME data:', error);
    throw error;
  }
}

// Example of how to use this code in a web application:
/*
document.addEventListener('DOMContentLoaded', async () => {
  try {
    // Get today's date in YYYY-MM-DD format
    const today = new Date().toISOString().split('T')[0];
    
    // Try to load today's data or fall back to latest
    try {
      const data = await loadAROMEDataForDate(today);
      console.log(`Loaded data for ${today}`);
    } catch (error) {
      console.warn(`No data for ${today}, falling back to latest`);
      const data = await loadAROMEDataForDate();
    }
    
    // Use the data to populate your UI
    // ...
  } catch (error) {
    console.error('Error loading AROME data:', error);
    // Show error in UI
  }
});
*/ 