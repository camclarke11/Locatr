const MAP_STYLE = {
    version: 8,
    name: 'Dark Grey',
    glyphs: 'https://api.maptiler.com/fonts/{fontstack}/{range}.pbf?key=YOUR_MAPTILER_KEY',
    sources: {
        'simple-tiles': {
            type: 'vector',
            tiles: ['https://api.maptiler.com/tiles/v3/{z}/{x}/{y}.pbf?key=YOUR_MAPTILER_KEY'],
            minzoom: 0,
            maxzoom: 14
        }
    },
    layers: [
        // Background
        {
            id: 'background',
            type: 'background',
            paint: {
                'background-color': '#f5f0d9'  // Vintage cream/parchment background
            }
        },
        // Water
        {
            id: 'water',
            type: 'fill',
            source: 'simple-tiles',
            'source-layer': 'water',
            paint: {
                'fill-color': '#e8e0c0'  // Slightly darker cream for water
            }
        },
        // Parks and green areas
        {
            id: 'landuse_park',
            type: 'fill',
            source: 'simple-tiles',
            'source-layer': 'landuse',
            filter: ['==', 'class', 'park'],
            paint: {
                'fill-color': '#ede5c9',  // Slightly darker cream for parks
                'fill-opacity': 0.7
            }
        },
        // Roads - minor
        {
            id: 'roads_minor',
            type: 'line',
            source: 'simple-tiles',
            'source-layer': 'transportation',
            filter: ['all', ['!=', 'class', 'motorway'], ['!=', 'class', 'trunk'], ['!=', 'class', 'primary']],
            paint: {
                'line-color': '#d9d0b3',  // Light tan for minor roads
                'line-width': 0.5
            }
        },
        // Roads - major
        {
            id: 'roads_major',
            type: 'line',
            source: 'simple-tiles',
            'source-layer': 'transportation',
            filter: ['in', 'class', 'motorway', 'trunk', 'primary'],
            paint: {
                'line-color': '#c5b99c',  // Darker tan for major roads
                'line-width': 1
            }
        },
        // Area labels (neighborhoods, districts)
        {
            id: 'area-labels',
            type: 'symbol',
            source: 'simple-tiles',
            'source-layer': 'place',
            filter: ['in', ['get', 'class'], ['literal', ['suburb', 'district', 'neighbourhood']]],
            layout: {
                'text-field': ['get', 'name'],
                'text-font': ['Noto Sans Mono Regular'],
                'text-size': 12,
                'text-transform': 'uppercase',
                'text-letter-spacing': 0.1,
                'text-max-width': 7,
                'text-allow-overlap': false
            },
            paint: {
                'text-color': '#000000',  // Black text
                'text-halo-color': 'rgba(245, 240, 217, 0.8)',  // Cream halo
                'text-halo-width': 1
            }
        },
        // Major place labels (cities, major areas)
        {
            id: 'major-labels',
            type: 'symbol',
            source: 'simple-tiles',
            'source-layer': 'place',
            filter: ['in', ['get', 'class'], ['literal', ['city', 'town']]],
            layout: {
                'text-field': ['get', 'name'],
                'text-font': ['Noto Sans Mono Regular'],
                'text-size': 14,
                'text-transform': 'uppercase',
                'text-letter-spacing': 0.1,
                'text-max-width': 7,
                'text-allow-overlap': false
            },
            paint: {
                'text-color': '#000000',  // Black text
                'text-halo-color': 'rgba(245, 240, 217, 0.8)',  // Cream halo
                'text-halo-width': 1
            }
        },
        {
            'id': '3d-buildings',
            'source': 'simple-tiles',
            'source-layer': 'building',
            'type': 'fill-extrusion',
            'minzoom': 10,
            'filter': ['!=', ['get', 'hide_3d'], true],
            'paint': {
                'fill-extrusion-color': '#8EEDC7',  // Mint green color for buildings
                'fill-extrusion-opacity': 0.4,  
                'fill-extrusion-height': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    13,
                    0,
                    15,
                    ['get', 'render_height']
                ],
                'fill-extrusion-base': ['case',
                    ['>=', ['get', 'zoom'], 13],
                    ['get', 'render_min_height'], 0
                ]
            }
        },
        // Place labels
        {
            id: 'place_labels',
            type: 'symbol',
            source: 'simple-tiles',
            'source-layer': 'place',
            layout: {
                'text-field': ['get', 'name'],
                'text-font': ['Noto Sans Mono Regular'],
                'text-size': 14,
                'text-transform': 'uppercase',
                'text-letter-spacing': 0.1,
                'text-max-width': 7,
                'text-allow-overlap': false
            },
            paint: {
                'text-color': '#000000',  // Black text
                'text-halo-color': 'rgba(245, 240, 217, 0.8)',  // Cream halo
                'text-halo-width': 1
            }
        },
    ]
};

// Function to fetch actions from the API
async function fetchTrainActions() {
    try {
        const response = await fetch('https://api.londonunderground.live/get_actions_for_all_trains');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        return data.actions;
    } catch (error) {
        console.error('Error fetching train actions:', error);
        return null;
    }
}

// Cache for detailed paths between stations
let detailedPathsCache = null;
// Cache for station coordinates (ID -> [longitude, latitude])
let stationCoordinatesCache = null;

// Function to load and parse the detailed paths from CSV
async function loadDetailedPaths() {
    // Return cached data if already loaded
    if (detailedPathsCache && stationCoordinatesCache) {
        return {
            paths: detailedPathsCache,
            stationCoordinates: stationCoordinatesCache
        };
    }
    
    try {
        const response = await fetch('4_tube_connections_with_paths.csv');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const csvText = await response.text();
        const lines = csvText.split('\n');
        const headers = lines[0].split(',');
        
        // Temporary structure to identify multiple paths between stations
        const pathsByStations = {};
        // Final paths map
        const pathsMap = {};
        // Create a map of station_id -> [longitude, latitude]
        const stationCoordinates = {};
        
        // First pass: collect all paths by station pairs to identify duplicates
        for (let i = 1; i < lines.length; i++) {
            if (!lines[i].trim()) continue;
            
            // Handle CSV parsing with potential commas inside quoted fields
            const values = [];
            let inQuotes = false;
            let currentValue = '';
            
            for (let j = 0; j < lines[i].length; j++) {
                const char = lines[i][j];
                
                if (char === '"') {
                    inQuotes = !inQuotes;
                } else if (char === ',' && !inQuotes) {
                    values.push(currentValue);
                    currentValue = '';
                } else {
                    currentValue += char;
                }
            }
            
            values.push(currentValue); // Add the last value
            
            const lineId = values[0];
            const fromStationId = values[1];
            const toStationId = values[2];
            const lineName = lineId// Assuming line name is in the 5th column
            
            // Parse the path from the JSON string
            let path;
            try {
                // The path is in the 4th column (index 3)
                path = JSON.parse(values[3]);
            } catch (e) {
                console.error(`Error parsing path for ${fromStationId} to ${toStationId}:`, e);
                continue;
            }
            
            // Create keys for both directions
            const forwardKey = `${fromStationId}-${toStationId}`;
            const reverseKey = `${toStationId}-${fromStationId}`;
            
            // Initialize if needed
            if (!pathsByStations[forwardKey]) {
                pathsByStations[forwardKey] = {};
            }
            if (!pathsByStations[reverseKey]) {
                pathsByStations[reverseKey] = {};
            }
            
            // Store the path and line
            pathsByStations[forwardKey][lineName] = path;
            // For the reverse direction, reverse the path
            pathsByStations[reverseKey][lineName] = [...path].reverse();
            
            // Store station coordinates if we have a valid path
            if (path && path.length > 0) {
                // Store the first coordinate for the 'from' station if not already stored
                if (!stationCoordinates[fromStationId]) {
                    stationCoordinates[fromStationId] = path[0]; // [longitude, latitude]
                }
                
                // Store the last coordinate for the 'to' station if not already stored
                if (!stationCoordinates[toStationId]) {
                    stationCoordinates[toStationId] = path[path.length - 1]; // [longitude, latitude]
                }
            }
        }
        
        // Second pass: create the final paths map based on single/multiple path detection
        for (const [stationPair, linePaths] of Object.entries(pathsByStations)) {
            const lineNames = Object.keys(linePaths);
            
            if (lineNames.length === 1) {
                // If there's only one path between stations, use simple key
                pathsMap[stationPair] = linePaths[lineNames[0]];
            } else {
                // If there are multiple paths, create line-specific keys
                for (const [lineName, path] of Object.entries(linePaths)) {
                    pathsMap[`${stationPair}-${lineName}`] = path;
                }
            }
        }
        
        // Save to cache
        detailedPathsCache = pathsMap;
        stationCoordinatesCache = stationCoordinates;
        
        console.log(`Loaded ${Object.keys(pathsMap).length} paths (with ${Object.keys(pathsByStations).length} station pairs)`);
        console.log(`Built coordinates cache for ${Object.keys(stationCoordinatesCache).length} stations`);
        
        return {
            paths: pathsMap,
            stationCoordinates: stationCoordinates
        };
    } catch (error) {
        console.error('Error loading detailed paths:', error);
        return {
            paths: {},
            stationCoordinates: {}
        };
    }
}

// Calculate distance between two points
function calculateDistance(point1, point2) {
    const [lon1, lat1] = point1;
    const [lon2, lat2] = point2;
    
    // Simple Euclidean distance - sufficient for our interpolation needs
    return Math.sqrt(Math.pow(lon2 - lon1, 2) + Math.pow(lat2 - lat1, 2));
}

// Function to convert server actions to trip paths with detailed routing
async function convertActionsToPaths(actions) {
    if (!actions) {
        console.log("No actions provided to convert");
        return [];
    }
    
    try {
        // Load the detailed paths and station coordinates
        const { paths: detailedPaths, stationCoordinates } = await loadDetailedPaths();
        
        const paths = [];
        const failedPaths = [];
        
        for (const [line_name, trains] of Object.entries(actions)) {
            console.log(`Processing line ${line_name}, ${Object.keys(trains).length} trains`);
            for (const [train_id, trainActions] of Object.entries(trains)) {
                // Skip if no actions
                if (!trainActions || trainActions.length < 2) continue;

                let combinedPath = [];
                let combinedTimestamps = [];
                let renderTrain = true;

                // Process each action
                for (let i = 0; i < trainActions.length - 1; i++) {
                    const currentAction = trainActions[i];

                    const fromStationId = currentAction.from_stop_id;
                    const toStationId = currentAction.to_stop_id;
                    // Skip if same station (train is stopped)
                    if (fromStationId === toStationId) {
                        // Use station coordinates 
                        combinedPath.push(stationCoordinates[fromStationId]);
                        combinedTimestamps.push(currentAction.from_timestamp);
                        combinedPath.push(stationCoordinates[fromStationId]);
                        combinedTimestamps.push(currentAction.to_timestamp);
                        continue;
                    }

                    // Get the detailed path between these stations
                    const basePathKey = `${fromStationId}-${toStationId}`;
                    const linePathKey = `${basePathKey}-${line_name}`;
                    const pathKeyWords = `${tubeStationsData[fromStationId].name} -> ${tubeStationsData[toStationId].name}`;
                    
                    // Try to get the line-specific path first, fall back to common path
                    let detailedPath = detailedPaths[linePathKey] || detailedPaths[basePathKey];
                    
                    // Check if detailed path is undefined
                    if (!detailedPath) {
                        failedPaths.push(pathKeyWords);
                        renderTrain = false;
                        break;
                    }
                    
                    // Calculate total distance of the path
                    let totalDistance = 0;
                    for (let j = 0; j < detailedPath.length - 1; j++) {
                        totalDistance += calculateDistance(detailedPath[j], detailedPath[j + 1]);
                    }
                    
                    // Calculate timestamps for each point in the path
                    const startTime = currentAction.from_timestamp;
                    const endTime = currentAction.to_timestamp;
                    const totalTime = endTime - startTime;
                    
                    let cumulativeDistance = 0;
                    
                    // Add each point in the detailed path with interpolated timestamps
                    for (let j = 0; j < detailedPath.length; j++) {
                        combinedPath.push(detailedPath[j]);
                        
                        if (j === 0) {
                            // First point gets the start time
                            combinedTimestamps.push(startTime);
                        } else if (j === detailedPath.length - 1) {
                            // Last point gets the end time
                            combinedTimestamps.push(endTime);
                        } else {
                            // Calculate distance from start to this point
                            cumulativeDistance += calculateDistance(detailedPath[j - 1], detailedPath[j]);
                            
                            // Interpolate timestamp based on distance proportion
                            const proportion = cumulativeDistance / totalDistance;
                            const interpolatedTime = startTime + Math.round(proportion * totalTime);
                            combinedTimestamps.push(interpolatedTime);
                        }
                    }
                }

                if (combinedPath.length >= 2 && renderTrain) {
                    paths.push({
                        line_name,
                        train_id,
                        path: combinedPath,
                        timestamps: combinedTimestamps,
                        actions: trainActions
                    });
                }
            }
        }
        console.log(`Failed paths: ${failedPaths.length}`);
        
        console.log(`Generated ${paths.length} total paths with detailed routing`);
        return paths;
    } catch (error) {
        console.error("Error in convertActionsToPaths:", error);
        return []; // Return empty array in case of error
    }
}

// Export functions
window.TrainTrips = {
    fetchTrainActions,
    convertActionsToPaths
}; 