// backend/zoning/modules/snowParking.js

import fs from "fs";
import path from "path";
import booleanPointInPolygon from "@turf/boolean-point-in-polygon";
import distance from "@turf/distance";
import { point as turfPoint } from "@turf/helpers";

/**
 * SNOW PARKING MODULE
 * -------------------
 * Handles:
 *  - snow parking zones (polygons or line-based corridors)
 *  - snow emergency routes (usually LineString / MultiLineString)
 *
 * Folder convention:
 * publicData/boundaries/<city>/snow/*.geojson
 *
 * We classify files by filename patterns:
 *  - "*route*"  -> routes
 *  - everything else -> parking zones
 */

const loadGeoJSON = (filepath) => {
  try {
    const raw = fs.readFileSync(filepath, "utf8");
    const json = JSON.parse(raw);
    if (!json.features) return [];
    return json.features.map((f) => ({
      ...f,
      _source: filepath,
    }));
  } catch (err) {
    return [];
  }
};

function isRouteFile(filename) {
  const lower = filename.toLowerCase();
  return lower.includes("route") || lower.includes("corridor");
}

/**
 * Load snow layers for ALL cities.
 *
 * Returns:
 * {
 *   parking: [ { city, file, feature } ],
 *   routes:  [ { city, file, feature } ]
 * }
 */
export function loadSnowLayers({ DATA_ROOT }) {
  const boundariesRoot = path.join(DATA_ROOT, "boundaries");

  const snowLayers = {
    parking: [],
    routes: [],
  };

  if (!fs.existsSync(boundariesRoot)) {
    return snowLayers;
  }

  const cities = fs
    .readdirSync(boundariesRoot)
    .filter((dir) => fs.statSync(path.join(boundariesRoot, dir)).isDirectory());

  for (const city of cities) {
    const snowDir = path.join(boundariesRoot, city, "snow");
    if (!fs.existsSync(snowDir)) continue;

    const files = fs
      .readdirSync(snowDir)
      .filter((f) => f.endsWith(".geojson"));

    for (const file of files) {
      const full = path.join(snowDir, file);
      const features = loadGeoJSON(full);

      for (const feature of features) {
        const entry = { city, file, feature };
        if (isRouteFile(file)) {
          snowLayers.routes.push(entry);
        } else {
          snowLayers.parking.push(entry);
        }
      }
    }
  }

  return snowLayers;
}

/**
 * Helper: First polygon hit among parking entries.
 */
function firstPolygonHit(entries, pt) {
  for (const entry of entries) {
    const geom = entry.feature.geometry;
    if (!geom) continue;

    if (geom.type === "Polygon" || geom.type === "MultiPolygon") {
      const hit = booleanPointInPolygon(pt, entry.feature);
      if (hit) {
        return entry;
      }
    }
  }
  return null;
}

/**
 * Helper: nearest feature among any geometry (LineString, Polygon, etc)
 */
function nearestFeature(pt, entries) {
  let best = null;
  let bestDist = Infinity;

  for (const entry of entries) {
    const geom = entry.feature.geometry;
    if (!geom) continue;

    let targetPoint = null;

    if (geom.type === "Point") {
      targetPoint = entry.feature;
    } else if (geom.type === "LineString") {
      if (geom.coordinates && geom.coordinates[0]) {
        targetPoint = turfPoint(geom.coordinates[0]);
      }
    } else if (geom.type === "MultiLineString") {
      if (geom.coordinates && geom.coordinates[0] && geom.coordinates[0][0]) {
        targetPoint = turfPoint(geom.coordinates[0][0]);
      }
    } else if (geom.type === "Polygon") {
      if (geom.coordinates && geom.coordinates[0] && geom.coordinates[0][0]) {
        targetPoint = turfPoint(geom.coordinates[0][0]);
      }
    } else if (geom.type === "MultiPolygon") {
      if (
        geom.coordinates &&
        geom.coordinates[0] &&
        geom.coordinates[0][0] &&
        geom.coordinates[0][0][0]
      ) {
        targetPoint = turfPoint(geom.coordinates[0][0][0]);
      }
    }

    if (!targetPoint) continue;

    const d = distance(pt, targetPoint); // km
    if (d < bestDist) {
      bestDist = d;
      best = entry;
    }
  }

  if (!best || bestDist === Infinity) return null;
  return { entry: best, distanceKm: bestDist };
}

/**
 * Snow lookup:
 *
 * @param {Object} snowLayers    Output of loadSnowLayers()
 * @param {Feature<Point>} pt    Turf point([lon, lat])
 *
 * Returns:
 * {
 *   inParkingZone: boolean,
 *   parkingZone: {
 *      city, distanceKm|null, distanceMeters|null, attrs, feature
 *   } | null,
 *   inSnowRouteCorridor: boolean,
 *   snowRoute: {
 *      city, distanceKm, distanceMeters, attrs, feature
 *   } | null
 * }
 */
export function snowLookup(snowLayers, pt) {
  // Parking polygons first
  let parkingEntry = firstPolygonHit(snowLayers.parking, pt);
  let inParkingZone = !!parkingEntry;
  let parkingDistanceKm = null;

  // Fallback: nearest parking (if no polygon hit)
  if (!parkingEntry && snowLayers.parking.length > 0) {
    const nearestParking = nearestFeature(pt, snowLayers.parking);
    const PARKING_BUFFER_KM = 0.03; // ~30m buffer
    if (nearestParking && nearestParking.distanceKm <= PARKING_BUFFER_KM) {
      parkingEntry = nearestParking.entry;
      parkingDistanceKm = nearestParking.distanceKm;
      inParkingZone = true;
    }
  }

  // Routes: treated as corridors with distance buffer
  let snowRouteInfo = null;
  let inSnowRouteCorridor = false;

  if (snowLayers.routes.length > 0) {
    const nearestRoute = nearestFeature(pt, snowLayers.routes);
    const ROUTE_BUFFER_KM = 0.05; // ~50m

    if (nearestRoute && nearestRoute.distanceKm <= ROUTE_BUFFER_KM) {
      snowRouteInfo = {
        city: nearestRoute.entry.city,
        distanceKm: nearestRoute.distanceKm,
        distanceMeters: nearestRoute.distanceKm * 1000,
        attrs: nearestRoute.entry.feature.properties || {},
        feature: nearestRoute.entry.feature,
      };
      inSnowRouteCorridor = true;
    }
  }

  return {
    inParkingZone,
    parkingZone: parkingEntry
      ? {
          city: parkingEntry.city,
          distanceKm: parkingDistanceKm,
          distanceMeters: parkingDistanceKm
            ? parkingDistanceKm * 1000
            : null,
          attrs: parkingEntry.feature.properties || {},
          feature: parkingEntry.feature,
        }
      : null,
    inSnowRouteCorridor,
    snowRoute: snowRouteInfo,
  };
}
