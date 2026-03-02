// backend/zoning/modules/waterProximity.js

import fs from "fs";
import path from "path";
import booleanPointInPolygon from "@turf/boolean-point-in-polygon";
import distance from "@turf/distance";
import { point as turfPoint } from "@turf/helpers";

/**
 * WATER PROXIMITY MODULE
 * ----------------------
 * Handles:
 *  - detecting if a point is ON water
 *  - finding nearest water body (for "waterfront" style signals)
 *
 * Folder convention:
 * publicData/boundaries/<city>/water/*.geojson
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

/**
 * Load water polygons (and lines, defensively) for ALL cities.
 *
 * Returns:
 * [
 *   { city, file, feature }
 * ]
 */
export function loadWaterLayers({ DATA_ROOT }) {
  const boundariesRoot = path.join(DATA_ROOT, "boundaries");
  const waterLayers = [];

  if (!fs.existsSync(boundariesRoot)) return waterLayers;

  const cities = fs
    .readdirSync(boundariesRoot)
    .filter((dir) => fs.statSync(path.join(boundariesRoot, dir)).isDirectory());

  for (const city of cities) {
    const waterDir = path.join(boundariesRoot, city, "water");
    if (!fs.existsSync(waterDir)) continue;

    const files = fs
      .readdirSync(waterDir)
      .filter((f) => f.endsWith(".geojson"));

    for (const file of files) {
      const full = path.join(waterDir, file);
      const features = loadGeoJSON(full);

      for (const feature of features) {
        waterLayers.push({ city, file, feature });
      }
    }
  }

  return waterLayers;
}

/**
 * Helper: first polygon hit (strict on-water test).
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
 * Helper: nearest water polygon/line center point.
 */
function nearestWater(entries, pt) {
  let best = null;
  let bestDist = Infinity;

  for (const entry of entries) {
    const geom = entry.feature.geometry;
    if (!geom) continue;

    let targetPoint = null;

    if (geom.type === "Point") {
      targetPoint = entry.feature;
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
    } else if (geom.type === "LineString") {
      if (geom.coordinates && geom.coordinates[0]) {
        targetPoint = turfPoint(geom.coordinates[0]);
      }
    } else if (geom.type === "MultiLineString") {
      if (geom.coordinates && geom.coordinates[0] && geom.coordinates[0][0]) {
        targetPoint = turfPoint(geom.coordinates[0][0]);
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
 * Water proximity lookup.
 *
 * @param {Array} waterLayers    Output of loadWaterLayers()
 * @param {Feature<Point>} pt    Turf point([lon, lat])
 *
 * Returns:
 * {
 *   onWater: boolean,
 *   waterFeature: {
 *     city,
 *     attrs,
 *     feature
 *   } | null,
 *   nearestWater: {
 *     city,
 *     distanceKm,
 *     distanceMeters,
 *     attrs,
 *     feature
 *   } | null
 * }
 */
export function waterProximityLookup(waterLayers, pt) {
  if (!waterLayers || waterLayers.length === 0) {
    return { onWater: false, waterFeature: null, nearestWater: null };
  }

  const onWaterEntry = firstPolygonHit(waterLayers, pt);
  const nearest = nearestWater(waterLayers, pt);

  return {
    onWater: !!onWaterEntry,
    waterFeature: onWaterEntry
      ? {
          city: onWaterEntry.city,
          attrs: onWaterEntry.feature.properties || {},
          feature: onWaterEntry.feature,
        }
      : null,
    nearestWater: nearest
      ? {
          city: nearest.entry.city,
          distanceKm: nearest.distanceKm,
          distanceMeters: nearest.distanceKm * 1000,
          attrs: nearest.entry.feature.properties || {},
          feature: nearest.entry.feature,
        }
      : null,
  };
}
