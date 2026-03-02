// backend/zoning/modules/transportationDistrict.js

import fs from "fs";
import path from "path";
import booleanPointInPolygon from "@turf/boolean-point-in-polygon";
import distance from "@turf/distance";
import { point as turfPoint } from "@turf/helpers";

/**
 * TRANSPORTATION DISTRICT LOOKUP
 * -------------------------------
 * Handles local or regional transportation districts, typically from:
 *   publicData/boundaries/<city>/transportation/*.geojson
 *
 * Works for:
 *  - Boston BTD districts
 *  - Worcester DPW / traffic analysis districts
 *  - Springfield / Western Mass equivalents
 *  - ANY future city that defines transportation zones
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
 * Load all transportation district layers for **all cities**.
 *
 * Returns:
 * [
 *   { city, file, feature }
 * ]
 */
export function loadTransportationDistricts({ DATA_ROOT }) {
  const boundariesRoot = path.join(DATA_ROOT, "boundaries");
  const districts = [];

  if (!fs.existsSync(boundariesRoot)) return districts;

  const cities = fs
    .readdirSync(boundariesRoot)
    .filter((d) => fs.statSync(path.join(boundariesRoot, d)).isDirectory());

  for (const city of cities) {
    const transDir = path.join(boundariesRoot, city, "transportation");
    if (!fs.existsSync(transDir)) continue;

    const files = fs
      .readdirSync(transDir)
      .filter((f) => f.endsWith(".geojson"));

    for (const file of files) {
      const full = path.join(transDir, file);
      const features = loadGeoJSON(full);

      for (const feature of features) {
        districts.push({ city, file, feature });
      }
    }
  }

  return districts;
}

/**
 * Helper: polygon hit
 */
function polygonHit(entries, pt) {
  for (const entry of entries) {
    const geom = entry.feature.geometry;
    if (!geom) continue;

    if (geom.type === "Polygon" || geom.type === "MultiPolygon") {
      if (booleanPointInPolygon(pt, entry.feature)) {
        return entry;
      }
    }
  }
  return null;
}

/**
 * Helper: nearest entry point for non-polygon geometries
 */
function nearestEntry(entries, pt) {
  let best = null;
  let bestDist = Infinity;

  for (const entry of entries) {
    const geom = entry.feature.geometry;
    if (!geom) continue;

    let targetPoint = null;

    if (geom.type === "Point") {
      targetPoint = entry.feature;
    } else if (geom.type === "LineString") {
      if (geom.coordinates?.[0]) {
        targetPoint = turfPoint(geom.coordinates[0]);
      }
    } else if (geom.type === "MultiLineString") {
      if (geom.coordinates?.[0]?.[0]) {
        targetPoint = turfPoint(geom.coordinates[0][0]);
      }
    } else if (geom.type === "Polygon") {
      if (geom.coordinates?.[0]?.[0]) {
        targetPoint = turfPoint(geom.coordinates[0][0]);
      }
    } else if (geom.type === "MultiPolygon") {
      if (geom.coordinates?.[0]?.[0]?.[0]) {
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

  if (!best) return null;

  return {
    entry: best,
    distanceKm: bestDist,
    distanceMeters: bestDist * 1000,
  };
}

/**
 * Main lookup
 *
 * @returns:
 * {
 *   inDistrict: boolean,
 *   district: { city, attrs, feature } | null,
 *   nearestDistrict: {
 *      city,
 *      distanceKm,
 *      distanceMeters,
 *      attrs,
 *      feature
 *   } | null
 * }
 */
export function transportationDistrictLookup(districts, pt) {
  if (!districts || districts.length === 0) {
    return { inDistrict: false, district: null, nearestDistrict: null };
  }

  // Try polygon hit first
  const inside = polygonHit(districts, pt);

  // Then nearest corridor (if applicable)
  const nearest = nearestEntry(districts, pt);

  return {
    inDistrict: !!inside,
    district: inside
      ? {
          city: inside.city,
          attrs: inside.feature.properties || {},
          feature: inside.feature,
        }
      : null,
    nearestDistrict: nearest
      ? {
          city: nearest.entry.city,
          distanceKm: nearest.distanceKm,
          distanceMeters: nearest.distanceMeters,
          attrs: nearest.entry.feature.properties || {},
          feature: nearest.entry.feature,
        }
      : null,
  };
}
