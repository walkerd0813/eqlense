// backend/zoning/modules/openSpaceLookup.js

import fs from "fs";
import path from "path";
import booleanPointInPolygon from "@turf/boolean-point-in-polygon";
import distance from "@turf/distance";
import { point as turfPoint } from "@turf/helpers";

/**
 * OPEN SPACE LOOKUP MODULE
 * ------------------------
 * Handles:
 *  - Public open space
 *  - Parks, greenways, urban wilds
 *
 * NOTE:
 *  - Not every city will have open-space layers.
 *  - This module is designed to safely no-op when folders/files are missing.
 *
 * Folder convention:
 *   publicData/boundaries/<city>/openSpace/*.geojson
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
 * Load open space polygons for ALL cities that have them.
 *
 * Returns:
 * [
 *   { city, file, feature }
 * ]
 */
export function loadOpenSpaceLayers({ DATA_ROOT }) {
  const boundariesRoot = path.join(DATA_ROOT, "boundaries");
  const openSpaces = [];

  if (!fs.existsSync(boundariesRoot)) return openSpaces;

  const cities = fs
    .readdirSync(boundariesRoot)
    .filter((dir) => fs.statSync(path.join(boundariesRoot, dir)).isDirectory());

  for (const city of cities) {
    const openDir = path.join(boundariesRoot, city, "openSpace");
    if (!fs.existsSync(openDir)) continue; // gracefully skip cities with no open space data

    const files = fs
      .readdirSync(openDir)
      .filter((f) => f.endsWith(".geojson"));

    for (const file of files) {
      const full = path.join(openDir, file);
      const features = loadGeoJSON(full);
      for (const feature of features) {
        openSpaces.push({ city, file, feature });
      }
    }
  }

  return openSpaces;
}

function firstPolygonHit(entries, pt) {
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

function nearestOpenSpace(entries, pt) {
  let best = null;
  let bestDist = Infinity;

  for (const entry of entries) {
    const geom = entry.feature.geometry;
    if (!geom) continue;

    let targetPoint = null;

    if (geom.type === "Point") {
      targetPoint = entry.feature;
    } else if (geom.type === "Polygon") {
      if (geom.coordinates?.[0]?.[0]) {
        targetPoint = turfPoint(geom.coordinates[0][0]);
      }
    } else if (geom.type === "MultiPolygon") {
      if (geom.coordinates?.[0]?.[0]?.[0]) {
        targetPoint = turfPoint(geom.coordinates[0][0][0]);
      }
    } else if (geom.type === "LineString") {
      if (geom.coordinates?.[0]) {
        targetPoint = turfPoint(geom.coordinates[0]);
      }
    } else if (geom.type === "MultiLineString") {
      if (geom.coordinates?.[0]?.[0]) {
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
 * Open space lookup.
 *
 * Returns:
 * {
 *   onOpenSpace: boolean,
 *   openSpace: {
 *     city,
 *     attrs,
 *     feature
 *   } | null,
 *   nearestOpenSpace: {
 *     city,
 *     distanceKm,
 *     distanceMeters,
 *     attrs,
 *     feature
 *   } | null
 * }
 */
export function openSpaceLookup(openSpaces, pt) {
  if (!openSpaces || openSpaces.length === 0) {
    return { onOpenSpace: false, openSpace: null, nearestOpenSpace: null };
  }

  const inside = firstPolygonHit(openSpaces, pt);
  const nearest = nearestOpenSpace(openSpaces, pt);

  return {
    onOpenSpace: !!inside,
    openSpace: inside
      ? {
          city: inside.city,
          attrs: inside.feature.properties || {},
          feature: inside.feature,
        }
      : null,
    nearestOpenSpace: nearest
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
