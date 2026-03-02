// backend/zoning/modules/overlayLookup.js

import fs from "fs";
import path from "path";
import booleanPointInPolygon from "@turf/boolean-point-in-polygon";

/**
 * OVERLAY LOOKUP MODULE
 * ----------------------
 * This module automatically loads ALL overlay layers for ALL cities.
 * It supports:
 *   - GCOD
 *   - Coastal Flood Overlay
 *   - Main Street Districts
 *   - Future overlays (when added to /overlays folder per city)
 *
 * Folder convention:
 * publicData/zoning/<city>/overlays/*.geojson
 *
 * This module:
 *   - Auto-detects cities
 *   - Auto-loads overlays
 *   - Supports MultiPolygon / Polygon
 *   - Supports multiple overlay matches
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
 * Loads ALL overlays from ALL cities dynamically.
 *
 * Returns an array of:
 * {
 *    city,
 *    overlayName,
 *    file,
 *    feature
 * }
 */
export function loadOverlayLayers({ DATA_ROOT }) {
  const zoningRoot = path.join(DATA_ROOT, "zoning");

  const overlayLayers = [];

  const cities = fs.readdirSync(zoningRoot).filter((dir) => {
    const full = path.join(zoningRoot, dir);
    return fs.statSync(full).isDirectory();
  });

  for (const city of cities) {
    const overlayDir = path.join(zoningRoot, city, "overlays");
    if (!fs.existsSync(overlayDir)) continue;

    const overlayFiles = fs
      .readdirSync(overlayDir)
      .filter((f) => f.endsWith(".geojson"));

    for (const file of overlayFiles) {
      const full = path.join(overlayDir, file);
      const features = loadGeoJSON(full);

      const overlayName = file.replace(".geojson", "");

      for (const feature of features) {
        overlayLayers.push({
          city,
          overlayName,
          file,
          feature,
        });
      }
    }
  }

  return overlayLayers;
}

/**
 * Checks which overlays a point falls inside.
 * Returns:
 *
 * {
 *   overlays: [
 *      {
 *        city,
 *        overlayName,
 *        attrs,
 *        feature
 *      }
 *   ],
 *   count: N
 * }
 */
export function overlayLookup(overlayLayers, pt) {
  const hits = [];

  for (const entry of overlayLayers) {
    const geom = entry.feature.geometry;
    if (!geom) continue;

    const type = geom.type;

    if (type === "Polygon" || type === "MultiPolygon") {
      const hit = booleanPointInPolygon(pt, entry.feature);

      if (hit) {
        hits.push({
          city: entry.city,
          overlayName: entry.overlayName,
          attrs: entry.feature.properties || {},
          feature: entry.feature,
        });
      }
    }
  }

  return {
    count: hits.length,
    overlays: hits,
  };
}
