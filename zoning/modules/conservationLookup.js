// backend/zoning/modules/conservationLookup.js

import fs from "fs";
import path from "path";
import booleanPointInPolygon from "@turf/boolean-point-in-polygon";
import distance from "@turf/distance";
import { point as turfPoint } from "@turf/helpers";

/**
 * CONSERVATION LAND LOOKUP MODULE
 * -------------------------------
 * Handles:
 *  - conservation land
 *  - protected open space
 *  - resource protection zones
 *
 * NOTE:
 *  - Not every city will have conservation layers.
 *  - This module is safe when folders/files are missing.
 *
 * Folder convention:
 *   publicData/boundaries/<city>/conservation/*.geojson
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

export function loadConservationLayers({ DATA_ROOT }) {
  const boundariesRoot = path.join(DATA_ROOT, "boundaries");
  const areas = [];

  if (!fs.existsSync(boundariesRoot)) return areas;

  const cities = fs
    .readdirSync(boundariesRoot)
    .filter((dir) => fs.statSync(path.join(boundariesRoot, dir)).isDirectory());

  for (const city of cities) {
    const consDir = path.join(boundariesRoot, city, "conservation");
    if (!fs.existsSync(consDir)) continue; // some cities won't have it

    const files = fs
      .readdirSync(consDir)
      .filter((f) => f.endsWith(".geojson"));

    for (const file of files) {
      const full = path.join(consDir, file);
      const features = loadGeoJSON(full);
      for (const feature of features) {
        areas.push({ city, file, feature });
      }
    }
  }

  return areas;
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

function nearestConservation(entries, pt) {
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

    const d = distance(pt, targetPoint);
    if (d < bestDist) {
      bestDist = d;
      best = entry;
    }
  }

  if (!best || bestDist === Infinity) return null;
  return { entry: best, distanceKm: bestDist };
}

/**
 * Conservation lookup.
 *
 * Returns:
 * {
 *   inConservationArea: boolean,
 *   conservationArea: {
 *     city,
 *     attrs,
 *     feature
 *   } | null,
 *   nearestConservationArea: {
 *     city,
 *     distanceKm,
 *     distanceMeters,
 *     attrs,
 *     feature
 *   } | null
 * }
 */
export function conservationLookup(conservationAreas, pt) {
  if (!conservationAreas || conservationAreas.length === 0) {
    return {
      inConservationArea: false,
      conservationArea: null,
      nearestConservationArea: null,
    };
  }

  const inside = firstPolygonHit(conservationAreas, pt);
  const nearest = nearestConservation(conservationAreas, pt);

  return {
    inConservationArea: !!inside,
    conservationArea: inside
      ? {
          city: inside.city,
          attrs: inside.feature.properties || {},
          feature: inside.feature,
        }
      : null,
    nearestConservationArea: nearest
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
