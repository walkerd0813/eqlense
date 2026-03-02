// backend/zoning/modules/easementLookup.js

import fs from "fs";
import path from "path";
import booleanPointInPolygon from "@turf/boolean-point-in-polygon";
import distance from "@turf/distance";
import { point as turfPoint } from "@turf/helpers";

/**
 * EASEMENT LOOKUP MODULE
 * ----------------------
 * Handles:
 *  - conservation easements
 *  - open space easements
 *  - other mapped easement layers
 *
 * NOTE:
 *  - Many cities will NOT have any easement data at all.
 *  - This module is built so missing folders/files simply mean: "no easements".
 *
 * Folder convention:
 *   publicData/boundaries/<city>/easements/*.geojson
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

export function loadEasementLayers({ DATA_ROOT }) {
  const boundariesRoot = path.join(DATA_ROOT, "boundaries");
  const easements = [];

  if (!fs.existsSync(boundariesRoot)) return easements;

  const cities = fs
    .readdirSync(boundariesRoot)
    .filter((dir) => fs.statSync(path.join(boundariesRoot, dir)).isDirectory());

  for (const city of cities) {
    const easDir = path.join(boundariesRoot, city, "easements");
    if (!fs.existsSync(easDir)) continue; // most cities: no easement folder

    const files = fs
      .readdirSync(easDir)
      .filter((f) => f.endsWith(".geojson"));

    for (const file of files) {
      const full = path.join(easDir, file);
      const features = loadGeoJSON(full);
      for (const feature of features) {
        easements.push({ city, file, feature });
      }
    }
  }

  return easements;
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

function nearestEasement(entries, pt) {
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
 * Easement lookup.
 *
 * Returns:
 * {
 *   hasEasement: boolean,
 *   easement: {
 *     city,
 *     attrs,
 *     feature
 *   } | null,
 *   nearestEasementArea: {
 *     city,
 *     distanceKm,
 *     distanceMeters,
 *     attrs,
 *     feature
 *   } | null
 * }
 */
export function easementLookup(easements, pt) {
  if (!easements || easements.length === 0) {
    return {
      hasEasement: false,
      easement: null,
      nearestEasementArea: null,
    };
  }

  const inside = firstPolygonHit(easements, pt);
  const nearest = nearestEasement(easements, pt);

  return {
    hasEasement: !!inside,
    easement: inside
      ? {
          city: inside.city,
          attrs: inside.feature.properties || {},
          feature: inside.feature,
        }
      : null,
    nearestEasementArea: nearest
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
