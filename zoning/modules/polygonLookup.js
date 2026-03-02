// polygonLookup.js — Updated for Civic Boundaries NativeX
// Fast point-in-polygon with spatial grid acceleration

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load civic atlas
const CIVIC_ATLAS_PATH = path.resolve(
  __dirname,
  "../../publicData/civic/civicBoundariesData.geojson"
);

// --- Utility: point-in-polygon (ray casting) ---
function pointInPolygon(point, polygon) {
  const [x, y] = point;
  let inside = false;

  for (let r = 0; r < polygon.length; r++) {
    const ring = polygon[r];
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      const xi = ring[i][0],
        yi = ring[i][1];
      const xj = ring[j][0],
        yj = ring[j][1];

      const intersect =
        yi > y !== yj > y &&
        x < ((xj - xi) * (y - yi)) / (yj - yi) + xi;

      if (intersect) inside = !inside;
    }
  }

  return inside;
}

// Convert GeoJSON polygon → flat ring arrays
function normalizeGeometry(geom) {
  if (!geom) return null;

  if (geom.type === "Polygon") {
    return [geom.coordinates];
  }
  if (geom.type === "MultiPolygon") {
    return geom.coordinates;
  }
  return null;
}

// Build grid index
function buildSpatialIndex(features, binSize = 0.01) {
  const grid = new Map();

  for (const f of features) {
    const geom = normalizeGeometry(f.geometry);
    if (!geom) continue;

    // Compute bounding box
    let minX = Infinity,
      minY = Infinity,
      maxX = -Infinity,
      maxY = -Infinity;

    geom.forEach((poly) => {
      poly.forEach(([x, y]) => {
        if (x < minX) minX = x;
        if (x > maxX) maxX = x;
        if (y < minY) minY = y;
        if (y > maxY) maxY = y;
      });
    });

    const minBinX = Math.floor(minX / binSize);
    const maxBinX = Math.floor(maxX / binSize);
    const minBinY = Math.floor(minY / binSize);
    const maxBinY = Math.floor(maxY / binSize);

    for (let bx = minBinX; bx <= maxBinX; bx++) {
      for (let by = minBinY; by <= maxBinY; by++) {
        const key = `${bx},${by}`;
        if (!grid.has(key)) grid.set(key, []);
        grid.get(key).push({
          geom,
          props: f.properties,
        });
      }
    }
  }

  return { grid, binSize };
}

let civicData = null;
let spatialIndex = null;

// Load atlas on first import
export function loadCivicAtlas() {
  if (civicData) return civicData;

  console.log("[polygonLookup] Loading civicBoundariesData.geojson…");
  const raw = fs.readFileSync(CIVIC_ATLAS_PATH, "utf8");
  civicData = JSON.parse(raw);

  console.log(
    `[polygonLookup] Loaded ${civicData.features.length} civic polygons`
  );

  spatialIndex = buildSpatialIndex(civicData.features);
  console.log("[polygonLookup] Spatial index built.");

  return civicData;
}

export function lookupPoint(lng, lat) {
  if (!spatialIndex) loadCivicAtlas();

  const { grid, binSize } = spatialIndex;
  const binX = Math.floor(lng / binSize);
  const binY = Math.floor(lat / binSize);
  const key = `${binX},${binY}`;

  const candidates = grid.get(key);
  if (!candidates) return [];

  const hits = [];

  for (const c of candidates) {
    if (pointInPolygon([lng, lat], c.geom)) {
      hits.push(c.props);
    }
  }

  return hits;
}
