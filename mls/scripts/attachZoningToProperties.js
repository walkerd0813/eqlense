/**
 * ATTACH ZONING → PROPERTIES (DROP-IN v3)
 * --------------------------------------
 * Fixes mixed-CRS zoning GeoJSON (WGS84 + StatePlane) by normalizing features to WGS84
 * BEFORE building the grid index (prevents Map max size exceeded).
 *
 * Institutional rules:
 *  - Keep raw zoning file unchanged
 *  - Normalize geometry in-memory, log counts, write hashes + meta
 *  - Attach districts first; overlays/subdistricts are separate passes
 *
 * Run:
 *  node --max-old-space-size=8192 mls/scripts/attachZoningToProperties.js
 */

import fs from "fs";
import path from "path";
import crypto from "crypto";
import readline from "readline";
import { fileURLToPath } from "url";

// ----------------------------- paths -----------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..", "..");

const IN_PROPS =
  process.env.IN_PROPS ??
  path.join(ROOT, "publicData", "properties", "properties_statewide_geo.ndjson");

const ZONING_GEOJSON =
  process.env.ZONING_GEOJSON ??
  path.join(ROOT, "publicData", "zoning", "zoningBoundariesData_DISTRICTS_v2.geojson");

const OUT_PROPS =
  process.env.OUT_PROPS ??
  path.join(ROOT, "publicData", "properties", "properties_statewide_geo_district.ndjson");

const OUT_META =
  process.env.OUT_META ??
  path.join(ROOT, "publicData", "properties", "properties_statewide_geo_district_meta.json");

const CELL_DEG = Number(process.env.CELL_DEG ?? "0.02");
const LAYER_ALLOW = (process.env.LAYER_ALLOW ?? "district").split(",").map(s => s.trim()).filter(Boolean);

// ----------------------------- proj4 (optional) -----------------------------
let proj4 = null;
try {
  const mod = await import("proj4");
  proj4 = mod.default ?? mod;

  // EPSG:26986 — NAD83 / Massachusetts Mainland (meters)
  // (PROJ string commonly used; LCC parameters match EPSG registry usage)
  proj4.defs(
    "EPSG:26986",
    "+proj=lcc +lat_1=41.71666666666667 +lat_2=42.68333333333333 +lat_0=41 +lon_0=-71.5 +x_0=200000 +y_0=750000 +datum=NAD83 +units=m +no_defs"
  );

  // EPSG:2249 — NAD83 / Massachusetts Mainland (US feet)
  // Uses the same LCC parameters but US-ft units (+to_meter is implicit in proj4 units handling)
  proj4.defs(
    "EPSG:2249",
    "+proj=lcc +lat_1=41.71666666666667 +lat_2=42.68333333333333 +lat_0=41 +lon_0=-71.5 +x_0=656166.6666666666 +y_0=2460625 +datum=NAD83 +units=us-ft +no_defs"
  );
} catch {
  proj4 = null;
}

// ----------------------------- helpers -----------------------------
function sha256File(fp) {
  const h = crypto.createHash("sha256");
  const buf = fs.readFileSync(fp);
  h.update(buf);
  return h.digest("hex");
}

function looksLikeWgs84(x, y) {
  return (
    Number.isFinite(x) &&
    Number.isFinite(y) &&
    Math.abs(x) <= 180 &&
    Math.abs(y) <= 90
  );
}

function looksLikeMA(lng, lat) {
  // Loose MA-ish bounds; used only as a sanity check.
  return (
    Number.isFinite(lng) &&
    Number.isFinite(lat) &&
    lng < -69 && lng > -74 &&
    lat > 41 && lat < 43.2
  );
}

function normalizeWgsPairMaybeSwap(lng, lat) {
  // sometimes data is [lat,lng]; fix if swapped
  if (looksLikeMA(lng, lat)) return { lng, lat };
  if (looksLikeMA(lat, lng)) return { lng: lat, lat: lng };
  return { lng, lat };
}

function convertStatePlaneToWgs84(x, y) {
  if (!proj4) return null;

  // Heuristic:
  //  - EPSG:2249 (ftUS) often has northing ~ 2.5M–3.2M and easting ~ 0.3M–1.2M
  //  - EPSG:26986 (m)   often has northing ~ 0.7M–1.2M and easting ~ 0.1M–0.4M
  const tryOrder = (x > 500000 || y > 1500000) ? ["EPSG:2249", "EPSG:26986"] : ["EPSG:26986", "EPSG:2249"];

  for (const src of tryOrder) {
    try {
      const [lng0, lat0] = proj4(src, "WGS84", [x, y]);
      const fixed = normalizeWgsPairMaybeSwap(lng0, lat0);
      if (looksLikeMA(fixed.lng, fixed.lat)) return { ...fixed, src };
    } catch {
      // ignore and try next
    }
  }
  return null;
}

function walkCoords(coords, fn) {
  // coords can be nested: Polygon: [ring[]], MultiPolygon: [[ring[]]]
  if (!Array.isArray(coords)) return coords;
  if (coords.length === 0) return coords;

  // coordinate pair
  if (typeof coords[0] === "number" && typeof coords[1] === "number") {
    return fn(coords);
  }
  return coords.map(c => walkCoords(c, fn));
}

function computeBboxOfCoords(coords, bbox = { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity }) {
  walkCoords(coords, ([x, y]) => {
    if (Number.isFinite(x) && Number.isFinite(y)) {
      if (x < bbox.minX) bbox.minX = x;
      if (y < bbox.minY) bbox.minY = y;
      if (x > bbox.maxX) bbox.maxX = x;
      if (y > bbox.maxY) bbox.maxY = y;
    }
    return [x, y];
  });
  return bbox;
}

function pointInRing(lng, lat, ring) {
  // Ray casting; ring is array of [lng,lat]
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0], yi = ring[i][1];
    const xj = ring[j][0], yj = ring[j][1];

    const intersect =
      ((yi > lat) !== (yj > lat)) &&
      (lng < ((xj - xi) * (lat - yi)) / ((yj - yi) || 1e-12) + xi);
    if (intersect) inside = !inside;
  }
  return inside;
}

function pointInPolygon(lng, lat, polyCoords) {
  // polyCoords: [outerRing, hole1, hole2...]
  if (!polyCoords?.length) return false;
  const outer = polyCoords[0];
  if (!pointInRing(lng, lat, outer)) return false;
  // if inside any hole, it's outside
  for (let i = 1; i < polyCoords.length; i++) {
    if (pointInRing(lng, lat, polyCoords[i])) return false;
  }
  return true;
}

function pointInGeometry(lng, lat, geom) {
  if (!geom) return false;
  const { type, coordinates } = geom;
  if (!type || !coordinates) return false;

  if (type === "Polygon") {
    return pointInPolygon(lng, lat, coordinates);
  }
  if (type === "MultiPolygon") {
    for (const poly of coordinates) {
      if (pointInPolygon(lng, lat, poly)) return true;
    }
    return false;
  }
  return false;
}

// ----------------------------- main -----------------------------
console.log("====================================================");
console.log(" ATTACH ZONING → PROPERTIES (DROP-IN v3)");
console.log("====================================================");
console.log("IN_PROPS:      ", IN_PROPS);
console.log("ZONING_GEOJSON:", ZONING_GEOJSON);
console.log("OUT_PROPS:     ", OUT_PROPS);
console.log("OUT_META:      ", OUT_META);
console.log("CELL_DEG:      ", CELL_DEG);
console.log("LAYER_ALLOW:   ", LAYER_ALLOW.join(",") || "(none)");
console.log("proj4:         ", proj4 ? "loaded" : "NOT loaded (stateplane conversion disabled)");
console.log("====================================================");

if (!fs.existsSync(IN_PROPS)) throw new Error(`Missing IN_PROPS: ${IN_PROPS}`);
if (!fs.existsSync(ZONING_GEOJSON)) throw new Error(`Missing ZONING_GEOJSON: ${ZONING_GEOJSON}`);

const zoningHash = sha256File(ZONING_GEOJSON);
console.log("[hash] zoning sha256 =", zoningHash);

console.log("[load] zoning geojson ...");
const zoning = JSON.parse(fs.readFileSync(ZONING_GEOJSON, "utf8"));
const rawFeatures = Array.isArray(zoning?.features) ? zoning.features : [];
console.log(`[load] zoning features (raw): ${rawFeatures.length.toLocaleString()}`);

const stats = {
  rawFeatures: rawFeatures.length,
  keptAfterLayer: 0,
  convertedFromStatePlane: 0,
  convertedSrc2249: 0,
  convertedSrc26986: 0,
  alreadyWgs84: 0,
  suspiciousSkipped: 0,
  missingGeomSkipped: 0,
};

const features = [];
for (const f of rawFeatures) {
  const geom = f?.geometry;
  if (!geom?.type || !geom?.coordinates) {
    stats.missingGeomSkipped++;
    continue;
  }

  const layer = (f.properties?.__layer ?? f.properties?.layer ?? "").toString().toLowerCase();
  if (LAYER_ALLOW.length && !LAYER_ALLOW.includes(layer)) continue;

  // detect if this feature is wgs84 or stateplane by sampling first coord found
  let sample = null;
  walkCoords(geom.coordinates, (pt) => {
    if (!sample && Array.isArray(pt) && pt.length >= 2) sample = pt;
    return pt;
  });

  if (!sample || sample.length < 2) {
    stats.missingGeomSkipped++;
    continue;
  }

  const x0 = Number(sample[0]);
  const y0 = Number(sample[1]);

  let newGeom = geom;

  if (looksLikeWgs84(x0, y0)) {
    stats.alreadyWgs84++;
  } else {
    if (!proj4) {
      // Cannot safely normalize; skip to avoid grid blow-up
      stats.suspiciousSkipped++;
      continue;
    }

    const converted = convertStatePlaneToWgs84(x0, y0);
    if (!converted) {
      stats.suspiciousSkipped++;
      continue;
    }

    // convert all coords
    const coords2 = walkCoords(geom.coordinates, ([x, y]) => {
      const c = convertStatePlaneToWgs84(Number(x), Number(y));
      if (!c) return [x, y]; // keep raw if cannot convert (will be caught by bbox guard)
      return [c.lng, c.lat];
    });

    newGeom = { ...geom, coordinates: coords2 };
    stats.convertedFromStatePlane++;
    if (converted.src === "EPSG:2249") stats.convertedSrc2249++;
    if (converted.src === "EPSG:26986") stats.convertedSrc26986++;
  }

  // bbox guard (prevents Map explosion if something still has crazy coords)
  const bb = computeBboxOfCoords(newGeom.coordinates);
  const width = bb.maxX - bb.minX;
  const height = bb.maxY - bb.minY;

  // If still not plausibly degrees, skip.
  if (!Number.isFinite(width) || !Number.isFinite(height) || width > 10 || height > 10 || bb.maxX > 180 || bb.minX < -180 || bb.maxY > 90 || bb.minY < -90) {
    stats.suspiciousSkipped++;
    continue;
  }

  // Keep minimal, precomputed bbox for speed
  const props = f.properties ?? {};
  const label =
    props.DISTRICT ??
    props.__label ??
    props.NAME ??
    props.name ??
    props.ZONE ??
    props.zone ??
    null;

  features.push({
    bbox: bb,
    geometry: newGeom,
    properties: props,
    _label: label,
    _layer: layer,
  });
}

stats.keptAfterLayer = features.length;

console.log(`[load] zoning features (after layer filter + CRS normalize): ${features.length.toLocaleString()}`);
console.log("[crs] alreadyWgs84=", stats.alreadyWgs84.toLocaleString(),
            "converted=", stats.convertedFromStatePlane.toLocaleString(),
            "(2249=", stats.convertedSrc2249.toLocaleString(), "26986=", stats.convertedSrc26986.toLocaleString() + ")",
            "skippedSuspicious=", stats.suspiciousSkipped.toLocaleString(),
            "missingGeom=", stats.missingGeomSkipped.toLocaleString());

// compute overall bbox
let overall = { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity };
for (const f of features) {
  overall.minX = Math.min(overall.minX, f.bbox.minX);
  overall.minY = Math.min(overall.minY, f.bbox.minY);
  overall.maxX = Math.max(overall.maxX, f.bbox.maxX);
  overall.maxY = Math.max(overall.maxY, f.bbox.maxY);
}
console.log("[ok] zoning bbox (degrees):", overall);

// build grid
console.log(`[index] building zoning grid (cell=${CELL_DEG}°) ...`);
const grid = new Map();
function cellKey(ix, iy) {
  return `${ix},${iy}`;
}
function addToGrid(ix, iy, idx) {
  const k = cellKey(ix, iy);
  const arr = grid.get(k);
  if (arr) arr.push(idx);
  else grid.set(k, [idx]);
}

for (let i = 0; i < features.length; i++) {
  const bb = features[i].bbox;
  const ix0 = Math.floor((bb.minX - overall.minX) / CELL_DEG);
  const ix1 = Math.floor((bb.maxX - overall.minX) / CELL_DEG);
  const iy0 = Math.floor((bb.minY - overall.minY) / CELL_DEG);
  const iy1 = Math.floor((bb.maxY - overall.minY) / CELL_DEG);

  // guard against pathological bboxes
  const spanX = ix1 - ix0 + 1;
  const spanY = iy1 - iy0 + 1;
  if (spanX > 5000 || spanY > 5000) {
    stats.suspiciousSkipped++;
    continue;
  }

  for (let ix = ix0; ix <= ix1; ix++) {
    for (let iy = iy0; iy <= iy1; iy++) {
      addToGrid(ix, iy, i);
    }
  }
}
console.log(`[index] grid cells: ${grid.size.toLocaleString()}`);

// stream properties and attach
const out = fs.createWriteStream(OUT_PROPS, { flags: "w" });
const rl = readline.createInterface({ input: fs.createReadStream(IN_PROPS) });

let read = 0;
let wrote = 0;
let hasCoords = 0;
let attached = 0;
let missingCoords = 0;
let multiHits = 0;

function pickLngLat(o) {
  const lng = o?.lng ?? o?.lon ?? o?.longitude ?? o?.LON ?? null;
  const lat = o?.lat ?? o?.latitude ?? o?.LAT ?? null;
  if (!Number.isFinite(Number(lng)) || !Number.isFinite(Number(lat))) return null;
  const fixed = normalizeWgsPairMaybeSwap(Number(lng), Number(lat));
  return fixed;
}

for await (const line of rl) {
  if (!line.trim()) continue;
  read++;

  let o;
  try { o = JSON.parse(line); } catch { continue; }

  const ll = pickLngLat(o);
  if (!ll) {
    missingCoords++;
    out.write(JSON.stringify(o) + "\n");
    wrote++;
    continue;
  }

  hasCoords++;
  const { lng, lat } = ll;

  const ix = Math.floor((lng - overall.minX) / CELL_DEG);
  const iy = Math.floor((lat - overall.minY) / CELL_DEG);
  const cands = grid.get(cellKey(ix, iy)) ?? [];

  let hits = [];
  for (const idx of cands) {
    const f = features[idx];
    if (lng < f.bbox.minX || lng > f.bbox.maxX || lat < f.bbox.minY || lat > f.bbox.maxY) continue;
    if (pointInGeometry(lng, lat, f.geometry)) hits.push(f);
  }

  if (hits.length) {
    attached++;
    if (hits.length > 1) multiHits++;

    // choose primary hit (first); keep all as evidence
    const primary = hits[0];
    const zoning_out = {
      layer: primary._layer,
      label: primary._label,
      district: primary.properties?.DISTRICT ?? primary._label ?? null,
      city: primary.properties?.__city ?? primary.properties?.CITY ?? null,
      sourceFile: primary.properties?.__sourceFile ?? null,
      sourcePath: primary.properties?.__sourcePath ?? null,
      stage: primary.properties?.STAGE ?? null,
      mapno: primary.properties?.MAPNO ?? null,
      article: primary.properties?.ARTICLE ?? null,
      volume: primary.properties?.VOLUME ?? null,
      evidence: hits.slice(0, 5).map(h => ({
        layer: h._layer,
        label: h._label,
        city: h.properties?.__city ?? null,
        sourceFile: h.properties?.__sourceFile ?? null,
      })),
      matchedAt: new Date().toISOString(),
      zoningHash,
    };

    o = { ...o, zoning_district: zoning_out };
  }

  out.write(JSON.stringify(o) + "\n");
  wrote++;

  if (read % 200000 === 0) {
    console.log(`[progress] read=${read.toLocaleString()} attached=${attached.toLocaleString()} missingCoords=${missingCoords.toLocaleString()} gridCells=${grid.size.toLocaleString()}`);
  }
}

out.end();

const meta = {
  inputs: { inProps: IN_PROPS, zoningGeojson: ZONING_GEOJSON, zoningSha256: zoningHash },
  outputs: { outProps: OUT_PROPS, outMeta: OUT_META },
  params: { cellDeg: CELL_DEG, layerAllow: LAYER_ALLOW },
  zoningPrep: stats,
  stats: {
    read,
    wrote,
    hasCoords,
    missingCoords,
    attached,
    multiHits,
  },
  builtAt: new Date().toISOString(),
};

fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2));
console.log("----------------------------------------------------");
console.log("✅ ZONING ATTACH COMPLETE (v3)");
console.log("Read:         ", read.toLocaleString());
console.log("Wrote:        ", wrote.toLocaleString());
console.log("Has coords:   ", hasCoords.toLocaleString());
console.log("Missing coords:", missingCoords.toLocaleString());
console.log("Attached:     ", attached.toLocaleString());
console.log("Multi-hits:   ", multiHits.toLocaleString());
console.log("OUT_PROPS:    ", OUT_PROPS);
console.log("OUT_META:     ", OUT_META);
console.log("====================================================");
