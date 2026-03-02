/**
 * NORMALIZE zoningBoundariesData_DISTRICTS_v2.geojson -> WGS84 (EPSG:4326)
 * ----------------------------------------------------------------------
 * Your merged districts geojson is MIXED CRS:
 *   - some features are WGS84 degrees (lon/lat)
 *   - others are MA StatePlane meters (EPSG:26986 or EPSG:26987)
 *
 * This script converts ANY non-WGS84 feature coords to WGS84 safely by:
 *   - trying EPSG:26986 -> WGS84
 *   - if that fails plausibility, trying EPSG:26987 -> WGS84
 *
 * Input:
 *   publicData/zoning/zoningBoundariesData_DISTRICTS_v2.geojson
 * Output:
 *   publicData/zoning/zoningBoundariesData_DISTRICTS_v3_wgs84.geojson
 *   publicData/zoning/zoningBoundariesData_DISTRICTS_v3_wgs84_meta.json
 */

import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..", "..");

const IN = path.join(ROOT, "publicData", "zoning", "zoningBoundariesData_DISTRICTS_v2.geojson");
const OUT = path.join(ROOT, "publicData", "zoning", "zoningBoundariesData_DISTRICTS_v3_wgs84.geojson");
const META = path.join(ROOT, "publicData", "zoning", "zoningBoundariesData_DISTRICTS_v3_wgs84_meta.json");

function sha256File(fp) {
  const h = crypto.createHash("sha256");
  h.update(fs.readFileSync(fp));
  return h.digest("hex");
}

function looksWgs84(lon, lat) {
  return Number.isFinite(lon) && Number.isFinite(lat) && Math.abs(lon) <= 180 && Math.abs(lat) <= 90;
}

// Massachusetts plausibility window (tight-ish, protects mis-projection)
function looksLikeMA(lon, lat) {
  return looksWgs84(lon, lat) && lon >= -74.5 && lon <= -69.0 && lat >= 40.5 && lat <= 43.5;
}

// StatePlane meters are huge relative to degrees
function looksLikeMeters(x, y) {
  return Number.isFinite(x) && Number.isFinite(y) && (Math.abs(x) > 1000 || Math.abs(y) > 1000);
}

let proj4 = null;
try {
  const mod = await import("proj4");
  proj4 = mod.default ?? mod;

  // NAD83 / Massachusetts Mainland (EPSG:26986) — meters
  proj4.defs(
    "EPSG:26986",
    "+proj=lcc +lat_1=41.71666666666667 +lat_2=42.68333333333333 +lat_0=41 +lon_0=-71.5 +x_0=200000 +y_0=750000 +datum=NAD83 +units=m +no_defs"
  );

  // NAD83 / Massachusetts Island (EPSG:26987) — meters
  proj4.defs(
    "EPSG:26987",
    "+proj=lcc +lat_1=41.28333333333333 +lat_2=41.48333333333333 +lat_0=41 +lon_0=-70.5 +x_0=100000 +y_0=0 +datum=NAD83 +units=m +no_defs"
  );

  // NAD83 / Massachusetts Mainland (EPSG:2249) — US survey feet  ✅ IMPORTANT
  proj4.defs(
    "EPSG:2249",
    "+proj=lcc +lat_1=41.71666666666667 +lat_2=42.68333333333333 +lat_0=41 +lon_0=-71.5 +x_0=656166.6666666666 +y_0=2460625 +datum=NAD83 +units=us-ft +no_defs"
  );
} catch {
  proj4 = null;
}


function spToWgsTry(epsg, x, y) {
  try {
    const [lon, lat] = proj4(epsg, "WGS84", [x, y]);
    return { lon, lat };
  } catch {
    return null;
  }
}

function convertMetersPairToWgs(x, y) {
  // Try mainland first
  const a = spToWgsTry("EPSG:26986", x, y);
  if (a && looksLikeMA(a.lon, a.lat)) return { ...a, epsg: "26986" };

  // Then island
  const b = spToWgsTry("EPSG:26987", x, y);
  if (b && looksLikeMA(b.lon, b.lat)) return { ...b, epsg: "26987" };

  // If both are “WGS” but not MA-like, still prefer the one that at least is valid WGS
  if (a && looksWgs84(a.lon, a.lat)) return { ...a, epsg: "26986_suspect" };
  if (b && looksWgs84(b.lon, b.lat)) return { ...b, epsg: "26987_suspect" };

  return null;
}

function firstPair(coords) {
  let c = coords;
  while (Array.isArray(c) && Array.isArray(c[0])) c = c[0];
  if (Array.isArray(c) && c.length >= 2 && typeof c[0] === "number" && typeof c[1] === "number") return c;
  return null;
}

function walkCoords(coords, stats, perCity) {
  if (!Array.isArray(coords)) return coords;

  // coordinate pair
  if (typeof coords[0] === "number" && typeof coords[1] === "number") {
    const x = Number(coords[0]);
    const y = Number(coords[1]);
    stats.pointsTotal++;

    if (looksWgs84(x, y)) {
      stats.pointsWgs++;
      return coords;
    }

    if (looksLikeMeters(x, y)) {
      const wgs = convertMetersPairToWgs(x, y);
      if (wgs) {
        stats.pointsConverted++;
        stats.convertedByEpsg[wgs.epsg] = (stats.convertedByEpsg[wgs.epsg] ?? 0) + 1;
        return [wgs.lon, wgs.lat];
      }
    }

    stats.pointsSuspicious++;
    return coords;
  }

  return coords.map((c) => walkCoords(c, stats, perCity));
}

console.log("====================================================");
console.log(" NORMALIZE ZONING DISTRICTS -> WGS84 (v3)");
console.log("====================================================");
console.log("IN:   ", IN);
console.log("OUT:  ", OUT);
console.log("META: ", META);
console.log("proj4:", proj4 ? "loaded" : "MISSING (npm i proj4)");
console.log("====================================================");

if (!fs.existsSync(IN)) throw new Error(`Missing: ${IN}`);
if (!proj4) throw new Error(`proj4 not available. Run: npm i proj4`);

const inputHash = sha256File(IN);
const gj = JSON.parse(fs.readFileSync(IN, "utf8"));

const stats = {
  features: 0,
  featuresWgs: 0,
  featuresStatePlane: 0,
  featuresConverted: 0,
  pointsTotal: 0,
  pointsWgs: 0,
  pointsConverted: 0,
  pointsSuspicious: 0,
  convertedByEpsg: {},
};

const byCityBefore = new Map();
const byCityAfter = new Map();

function bump(map, k) {
  map.set(k, (map.get(k) ?? 0) + 1);
}

for (const f of (gj.features || [])) {
  stats.features++;

  const city = String(f.properties?.__city ?? "unknown").toLowerCase();
  const fp = firstPair(f.geometry?.coordinates);
  if (fp) {
    const isWgs = looksWgs84(Number(fp[0]), Number(fp[1]));
    bump(byCityBefore, `${city}|${isWgs ? "wgs" : "stateplane"}`);
    if (isWgs) stats.featuresWgs++;
    else stats.featuresStatePlane++;
  }

  // convert coords (any non-wgs points)
  const before0 = fp ? `${fp[0]},${fp[1]}` : null;
  f.geometry.coordinates = walkCoords(f.geometry.coordinates, stats);
  const fp2 = firstPair(f.geometry?.coordinates);
  const after0 = fp2 ? `${fp2[0]},${fp2[1]}` : null;
  if (before0 !== after0 && before0 != null) stats.featuresConverted++;

  if (fp2) {
    const isWgs2 = looksWgs84(Number(fp2[0]), Number(fp2[1]));
    bump(byCityAfter, `${city}|${isWgs2 ? "wgs" : "stateplane"}`);
  }
}

// write outputs
fs.writeFileSync(OUT, JSON.stringify(gj));
const meta = {
  input: { path: IN, sha256: inputHash },
  output: { path: OUT },
  stats,
  cityBreakdownBefore: [...byCityBefore.entries()].sort((a, b) => b[1] - a[1]),
  cityBreakdownAfter: [...byCityAfter.entries()].sort((a, b) => b[1] - a[1]),
  builtAt: new Date().toISOString(),
};
fs.writeFileSync(META, JSON.stringify(meta, null, 2));

console.log("----------------------------------------------------");
console.log("✅ NORMALIZATION COMPLETE");
console.log(stats);
console.log("OUT:", OUT);
console.log("META:", META);
console.log("====================================================");
