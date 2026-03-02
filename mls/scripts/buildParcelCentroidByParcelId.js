/**
 * BUILD parcelCentroidByParcelId.json (DROP-IN v8)
 * ------------------------------------------------
 * Goal: create a fast lookup map: { "<LOC_ID>": { lon, lat } }
 * so we can anchor coordinates to PARCELS (properties), not MLS listings.
 *
 * Why v8:
 * - Your parcels.gpkg export was failing during GDAL reprojection (-t_srs EPSG:4326)
 *   on a subset of features (mixed Mainland/Island StatePlane or out-of-bounds geometries).
 * - v8 avoids GDAL reprojection entirely:
 *     ogr2ogr streams centroid POINTs in the source CRS,
 *     then Node converts to WGS84 using proj4 (tries EPSG:26986 and EPSG:26987).
 *
 * Inputs:
 *  - publicData/parcels/parcels.gpkg
 *
 * Outputs:
 *  - publicData/parcels/parcelCentroidByParcelId.json
 *  - publicData/parcels/parcelCentroidByParcelId_meta.json
 *
 * Run (PowerShell):
 *   cd C:\seller-app\backend
 *   $env:LAYER="parcels"; $env:PID_FIELD="LOC_ID"; $env:GEOM_COL="geom"
 *   node --max-old-space-size=8192 .\mls\scripts\buildParcelCentroidByParcelId.js
 *   Remove-Item env:LAYER,env:PID_FIELD,env:GEOM_COL -ErrorAction SilentlyContinue
 */

import fs from "fs";
import path from "path";
import readline from "readline";
import { spawn } from "child_process";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..", "..");

// ------------------------- config -------------------------
const PARCELS_GPKG =
  process.env.PARCELS_GPKG ||
  path.join(ROOT, "publicData", "parcels", "parcels.gpkg");

const LAYER = (process.env.LAYER || "parcels").trim();
const PID_FIELD = (process.env.PID_FIELD || "LOC_ID").trim();
const GEOM_COL = (process.env.GEOM_COL || "geom").trim();

const OUT_JSON =
  process.env.OUT_JSON ||
  path.join(ROOT, "publicData", "parcels", "parcelCentroidByParcelId.json");

const OUT_META =
  process.env.OUT_META ||
  path.join(ROOT, "publicData", "parcels", "parcelCentroidByParcelId_meta.json");

// progress interval
const PROGRESS_EVERY = Number(process.env.PROGRESS_EVERY || "200000");

// Massachusetts bounds (rough). Used to choose correct StatePlane zone.
const MA_BOUNDS = {
  minLon: -73.7,
  maxLon: -69.7,
  minLat: 41.0,
  maxLat: 43.2,
};

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function cleanStr(s) {
  if (s == null) return "";
  return String(s).trim();
}

function looksLikeWgs84(x, y) {
  return (
    Number.isFinite(x) &&
    Number.isFinite(y) &&
    Math.abs(x) <= 180 &&
    Math.abs(y) <= 90
  );
}

function inMassachusetts(lon, lat) {
  return (
    Number.isFinite(lon) &&
    Number.isFinite(lat) &&
    lon >= MA_BOUNDS.minLon &&
    lon <= MA_BOUNDS.maxLon &&
    lat >= MA_BOUNDS.minLat &&
    lat <= MA_BOUNDS.maxLat
  );
}

function normalizeWgsPairMaybeSwap(lon, lat) {
  // Many datasets sometimes flip lon/lat. Make it safe.
  if (inMassachusetts(lon, lat)) return { lon, lat, swapped: false };
  if (inMassachusetts(lat, lon)) return { lon: lat, lat: lon, swapped: true };
  return { lon, lat, swapped: false };
}

// ------------------------- proj4 -------------------------
let proj4 = null;
try {
  const mod = await import("proj4");
  proj4 = mod.default ?? mod;

  // EPSG:26986 NAD83 / Massachusetts Mainland (meters)
  // Derived from EPSG registry parameters.
  proj4.defs(
    "EPSG:26986",
    "+proj=lcc +lat_1=42.6833333333333 +lat_2=41.7166666666667 +lat_0=41 +lon_0=-71.5 +x_0=200000 +y_0=750000 +datum=NAD83 +units=m +no_defs"
  );

  // EPSG:26987 NAD83 / Massachusetts Island (meters)
  // Based on EPSG registry parameters (without grid shift file, using datum NAD83).
  proj4.defs(
    "EPSG:26987",
    "+proj=lcc +lat_0=41 +lon_0=-70.5 +lat_1=41.4833333333333 +lat_2=41.2833333333333 +x_0=500000 +y_0=0 +datum=NAD83 +units=m +no_defs"
  );
} catch {
  proj4 = null;
}

function toWgs84FromMeters(x, y) {
  if (!proj4) return null;

  // Try Mainland first, then Island. Also try swapped x/y as a last resort.
  const attempts = [
    { crs: "EPSG:26986", x, y, swappedXY: false },
    { crs: "EPSG:26987", x, y, swappedXY: false },
    { crs: "EPSG:26986", x: y, y: x, swappedXY: true },
    { crs: "EPSG:26987", x: y, y: x, swappedXY: true },
  ];

  for (const a of attempts) {
    try {
      const [lon0, lat0] = proj4(a.crs, "WGS84", [a.x, a.y]);
      const fixed = normalizeWgsPairMaybeSwap(lon0, lat0);
      if (inMassachusetts(fixed.lon, fixed.lat)) {
        return { lon: fixed.lon, lat: fixed.lat, crs: a.crs, swappedXY: a.swappedXY, swappedLonLat: fixed.swapped };
      }
    } catch {
      // ignore and try next
    }
  }

  // Fallback: return Mainland transform even if out-of-bounds (better than nothing),
  // but caller should count it as "suspicious".
  try {
    const [lon0, lat0] = proj4("EPSG:26986", "WGS84", [x, y]);
    const fixed = normalizeWgsPairMaybeSwap(lon0, lat0);
    return { lon: fixed.lon, lat: fixed.lat, crs: "EPSG:26986", swappedXY: false, swappedLonLat: fixed.swapped, suspicious: true };
  } catch {
    return null;
  }
}

function coordsToWgs84(x, y) {
  if (!Number.isFinite(x) || !Number.isFinite(y)) return null;

  if (looksLikeWgs84(x, y)) {
    const fixed = normalizeWgsPairMaybeSwap(x, y);
    return { lon: fixed.lon, lat: fixed.lat, crs: "WGS84", swappedXY: false, swappedLonLat: fixed.swapped };
  }

  // Treat as meters (StatePlane)
  return toWgs84FromMeters(x, y);
}

// ------------------------- ogr2ogr stream -------------------------
function spawnOgr2OgrCentroids() {
  // IMPORTANT: no -t_srs here (GDAL reprojection was your failure mode).
  const sql =
    `SELECT "${PID_FIELD}" AS pid, ST_Centroid("${GEOM_COL}") AS geom ` +
    `FROM "${LAYER}" ` +
    `WHERE "${PID_FIELD}" IS NOT NULL`;

  const args = [
    "-f", "GeoJSONSeq",
    "/vsistdout/",
    "-dialect", "SQLite",
    "-sql", sql,
    "-skipfailures",
    PARCELS_GPKG,
  ];

  // We do NOT pass LAYER after the dataset when using -sql (GDAL warns it's ignored),
  // but it's harmless; leaving it out keeps stderr cleaner.

  const child = spawn("ogr2ogr", args, { windowsHide: true });

  return { child, sql, args };
}

// ------------------------- main -------------------------
console.log("====================================================");
console.log("BUILD parcelCentroidByParcelId.json (DROP-IN v8)");
console.log("====================================================");
console.log("PARCELS_GPKG:", PARCELS_GPKG);
console.log("OUT_JSON:    ", OUT_JSON);
console.log("OUT_META:    ", OUT_META);
console.log("CFG:         ", `layer="${LAYER}" pidField="${PID_FIELD}" geomCol="${GEOM_COL}"`);
console.log("proj4:       ", proj4 ? "loaded" : "MISSING (npm i proj4)");
console.log("====================================================");

if (!fs.existsSync(PARCELS_GPKG)) {
  throw new Error(`Missing parcels.gpkg: ${PARCELS_GPKG}`);
}
if (!proj4) {
  throw new Error(`proj4 is not installed/available. From backend/, run: npm i proj4`);
}

ensureDir(path.dirname(OUT_JSON));
ensureDir(path.dirname(OUT_META));

// stream-write a JSON object (so we don't hold 2.5M keys in RAM)
const out = fs.createWriteStream(OUT_JSON, { flags: "w" });
out.write("{\n");
let first = true;

const meta = {
  inputs: { parcelsGpkg: PARCELS_GPKG, layer: LAYER, pidField: PID_FIELD, geomCol: GEOM_COL },
  outputs: { outJson: OUT_JSON, outMeta: OUT_META },
  stats: {
    read: 0,
    written: 0,
    missingPid: 0,
    missingGeom: 0,
    parseErrors: 0,
    suspicious: 0,
    used26986: 0,
    used26987: 0,
    usedWgs84: 0,
    swappedXY: 0,
    swappedLonLat: 0,
  },
  builtAt: null,
};

const { child, sql, args } = spawnOgr2OgrCentroids();

child.stderr.setEncoding("utf8");
let stderrBuf = "";
child.stderr.on("data", (d) => {
  stderrBuf += d;
});

const rl = readline.createInterface({ input: child.stdout });

try {
  for await (const line of rl) {
    const s = line.trim();
    if (!s) continue;

    meta.stats.read++;

    let feat;
    try {
      feat = JSON.parse(s);
    } catch {
      meta.stats.parseErrors++;
      continue;
    }

    const pidRaw = feat?.properties?.pid ?? feat?.properties?.PID ?? feat?.properties?.LOC_ID ?? null;
    const pid = pidRaw == null ? null : cleanStr(pidRaw);
    if (!pid) {
      meta.stats.missingPid++;
      continue;
    }

    const coords = feat?.geometry?.coordinates;
    if (!Array.isArray(coords) || coords.length < 2) {
      meta.stats.missingGeom++;
      continue;
    }

    const x = Number(coords[0]);
    const y = Number(coords[1]);
    const wgs = coordsToWgs84(x, y);
    if (!wgs || !Number.isFinite(wgs.lon) || !Number.isFinite(wgs.lat)) {
      meta.stats.suspicious++;
      continue;
    }

    if (wgs.crs === "EPSG:26986") meta.stats.used26986++;
    else if (wgs.crs === "EPSG:26987") meta.stats.used26987++;
    else meta.stats.usedWgs84++;

    if (wgs.swappedXY) meta.stats.swappedXY++;
    if (wgs.swappedLonLat) meta.stats.swappedLonLat++;
    if (wgs.suspicious) meta.stats.suspicious++;

    const rec = `"${pid}":{"lon":${wgs.lon},"lat":${wgs.lat}}`;

    if (!first) out.write(",\n");
    first = false;
    out.write(rec);
    meta.stats.written++;

    if (meta.stats.read % PROGRESS_EVERY === 0) {
      console.log(
        `[progress] read=${meta.stats.read.toLocaleString()} written=${meta.stats.written.toLocaleString()} missingPid=${meta.stats.missingPid.toLocaleString()} missingGeom=${meta.stats.missingGeom.toLocaleString()} suspicious=${meta.stats.suspicious.toLocaleString()}`
      );
    }
  }
} finally {
  // wait for ogr2ogr exit
  const exitCode = await new Promise((resolve) => child.on("close", resolve));

  out.write("\n}\n");
  out.end();

  meta.builtAt = new Date().toISOString();

  fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2));

  if (exitCode !== 0) {
    // We still may have produced a usable partial output; expose stderr to help.
    throw new Error(
      `ogr2ogr exited with code ${exitCode}. Stderr:\n${stderrBuf}\n\n` +
      `Command was: ogr2ogr ${args.map((a) => (a.includes(" ") ? `"${a}"` : a)).join(" ")}\n` +
      `SQL was: ${sql}`
    );
  }

  console.log("----------------------------------------------------");
  console.log("✅ parcelCentroidByParcelId build complete (v8)");
  console.log("Read:       ", meta.stats.read.toLocaleString());
  console.log("Written:    ", meta.stats.written.toLocaleString());
  console.log("Missing pid:", meta.stats.missingPid.toLocaleString());
  console.log("Missing geom:", meta.stats.missingGeom.toLocaleString());
  console.log("Suspicious: ", meta.stats.suspicious.toLocaleString());
  console.log("OUT_JSON:   ", OUT_JSON);
  console.log("OUT_META:   ", OUT_META);
  console.log("====================================================");
}
