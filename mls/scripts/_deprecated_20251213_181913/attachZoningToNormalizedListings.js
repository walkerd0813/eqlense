import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";

import { point } from "@turf/helpers";
import booleanPointInPolygon from "@turf/boolean-point-in-polygon";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function argValue(flag, def = null) {
  const i = process.argv.indexOf(flag);
  if (i === -1) return def;
  return process.argv[i + 1] ?? def;
}

const INPUT = process.argv[2];
const OUT_OK = process.argv[3];
const OUT_BAD = process.argv[4];

const BOUNDARIES = argValue("--boundaries", path.resolve(__dirname, "../../publicData/zoning/zoningBoundariesData.geojson"));
const CELL = parseFloat(argValue("--cell", "0.02"));

if (!INPUT || !OUT_OK || !OUT_BAD) {
  console.log("Usage:");
  console.log("  node mls/scripts/attachZoningToNormalizedListings.js <input.ndjson> <out_ok.ndjson> <out_unmatched.ndjson> [--boundaries <geojson>] [--cell 0.02]");
  process.exit(1);
}

const inputAbs = path.resolve(INPUT);
const outOkAbs = path.resolve(OUT_OK);
const outBadAbs = path.resolve(OUT_BAD);
const boundariesAbs = path.resolve(BOUNDARIES);

if (!fs.existsSync(inputAbs)) {
  console.error("❌ Missing input:", inputAbs);
  process.exit(1);
}
if (!fs.existsSync(boundariesAbs)) {
  console.error("❌ Missing boundaries:", boundariesAbs);
  process.exit(1);
}

function getBBox(geom) {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;

  const visit = (coords) => {
    if (!Array.isArray(coords)) return;
    if (typeof coords[0] === "number" && typeof coords[1] === "number") {
      const x = coords[0], y = coords[1];
      if (!Number.isFinite(x) || !Number.isFinite(y)) return;
      if (x < minX) minX = x;
      if (y < minY) minY = y;
      if (x > maxX) maxX = x;
      if (y > maxY) maxY = y;
      return;
    }
    for (const c of coords) visit(c);
  };

  visit(geom?.coordinates);
  if (!isFinite(minX)) return null;
  return [minX, minY, maxX, maxY];
}

function isWGS84BBox(bb) {
  // GeoJSON WGS84 sanity: lon within [-180, 180], lat within [-90, 90]
  return (
    bb &&
    bb[0] >= -180 && bb[2] <= 180 &&
    bb[1] >= -90  && bb[3] <= 90
  );
}

function cellKey(lat, lon, cell) {
  const y = Math.floor(lat / cell);
  const x = Math.floor(lon / cell);
  return `${y},${x}`;
}

function pickDistrictProps(props = {}) {
  const keys = [
    "DISTRICT","DIST_NAME","DISTRICT_N","ZONE","ZONING","ZONING_ID",
    "ZONING_CODE","ZONE_CODE","ZONEDIST","ZONINGDIST","LABEL","NAME"
  ];
  let val = null;
  for (const k of keys) {
    if (props[k] != null && String(props[k]).trim() !== "") { val = props[k]; break; }
  }
  return { district: val != null ? String(val).trim() : null, props };
}

console.log("====================================================");
console.log(" ATTACH ZONING (DISTRICTS ONLY — GRID PIP)");
console.log("====================================================");
console.log("Input:      ", inputAbs);
console.log("Boundaries: ", boundariesAbs);
console.log("Cell°:      ", CELL);
console.log("OK Out:     ", outOkAbs);
console.log("Bad Out:    ", outBadAbs);
console.log("----------------------------------------------------");

const boundaries = JSON.parse(fs.readFileSync(boundariesAbs, "utf8"));
const feats = Array.isArray(boundaries?.features) ? boundaries.features : [];
console.log(`[zoningLookup] Loaded zoning features: ${feats.length.toLocaleString()}`);

const bboxes = new Array(feats.length);
const grid = new Map();

let skippedNonWgs = 0;
let skippedHugeSpan = 0;

let global = { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity };

for (let i = 0; i < feats.length; i++) {
  const f = feats[i];
  const bb = getBBox(f?.geometry);
  if (!bb) continue;

  // track global range (for debugging)
  global.minX = Math.min(global.minX, bb[0]);
  global.minY = Math.min(global.minY, bb[1]);
  global.maxX = Math.max(global.maxX, bb[2]);
  global.maxY = Math.max(global.maxY, bb[3]);

  // IMPORTANT: if not WGS84 degrees, DO NOT index (prevents Map explosion)
  if (!isWGS84BBox(bb)) {
    skippedNonWgs++;
    continue;
  }

  bboxes[i] = bb;

  const minCellY = Math.floor(bb[1] / CELL);
  const maxCellY = Math.floor(bb[3] / CELL);
  const minCellX = Math.floor(bb[0] / CELL);
  const maxCellX = Math.floor(bb[2] / CELL);

  const spanY = (maxCellY - minCellY + 1);
  const spanX = (maxCellX - minCellX + 1);
  const cellCount = spanY * spanX;

  // safety: a single rogue bbox can still explode memory even if “looks” valid
  if (cellCount > 25000) {
    skippedHugeSpan++;
    continue;
  }

  for (let cy = minCellY; cy <= maxCellY; cy++) {
    for (let cx = minCellX; cx <= maxCellX; cx++) {
      const k = `${cy},${cx}`;
      let arr = grid.get(k);
      if (!arr) { arr = []; grid.set(k, arr); }
      arr.push(i);
    }
  }
}

console.log("[zoningLookup] Global bbox seen:", global);
console.log(`[zoningLookup] Grid cells: ${grid.size.toLocaleString()}`);
console.log(`[zoningLookup] Skipped non-WGS84 features: ${skippedNonWgs.toLocaleString()}`);
console.log(`[zoningLookup] Skipped huge-span features: ${skippedHugeSpan.toLocaleString()}`);

if (grid.size === 0) {
  console.log("====================================================");
  console.log("❌ No indexable WGS84 zoning features were found.");
  console.log("This means your zoningBoundariesData.geojson is likely in a projected CRS (ex: EPSG:26986 or EPSG:2249).");
  console.log("Reproject it to EPSG:4326, then rerun.");
  console.log("====================================================");
  process.exit(1);
}

let processed = 0, matched = 0, unmatched = 0, badJson = 0;

const outOk = fs.createWriteStream(outOkAbs, { flags: "w" });
const outBad = fs.createWriteStream(outBadAbs, { flags: "w" });

const rl = readline.createInterface({
  input: fs.createReadStream(inputAbs),
  crlfDelay: Infinity
});

for await (const line of rl) {
  const l = line.trim();
  if (!l) continue;
  processed++;

  let o;
  try { o = JSON.parse(l); }
  catch { badJson++; continue; }

  const lat = o?.latitude;
  const lon = o?.longitude;

  if (typeof lat !== "number" || typeof lon !== "number") {
    unmatched++;
    outBad.write(JSON.stringify(o) + "\n");
    continue;
  }

  const k = cellKey(lat, lon, CELL);
  const candidates = grid.get(k);

  let hitIndex = -1;

  if (candidates && candidates.length) {
    const pt = point([lon, lat]);
    for (const idx of candidates) {
      const bb = bboxes[idx];
      if (!bb) continue;
      if (lon < bb[0] || lon > bb[2] || lat < bb[1] || lat > bb[3]) continue;

      const poly = feats[idx];
      try {
        if (booleanPointInPolygon(pt, poly)) { hitIndex = idx; break; }
      } catch {}
    }
  }

  if (hitIndex >= 0) {
    const feat = feats[hitIndex];
    const { district, props } = pickDistrictProps(feat?.properties || {});
    o.zoning = {
      district,
      districtProps: {
        district,
        source: props.SOURCE ?? props.Source ?? null,
        town: props.TOWN ?? props.TOWN_DESC ?? props.CITY ?? props.MUNI ?? null
      }
    };
    o.zoningConfidence = 0.95;
    o.zoningMatchMethod = "point_in_polygon_grid";
    matched++;
    outOk.write(JSON.stringify(o) + "\n");
  } else {
    unmatched++;
    outBad.write(JSON.stringify(o) + "\n");
  }

  if (processed % 25000 === 0) {
    console.log(`[zoning] processed=${processed.toLocaleString()} matched=${matched.toLocaleString()} unmatched=${unmatched.toLocaleString()} badJson=${badJson.toLocaleString()}`);
  }
}

outOk.end();
outBad.end();

console.log("====================================================");
console.log(" ZONING ATTACH COMPLETE");
console.log(" Total:     ", processed);
console.log(" Matched:   ", matched);
console.log(" Unmatched: ", unmatched);
console.log(" Bad JSON:  ", badJson);
console.log("----------------------------------------------------");
console.log(" OK File:  ", outOkAbs);
console.log(" BAD File: ", outBadAbs);
console.log("====================================================");
