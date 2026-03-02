/**
 * ATTACH ZIP CODE TO PROPERTIES (MassGIS ZIPCODES_NT_POLY) — vNext
 * --------------------------------------------------------------
 * Goal:
 *   Fill missing property ZIPs using point-in-polygon against MassGIS ZIP polygons.
 *
 * Inputs:
 *   - publicData/properties/properties_statewide_geo.ndjson
 *   - publicData/zipcodes/ZIPCODES_NT_POLY.geojson   (convert from shp first; see commands below)
 *
 * Outputs:
 *   - publicData/properties/properties_statewide_geo_zip.ndjson
 *   - publicData/properties/properties_statewide_geo_zip_meta.json
 *
 * Convert SHP → GeoJSON (run in C:\seller-app\backend):
 *   mkdir publicData\zipcodes -Force | Out-Null
 *   mkdir publicData\zipcodes\statewide -Force | Out-Null
 *
 *   # Requires GDAL (ogr2ogr). Check:
 *   where ogr2ogr
 *
 *   # Convert to RFC7946 GeoJSON in WGS84:
 *   ogr2ogr -f GeoJSON -t_srs EPSG:4326 -lco RFC7946=YES ^
 *     publicData\zipcodes\ZIPCODES_NT_POLY.geojson ^
 *     publicData\zipcodes\statewide\ZIPCODES_NT_POLY.shp
 *
 * Run:
 *   node --max-old-space-size=8192 mls/scripts/attachZipToProperties.js
 *
 * Notes:
 *   - MassGIS ZIP polygon attribute is typically POSTCODE (5-digit zip).
 *   - We do a fast grid index (default cell size 0.05°) to avoid scanning all polygons per point.
 */

import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ROOT = path.resolve(__dirname, "..", "..");

const IN_PROPS = path.join(ROOT, "publicData", "properties", "properties_statewide_geo.ndjson");
const ZIP_GEOJSON = path.join(ROOT, "publicData", "zipcodes", "ZIPCODES_NT_POLY.geojson");

const OUT_DIR = path.join(ROOT, "publicData", "properties");
const OUT_PROPS = path.join(OUT_DIR, "properties_statewide_geo_zip.ndjson");
const OUT_META = path.join(OUT_DIR, "properties_statewide_geo_zip_meta.json");

// Grid index cell size in degrees (tune: 0.02 → more cells / fewer candidates; 0.05 → fewer cells / more candidates)
const CELL = Number(process.env.ZIP_GRID_CELL_DEG ?? "0.05");

// Optional: if you only want to fill when zip is missing (default true). Set FILL_ALWAYS=1 to overwrite.
const FILL_ALWAYS = String(process.env.FILL_ALWAYS ?? "").trim() === "1";

function padZip(z) {
  const d = String(z ?? "").replace(/[^\d]/g, "");
  if (!d) return null;
  return d.padStart(5, "0").slice(0, 5);
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function gridKey(lat, lng) {
  const i = Math.floor(lat / CELL);
  const j = Math.floor(lng / CELL);
  return `${i},${j}`;
}

/** Ray casting point-in-ring (ring = [[lng,lat], ...] ) */
function pointInRing(lng, lat, ring) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0], yi = ring[i][1];
    const xj = ring[j][0], yj = ring[j][1];
    const denom = (yj - yi) || 1e-12;
    const intersect = ((yi > lat) !== (yj > lat)) && (lng < (xj - xi) * (lat - yi) / denom + xi);
    if (intersect) inside = !inside;
  }
  return inside;
}

function pointInPolygon(lng, lat, geom) {
  if (!geom) return false;

  if (geom.type === "Polygon") {
    const rings = geom.coordinates;
    if (!rings?.length) return false;
    if (!pointInRing(lng, lat, rings[0])) return false;
    for (let k = 1; k < rings.length; k++) {
      if (pointInRing(lng, lat, rings[k])) return false; // in a hole
    }
    return true;
  }

  if (geom.type === "MultiPolygon") {
    const polys = geom.coordinates;
    for (const poly of polys) {
      if (!poly?.length) continue;
      if (!pointInRing(lng, lat, poly[0])) continue;
      let inHole = false;
      for (let k = 1; k < poly.length; k++) {
        if (pointInRing(lng, lat, poly[k])) {
          inHole = true;
          break;
        }
      }
      if (!inHole) return true;
    }
    return false;
  }

  return false;
}

function bboxOfGeometry(geom) {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;

  const bump = (x, y) => {
    if (x < minX) minX = x;
    if (y < minY) minY = y;
    if (x > maxX) maxX = x;
    if (y > maxY) maxY = y;
  };

  const walkCoords = (coords) => {
    if (!coords) return;
    if (typeof coords[0] === "number" && typeof coords[1] === "number") {
      bump(coords[0], coords[1]);
      return;
    }
    for (const c of coords) walkCoords(c);
  };

  walkCoords(geom.coordinates);

  if (!isFinite(minX) || !isFinite(minY) || !isFinite(maxX) || !isFinite(maxY)) return null;
  return { minX, minY, maxX, maxY };
}

function loadZipFeatures() {
  if (!fs.existsSync(ZIP_GEOJSON)) {
    throw new Error(`Missing ZIP GeoJSON: ${ZIP_GEOJSON}\nConvert ZIPCODES_NT_POLY.shp → GeoJSON (see header comments).`);
  }
  console.log("[load] zip polygons geojson ...");
  const gj = JSON.parse(fs.readFileSync(ZIP_GEOJSON, "utf8"));
  const feats = Array.isArray(gj?.features) ? gj.features : [];
  if (!feats.length) throw new Error("ZIP GeoJSON has no features.");

  const out = [];
  for (const f of feats) {
    const props = f?.properties ?? {};
    const zip =
      padZip(props.POSTCODE ?? props.POST_CODE ?? props.ZIP ?? props.ZIPCODE ?? props.ZCTA5 ?? props.ZCTA5CE10 ?? props.ZIP5);

    const geom = f?.geometry;
    if (!zip || !geom) continue;

    const bbox = bboxOfGeometry(geom);
    if (!bbox) continue;

    out.push({ zip, bbox, geom });
  }
  console.log(`[load] zip features: ${out.length.toLocaleString()}`);
  return out;
}

function buildZipGridIndex(zipFeatures) {
  console.log(`[index] building grid index (cell=${CELL}°) ...`);
  const grid = new Map();

  for (let idx = 0; idx < zipFeatures.length; idx++) {
    const b = zipFeatures[idx].bbox;
    const minLat = b.minY;
    const maxLat = b.maxY;
    const minLng = b.minX;
    const maxLng = b.maxX;

    const i0 = Math.floor(minLat / CELL);
    const i1 = Math.floor(maxLat / CELL);
    const j0 = Math.floor(minLng / CELL);
    const j1 = Math.floor(maxLng / CELL);

    for (let i = i0; i <= i1; i++) {
      for (let j = j0; j <= j1; j++) {
        const k = `${i},${j}`;
        let arr = grid.get(k);
        if (!arr) {
          arr = [];
          grid.set(k, arr);
        }
        arr.push(idx);
      }
    }
  }

  console.log(`[index] grid cells: ${grid.size.toLocaleString()}`);
  return grid;
}

function pickLatLng(o) {
  const lat = Number(o?.lat ?? o?.LAT ?? o?.latitude ?? o?.Latitude);
  const lng = Number(o?.lng ?? o?.LNG ?? o?.lon ?? o?.LON ?? o?.longitude ?? o?.Longitude);
  if (!isFinite(lat) || !isFinite(lng)) return null;
  if (lat < -90 || lat > 90 || lng < -180 || lng > 180) return null;
  return { lat, lng };
}

console.log("====================================================");
console.log("   ATTACH ZIP → PROPERTIES (MassGIS ZIPCODES_NT_POLY)");
console.log("====================================================");
console.log("IN_PROPS:    ", IN_PROPS);
console.log("ZIP_GEOJSON: ", ZIP_GEOJSON);
console.log("OUT_PROPS:   ", OUT_PROPS);
console.log("OUT_META:    ", OUT_META);
console.log("CELL(deg):   ", CELL);
console.log("FILL_ALWAYS: ", FILL_ALWAYS);
console.log("====================================================");

if (!fs.existsSync(IN_PROPS)) throw new Error(`Missing: ${IN_PROPS}`);
ensureDir(OUT_DIR);

const zipFeatures = loadZipFeatures();
const grid = buildZipGridIndex(zipFeatures);

const rl = readline.createInterface({ input: fs.createReadStream(IN_PROPS) });
const out = fs.createWriteStream(OUT_PROPS, { flags: "w" });

let read = 0;
let written = 0;

let hadZip = 0;
let filledZip = 0;
let stillMissingZip = 0;

let hasCoords = 0;
let missingCoords = 0;

let badJson = 0;

for await (const line of rl) {
  if (!line.trim()) continue;
  read++;

  let o;
  try {
    o = JSON.parse(line);
  } catch {
    badJson++;
    continue;
  }

  const existingZip = padZip(o.zip ?? o.ZIP ?? o.zip_code);
  const coords = pickLatLng(o);

  if (coords) hasCoords++;
  else missingCoords++;

  if (existingZip && !FILL_ALWAYS) {
    hadZip++;
    out.write(JSON.stringify(o) + "\n");
    written++;
    continue;
  }

  if (!coords) {
    if (!existingZip) stillMissingZip++;
    out.write(JSON.stringify(o) + "\n");
    written++;
    continue;
  }

  const { lat, lng } = coords;

  const k = gridKey(lat, lng);
  const candIdxs = grid.get(k) ?? [];
  let found = null;

  for (const idx of candIdxs) {
    const f = zipFeatures[idx];
    const b = f.bbox;
    if (lng < b.minX || lng > b.maxX || lat < b.minY || lat > b.maxY) continue;
    if (pointInPolygon(lng, lat, f.geom)) {
      found = f.zip;
      break;
    }
  }

  if (found) {
    filledZip++;
    const outObj = { ...o, zip: found, zip_source: "zipcodes_nt_poly" };
    out.write(JSON.stringify(outObj) + "\n");
  } else {
    if (!existingZip) stillMissingZip++;
    out.write(JSON.stringify(o) + "\n");
  }
  written++;

  if (read % 250000 === 0) {
    console.log(`[progress] read=${read.toLocaleString()} filledZip=${filledZip.toLocaleString()} hadZip=${hadZip.toLocaleString()} missingZip=${stillMissingZip.toLocaleString()}`);
  }
}

out.end();
await new Promise((r) => out.on("finish", r));

const meta = {
  input: IN_PROPS,
  zipGeojson: ZIP_GEOJSON,
  output: OUT_PROPS,
  cellDeg: CELL,
  fillAlways: FILL_ALWAYS,
  linesRead: read,
  linesWritten: written,
  hadZip,
  filledZip,
  stillMissingZip,
  hasCoords,
  missingCoords,
  badJson,
  builtAt: new Date().toISOString(),
};

fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2));

console.log("----------------------------------------------------");
console.log("✅ ZIP ATTACH COMPLETE");
console.log("Lines read:        ", read.toLocaleString());
console.log("Lines written:     ", written.toLocaleString());
console.log("Had zip:           ", hadZip.toLocaleString());
console.log("Filled zip:        ", filledZip.toLocaleString());
console.log("Still missing zip: ", stillMissingZip.toLocaleString());
console.log("Has coords:        ", hasCoords.toLocaleString());
console.log("Missing coords:    ", missingCoords.toLocaleString());
console.log("Bad JSON:          ", badJson.toLocaleString());
console.log("OUT_PROPS:         ", OUT_PROPS);
console.log("OUT_META:          ", OUT_META);
console.log("====================================================");
