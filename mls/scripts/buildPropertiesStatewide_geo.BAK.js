/**
 * BUILD PROPERTIES_STATEWIDE_GEO (vNext) — DROP-IN v3
 * ---------------------------------------------------
 * Reads parcel NDJSON (2.55M MA parcels) and attaches WGS84 lat/lng.
 *
 * Primary source:
 *   - publicData/addresses/addressIndex.json  (MassGIS Master Address Points index)
 *     Keys look like: "115|SHORE ROAD|01516" and "115|SHORE ROAD"
 *     Values often look like: { lat: 862419.66, lon: 177931.40 }  (EPSG:26986 StatePlane meters)
 *
 * Fallback source (optional):
 *   - publicData/parcels/parcelCentroidIndex.json
 *     NOTE: In your repo this file is typically keyed by FULL ADDRESS (space style),
 *           e.g. "68 KINGSBURY ST NEEDHAM" or "9 PEACH ST BRAINTREE 02184"
 *     and values can be [x,y] or {x,y} (often EPSG:26986).
 *
 * Output:
 *   - publicData/properties/properties_statewide_geo.ndjson
 *   - publicData/properties/properties_statewide_geo_meta.json
 *
 * Run:
 *   node --max-old-space-size=8192 mls/scripts/buildPropertiesStatewide_geo.js
 */

import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..", "..");

// ---------- inputs ----------
const IN_NDJSON = path.join(ROOT, "publicData", "parcels", "parcels.ndjson"); // <- your existing file
const ADDRESS_INDEX = path.join(ROOT, "publicData", "addresses", "addressIndex.json"); // <- confirmed path
const CENTROID_INDEX = path.join(ROOT, "publicData", "parcels", "parcelCentroidIndex.json"); // optional fallback

// ---------- output ----------
const OUT_DIR = path.join(ROOT, "publicData", "properties");
const OUT_NDJSON = path.join(OUT_DIR, "properties_statewide_geo.ndjson");
const OUT_META = path.join(OUT_DIR, "properties_statewide_geo_meta.json");

// ---------- flags ----------
const USE_CENTROID_INDEX = true; // set false if you want to disable centroid fallback
const LOG_EVERY = 200_000;

// ---------- optional proj4 (for EPSG:26986 -> WGS84) ----------
let proj4 = null;
try {
  const mod = await import("proj4");
  proj4 = mod.default ?? mod;
  // NAD83 / Massachusetts Mainland (EPSG:26986) — meters
  proj4.defs(
    "EPSG:26986",
    "+proj=lcc +lat_1=41.71666666666667 +lat_2=42.68333333333333 +lat_0=41 +lon_0=-71.5 +x_0=200000 +y_0=750000 +datum=NAD83 +units=m +no_defs"
  );
} catch {
  proj4 = null;
}

// =============================
// helpers
// =============================
function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function cleanStr(s) {
  if (s == null) return "";
  return String(s).trim();
}
function padZip(z) {
  const d = cleanStr(z).replace(/[^\d]/g, "");
  if (!d) return null;
  return d.padStart(5, "0").slice(0, 5);
}
function normTown(t) {
  const s = cleanStr(t).toUpperCase();
  if (!s) return null;
  return s.replace(/\s+/g, " ").trim();
}
function stripPunct(s) {
  return s.replace(/[^A-Za-z0-9\s]/g, " ");
}
function stripUnitNoise(s) {
  let t = cleanStr(s);
  // remove "# 3", "UNIT 2", "APT 4B", "STE 12", etc
  t = t.replace(/\s+\b(?:UNIT|APT|APARTMENT|STE|SUITE)\b\s*[A-Z0-9\-]+/gi, "");
  t = t.replace(/\s+#\s*[A-Z0-9\-]+/gi, "");
  return t.trim();
}

// Street suffix normalization (listing-style + parcel-style)
const SUFFIX_MAP = new Map([
  ["RD", "ROAD"], ["RD.", "ROAD"], ["ROAD", "ROAD"],
  ["ST", "STREET"], ["ST.", "STREET"], ["STREET", "STREET"],
  ["AVE", "AVENUE"], ["AV", "AVENUE"], ["AVE.", "AVENUE"], ["AV.", "AVENUE"], ["AVENUE", "AVENUE"],
  ["BLVD", "BOULEVARD"], ["BLVD.", "BOULEVARD"], ["BOULEVARD", "BOULEVARD"],
  ["DR", "DRIVE"], ["DR.", "DRIVE"], ["DRIVE", "DRIVE"],
  ["LN", "LANE"], ["LN.", "LANE"], ["LANE", "LANE"],
  ["CT", "COURT"], ["CT.", "COURT"], ["COURT", "COURT"],
  ["PL", "PLACE"], ["PL.", "PLACE"], ["PLACE", "PLACE"],
  ["PKWY", "PARKWAY"], ["PARKWAY", "PARKWAY"],
  ["CIR", "CIRCLE"], ["CIRCLE", "CIRCLE"],
  ["HWY", "HIGHWAY"], ["HIGHWAY", "HIGHWAY"],
  ["TER", "TERRACE"], ["TERRACE", "TERRACE"],
  ["TRL", "TRAIL"], ["TRAIL", "TRAIL"],
  ["EXT", "EXTENSION"], ["EXTENSION", "EXTENSION"],
  ["SQ", "SQUARE"], ["SQUARE", "SQUARE"],
  ["CTR", "CENTER"], ["CENTER", "CENTER"],
]);

function normalizeStreetName(streetNameRaw) {
  const s0 = cleanStr(streetNameRaw);
  if (!s0) return null;

  const noUnit = stripUnitNoise(s0);
  let s = stripPunct(noUnit).toUpperCase().replace(/\s+/g, " ").trim();
  if (!s) return null;

  const parts = s.split(" ").filter(Boolean);
  if (!parts.length) return null;

  // Handle trailing directional (E/W/N/S/NE/NW/SE/SW)
  const DIRS = new Set(["N", "S", "E", "W", "NE", "NW", "SE", "SW"]);
  const last = parts[parts.length - 1];
  const hasDir = DIRS.has(last);
  const idx = hasDir ? parts.length - 2 : parts.length - 1;

  if (idx >= 0) {
    const cand = parts[idx];
    const repl = SUFFIX_MAP.get(cand);
    if (repl) parts[idx] = repl;
  }
  return parts.join(" ").trim();
}

function parseStreetNoVariants(streetNoRaw) {
  const s0 = cleanStr(streetNoRaw);
  if (!s0) return [];

  // ranges like "90-96"
  const m = s0.match(/^\s*(\d+)\s*[-–]\s*(\d+)\s*$/);
  if (m) {
    const a = m[1];
    const b = m[2];
    const out = [];
    if (a) out.push(a);
    if (b && b !== a) out.push(b);
    return out;
  }

  // suffix like "12A"
  const m2 = s0.match(/^\s*(\d+)([A-Za-z])?\s*$/);
  if (m2) {
    const base = m2[1];
    const suf = m2[2];
    if (base && suf) return [`${base}${suf.toUpperCase()}`, base];
    if (base) return [base];
  }

  // leading digits
  const m3 = s0.match(/^\s*(\d+)/);
  if (m3) return [m3[1]];

  return [];
}

function getPropsContainer(o) {
  // supports either plain NDJSON rows or GeoJSON Feature rows
  if (o && o.type === "Feature" && o.properties && typeof o.properties === "object") return o.properties;
  return o;
}

function pickAny(obj, keys) {
  for (const k of keys) {
    const v = obj?.[k];
    if (v != null && cleanStr(v)) return v;
  }
  return null;
}

function pickParcelId(o) {
  const p = getPropsContainer(o);
  const v = pickAny(p, [
    "parcel_id", "parcelId",
    "LOC_ID", "loc_id",
    "MAP_PAR_ID", "map_par_id",
    "PARCEL_ID",
  ]);
  if (v == null) return null;
  return String(v).trim().replace(/\s+/g, " ");
}

function pickAddressFields(o, townsMap) {
  const p = getPropsContainer(o);

  // Most common parcel fields: SITE_ADDR, SITE_CITY, SITE_ZIP
  const siteAddr = pickAny(p, ["SITE_ADDR", "site_addr", "siteAddr", "ADDR", "addr", "FULL_ADDR", "full_addr", "fullAddress"]);
  const siteCity = pickAny(p, ["SITE_CITY", "site_city", "siteCity", "TOWN", "town", "CITY", "city", "MUNICIPALI", "municipality"]);
  const siteZip = pickAny(p, ["SITE_ZIP", "site_zip", "siteZip", "ZIP", "zip", "ZIP_CODE", "zip_code"]);

  // Sometimes the parcel row has street_no / street_name directly
  const streetNo = pickAny(p, ["street_no", "streetNo", "STREET_NO", "ST_NUM", "st_num", "ADDR_NUM", "addr_num", "HOUSE_NUM", "house_num"]);
  const streetName = pickAny(p, ["street_name", "streetName", "STREET_NAME", "ST_NAME", "st_name", "ROADNAME", "roadname"]);

  // MLS towns map is for town_num -> town; parcels usually already have town
  const town_num = pickAny(p, ["town_num", "townNum", "TOWN_NUM"]);
  const townFromNum = townsMap && town_num ? townsMap.get(String(town_num).replace(/[^\d]/g, "")) : null;

  // If we have SITE_ADDR but not street parts, parse "123 MAIN ST"
  let no = streetNo ?? null;
  let name = streetName ?? null;

  if ((!no || !name) && siteAddr) {
    const m = String(siteAddr).trim().match(/^\s*([0-9A-Za-z\-–]+)\s+(.*)\s*$/);
    if (m) {
      if (!no) no = m[1];
      if (!name) name = m[2];
    }
  }

  return {
    street_no: no != null ? String(no).trim() : null,
    street_name: name != null ? String(name).trim() : null,
    town: siteCity ?? townFromNum ?? null,
    zip: siteZip ?? null,
    raw_site_addr: siteAddr ?? null,
  };
}

// =============================
// key building
// =============================
function buildAddressIndexKeys(addr) {
  // For addressIndex.json (pipe style):
  //   "{no}|{STREET}|{ZIP}" and "{no}|{STREET}"
  const zip5 = padZip(addr.zip);
  const st = normalizeStreetName(addr.street_name);
  const noVars = parseStreetNoVariants(addr.street_no);

  if (!st || !noVars.length) return [];

  const keys = [];
  for (const no of noVars) {
    if (zip5) keys.push(`${no}|${st}|${zip5}`);
    keys.push(`${no}|${st}`);
  }
  return keys;
}

function buildCentroidKeys(addr) {
  // For parcelCentroidIndex.json (space style):
  // Examples in your file: "68 KINGSBURY ST NEEDHAM" / "9 PEACH ST BRAINTREE 02184"
  const zip5 = padZip(addr.zip);
  const town = normTown(addr.town);
  const st = normalizeStreetName(addr.street_name);
  const noVars = parseStreetNoVariants(addr.street_no);

  if (!st || !noVars.length) return [];

  const keys = [];
  for (const no of noVars) {
    if (town && zip5) keys.push(`${no} ${st} ${town} ${zip5}`);
    if (town) keys.push(`${no} ${st} ${town}`);
    if (zip5) keys.push(`${no} ${st} ${zip5}`);
    keys.push(`${no} ${st}`);
  }
  return keys;
}

// =============================
// coords parsing + projection
// =============================
function looksLikeWgs84(x, y) {
  return (
    typeof x === "number" &&
    typeof y === "number" &&
    Number.isFinite(x) &&
    Number.isFinite(y) &&
    Math.abs(x) <= 180 &&
    Math.abs(y) <= 90
  );
}

function normalizeWgsPairMaybeSwap(a, b) {
  // We expect (lng,lat). If it's (lat,lng), swap.
  let lng = Number(a);
  let lat = Number(b);
  if (!Number.isFinite(lng) || !Number.isFinite(lat)) return { lng: null, lat: null, swapped: false };

  // If first looks like lat and second like lng (MA: lat ~41-43, lng ~ -73..-69)
  const firstIsLat = lng > 30 && lng < 50;
  const secondIsLng = lat < -60 && lat > -80;
  if (firstIsLat && secondIsLng) {
    const tmp = lng;
    lng = lat;
    lat = tmp;
    return { lng, lat, swapped: true };
  }
  return { lng, lat, swapped: false };
}

function statePlaneToWgs84(x, y) {
  if (!proj4) return null;
  if (!Number.isFinite(x) || !Number.isFinite(y)) return null;

  // proj4 expects [x,y] = [easting,northing] meters
  try {
    // proj4 returns [lng,lat] for WGS84 when using array form
    const out = proj4("EPSG:26986", "WGS84", [x, y]);
    if (!Array.isArray(out) || out.length < 2) return null;

    const fixed = normalizeWgsPairMaybeSwap(out[0], out[1]);
    if (fixed.lat == null || fixed.lng == null) return null;

    return { lat: fixed.lat, lng: fixed.lng, order: "lnglat", swapped: fixed.swapped };
  } catch {
    return null;
  }
}

function extractXY(v) {
  // Supported input shapes:
  //  - { lat: 862419.66, lon: 177931.40 } (addressIndex legacy names, but stateplane meters)
  //  - { x, y } / { easting, northing } / { lon, lat } / { lng, lat }
  //  - [x,y]
  if (v == null) return null;

  // array
  if (Array.isArray(v) && v.length >= 2) {
    const x = Number(v[0]);
    const y = Number(v[1]);
    if (Number.isFinite(x) && Number.isFinite(y)) return { x, y, src: "array" };
    return null;
  }

  if (typeof v === "object") {
    const x =
      v.x ?? v.X ??
      v.lon ?? v.lng ?? v.LON ?? v.LNG ??
      v.easting ?? v.E ??
      // addressIndex uses { lat: <x>, lon: <y> } in your file (legacy)
      v.lat ??
      null;

    const y =
      v.y ?? v.Y ??
      v.lat ?? v.LAT ??
      v.northing ?? v.N ??
      // addressIndex uses { lat: <x>, lon: <y> } in your file (legacy)
      v.lon ??
      null;

    const xn = Number(x);
    const yn = Number(y);
    if (Number.isFinite(xn) && Number.isFinite(yn)) return { x: xn, y: yn, src: "obj" };
  }

  return null;
}

function parseCoordLikeAddressIndex(v) {
  // addressIndex sample: { lat: 862419.66, lon: 177931.40 }
  // Those are EPSG:26986 meters (StatePlane), not WGS84 degrees.
  if (v == null) return null;

  const xy = extractXY(v);
  if (!xy) return null;

  const x = xy.x;
  const y = xy.y;

  // already degrees?
  if (looksLikeWgs84(x, y)) {
    const fixed = normalizeWgsPairMaybeSwap(x, y);
    return { lat: fixed.lat, lng: fixed.lng, crs: "WGS84", fmt: `wgs84:${xy.src}${fixed.swapped ? ":swap" : ""}` };
  }

  // treat as EPSG:26986
  const proj = statePlaneToWgs84(x, y);
  if (proj && proj.lat != null && proj.lng != null) {
    return {
      lat: proj.lat,
      lng: proj.lng,
      x_sp: x,
      y_sp: y,
      crs: "EPSG:26986",
      fmt: `26986:${xy.src}->wgs84:${proj.order}${proj.swapped ? ":swap" : ""}`,
    };
  }

  // if proj4 missing or failed, keep raw as debug
  return { lat: null, lng: null, x_sp: x, y_sp: y, crs: "EPSG:26986", fmt: `26986:${xy.src}:no-proj4` };
}

// =============================
// towns map (optional)
 // =============================
const TOWNS_FILE = path.join(ROOT, "mls", "raw", "reference", "towns.txt");
function readTownsMapIfPresent() {
  if (!fs.existsSync(TOWNS_FILE)) return null;
  const txt = fs.readFileSync(TOWNS_FILE, "utf8");
  const lines = txt.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  const map = new Map();
  for (const line of lines) {
    let parts = line.split("|");
    if (parts.length < 2) parts = line.split("\t");
    if (parts.length < 2) parts = line.split(",");
    if (parts.length < 2) parts = line.split(/\s{2,}/);
    if (parts.length >= 2) {
      const a = cleanStr(parts[0]).replace(/[^\d]/g, "");
      const b = cleanStr(parts[1]);
      if (a && b) map.set(a, b);
      continue;
    }
    const m = line.match(/^\s*(\d+)\s+(.+)\s*$/);
    if (m) map.set(m[1], m[2]);
  }
  return map.size ? map : null;
}

// =============================
// main
// =============================
console.log("====================================================");
console.log("   BUILD PROPERTIES_STATEWIDE_GEO (vNext) — DROP-IN v3");
console.log("====================================================");
console.log("IN_NDJSON:     ", IN_NDJSON);
console.log("ADDRESS_INDEX: ", ADDRESS_INDEX);
console.log("CENTROID_INDEX:", CENTROID_INDEX, `(USE_CENTROID_INDEX=${USE_CENTROID_INDEX})`);
console.log("OUT_NDJSON:    ", OUT_NDJSON);
console.log("OUT_META:      ", OUT_META);
console.log("proj4:         ", proj4 ? "loaded" : "NOT available");
console.log("====================================================");

if (!fs.existsSync(IN_NDJSON)) throw new Error(`Missing: ${IN_NDJSON}`);
if (!fs.existsSync(ADDRESS_INDEX)) throw new Error(`Missing: ${ADDRESS_INDEX}`);

ensureDir(OUT_DIR);

const townsMap = readTownsMapIfPresent();
if (townsMap) console.log(`[load] towns map: ${townsMap.size.toLocaleString()} entries (${path.basename(TOWNS_FILE)})`);
else console.log(`[info] no towns map found at ${TOWNS_FILE} (ok)`);

console.log("[load] addressIndex.json ...");
const addressIndex = JSON.parse(fs.readFileSync(ADDRESS_INDEX, "utf8"));
const addressIndexKeyCount = Object.keys(addressIndex).length;
const addressIndexSampleKey = Object.keys(addressIndex)[0];
console.log(`[load] addressIndex keys: ${addressIndexKeyCount.toLocaleString()} sampleKey=${JSON.stringify(addressIndexSampleKey)}`);

let centroidIndex = null;
let centroidSampleKey = null;
if (USE_CENTROID_INDEX && fs.existsSync(CENTROID_INDEX)) {
  console.log("[load] parcelCentroidIndex.json ...");
  centroidIndex = JSON.parse(fs.readFileSync(CENTROID_INDEX, "utf8"));
  const keys = Object.keys(centroidIndex);
  centroidSampleKey = keys[0] ?? null;
  console.log(`[load] centroidIndex keys: ${keys.length.toLocaleString()} sampleKey=${JSON.stringify(centroidSampleKey)}`);
} else {
  console.log(`[info] centroidIndex not loaded (USE_CENTROID_INDEX=${USE_CENTROID_INDEX}, exists=${fs.existsSync(CENTROID_INDEX)})`);
}

const out = fs.createWriteStream(OUT_NDJSON, { flags: "w" });

const meta = {
  inputs: {
    parcels: IN_NDJSON,
    addressIndex: ADDRESS_INDEX,
    centroidIndex: USE_CENTROID_INDEX ? CENTROID_INDEX : null,
  },
  samples: {
    addressIndexSampleKey,
    centroidSampleKey,
  },
  totals: {
    linesRead: 0,
    rowsWritten: 0,
    addrPresent: 0,
    addrMissing: 0,

    coordsMatched: 0,
    coordsFromAddressIndex: 0,
    coordsFromCentroidIndex: 0,

    hadLatLng: 0,
    missingLatLng: 0,

    stateplaneOnly: 0,
    badJson: 0,
  },
  builtAt: new Date().toISOString(),
};

const rl = readline.createInterface({ input: fs.createReadStream(IN_NDJSON) });

for await (const line of rl) {
  if (!line.trim()) continue;
  meta.totals.linesRead++;

  let o;
  try {
    o = JSON.parse(line);
  } catch {
    meta.totals.badJson++;
    continue;
  }

  const parcelId = pickParcelId(o);

  // address parts from parcel row
  const addr = pickAddressFields(o, townsMap);

  const hasAnyAddr = !!(addr.street_no && addr.street_name);
  if (hasAnyAddr) meta.totals.addrPresent++;
  else meta.totals.addrMissing++;

  let lat = null;
  let lng = null;
  let coord_source = null;
  let coord_key_used = null;

  // 0) If parcel already has WGS84 lat/lng in-row (rare), use it.
  const p = getPropsContainer(o);
  const lat0 = Number(p?.lat ?? p?.LAT ?? p?.latitude ?? p?.LATITUDE ?? NaN);
  const lng0 = Number(p?.lng ?? p?.LNG ?? p?.lon ?? p?.LON ?? p?.longitude ?? p?.LONGITUDE ?? NaN);
  if (Number.isFinite(lat0) && Number.isFinite(lng0) && looksLikeWgs84(lng0, lat0)) {
    lat = lat0;
    lng = lng0;
    coord_source = "parcel:existing_wgs84";
  }

  // 1) Try addressIndex (pipe keys)
  if ((lat == null || lng == null) && hasAnyAddr) {
    const keys = buildAddressIndexKeys(addr);
    for (const k of keys) {
      const v = addressIndex[k];
      if (!v) continue;
      const c = parseCoordLikeAddressIndex(v);
      if (c && c.lat != null && c.lng != null) {
        lat = c.lat;
        lng = c.lng;
        coord_source = `addressIndex:${c.fmt}`;
        coord_key_used = k;

        meta.totals.coordsMatched++;
        meta.totals.coordsFromAddressIndex++;
        if (c.crs === "EPSG:26986" && (lat == null || lng == null)) meta.totals.stateplaneOnly++;
        break;
      } else if (c && c.crs === "EPSG:26986" && (c.lat == null || c.lng == null)) {
        meta.totals.stateplaneOnly++;
      }
    }
  }

  // 2) Fallback: centroidIndex by ADDRESS (space keys)
  if ((lat == null || lng == null) && centroidIndex && hasAnyAddr) {
    const keys = buildCentroidKeys(addr);
    for (const k of keys) {
      const v = centroidIndex[k];
      if (!v) continue;
      const c = parseCoordLikeAddressIndex(v);
      if (c && c.lat != null && c.lng != null) {
        lat = c.lat;
        lng = c.lng;
        coord_source = `centroidIndex:${c.fmt}`;
        coord_key_used = k;

        meta.totals.coordsMatched++;
        meta.totals.coordsFromCentroidIndex++;
        break;
      } else if (c && c.crs === "EPSG:26986" && (c.lat == null || c.lng == null)) {
        meta.totals.stateplaneOnly++;
      }
    }
  }

  if (lat != null && lng != null) meta.totals.hadLatLng++;
  else meta.totals.missingLatLng++;

  // Emit property record (parcel-first, property_id == parcelId)
  const record = {
    property_id: parcelId,
    parcel_id: parcelId, // keep both for now
    // keep minimal address fields
    street_no: addr.street_no ?? null,
    street_name: addr.street_name ?? null,
    town: addr.town ?? null,
    zip: padZip(addr.zip),
    full_address:
      addr.street_no && addr.street_name
        ? `${String(addr.street_no).trim()} ${String(addr.street_name).trim()}`.trim()
        : (addr.raw_site_addr ?? null),
    lat,
    lng,
    coord_source,
    coord_key_used,
  };

  out.write(JSON.stringify(record) + "\n");
  meta.totals.rowsWritten++;

  if (meta.totals.linesRead % LOG_EVERY === 0) {
    console.log(
      `[progress] read=${meta.totals.linesRead.toLocaleString()} matched=${meta.totals.coordsMatched.toLocaleString()} ` +
      `addrIdx=${meta.totals.coordsFromAddressIndex.toLocaleString()} centroid=${meta.totals.coordsFromCentroidIndex.toLocaleString()} ` +
      `hasLatLng=${meta.totals.hadLatLng.toLocaleString()} missingLatLng=${meta.totals.missingLatLng.toLocaleString()}`
    );
  }
}

out.end();
fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2));

console.log("----------------------------------------------------");
console.log("✅ PROPERTIES_STATEWIDE_GEO COMPLETE");
console.log("Lines read:        ", meta.totals.linesRead.toLocaleString());
console.log("Rows written:      ", meta.totals.rowsWritten.toLocaleString());
console.log("Addr present:      ", meta.totals.addrPresent.toLocaleString());
console.log("Addr missing:      ", meta.totals.addrMissing.toLocaleString());
console.log("Coords matched:    ", meta.totals.coordsMatched.toLocaleString());
console.log("  from addressIdx: ", meta.totals.coordsFromAddressIndex.toLocaleString());
console.log("  from centroidIdx:", meta.totals.coordsFromCentroidIndex.toLocaleString());
console.log("Had lat/lng:       ", meta.totals.hadLatLng.toLocaleString());
console.log("Missing lat/lng:   ", meta.totals.missingLatLng.toLocaleString());
console.log("StatePlane only:   ", meta.totals.stateplaneOnly.toLocaleString());
console.log("Bad JSON:          ", meta.totals.badJson.toLocaleString());
console.log("Output:            ", OUT_NDJSON);
console.log("Meta:              ", OUT_META);
console.log("====================================================");
