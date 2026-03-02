/**
 * BUILD PROPERTIES_STATEWIDE_GEO (DROP-IN v12)
 * -------------------------------------------
 * Institutional rule: parcels are the stable anchor.
 *
 * This script builds a canonical properties NDJSON where:
 *  - parcel_id is the stable key (LOC_ID by default)
 *  - coordinates come from MassGIS Address Points (addressIndex.json) when possible
 *  - otherwise fall back to parcelCentroidByParcelId.json (already WGS84 lon/lat)
 *
 * Why this exists:
 *  - Your current parcels.ndjson appears to be missing address attributes, so "addr present: 0"
 *    and addressIndex can never be used (because there is no address key to match).
 *  - parcels.gpkg DOES contain address/assessor attributes; we stream those via ogr2ogr.
 *
 * Inputs (defaults):
 *  - publicData/parcels/parcels.gpkg  (layer=parcels, pid field=LOC_ID)
 *  - publicData/addresses/addressIndex.json
 *  - publicData/parcels/parcelCentroidByParcelId.json
 *
 * Output:
 *  - publicData/properties/properties_statewide_geo.ndjson
 *  - publicData/properties/properties_statewide_geo_meta.json
 *
 * Run (PowerShell):
 *   cd C:\seller-app\backend
 *   Copy-Item "$env:USERPROFILE\Downloads\buildPropertiesStatewide_geo_DROPIN_v12.js" .\mls\scripts\buildPropertiesStatewide_geo.js -Force
 *   node --max-old-space-size=8192 .\mls\scripts\buildPropertiesStatewide_geo.js
 */

import fs from "fs";
import path from "path";
import crypto from "crypto";
import readline from "readline";
import { spawn } from "child_process";

// ---------- tiny utils ----------
const nowIso = () => new Date().toISOString();
const cleanStr = (v) =>
  v == null ? "" : String(v).replace(/\u00a0/g, " ").trim();
const upper = (v) => cleanStr(v).toUpperCase();
const collapse = (s) => upper(s).replace(/\s+/g, " ").trim();
const exists = (p) => fs.existsSync(p);
const sha256File = (p) => {
  const h = crypto.createHash("sha256");
  const fd = fs.openSync(p, "r");
  const buf = Buffer.alloc(1024 * 1024);
  try {
    while (true) {
      const n = fs.readSync(fd, buf, 0, buf.length, null);
      if (!n) break;
      h.update(buf.slice(0, n));
    }
  } finally {
    fs.closeSync(fd);
  }
  return h.digest("hex");
};
const safeJson = (s) => {
  try { return JSON.parse(s); } catch { return null; }
};

function cleanBool(v) {
  const t = String(v ?? "").trim().toLowerCase();
  return t === "1" || t === "true" || t === "yes" || t === "y";
}

// ---------- paths ----------
const ROOT = process.cwd();
const PARCELS_GPKG = process.env.PARCELS_GPKG
  ? path.resolve(process.env.PARCELS_GPKG)
  : path.resolve(ROOT, "publicData/parcels/parcels.gpkg");

const PARCELS_LAYER = (process.env.PARCELS_LAYER || process.env.LAYER || "parcels").trim();
const PID_FIELD = (process.env.PID_FIELD || "LOC_ID").trim();

const ADDRESS_INDEX = process.env.ADDRESS_INDEX
  ? path.resolve(process.env.ADDRESS_INDEX)
  : path.resolve(ROOT, "publicData/addresses/addressIndex.json");

const CENTROID_BY_PID = process.env.CENTROID_BY_PID
  ? path.resolve(process.env.CENTROID_BY_PID)
  : path.resolve(ROOT, "publicData/parcels/parcelCentroidByParcelId.json");

const OUT_NDJSON = process.env.OUT_NDJSON
  ? path.resolve(process.env.OUT_NDJSON)
  : path.resolve(ROOT, "publicData/properties/properties_statewide_geo.ndjson");

const OUT_META = process.env.OUT_META
  ? path.resolve(process.env.OUT_META)
  : path.resolve(ROOT, "publicData/properties/properties_statewide_geo_meta.json");

const USE_ADDRESS_INDEX = cleanBool(process.env.USE_ADDRESS_INDEX ?? "1");

// ---------- proj4 optional ----------
let proj4 = null;
try {
  const mod = await import("proj4");
  proj4 = mod.default ?? mod;

  // NAD83 / Massachusetts Mainland (EPSG:26986) — meters
  proj4.defs(
    "EPSG:26986",
    "+proj=lcc +lat_1=41.71666666666667 +lat_2=42.68333333333333 +lat_0=41 +lon_0=-71.5 +x_0=200000 +y_0=750000 +datum=NAD83 +units=m +no_defs"
  );

  // NAD83 / Massachusetts Mainland (EPSG:2249) — US survey feet
  proj4.defs(
    "EPSG:2249",
    "+proj=lcc +lat_1=41.71666666666667 +lat_2=42.68333333333333 +lat_0=41 +lon_0=-71.5 +x_0=656166.6666666666 +y_0=2460625 +datum=NAD83 +units=us-ft +no_defs"
  );

  // NAD83 / Massachusetts Island (EPSG:26987) — meters
  proj4.defs(
    "EPSG:26987",
    "+proj=lcc +lat_1=41.28333333333333 +lat_2=41.48333333333333 +lat_0=41 +lon_0=-70.5 +x_0=100000 +y_0=0 +datum=NAD83 +units=m +no_defs"
  );
} catch {
  proj4 = null;
}

function looksWgs84(lon, lat) {
  return (
    Number.isFinite(lon) &&
    Number.isFinite(lat) &&
    Math.abs(lon) <= 180 &&
    Math.abs(lat) <= 90
  );
}
function looksLikeLargeXY(x, y) {
  return Number.isFinite(x) && Number.isFinite(y) && (Math.abs(x) > 1000 || Math.abs(y) > 1000);
}
function looksLikeMA(lon, lat) {
  return (
    looksWgs84(lon, lat) &&
    lon <= -69.5 && lon >= -73.8 &&
    lat >= 41.0 && lat <= 43.0
  );
}
function spToWgsTry(epsg, x, y) {
  if (!proj4) return null;
  try {
    const [lon, lat] = proj4(epsg, "WGS84", [x, y]);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) return null;
    return { lon, lat };
  } catch {
    return null;
  }
}
function convertStatePlaneToWgs(x, y) {
  // Try 2249 first (feet), then 26986/26987 (meters)
  const a = spToWgsTry("EPSG:2249", x, y);
  if (a && looksLikeMA(a.lon, a.lat)) return { ...a, epsg: "2249" };

  const b = spToWgsTry("EPSG:26986", x, y);
  if (b && looksLikeMA(b.lon, b.lat)) return { ...b, epsg: "26986" };

  const c = spToWgsTry("EPSG:26987", x, y);
  if (c && looksLikeMA(c.lon, c.lat)) return { ...c, epsg: "26987" };

  // Heuristic fallback: many datasets are 26986 meters stored as feet-ish. If it's huge (millions),
  // try ft->m then 26986. (v7 approach)
  const xM = x * 0.3048006096;
  const yM = y * 0.3048006096;
  const d = spToWgsTry("EPSG:26986", xM, yM);
  if (d && looksLikeMA(d.lon, d.lat)) return { ...d, epsg: "26986_ftToM" };

  return null;
}

function parseCoordLikeAddressIndex(v) {
  // addressIndex entries may be:
  //  - { lat: <deg>, lon: <deg> }
  //  - { lat: <stateplane_y>, lon: <stateplane_x> }  (yes, unfortunate naming)
  if (!v || typeof v !== "object") return null;

  const rawLat = Number(v.lat ?? v.latitude ?? v.y ?? v.Y ?? v.northing ?? v.N ?? NaN);
  const rawLon = Number(v.lon ?? v.lng ?? v.longitude ?? v.x ?? v.X ?? v.easting ?? v.E ?? NaN);

  if (!Number.isFinite(rawLat) || !Number.isFinite(rawLon)) return null;

  // If already degrees (lon/lat)
  if (looksWgs84(rawLon, rawLat) && looksLikeMA(rawLon, rawLat)) {
    return { lat: rawLat, lng: rawLon, crs: "WGS84" };
  }

  // If looks like projected XY, convert
  if (looksLikeLargeXY(rawLon, rawLat)) {
    const w = convertStatePlaneToWgs(rawLon, rawLat);
    if (w && looksLikeMA(w.lon, w.lat)) return { lat: w.lat, lng: w.lon, crs: w.epsg };
  }

  return null;
}

// ---------- address normalization ----------
const SUFFIX_EXPAND = new Map([
  ["RD", "ROAD"],
  ["ST", "STREET"],
  ["AVE", "AVENUE"],
  ["AV", "AVENUE"],
  ["BLVD", "BOULEVARD"],
  ["DR", "DRIVE"],
  ["LN", "LANE"],
  ["CT", "COURT"],
  ["PL", "PLACE"],
  ["PKWY", "PARKWAY"],
  ["CIR", "CIRCLE"],
  ["HWY", "HIGHWAY"],
  ["TER", "TERRACE"],
  ["TRL", "TRAIL"],
  ["SQ", "SQUARE"],
  ["CTR", "CENTER"],
  ["EXT", "EXTENSION"],
]);

function stripUnitNoise(s) {
  let t = collapse(s);
  t = t.replace(/\s+\b(?:UNIT|APT|APARTMENT|STE|SUITE)\b\s*[A-Z0-9\-]+/gi, "");
  t = t.replace(/\s+#\s*[A-Z0-9\-]+/gi, "");
  return t.trim();
}

function normalizeTown(townRaw) {
  let t = collapse(townRaw);
  // Common MassGIS/assessor patterns
  t = t.replace(/\s*,\s*(?:TOWN OF|CITY OF)\s*$/i, "");
  t = t.replace(/\b(?:TOWN OF|CITY OF)\b/i, "");
  t = t.replace(/\s+/g, " ").trim();
  return t;
}
// Back-compat: some callers use normalizeTownName()
function normalizeTownName(townRaw) {
  return normalizeTown(townRaw);
}

function expandSuffixOnce(street) {
  const parts = collapse(street).split(" ");
  if (!parts.length) return street;
  const last = parts[parts.length - 1];
  const rep = SUFFIX_EXPAND.get(last);
  if (!rep) return street;
  parts[parts.length - 1] = rep;
  return parts.join(" ");
}
// Back-compat: buildKeyVariants() calls expandSuffix(), so map it to your existing function.
function expandSuffix(street) {
  return expandSuffixOnce(street);
}

function buildKeyVariants({ streetNo, streetName, town, zip }) {
  const no = collapse(streetNo);
  const st0 = stripUnitNoise(streetName);
  const st1 = expandSuffixOnce(st0);
  const tw = normalizeTown(town);
  const z = collapse(zip);

  const base = (s) => `${no} ${s} ${tw}`.trim().replace(/\s+/g, " ");
  const out = [];

  if (no && st0 && tw) out.push(base(st0));
  if (no && st1 && tw && st1 !== st0) out.push(base(st1));

  // Some addressIndex keys include ZIP at end
  if (z) {
    if (no && st0 && tw) out.push(`${base(st0)} ${z}`.trim());
    if (no && st1 && tw && st1 !== st0) out.push(`${base(st1)} ${z}`.trim());
  }

  return [...new Set(out)];
}

// ---------- main ----------
function banner() {
  console.log("====================================================");
  console.log("   BUILD PROPERTIES_STATEWIDE_GEO (DROP-IN v12)");
  console.log("====================================================");
  console.log("PARCELS_GPKG:    ", PARCELS_GPKG);
  console.log("PARCELS_LAYER:   ", PARCELS_LAYER);
  console.log("PID_FIELD:       ", PID_FIELD);
  console.log("ADDRESS_INDEX:   ", ADDRESS_INDEX);
  console.log("CENTROID_BY_PID: ", CENTROID_BY_PID);
  console.log("OUT_NDJSON:      ", OUT_NDJSON);
  console.log("OUT_META:        ", OUT_META);
  console.log("USE_ADDRESS_INDEX:", USE_ADDRESS_INDEX);
  console.log("proj4:           ", proj4 ? "loaded" : "missing (centroid fallback still works)");
  console.log("====================================================");
}

function assertInputs() {
  const miss = [];
  if (!exists(PARCELS_GPKG)) miss.push(PARCELS_GPKG);
  if (USE_ADDRESS_INDEX && !exists(ADDRESS_INDEX)) miss.push(ADDRESS_INDEX);
  if (!exists(CENTROID_BY_PID)) miss.push(CENTROID_BY_PID);
  if (miss.length) throw new Error("Missing input(s):\n" + miss.map((p) => " - " + p).join("\n"));
  fs.mkdirSync(path.dirname(OUT_NDJSON), { recursive: true });
  fs.mkdirSync(path.dirname(OUT_META), { recursive: true });
}

function spawnOgr2OgrStream() {
  // Stream ONLY attributes we need (no geometry in SELECT)
  const sql = `SELECT ${PID_FIELD} AS parcel_id, MAP_PAR_ID AS map_par_id, FULL_STR AS full_str, ADDR_NUM AS addr_num, SITE_ADDR AS site_addr, CITY AS city, ZIP AS zip FROM ${PARCELS_LAYER}`;
  const args = [
    "-f", "GeoJSONSeq",
    "/vsistdout/",
    PARCELS_GPKG,
    "-dialect", "sqlite",
    "-sql", sql,
  ];

  const p = spawn("ogr2ogr", args, { stdio: ["ignore", "pipe", "pipe"] });
  return { p, args, sql };
}

async function main() {
  banner();
  assertInputs();

  const meta = {
    version: "v12",
    builtAt: nowIso(),
    inputs: {
      parcelsGpkg: PARCELS_GPKG,
      parcelsLayer: PARCELS_LAYER,
      pidField: PID_FIELD,
      addressIndex: USE_ADDRESS_INDEX ? ADDRESS_INDEX : null,
      centroidByPid: CENTROID_BY_PID,
      sha: {
        parcelsGpkg: sha256File(PARCELS_GPKG),
        addressIndex: USE_ADDRESS_INDEX ? sha256File(ADDRESS_INDEX) : null,
        centroidByPid: sha256File(CENTROID_BY_PID),
      },
    },
    outputs: { outNdjson: OUT_NDJSON, outMeta: OUT_META },
    totals: {
      read: 0,
      written: 0,
      addrPresent: 0,
      addrMissing: 0,
      coordsMatched: 0,
      coordsMissing: 0,
      coordsFromAddressIndex: 0,
      coordsFromCentroidByPid: 0,
      statePlaneOnly: 0,
      badJson: 0,
    },
    addressIndex: {
      hits: 0,
      misses: 0,
      keyFlavorHits: {}, // which variant matched most
      crsHits: {},
    },
  };

  console.log("[load] addressIndex.json ...");
  const addressIndex = USE_ADDRESS_INDEX ? safeJson(fs.readFileSync(ADDRESS_INDEX, "utf8")) : null;
  if (USE_ADDRESS_INDEX) console.log("[load] addressIndex keys:", Object.keys(addressIndex || {}).length);

  console.log("[load] parcelCentroidByParcelId.json ...");
  const centroidByPid = safeJson(fs.readFileSync(CENTROID_BY_PID, "utf8")) || {};
  console.log("[load] centroidByPid keys:", Object.keys(centroidByPid).length);

  const out = fs.createWriteStream(OUT_NDJSON, { encoding: "utf8" });

  const { p, args, sql } = spawnOgr2OgrStream();
  console.log(`[run] streaming parcels via ogr2ogr (GeoJSONSeq -> stdout) ...`);
  // Useful when debugging:
  // console.log("ogr2ogr args:", args.join(" "));
  // console.log("sql:", sql);

  const rl = readline.createInterface({ input: p.stdout, crlfDelay: Infinity });

  let lastLog = 0;

  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    meta.totals.read++;

    const feat = safeJson(line);
    if (!feat) { meta.totals.badJson++; continue; }

    const props = feat.properties || feat;
    const parcel_id = cleanStr(props.parcel_id);
    if (!parcel_id) continue;

    const map_par_id = cleanStr(props.map_par_id) || null;

    // Address attrs
    const full_str = cleanStr(props.full_str) || null;
    const addr_num = cleanStr(props.addr_num) || null;
    const site_addr = cleanStr(props.site_addr) || null;
    const city = cleanStr(props.city) || null;
    const zip = cleanStr(props.zip) || null;

    const addr = {
      full_address: full_str ?? null,
      street_no: addr_num ?? null,
      street_name: site_addr ? collapse(site_addr) : null,
      town: city ? normalizeTown(city) : null,
      zip: zip ? collapse(zip) : null,
    };

    if (addr.full_address || (addr.street_no && addr.street_name)) meta.totals.addrPresent++;
    else meta.totals.addrMissing++;

    let lat = null;
    let lng = null;
    let coord_source = null;
    let coord_key_used = null;

    // Tier 1: address points (addressIndex) if we have enough address to form a key
    if (USE_ADDRESS_INDEX && addressIndex && addr.street_no && addr.street_name && addr.town) {
      const keyVars = buildKeyVariants({
        streetNo: addr.street_no,
        streetName: addr.street_name,
        town: addr.town,
        zip: addr.zip,
      });

      let hit = null;
      let hitKey = null;
      for (const k of keyVars) {
        const v = addressIndex[k];
        if (v != null) { hit = v; hitKey = k; break; }
      }

      if (hit && hitKey) {
        const c = parseCoordLikeAddressIndex(hit);
        if (c && c.lat != null && c.lng != null) {
          lat = c.lat;
          lng = c.lng;
          coord_source = `addressIndex:${c.crs}`;
          coord_key_used = hitKey;
          meta.totals.coordsMatched++;
          meta.totals.coordsFromAddressIndex++;
          meta.addressIndex.hits++;
          meta.addressIndex.crsHits[c.crs] = (meta.addressIndex.crsHits[c.crs] || 0) + 1;
          meta.addressIndex.keyFlavorHits[hitKey.endsWith(addr.zip || "") ? "withZip" : "noZip"] =
            (meta.addressIndex.keyFlavorHits[hitKey.endsWith(addr.zip || "") ? "withZip" : "noZip"] || 0) + 1;
        } else {
          // matched key but couldn't parse coords
          meta.addressIndex.misses++;
        }
      } else {
        meta.addressIndex.misses++;
      }
    }

    // Tier 2: centroidByPid (already wgs84)
    if ((lat == null || lng == null) && centroidByPid) {
      const cRaw = centroidByPid[parcel_id] ?? centroidByPid[parcel_id.replace(/\s+/g, " ")] ?? null;
      if (cRaw && typeof cRaw === "object") {
        const lat2 = Number(cRaw.lat ?? cRaw.latitude ?? NaN);
        const lng2 = Number(cRaw.lng ?? cRaw.lon ?? cRaw.longitude ?? NaN);
        if (Number.isFinite(lat2) && Number.isFinite(lng2) && looksLikeMA(lng2, lat2)) {
          lat = lat2;
          lng = lng2;
          coord_source = "centroidByParcelId:wgs84";
          coord_key_used = null;
          meta.totals.coordsMatched++;
          meta.totals.coordsFromCentroidByPid++;
        }
      }
    }

    if (lat == null || lng == null) meta.totals.coordsMissing++;

    const outRow = {
      parcel_id,
      map_par_id,
      full_address: addr.full_address,
      street_no: addr.street_no,
      street_name: addr.street_name,
      town: addr.town,
      zip: addr.zip,
      lat,
      lng,
      coord_source,
      coord_key_used,
    };

    out.write(JSON.stringify(outRow) + "\n");
    meta.totals.written++;

    if (meta.totals.read - lastLog >= 200000) {
      lastLog = meta.totals.read;
      console.log(
        `[progress] read=${meta.totals.read.toLocaleString()} ` +
        `addrPresent=${meta.totals.addrPresent.toLocaleString()} ` +
        `addrIdx=${meta.totals.coordsFromAddressIndex.toLocaleString()} ` +
        `centroid=${meta.totals.coordsFromCentroidByPid.toLocaleString()} ` +
        `missingLatLng=${meta.totals.coordsMissing.toLocaleString()}`
      );
    }
  }

  // wait for ogr2ogr to finish and check status
  const [code, stderr] = await new Promise((resolve) => {
    let err = "";
    p.stderr.on("data", (d) => (err += d.toString()));
    p.on("close", (c) => resolve([c, err]));
  });

  out.end();

  if (code !== 0) {
    throw new Error(
      `ogr2ogr failed (exit=${code}). Stderr:\n${stderr}\n\n` +
      `Try running:\n  ogrinfo -ro -al -so "${PARCELS_GPKG}" "${PARCELS_LAYER}"\n`
    );
  }

  fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2));

  console.log("----------------------------------------------------");
  console.log("✅ PROPERTIES_STATEWIDE_GEO BUILD COMPLETE (v12)");
  console.log("Read:                  ", meta.totals.read.toLocaleString());
  console.log("Written:               ", meta.totals.written.toLocaleString());
  console.log("Addr present:          ", meta.totals.addrPresent.toLocaleString());
  console.log("Addr missing:          ", meta.totals.addrMissing.toLocaleString());
  console.log("Coords matched:        ", meta.totals.coordsMatched.toLocaleString());
  console.log("  from addressIndex:   ", meta.totals.coordsFromAddressIndex.toLocaleString());
  console.log("  from centroidByPid:  ", meta.totals.coordsFromCentroidByPid.toLocaleString());
  console.log("Coords missing:        ", meta.totals.coordsMissing.toLocaleString());
  console.log("OUT_NDJSON:            ", OUT_NDJSON);
  console.log("OUT_META:             ", OUT_META);
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ buildPropertiesStatewide_geo failed:", err?.stack || err);
  process.exitCode = 1;
});
