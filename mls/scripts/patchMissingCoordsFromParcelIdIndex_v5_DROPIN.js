
/**
 * PATCH MISSING COORDS FROM PARCEL POLYGONS/CENTROIDS (v5 - streaming + polygon centroid support)
 * ---------------------------------------------------------------------------------------------
 * Why you need v5:
 *   - v3/v4 expect Point geometry OR lat/lon fields; polygon parcel files drop everything as "no coords"
 *   - many of your properties rows may store coords as strings or under different fields
 *
 * v5 fixes both:
 *   1) INPUT (properties.ndjson): detects coords in many fields, parses numeric strings, and normalizes to {lat, lon}
 *   2) PARCEL INDEX (geojson): supports Point / Polygon / MultiPolygon. Computes centroid for polygons on-the-fly.
 *   3) Streams both large files safely (no readFileSync giant string)
 *   4) Only computes polygon centroid when the feature's parcel id matches a needed parcel id
 *
 * Usage:
 *   node .\mls\scripts\patchMissingCoordsFromParcelIdIndex_v5_DROPIN.js `
 *     --in  C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v14_coords.ndjson `
 *     --parcelIndex C:\seller-app\backend\publicData\parcels\parcelCentroids_wgs84_WITH_IDS.geojson `
 *     --out C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v15_coords.ndjson `
 *     --meta C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v15_coords_meta.json
 *
 * Optional:
 *   --pidField LOC_ID           (force parcel id property key in parcelIndex features)
 *   --pidMode auto|loc|map|any  (which parcel id(s) to use from parcel features; default any)
 *   --validateOnly             (scan and report only, do not write patched output)
 */

import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function getArg(name, fallback = null) {
  const key = `--${name}`;
  const i = process.argv.findIndex((a) => a === key);
  if (i >= 0 && i + 1 < process.argv.length) return process.argv[i + 1];
  return fallback;
}
function hasFlag(name) {
  return process.argv.includes(`--${name}`);
}

function collapse(v) {
  return String(v ?? "")
    .replace(/\u00A0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function inMA(lat, lon) {
  return (
    Number.isFinite(lat) &&
    Number.isFinite(lon) &&
    lat >= 41.0 &&
    lat <= 43.6 &&
    lon >= -73.9 &&
    lon <= -69.0
  );
}

function toNum(v) {
  if (v === null || v === undefined) return NaN;
  if (typeof v === "number") return v;
  const s = String(v).trim();
  if (!s) return NaN;
  // remove trailing junk like "42.1," or "42.1]"
  const m = s.match(/-?\d+(\.\d+)?/);
  if (!m) return NaN;
  return Number(m[0]);
}

function fixLonSign(lon) {
  if (!Number.isFinite(lon)) return lon;
  // MA longitudes are negative
  if (lon > 0 && lon < 180) return -lon;
  return lon;
}

function normKey(k) {
  return String(k ?? "").toUpperCase().replace(/[^A-Z0-9]/g, "");
}

/** Normalize parcel id into a few matchable keys (keep small to avoid RAM explosion) */
function normParcelIdKeys(pid) {
  const raw = collapse(pid);
  if (!raw) return [];
  const upper = raw.toUpperCase();
  const compact = upper.replace(/\s+/g, "");
  const stripped = compact.replace(/[-_.\/]/g, "");
  return [...new Set([upper, compact, stripped].filter(Boolean))];
}

/** Extract any existing coords from a properties record (supports many field names). */
function extractAnyCoordsFromRecord(r) {
  if (!r || typeof r !== "object") return null;

  // direct lat/lon variants
  const latCandidates = [r.lat, r.latitude, r.LAT, r.Lat, r.y, r.Y];
  const lonCandidates = [r.lon, r.lng, r.longitude, r.LON, r.LNG, r.Lng, r.x, r.X];

  for (const la of latCandidates) {
    const lat = toNum(la);
    if (!Number.isFinite(lat)) continue;
    for (const lo of lonCandidates) {
      let lon = fixLonSign(toNum(lo));
      if (!Number.isFinite(lon)) continue;
      if (inMA(lat, lon)) return { lat, lon };
      if (inMA(lon, lat)) return { lat: lon, lon: lat }; // swapped
    }
  }

  // geometry / coordinates arrays
  const arrays = [
    r.coords,
    r.coordinates,
    r?.geometry?.coordinates,
    r?.location?.coordinates,
    r?.point?.coordinates,
    r?.center?.coordinates,
  ];

  for (const a of arrays) {
    if (!Array.isArray(a) || a.length < 2) continue;
    const a0 = toNum(a[0]);
    const a1 = toNum(a[1]);
    if (!Number.isFinite(a0) || !Number.isFinite(a1)) continue;

    // assume [lon,lat] then try [lat,lon]
    let lon = fixLonSign(a0);
    let lat = a1;
    if (inMA(lat, lon)) return { lat, lon };
    if (inMA(a0, fixLonSign(a1))) return { lat: a0, lon: fixLonSign(a1) };
  }

  // sometimes nested: { lat: "...", lon: "..." }
  const objs = [r.location, r.point, r.center];
  for (const o of objs) {
    if (!o || typeof o !== "object") continue;
    const lat = toNum(o.lat ?? o.latitude);
    let lon = fixLonSign(toNum(o.lon ?? o.lng ?? o.longitude));
    if (inMA(lat, lon)) return { lat, lon };
  }

  return null;
}

function pickParcelIdKeyFromProps(props) {
  if (!props || typeof props !== "object") return null;
  const keys = Object.keys(props);
  if (keys.length === 0) return null;

  const preferred = [
    (nk) => nk === "LOCID" || nk.startsWith("LOCID"),
    (nk) => nk === "MAPPARID" || nk.startsWith("MAPPARID") || nk.startsWith("MAPPAR"),
    (nk) => nk === "PARCELID" || nk.startsWith("PARCELID"),
    (nk) => nk.includes("PARID"),
  ];

  for (const test of preferred) {
    for (const k of keys) {
      const nk = normKey(k);
      if (test(nk)) return k;
    }
  }
  return null;
}

function extractParcelIdsFromRecord(r) {
  const cands = [
    r.parcel_id, r.parcelId, r.PARCEL_ID, r.LOC_ID, r.loc_id, r.MAP_PAR_ID, r.map_par_id
  ].filter((v) => v != null && String(v).trim() !== "");
  return [...new Set(cands.map((v) => String(v)))];
}

function extractParcelIdsFromFeature(feat, pidFieldHint, pidMode) {
  const out = [];
  const p = feat?.properties || {};

  const add = (v) => {
    if (v == null) return;
    const s = String(v).trim();
    if (s) out.push(s);
  };

  if (pidFieldHint && p[pidFieldHint] != null) add(p[pidFieldHint]);

  // common keys
  const loc = p.LOC_ID ?? p.loc_id ?? p.LOCID ?? p.locid;
  const map = p.MAP_PAR_ID ?? p.map_par_id ?? p.MAPPARID ?? p.mapparid;
  const par = p.PARCEL_ID ?? p.parcel_id ?? p.PARCELID ?? p.parcelId;

  if (pidMode === "loc") add(loc);
  else if (pidMode === "map") add(map);
  else if (pidMode === "auto") {
    // prefer whichever exists
    if (loc != null) add(loc);
    else if (map != null) add(map);
    else add(par);
  } else {
    add(loc); add(map); add(par);
  }

  // last resort
  if (feat?.id != null) add(feat.id);

  return [...new Set(out)];
}

/** StatePlane heuristic: numbers are huge, not lon/lat. */
function looksLikeStatePlanePair(pair) {
  if (!Array.isArray(pair) || pair.length < 2) return false;
  const x = toNum(pair[0]);
  const y = toNum(pair[1]);
  return Number.isFinite(x) && Number.isFinite(y) && Math.abs(x) > 10000 && Math.abs(y) > 10000;
}

/** Centroid for a ring using planar polygon centroid formula (lon=x, lat=y). */
function ringCentroid(ring) {
  if (!Array.isArray(ring) || ring.length < 3) return null;

  // ensure closed
  const pts = ring;
  const n = pts.length;
  const first = pts[0];
  const last = pts[n - 1];
  const closed =
    Array.isArray(first) &&
    Array.isArray(last) &&
    first.length >= 2 &&
    last.length >= 2 &&
    toNum(first[0]) === toNum(last[0]) &&
    toNum(first[1]) === toNum(last[1]);

  const m = closed ? n - 1 : n;

  let A = 0;
  let Cx = 0;
  let Cy = 0;

  for (let i = 0; i < m; i++) {
    const p0 = pts[i];
    const p1 = pts[(i + 1) % m];
    if (!Array.isArray(p0) || !Array.isArray(p1) || p0.length < 2 || p1.length < 2) continue;

    const x0 = toNum(p0[0]);
    const y0 = toNum(p0[1]);
    const x1 = toNum(p1[0]);
    const y1 = toNum(p1[1]);

    if (![x0, y0, x1, y1].every(Number.isFinite)) continue;
    const cross = x0 * y1 - x1 * y0;
    A += cross;
    Cx += (x0 + x1) * cross;
    Cy += (y0 + y1) * cross;
  }

  A = A / 2;
  if (!Number.isFinite(A) || Math.abs(A) < 1e-15) {
    // fallback: average
    let sx = 0, sy = 0, c = 0;
    for (let i = 0; i < m; i++) {
      const p = pts[i];
      if (!Array.isArray(p) || p.length < 2) continue;
      const x = toNum(p[0]);
      const y = toNum(p[1]);
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
      sx += x; sy += y; c++;
    }
    if (!c) return null;
    return { x: sx / c, y: sy / c, area: 0 };
  }

  Cx = Cx / (6 * A);
  Cy = Cy / (6 * A);

  if (!Number.isFinite(Cx) || !Number.isFinite(Cy)) return null;
  return { x: Cx, y: Cy, area: A };
}

function polygonCentroid(coords) {
  // coords: [outerRing, hole1, hole2, ...]
  if (!Array.isArray(coords) || coords.length === 0) return null;
  // compute area-weighted centroid, subtract holes
  let totalA = 0;
  let cx = 0;
  let cy = 0;

  for (let i = 0; i < coords.length; i++) {
    const ring = coords[i];
    const rc = ringCentroid(ring);
    if (!rc) continue;
    const a = rc.area;
    // by convention, holes may have opposite orientation; use signed area naturally
    totalA += a;
    cx += rc.x * a;
    cy += rc.y * a;
  }

  if (!Number.isFinite(totalA) || Math.abs(totalA) < 1e-15) {
    // fallback to outer ring avg
    const rc = ringCentroid(coords[0]);
    if (!rc) return null;
    return { lat: rc.y, lon: fixLonSign(rc.x) };
  }

  const x = cx / totalA;
  const y = cy / totalA;
  const lon = fixLonSign(x);
  const lat = y;
  if (!inMA(lat, lon)) return null;
  return { lat, lon };
}

function multiPolygonCentroid(coords) {
  if (!Array.isArray(coords) || coords.length === 0) return null;
  let totalA = 0;
  let cx = 0;
  let cy = 0;

  for (const poly of coords) {
    if (!Array.isArray(poly) || poly.length === 0) continue;
    // poly is polygon coords array [rings...]
    // compute centroid and also an approximate area from outer ring
    const outer = poly[0];
    const rc = ringCentroid(outer);
    const polyC = polygonCentroid(poly);
    if (!polyC || !rc) continue;
    const a = rc.area || 0;
    const weight = Math.abs(a) > 1e-15 ? a : 1; // if zero area, weight lightly
    totalA += weight;
    cx += polyC.lon * weight;
    cy += polyC.lat * weight;
  }

  if (!Number.isFinite(totalA) || Math.abs(totalA) < 1e-15) return null;
  const lon = fixLonSign(cx / totalA);
  const lat = cy / totalA;
  if (!inMA(lat, lon)) return null;
  return { lat, lon };
}

function centroidFromGeometry(geom) {
  if (!geom || typeof geom !== "object") return null;
  const t = geom.type;
  const c = geom.coordinates;

  if (t === "Point") {
    if (!Array.isArray(c) || c.length < 2) return null;
    if (looksLikeStatePlanePair(c)) return null;
    const lon = fixLonSign(toNum(c[0]));
    const lat = toNum(c[1]);
    if (inMA(lat, lon)) return { lat, lon };
    if (inMA(lon, fixLonSign(lat))) return { lat: lon, lon: fixLonSign(lat) };
    return null;
  }

  if (t === "Polygon") {
    // sample first vertex for stateplane hint
    const sample = Array.isArray(c) ? c?.[0]?.[0] : null;
    if (looksLikeStatePlanePair(sample)) return null;
    return polygonCentroid(c);
  }

  if (t === "MultiPolygon") {
    const sample = Array.isArray(c) ? c?.[0]?.[0]?.[0] : null;
    if (looksLikeStatePlanePair(sample)) return null;
    return multiPolygonCentroid(c);
  }

  return null;
}

/**
 * Streaming GeoJSON FeatureCollection parser (no deps)
 * Yields each feature object under top-level "features": [...]
 */
async function* streamGeojsonFeatures(filePath) {
  const rs = fs.createReadStream(filePath, { encoding: "utf8", highWaterMark: 1024 * 1024 });
  let buf = "";
  let state = "seekFeatures";
  let i = 0;

  let inString = false;
  let escape = false;
  let depth = 0;
  let objStart = -1;

  const keepTail = (n = 200) => {
    if (buf.length > n) buf = buf.slice(buf.length - n);
    i = 0;
  };

  for await (const chunk of rs) {
    buf += chunk;

    while (i < buf.length) {
      if (state === "seekFeatures") {
        const idx = buf.indexOf('"features"', i);
        if (idx < 0) {
          keepTail(200);
          break;
        }
        i = idx + 10;
        state = "seekArrayStart";
        continue;
      }
      if (state === "seekArrayStart") {
        const idx = buf.indexOf("[", i);
        if (idx < 0) {
          keepTail(200);
          break;
        }
        i = idx + 1;
        state = "seekObjStart";
        continue;
      }
      if (state === "seekObjStart") {
        while (i < buf.length && (buf[i] === " " || buf[i] === "\n" || buf[i] === "\r" || buf[i] === "\t" || buf[i] === ",")) i++;
        if (i >= buf.length) break;
        if (buf[i] === "]") return;
        if (buf[i] !== "{") {
          i++;
          continue;
        }
        state = "inObj";
        objStart = i;
        inString = false;
        escape = false;
        depth = 0;
        continue;
      }
      if (state === "inObj") {
        const ch = buf[i];
        if (inString) {
          if (escape) escape = false;
          else if (ch === "\\") escape = true;
          else if (ch === '"') inString = false;
        } else {
          if (ch === '"') inString = true;
          else if (ch === "{") depth++;
          else if (ch === "}") depth--;
        }
        i++;
        if (!inString && depth === 0 && objStart >= 0 && i > objStart) {
          const text = buf.slice(objStart, i);
          let obj = null;
          try {
            obj = JSON.parse(text);
          } catch {
            objStart = -1;
            state = "seekObjStart";
            continue;
          }
          yield obj;
          buf = buf.slice(i);
          i = 0;
          objStart = -1;
          state = "seekObjStart";
        }
      }
    }

    if (state !== "inObj" && buf.length > 5 * 1024 * 1024) keepTail(2000);
  }
}

async function scanInputForNeeded(inPath) {
  const needed = new Set();
  const stats = { total: 0, missing: 0, missingNoParcelId: 0, alreadyHasCoords: 0 };

  const rl = readline.createInterface({
    input: fs.createReadStream(inPath, "utf8"),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    let r;
    try { r = JSON.parse(t); } catch { continue; }

    stats.total++;

    const coords = extractAnyCoordsFromRecord(r);
    if (coords) {
      stats.alreadyHasCoords++;
      continue;
    }

    stats.missing++;

    const pids = extractParcelIdsFromRecord(r);
    if (!pids.length) {
      stats.missingNoParcelId++;
      continue;
    }

    for (const pid of pids) {
      for (const k of normParcelIdKeys(pid)) needed.add(k);
    }

    if (stats.total % 500000 === 0) {
      console.log(`[scan-in] ${stats.total.toLocaleString()} lines... missing ${stats.missing.toLocaleString()} neededKeys ${needed.size.toLocaleString()}`);
    }
  }

  return { needed, stats };
}

async function scanParcelIndexForNeeded(parcelIndexPath, neededSet, pidFieldHint, pidMode) {
  let scanned = 0;
  let considered = 0;
  let computedCentroid = 0;
  let matchedFeatures = 0;

  let droppedNoPid = 0;
  let droppedNoCoords = 0;
  let droppedBadCoords = 0;
  let warnedStatePlane = 0;

  let samplePropsKeys = [];
  let sawFirst = false;
  let geomTypes = {};

  const coordsMap = new Map(); // neededKey -> {lat,lon}

  for await (const feat of streamGeojsonFeatures(parcelIndexPath)) {
    scanned++;

    if (!sawFirst) {
      samplePropsKeys = Object.keys(feat?.properties || {}).slice(0, 80);
      sawFirst = true;
    }

    const geomType = feat?.geometry?.type || "null";
    geomTypes[geomType] = (geomTypes[geomType] || 0) + 1;

    const pids = extractParcelIdsFromFeature(feat, pidFieldHint, pidMode);
    if (!pids.length) {
      droppedNoPid++;
      continue;
    }

    // Check if any pid variant is needed
    let neededKeysHere = [];
    for (const pid of pids) {
      for (const k of normParcelIdKeys(pid)) {
        if (neededSet.has(k)) neededKeysHere.push(k);
      }
    }
    neededKeysHere = [...new Set(neededKeysHere)];
    if (neededKeysHere.length === 0) continue;

    considered++;
    matchedFeatures++;

    // compute centroid only for these
    const geom = feat?.geometry;
    const t = geom?.type;
    // stateplane hint check on a quick sample point
    if (t === "Polygon") {
      const sample = geom?.coordinates?.[0]?.[0];
      if (looksLikeStatePlanePair(sample)) {
        warnedStatePlane++;
        continue;
      }
    } else if (t === "MultiPolygon") {
      const sample = geom?.coordinates?.[0]?.[0]?.[0];
      if (looksLikeStatePlanePair(sample)) {
        warnedStatePlane++;
        continue;
      }
    }

    const ll = centroidFromGeometry(geom);
    computedCentroid++;
    if (!ll) {
      droppedNoCoords++;
      continue;
    }
    if (!inMA(ll.lat, ll.lon)) {
      droppedBadCoords++;
      continue;
    }

    // store mapping for all needed keys that refer to this feature
    for (const nk of neededKeysHere) coordsMap.set(nk, ll);

    if (scanned % 500000 === 0) {
      console.log(`[scan-parcels] scanned ${scanned.toLocaleString()} considered ${considered.toLocaleString()} computedCentroid ${computedCentroid.toLocaleString()} foundNeeded ${coordsMap.size.toLocaleString()}`);
    }

    if (coordsMap.size >= neededSet.size) break;
  }

  const diagnostics = {
    scanned,
    considered,
    computedCentroid,
    matchedFeatures,
    foundNeededKeys: coordsMap.size,
    droppedNoPid,
    droppedNoCoords,
    droppedBadCoords,
    warnedStatePlane,
    samplePropsKeys,
    geomTypes,
  };

  return { coordsMap, diagnostics };
}

async function patchFile(inPath, outPath, coordsMap) {
  const out = fs.createWriteStream(outPath, "utf8");
  const totals = { total: 0, missing: 0, patched: 0, normalizedExisting: 0, stillMissing: 0 };

  const rl = readline.createInterface({
    input: fs.createReadStream(inPath, "utf8"),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    let r;
    try { r = JSON.parse(t); } catch { continue; }

    totals.total++;

    // If coords exist somewhere, normalize into lat/lon
    const existing = extractAnyCoordsFromRecord(r);
    if (existing) {
      const had = Number.isFinite(toNum(r.lat)) && Number.isFinite(toNum(r.lon));
      r.lat = existing.lat;
      r.lon = existing.lon;
      if (!had) totals.normalizedExisting++;
      out.write(JSON.stringify(r) + "\n");
      if (totals.total % 500000 === 0) console.log(`[patch] ${totals.total.toLocaleString()} lines...`);
      continue;
    }

    totals.missing++;

    const pids = extractParcelIdsFromRecord(r);
    let hit = null;

    for (const pid of pids) {
      for (const k of normParcelIdKeys(pid)) {
        hit = coordsMap.get(k);
        if (hit) break;
      }
      if (hit) break;
    }

    if (hit) {
      r.lat = hit.lat;
      r.lon = hit.lon;
      totals.patched++;
    } else {
      totals.stillMissing++;
    }

    out.write(JSON.stringify(r) + "\n");
    if (totals.total % 500000 === 0) console.log(`[patch] ${totals.total.toLocaleString()} lines...`);
  }

  out.end();
  return totals;
}

async function main() {
  const IN = path.resolve(__dirname, getArg("in", "../../publicData/properties/properties_statewide_geo_zip_district_v14_coords.ndjson"));
  const PARCEL_INDEX = path.resolve(__dirname, getArg("parcelIndex", "../../publicData/parcels/parcelCentroids_wgs84_WITH_IDS.geojson"));
  const OUT = path.resolve(__dirname, getArg("out", "../../publicData/properties/properties_statewide_geo_zip_district_v15_coords.ndjson"));
  const OUT_META = path.resolve(__dirname, getArg("meta", OUT.replace(/\.ndjson$/i, "_meta.json")));

  const PID_FIELD = getArg("pidField", null);
  const PID_MODE = (getArg("pidMode", "any") || "any").toLowerCase(); // any|auto|loc|map

  console.log("====================================================");
  console.log(" PATCH MISSING COORDS FROM PARCEL-ID INDEX (v5)");
  console.log("====================================================");
  console.log("IN : " + IN);
  console.log("IDX: " + PARCEL_INDEX);
  if (PID_FIELD) console.log("PID_FIELD:", PID_FIELD);
  console.log("PID_MODE :", PID_MODE);
  console.log("OUT: " + OUT);
  console.log("META: " + OUT_META);
  console.log("----------------------------------------------------");

  if (!fs.existsSync(IN)) throw new Error(`--in not found: ${IN}`);
  if (!fs.existsSync(PARCEL_INDEX)) throw new Error(`--parcelIndex not found: ${PARCEL_INDEX}`);

  console.log("[1/3] scanning input for missing coords + needed parcel_ids...");
  const { needed, stats: inStats } = await scanInputForNeeded(IN);
  console.log("[scan-in] done", inStats, "neededKeys:", needed.size.toLocaleString());

  console.log("[2/3] scanning parcel index (streaming) for needed ids...");
  const { coordsMap, diagnostics } = await scanParcelIndexForNeeded(PARCEL_INDEX, needed, PID_FIELD, PID_MODE);
  console.log("[scan-parcels] diagnostics:", diagnostics);

  if (hasFlag("validateOnly")) {
    console.log("✅ validateOnly set — exiting after scans (no patch write).");
    process.exit(0);
  }

  console.log("[3/3] patching input -> output (also normalizes existing coords)...");
  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  const totals = await patchFile(IN, OUT, coordsMap);

  const meta = {
    ranAt: new Date().toISOString(),
    version: "parcelIdIndex:v5-streaming-centroid",
    in: IN,
    out: OUT,
    parcelIndex: PARCEL_INDEX,
    pidField: PID_FIELD,
    pidMode: PID_MODE,
    inputStats: inStats,
    parcelScanDiagnostics: diagnostics,
    totals,
  };
  fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2));

  console.log("====================================================");
  console.log("[done]", totals);
  console.log("OUT:", OUT);
  console.log("META:", OUT_META);
  console.log("====================================================");
}

main().catch((e) => {
  console.error("❌ parcelIdIndex patch failed:", e);
  process.exit(1);
});
