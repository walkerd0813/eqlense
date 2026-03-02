/**
 * verifyHighOffset_v1_DROPIN.js (ESM)
 *
 * Verifies far MAD-nearest matches with cheap checks and buckets:
 *  1) MA bounds sanity check
 *  2) Town boundary containment (if townPolys provided)
 *  3) Address-point proximity check using MAD tiles:
 *     - Is there ANY MAD point within --nearCheckM of the parcel coordinate (same-town when available)?
 *
 * Outputs:
 *   --outApproved      => passes bounds + town containment (when applicable) + proximity check
 *   --outReview        => in bounds but not fully corroborated
 *   --outUnrecoverable => missing coords or out of bounds
 */

import fs from "node:fs";
import path from "node:path";
import { Transform } from "node:stream";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : "1";
    out[k] = v;
  }
  return out;
}

class LineSplitter extends Transform {
  constructor() {
    super({ readableObjectMode: true });
    this._buf = "";
  }
  _transform(chunk, enc, cb) {
    this._buf += chunk.toString("utf8");
    const parts = this._buf.split(/\r?\n/);
    this._buf = parts.pop() ?? "";
    for (const line of parts) {
      const t = line.trim();
      if (t) this.push(t);
    }
    cb();
  }
  _flush(cb) {
    const t = this._buf.trim();
    if (t) this.push(t);
    cb();
  }
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function pickField(obj, candidates) {
  for (const c of candidates) {
    if (obj[c] != null && String(obj[c]).trim() !== "") return c;
  }
  return null;
}

function normTown(s) {
  return String(s ?? "")
    .toUpperCase()
    .replace(/[^\w\s]/g, " ")
    .replace(/\b(TOWN|CITY|CDP)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function getLonLat(row) {
  const lon =
    row.lon ?? row.longitude ?? row.LON ?? row.LONGITUDE ?? row.x ?? row.X ?? row.lng ?? row.LNG;
  const lat =
    row.lat ?? row.latitude ?? row.LAT ?? row.LATITUDE ?? row.y ?? row.Y ?? row.lat_y ?? row.LAT_Y;
  if (lon == null || lat == null) return null;
  const lo = Number(lon), la = Number(lat);
  if (!Number.isFinite(lo) || !Number.isFinite(la)) return null;
  return [lo, la];
}

function inMassachusettsBounds(lon, lat) {
  return lon >= -73.6 && lon <= -69.5 && lat >= 41.0 && lat <= 43.6;
}

function haversineMeters(lon1, lat1, lon2, lat2) {
  const R = 6371000;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(a)));
}

// MAD tile helpers
function getMadLonLat(p) {
  const lon = p.lon ?? p.longitude ?? p.x ?? p.X ?? p.LON ?? p.Longitude ?? p.LONGITUDE;
  const lat = p.lat ?? p.latitude ?? p.y ?? p.Y ?? p.LAT ?? p.Latitude ?? p.LATITUDE;
  if (lon == null || lat == null) return null;
  const lo = Number(lon), la = Number(lat);
  if (!Number.isFinite(lo) || !Number.isFinite(la)) return null;
  return [lo, la];
}
function getMadTown(p) {
  const f = pickField(p, ["TOWN", "town", "MUNI", "municipality", "CITY", "city"]);
  return f ? p[f] : null;
}

// --- Tile cache ---
class LRUCache {
  constructor(limit = 80) {
    this.limit = limit;
    this.map = new Map();
  }
  get(k) {
    if (!this.map.has(k)) return null;
    const v = this.map.get(k);
    this.map.delete(k);
    this.map.set(k, v);
    return v;
  }
  set(k, v) {
    if (this.map.has(k)) this.map.delete(k);
    this.map.set(k, v);
    if (this.map.size > this.limit) {
      const first = this.map.keys().next().value;
      this.map.delete(first);
    }
  }
}

function tileIndex(lon, lat, tileSize) {
  const ix = Math.floor(lon / tileSize);
  const iy = Math.floor(lat / tileSize);
  return { ix, iy, key: `${ix},${iy}` };
}

function neighborKeys(ix, iy) {
  const keys = [];
  for (let dx = -1; dx <= 1; dx++) {
    for (let dy = -1; dy <= 1; dy++) {
      keys.push(`${ix + dx},${iy + dy}`);
    }
  }
  return keys;
}

function sampleFirstPointLonLat(filePath) {
  const fd = fs.openSync(filePath, "r");
  try {
    const buf = Buffer.alloc(65536);
    const n = fs.readSync(fd, buf, 0, buf.length, 0);
    if (!n) return null;
    const txt = buf.slice(0, n).toString("utf8");
    const lines = txt.split(/\r?\n/);
    for (const line of lines) {
      const t = line.trim();
      if (!t) continue;
      try {
        const obj = JSON.parse(t);
        const ll = getMadLonLat(obj);
        if (ll) return ll;
      } catch {}
    }
    return null;
  } finally {
    fs.closeSync(fd);
  }
}

function buildTileMapBySampling(tilesDir, tileSize) {
  const files = fs.readdirSync(tilesDir).filter((f) => f.toLowerCase().endsWith(".ndjson"));
  const map = new Map();
  for (const f of files) {
    const fp = path.join(tilesDir, f);
    const ll = sampleFirstPointLonLat(fp);
    if (!ll) continue;
    const t = tileIndex(ll[0], ll[1], tileSize);
    if (!map.has(t.key)) map.set(t.key, fp);
  }
  return { map, files: files.length, mapped: map.size };
}

function loadTilePoints(fp) {
  const raw = fs.readFileSync(fp, "utf8");
  const pts = [];
  const lines = raw.split(/\r?\n/);
  for (const line of lines) {
    const t = line.trim();
    if (!t) continue;
    try {
      pts.push(JSON.parse(t));
    } catch {}
  }
  return pts;
}

// --- Town polygons ---
function readTownPolys(townPolysPath) {
  const gj = JSON.parse(fs.readFileSync(townPolysPath, "utf8"));
  const feats = gj.type === "FeatureCollection" ? gj.features : [];
  const out = new Map();

  for (const f of feats) {
    const props = f.properties || {};
    const k = pickField(props, ["TOWN", "TOWN_NAME", "TOWNNAM", "MUNI", "MUNICIPAL", "MUNICIPALITY", "NAME", "namelsad"]);
    if (!k) continue;
    const t = normTown(props[k]);
    if (!t) continue;

    const g = f.geometry;
    if (!g) continue;

    const polys = [];
    if (g.type === "Polygon") polys.push(g.coordinates);
    else if (g.type === "MultiPolygon") polys.push(...g.coordinates);
    else continue;

    if (!out.has(t)) out.set(t, []);
    out.get(t).push(...polys);
  }

  return out;
}

// Point-in-polygon (ray casting), supports holes.
function pointInRing(x, y, ring) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0], yi = ring[i][1];
    const xj = ring[j][0], yj = ring[j][1];
    const intersect = ((yi > y) !== (yj > y)) && (x < ((xj - xi) * (y - yi)) / (yj - yi + 0.0) + xi);
    if (intersect) inside = !inside;
  }
  return inside;
}

function pointInPolygon(x, y, polyCoords) {
  if (!polyCoords || polyCoords.length === 0) return false;
  if (!pointInRing(x, y, polyCoords[0])) return false;
  for (let i = 1; i < polyCoords.length; i++) {
    if (pointInRing(x, y, polyCoords[i])) return false;
  }
  return true;
}

function pointInAnyTownPolygon(lon, lat, townNorm, townMap) {
  const polys = townMap.get(townNorm);
  if (!polys || polys.length === 0) return null;
  for (const poly of polys) {
    if (pointInPolygon(lon, lat, poly)) return true;
  }
  return false;
}

const args = parseArgs(process.argv);
const inPath = args.in;
const tilesDir = args.tilesDir;
const townPolysPath = args.townPolys || null;

const outApprovedPath = args.outApproved;
const outReviewPath = args.outReview;
const outUnrecoverablePath = args.outUnrecoverable;
const reportPath = args.report;

const nearCheckM = Number(args.nearCheckM ?? 25);
const tileSize = Number(args.tileSize ?? 0.01);
const cacheLimit = Number(args.cacheLimit ?? 80);

if (!inPath || !tilesDir || !outApprovedPath || !outReviewPath || !outUnrecoverablePath || !reportPath) {
  console.error("Missing args. Required: --in --tilesDir --outApproved --outReview --outUnrecoverable --report [--townPolys]");
  process.exit(1);
}
if (!fs.existsSync(inPath)) {
  console.error("Input not found:", inPath);
  process.exit(1);
}
if (!fs.existsSync(tilesDir)) {
  console.error("tilesDir not found:", tilesDir);
  process.exit(1);
}

ensureDir(path.dirname(outApprovedPath));
ensureDir(path.dirname(outReviewPath));
ensureDir(path.dirname(outUnrecoverablePath));
ensureDir(path.dirname(reportPath));

let townMap = null;
if (townPolysPath) {
  if (!fs.existsSync(townPolysPath)) {
    console.error("townPolys not found:", townPolysPath);
    process.exit(1);
  }
  console.log("Loading town polygons:", townPolysPath);
  townMap = readTownPolys(townPolysPath);
  console.log("Town polygon keys:", townMap.size.toLocaleString());
} else {
  console.log("No townPolys provided: town containment check skipped.");
}

console.log("Building MAD tile map (sampling)...");
const { map: tileMap, files: tileFiles, mapped: tileMapped } = buildTileMapBySampling(tilesDir, tileSize);
console.log("MAD tiles files:", tileFiles.toLocaleString(), "mappedKeys:", tileMapped.toLocaleString());

const tileCache = new LRUCache(cacheLimit);
function getTilePointsByKey(key) {
  const cached = tileCache.get(key);
  if (cached) return cached;
  const fp = tileMap.get(key);
  if (!fp) {
    tileCache.set(key, []);
    return [];
  }
  const pts = loadTilePoints(fp);
  tileCache.set(key, pts);
  return pts;
}

function hasNearbyMadPoint(lon, lat, rowTownNorm) {
  const t = tileIndex(lon, lat, tileSize);
  const keys = neighborKeys(t.ix, t.iy);

  let best = Infinity;

  for (const k of keys) {
    const pts = getTilePointsByKey(k);
    for (const p of pts) {
      const pll = getMadLonLat(p);
      if (!pll) continue;
      const d = haversineMeters(lon, lat, pll[0], pll[1]);
      if (d > nearCheckM) continue;

      const pTown = normTown(getMadTown(p));
      const townOk = (!rowTownNorm || !pTown) ? true : (rowTownNorm === pTown);
      if (!townOk) continue;

      if (d < best) best = d;
    }
  }

  return Number.isFinite(best) ? best : null;
}

const outApproved = fs.createWriteStream(outApprovedPath, { encoding: "utf8" });
const outReview = fs.createWriteStream(outReviewPath, { encoding: "utf8" });
const outUnrec = fs.createWriteStream(outUnrecoverablePath, { encoding: "utf8" });

const rs = fs.createReadStream(inPath);
const splitter = new LineSplitter();
rs.pipe(splitter);

let total = 0;
let parseErr = 0;

let autoApproved = 0;
let needsReview = 0;
let unrecoverable = 0;

let missingCoords = 0;
let failBounds = 0;

let townContainPass = 0;
let townContainFail = 0;
let townUnknown = 0;

let nearbyPass = 0;

splitter.on("data", (line) => {
  total++;
  let row;
  try {
    row = JSON.parse(line);
  } catch {
    parseErr++;
    return;
  }

  const ll = getLonLat(row);
  if (!ll) {
    missingCoords++;
    unrecoverable++;
    row.verify_bucket = "UNRECOVERABLE";
    row.verify_reason = "NO_COORDS";
    outUnrec.write(JSON.stringify(row) + "\n");
    return;
  }

  const [lon, lat] = ll;
  if (!inMassachusettsBounds(lon, lat)) {
    failBounds++;
    unrecoverable++;
    row.verify_bucket = "UNRECOVERABLE";
    row.verify_reason = "OUTSIDE_MA_BOUNDS";
    outUnrec.write(JSON.stringify(row) + "\n");
    return;
  }

  const rowTown = normTown(row.town ?? row.TOWN ?? row.municipality ?? row.MUNICIPALITY ?? "");
  let townOk = true;

  if (townMap && rowTown) {
    const inside = pointInAnyTownPolygon(lon, lat, rowTown, townMap);
    if (inside === null) {
      townUnknown++;
      row.verify_town_containment = "UNKNOWN_TOWN_POLY";
      townOk = true;
    } else if (inside === true) {
      townContainPass++;
      row.verify_town_containment = "PASS";
      townOk = true;
    } else {
      townContainFail++;
      row.verify_town_containment = "FAIL";
      townOk = false;
    }
  } else if (townMap && !rowTown) {
    townUnknown++;
    row.verify_town_containment = "NO_TOWN_ON_ROW";
  } else {
    row.verify_town_containment = "SKIPPED";
  }

  const nearbyD = hasNearbyMadPoint(lon, lat, rowTown);
  const nearbyOk = nearbyD != null;
  if (nearbyOk) {
    nearbyPass++;
    row.verify_nearby_mad_within_m = Math.round(nearbyD * 10) / 10;
  } else {
    row.verify_nearby_mad_within_m = null;
  }

  if (townOk && nearbyOk) {
    autoApproved++;
    row.verify_bucket = "AUTO_APPROVED";
    row.verify_reason = null;
    outApproved.write(JSON.stringify(row) + "\n");
  } else {
    needsReview++;
    row.verify_bucket = "NEEDS_REVIEW";
    row.verify_reason = !townOk ? "TOWN_CONTAINMENT_FAIL" : "NO_NEARBY_MAD_POINT";
    outReview.write(JSON.stringify(row) + "\n");
  }

  if (total % 200000 === 0) {
    console.log(`...processed ${total.toLocaleString()} approved=${autoApproved.toLocaleString()} review=${needsReview.toLocaleString()} unrecoverable=${unrecoverable.toLocaleString()}`);
  }
});

splitter.on("end", () => {
  outApproved.end();
  outReview.end();
  outUnrec.end();

  const report = {
    inPath,
    tilesDir,
    townPolysPath,
    nearCheckM,
    tileSize,
    cacheLimit,
    total,
    parseErr,
    buckets: { autoApproved, needsReview, unrecoverable },
    checks: { missingCoords, failBounds, townContainPass, townContainFail, townUnknown, nearbyPass },
    timestamp: new Date().toISOString(),
    decisionRule: "AUTO_APPROVED iff in MA bounds AND town containment passes/unknown AND nearby MAD point exists within nearCheckM (same-town when available).",
  };

  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
  console.log("DONE:", report);
});

splitter.on("error", (e) => {
  console.error("Stream error:", e);
  process.exit(1);
});
