/**
 * addressAuthority_applyMadNearest_sameTown_missingOnly_v2_DROPIN.js (ESM)
 *
 * Improvement over v1:
 *   - Does NOT depend on tile filename patterns.
 *   - Builds tile map by sampling the first valid point in each tile and computing (ix,iy) via tileSize.
 *
 * Purpose:
 *   Patch ONLY rows missing street_no or street_name by finding nearest MAD point
 *   from pre-tiled NDJSON tiles, with a SAME-TOWN constraint (audit-safe).
 *
 * Usage:
 *   node .\mls\scripts\addressAuthority_applyMadNearest_sameTown_missingOnly_v2_DROPIN.js --in <in.ndjson> --tilesDir <mad_tiles_0p01> --out <out.ndjson> --report <report.json> --maxDistM 180
 *
 * Optional:
 *   --tileSize 0.01   (default 0.01)
 *   --cacheLimit 60   (default 60 tiles)
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

// MAD point field resolvers (adjust if your tile schema differs)
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
function getMadStreetNo(p) {
  const f = pickField(p, ["ADDR_NUM", "addr_num", "HOUSE_NUM", "house_no", "STREET_NO", "street_no", "NUM", "num"]);
  return f ? p[f] : null;
}
function getMadStreetName(p) {
  const f = pickField(p, ["FULL_STREET_NAME", "full_street_name", "STREETNAME", "street_name", "STREET", "street"]);
  return f ? p[f] : null;
}
function getMadZip(p) {
  const f = pickField(p, ["ZIP", "zip", "POSTCODE", "postcode"]);
  return f ? p[f] : null;
}

function normalizeStreetNoBasic(v) {
  if (v == null) return null;
  const s = String(v).trim().toUpperCase();
  if (!s) return null;
  if (/^\d+$/.test(s)) {
    const n = String(parseInt(s, 10));
    return n === "0" ? null : n;
  }
  const s2 = s.replace(/[\s-]+/g, "");
  if (/^\d+[A-Z]$/.test(s2)) return s2;
  return null;
}

function normalizeStreetNameBasic(v) {
  if (v == null) return null;
  let s = String(v).trim();
  if (!s) return null;
  s = s.replace(/\s+/g, " ").trim();
  return s || null;
}

// --- Tile cache ---
class LRUCache {
  constructor(limit = 60) {
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
  // Read first chunk; find first JSON line with lon/lat
  const fd = fs.openSync(filePath, "r");
  try {
    const buf = Buffer.alloc(65536); // 64KB
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
      } catch {
        // ignore
      }
    }
    return null;
  } finally {
    fs.closeSync(fd);
  }
}

function buildTileMapBySampling(tilesDir, tileSize) {
  const files = fs.readdirSync(tilesDir).filter((f) => f.toLowerCase().endsWith(".ndjson"));
  const map = new Map(); // key -> filepath
  let sampled = 0;
  let skipped = 0;
  let collisions = 0;

  for (const f of files) {
    const fp = path.join(tilesDir, f);
    const ll = sampleFirstPointLonLat(fp);
    if (!ll) {
      skipped++;
      continue;
    }
    sampled++;
    const t = tileIndex(ll[0], ll[1], tileSize);
    if (map.has(t.key)) collisions++;
    if (!map.has(t.key)) map.set(t.key, fp); // keep first
  }

  return { map, stats: { files: files.length, sampled, skipped, collisions } };
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
    } catch {
      // ignore
    }
  }
  return pts;
}

const args = parseArgs(process.argv);
const inPath = args.in;
const tilesDir = args.tilesDir;
const outPath = args.out;
const reportPath = args.report;
const maxDistM = Number(args.maxDistM ?? 180);
const tileSize = Number(args.tileSize ?? 0.01);
const cacheLimit = Number(args.cacheLimit ?? 60);

if (!inPath || !tilesDir || !outPath || !reportPath) {
  console.error("Missing args. Required: --in --tilesDir --out --report [--maxDistM] [--tileSize]");
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

ensureDir(path.dirname(outPath));
ensureDir(path.dirname(reportPath));

console.log("===============================================");
console.log(" MAD Nearest (same-town, missing-only) — v2");
console.log("===============================================");
console.log("IN : ", inPath);
console.log("TIL: ", tilesDir);
console.log("OUT: ", outPath);
console.log("REP: ", reportPath);
console.log("maxDistM:", maxDistM);
console.log("tileSize:", tileSize);

console.log("Building tile map by sampling first point in each tile...");
const { map: tileMap, stats: tileStats } = buildTileMapBySampling(tilesDir, tileSize);
console.log("Tile map stats:", tileStats);
console.log("Mapped tiles:", tileMap.size.toLocaleString());

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

let total = 0;
let parseErr = 0;
let targets = 0;
let patched = 0;
let noCoords = 0;
let noTileCandidates = 0;
let noCandidateWithinDist = 0;
let rejectedTownMismatch = 0;

const out = fs.createWriteStream(outPath, { encoding: "utf8" });
const rs = fs.createReadStream(inPath);
const splitter = new LineSplitter();
rs.pipe(splitter);

splitter.on("data", (line) => {
  total++;
  let row;
  try {
    row = JSON.parse(line);
  } catch {
    parseErr++;
    return;
  }

  const streetNo = row.street_no ?? row.streetNo ?? row.STREET_NO ?? null;
  const streetName = row.street_name ?? row.streetName ?? row.STREET_NAME ?? null;

  const needsNo = streetNo == null || String(streetNo).trim() === "" || /^0+$/.test(String(streetNo).trim());
  const needsName = streetName == null || String(streetName).trim() === "";

  if (!needsNo && !needsName) {
    out.write(JSON.stringify(row) + "\n");
    return;
  }

  targets++;

  const ll = getLonLat(row);
  if (!ll) {
    noCoords++;
    row.addr_authority_reason = row.addr_authority_reason ?? "NO_COORDS";
    out.write(JSON.stringify(row) + "\n");
    return;
  }

  const [lon, lat] = ll;
  const t = tileIndex(lon, lat, tileSize);
  const keys = neighborKeys(t.ix, t.iy);

  let candidates = [];
  for (const k of keys) {
    const pts = getTilePointsByKey(k);
    if (pts.length) candidates = candidates.concat(pts);
  }

  if (candidates.length === 0) {
    noTileCandidates++;
    row.addr_authority_reason = row.addr_authority_reason ?? "NO_TILE_CANDIDATES";
    out.write(JSON.stringify(row) + "\n");
    return;
  }

  const rowTown = normTown(row.town ?? row.TOWN ?? row.municipality ?? row.MUNICIPALITY ?? "");

  let best = null;
  let bestD = Infinity;

  for (const p of candidates) {
    const pll = getMadLonLat(p);
    if (!pll) continue;
    const d = haversineMeters(lon, lat, pll[0], pll[1]);
    if (d > maxDistM) continue;

    const ptown = normTown(getMadTown(p));
    if (rowTown && ptown && rowTown !== ptown) continue; // same-town guard

    if (d < bestD) {
      bestD = d;
      best = p;
    }
  }

  if (!best) {
    if (rowTown) rejectedTownMismatch++;
    noCandidateWithinDist++;
    row.addr_authority_reason = row.addr_authority_reason ?? "NO_VALID_CANDIDATE_WITHIN_DIST";
    out.write(JSON.stringify(row) + "\n");
    return;
  }

  // Patch only missing fields
  const madNo = normalizeStreetNoBasic(getMadStreetNo(best));
  const madName = normalizeStreetNameBasic(getMadStreetName(best));
  const madZip = getMadZip(best);

  let didPatch = false;

  if (needsNo && madNo) {
    row.street_no = madNo;
    didPatch = true;
  }
  if (needsName && madName) {
    row.street_name = madName;
    didPatch = true;
  }
  if ((row.zip == null || String(row.zip).trim() === "") && madZip) {
    row.zip = String(madZip).trim().slice(0, 5);
    row.addr_zip_src = row.addr_zip_src ?? "MAD_ZIP_NEAREST_SAME_TOWN";
    didPatch = true;
  }

  if (didPatch) {
    patched++;
    row.addr_authority_src = "MAD_NEAREST_SAME_TOWN_MISSING_ONLY";
    row.addr_authority_dist_m = Math.round(bestD * 10) / 10;
    row.addr_authority_reason = null;
  } else {
    row.addr_authority_reason = row.addr_authority_reason ?? "BEST_CANDIDATE_MISSING_REQUIRED_FIELDS";
  }

  out.write(JSON.stringify(row) + "\n");

  if (total % 250000 === 0) {
    console.log(`...processed ${total.toLocaleString()} targets=${targets.toLocaleString()} patched=${patched.toLocaleString()}`);
  }
});

splitter.on("end", () => {
  out.end();
  const report = {
    inPath,
    tilesDir,
    outPath,
    maxDistM,
    tileSize,
    tileMapStats: tileStats,
    total,
    parseErr,
    targets,
    patched,
    noCoords,
    noTileCandidates,
    noCandidateWithinDist,
    rejectedTownMismatch,
    timestamp: new Date().toISOString(),
  };
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
  console.log("DONE.");
  console.log(report);
});

splitter.on("error", (e) => {
  console.error("Stream error:", e);
  process.exit(1);
});
