/**
 * addressAuthority_applyMadNearest_zipGuard_confidence_v1_DROPIN.js (ESM)
 *
 * Purpose:
 *   Patch missing street_no / street_name using nearest MassGIS MAD address point,
 *   while remaining institutional:
 *     - NO hard same-town string guard (because your row "town" appears unreliable / USPS-like)
 *     - ZIP guard for FAR matches: if dist > farZipGuardM and both zips exist, require zip match
 *     - Optional street-name guard for FAR matches: if row already has street_name, require match
 *     - Confidence scoring + distance buckets + audit fields on patched rows
 *
 * Designed for the situation you hit:
 *   rejectedTownMismatch huge (town string mismatch blocks good matches).
 *
 * Usage:
 *   node .\mls\scripts\addressAuthority_applyMadNearest_zipGuard_confidence_v1_DROPIN.js `
 *     --in <in.ndjson> `
 *     --tilesDir <mad_tiles_0p01> `
 *     --out <out.ndjson> `
 *     --report <report.json> `
 *     --maxDistM 120
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

function normZip5(z) {
  const s = String(z ?? "").trim();
  const m9 = s.match(/^(\d{5})-\d{4}$/);
  if (m9) return m9[1];
  if (/^\d{5}$/.test(s)) return s;
  return null;
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

// MAD field resolvers
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
function getMadZip(p) {
  const f = pickField(p, ["ZIP", "zip", "POSTCODE", "postcode"]);
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

function normalizeStreetNoBasic(v) {
  if (v == null) return null;
  const s = String(v).trim().toUpperCase();
  if (!s) return null;
  if (/^\d+$/.test(s)) {
    const n = String(parseInt(s, 10));
    return n === "0" ? null : n;
  }
  const s2 = s.replace(/[\s-]+/g, "");
  if (/^\d+[A-Z]$/.test(s2)) return s2; // allow 12A
  return null;
}

function normalizeStreetNameBasic(v) {
  if (v == null) return null;
  let s = String(v).trim();
  if (!s) return null;
  s = s.replace(/\s+/g, " ").trim();
  return s || null;
}

function normalizeStreetNameForCompare(v) {
  if (v == null) return null;
  let s = String(v).toUpperCase().trim();
  if (!s) return null;
  s = s.replace(/[^A-Z0-9\s]/g, " ").replace(/\s+/g, " ").trim();

  const parts = s.split(" ");
  if (parts.length >= 2) {
    const last = parts[parts.length - 1];
    const map = {
      ST: "STREET", STREET: "STREET",
      AVE: "AVENUE", AV: "AVENUE", AVENUE: "AVENUE",
      RD: "ROAD", ROAD: "ROAD",
      DR: "DRIVE", DRIVE: "DRIVE",
      LN: "LANE", LANE: "LANE",
      CT: "COURT", COURT: "COURT",
      PL: "PLACE", PLACE: "PLACE",
      BLVD: "BOULEVARD", BOULEVARD: "BOULEVARD",
      PKWY: "PARKWAY", PARKWAY: "PARKWAY",
      TER: "TERRACE", TERRACE: "TERRACE",
      CIR: "CIRCLE", CIRCLE: "CIRCLE",
      HWY: "HIGHWAY", HIGHWAY: "HIGHWAY",
      RTE: "ROUTE", ROUTE: "ROUTE",
    };
    if (map[last]) parts[parts.length - 1] = map[last];
  }
  return parts.join(" ");
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
    if (!map.has(t.key)) map.set(t.key, fp);
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
    } catch {}
  }
  return pts;
}

function distanceBucket(distM) {
  if (distM <= 30) return "A_0_30m";
  if (distM <= 60) return "B_30_60m";
  if (distM <= 120) return "C_60_120m";
  if (distM <= 180) return "D_120_180m";
  return "E_OVER_180m";
}

function confidenceScore({ distM, zipMismatch, streetMismatch, multiClose }) {
  let score = 100;

  if (distM <= 30) score -= (distM / 30) * 10;
  else if (distM <= 60) score -= 10 + ((distM - 30) / 30) * 15;
  else if (distM <= 120) score -= 25 + ((distM - 60) / 60) * 20;
  else if (distM <= 180) score -= 45 + ((distM - 120) / 60) * 20;
  else score -= 80;

  if (zipMismatch) score -= 15;
  if (streetMismatch) score -= 20;
  if (multiClose) score -= 10;

  score = Math.max(0, Math.min(100, Math.round(score)));
  return score;
}

function scoreBucket(score) {
  if (score >= 85) return "HIGH";
  if (score >= 70) return "MED_HIGH";
  if (score >= 55) return "MED";
  if (score >= 40) return "LOW";
  return "REJECT";
}

const args = parseArgs(process.argv);
const inPath = args.in;
const tilesDir = args.tilesDir;
const outPath = args.out;
const reportPath = args.report;

const maxDistM = Number(args.maxDistM ?? 120);
const tileSize = Number(args.tileSize ?? 0.01);
const cacheLimit = Number(args.cacheLimit ?? 60);
const farZipGuardM = Number(args.farZipGuardM ?? 60);
const farStreetGuardM = Number(args.farStreetGuardM ?? 60);
const nearAmbiguityM = Number(args.nearAmbiguityM ?? 20);
const rejectScoreBelow = Number(args.rejectScoreBelow ?? 40);

if (!inPath || !tilesDir || !outPath || !reportPath) {
  console.error("Missing args. Required: --in --tilesDir --out --report");
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

console.log("====================================================");
console.log(" MAD Nearest (missing-only) + ZIP Guard + Confidence");
console.log("====================================================");
console.log("IN :", inPath);
console.log("TIL:", tilesDir);
console.log("OUT:", outPath);
console.log("REP:", reportPath);
console.log("maxDistM:", maxDistM, "farZipGuardM:", farZipGuardM, "farStreetGuardM:", farStreetGuardM);

console.log("Building tile map by sampling...");
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

let rejectedZipMismatch = 0;
let rejectedStreetMismatch = 0;
let rejectedLowScore = 0;

const bandCounts = { A_0_30m: 0, B_30_60m: 0, C_60_120m: 0, D_120_180m: 0, E_OVER_180m: 0 };
const confCounts = { HIGH: 0, MED_HIGH: 0, MED: 0, LOW: 0, REJECT: 0 };

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

  const rowZip = normZip5(row.zip ?? row.ZIP ?? null);
  const rowStreetNameCmp = normalizeStreetNameForCompare(row.street_name ?? row.streetName ?? row.STREET_NAME ?? null);

  let best = null;
  let bestD = Infinity;
  let closeWithin = 0;

  for (const p of candidates) {
    const pll = getMadLonLat(p);
    if (!pll) continue;
    const d = haversineMeters(lon, lat, pll[0], pll[1]);
    if (d > maxDistM) continue;

    const pZip = normZip5(getMadZip(p));
    const zipMismatch = (rowZip && pZip) ? (rowZip !== pZip) : false;
    if (d > farZipGuardM && zipMismatch) continue;

    const pStreetCmp = normalizeStreetNameForCompare(getMadStreetName(p));
    const streetMismatch = (rowStreetNameCmp && pStreetCmp) ? (rowStreetNameCmp !== pStreetCmp) : false;
    if (d > farStreetGuardM && streetMismatch) continue;

    if (d <= nearAmbiguityM) closeWithin++;

    if (d < bestD) {
      bestD = d;
      best = p;
    }
  }

  if (!best) {
    let hadZipMismatch = false;
    let hadStreetMismatch = false;

    for (const p of candidates) {
      const pll = getMadLonLat(p);
      if (!pll) continue;
      const d = haversineMeters(lon, lat, pll[0], pll[1]);
      if (d > maxDistM) continue;

      const pZip = normZip5(getMadZip(p));
      const zipMismatch = (rowZip && pZip) ? (rowZip !== pZip) : false;
      if (d > farZipGuardM && zipMismatch) { hadZipMismatch = true; continue; }

      const pStreetCmp = normalizeStreetNameForCompare(getMadStreetName(p));
      const streetMismatch = (rowStreetNameCmp && pStreetCmp) ? (rowStreetNameCmp !== pStreetCmp) : false;
      if (d > farStreetGuardM && streetMismatch) { hadStreetMismatch = true; continue; }
    }

    if (hadZipMismatch) rejectedZipMismatch++;
    if (hadStreetMismatch) rejectedStreetMismatch++;

    noCandidateWithinDist++;
    row.addr_authority_reason = row.addr_authority_reason ?? "NO_VALID_CANDIDATE_WITHIN_GUARDS";
    out.write(JSON.stringify(row) + "\n");
    return;
  }

  const pTown = normTown(getMadTown(best));
  const pZip = normZip5(getMadZip(best));
  const pStreetCmp = normalizeStreetNameForCompare(getMadStreetName(best));

  const zipMismatchSoft = (rowZip && pZip) ? (rowZip !== pZip) : false;
  const streetMismatchSoft = (rowStreetNameCmp && pStreetCmp) ? (rowStreetNameCmp !== pStreetCmp) : false;

  const multiClose = closeWithin >= 2;

  const score = confidenceScore({
    distM: bestD,
    zipMismatch: (bestD <= farZipGuardM) ? zipMismatchSoft : false,
    streetMismatch: (bestD <= farStreetGuardM) ? streetMismatchSoft : false,
    multiClose,
  });

  const scoreBand = scoreBucket(score);
  const distBand = distanceBucket(bestD);

  if (score < rejectScoreBelow || scoreBand === "REJECT") {
    rejectedLowScore++;
    row.addr_authority_reason = row.addr_authority_reason ?? "REJECTED_LOW_CONFIDENCE";
    row.addr_authority_dist_m = Math.round(bestD * 10) / 10;
    row.addr_authority_confidence_score = score;
    row.addr_authority_confidence_bucket = scoreBand;
    row.addr_authority_distance_bucket = distBand;
    out.write(JSON.stringify(row) + "\n");
    return;
  }

  const madNo = normalizeStreetNoBasic(getMadStreetNo(best));
  const madName = normalizeStreetNameBasic(getMadStreetName(best));
  const madZipRaw = getMadZip(best);

  let didPatch = false;
  const notes = [];
  const guards = {
    zip_guard_applied: bestD > farZipGuardM ? 1 : 0,
    zip_match_or_unknown: (!rowZip || !pZip) ? 1 : (rowZip === pZip ? 1 : 0),
    street_guard_applied: bestD > farStreetGuardM ? 1 : 0,
    street_match_or_unknown: (!rowStreetNameCmp || !pStreetCmp) ? 1 : (rowStreetNameCmp === pStreetCmp ? 1 : 0),
    ambiguity_close_candidates: closeWithin,
  };

  if (bestD <= farZipGuardM && zipMismatchSoft) notes.push("ZIP_MISMATCH_WITHIN_GUARD");
  if (bestD <= farStreetGuardM && streetMismatchSoft) notes.push("STREET_MISMATCH_WITHIN_GUARD");
  if (multiClose) notes.push("AMBIGUOUS_MULTIPLE_CLOSE_CANDIDATES");

  if (needsNo && madNo) {
    row.street_no = madNo;
    didPatch = true;
  }
  if (needsName && madName) {
    row.street_name = madName;
    didPatch = true;
  }
  if ((row.zip == null || String(row.zip).trim() === "") && madZipRaw) {
    const z = normZip5(madZipRaw);
    if (z) {
      row.zip = z;
      row.addr_zip_src = row.addr_zip_src ?? "MAD_ZIP_NEAREST";
      didPatch = true;
    }
  }

  if (!didPatch) {
    row.addr_authority_reason = row.addr_authority_reason ?? "BEST_CANDIDATE_MISSING_REQUIRED_FIELDS";
    out.write(JSON.stringify(row) + "\n");
    return;
  }

  patched++;
  bandCounts[distBand] = (bandCounts[distBand] ?? 0) + 1;
  confCounts[scoreBand] = (confCounts[scoreBand] ?? 0) + 1;

  row.addr_authority_src = "MAD_NEAREST_ZIP_GUARD_CONFIDENCE";
  row.addr_authority_dist_m = Math.round(bestD * 10) / 10;
  row.addr_authority_distance_bucket = distBand;
  row.addr_authority_confidence_score = score;
  row.addr_authority_confidence_bucket = scoreBand;
  row.addr_authority_guards = guards;
  row.addr_authority_candidate = { mad_town: pTown || null, mad_zip: pZip || null };
  row.addr_authority_notes = notes.length ? notes : null;
  row.addr_authority_reason = null;

  out.write(JSON.stringify(row) + "\n");

  if (total % 250000 === 0) {
    console.log(`...processed ${total.toLocaleString()} targets=${targets.toLocaleString()} patched=${patched.toLocaleString()} HIGH=${(confCounts.HIGH||0).toLocaleString()}`);
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
    farZipGuardM,
    farStreetGuardM,
    nearAmbiguityM,
    rejectScoreBelow,
    tileMapStats: tileStats,
    total,
    parseErr,
    targets,
    patched,
    noCoords,
    noTileCandidates,
    noCandidateWithinDist,
    rejectedZipMismatch,
    rejectedStreetMismatch,
    rejectedLowScore,
    distance_band_counts: bandCounts,
    confidence_bucket_counts: confCounts,
    timestamp: new Date().toISOString(),
    policy: {
      townGuard: "OFF (row town not trusted). Use ZIP+street guards + confidence buckets instead.",
      zipGuard: `Required when dist > ${farZipGuardM}m and both zips exist`,
      streetGuard: `If row has street_name, required when dist > ${farStreetGuardM}m`,
      scoreBuckets: "HIGH>=85, MED_HIGH>=70, MED>=55, LOW>=40, else REJECT",
      rejectScoreBelow,
    },
  };
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
  console.log("DONE.");
  console.log(JSON.stringify({ patched, distance_band_counts: bandCounts, confidence_bucket_counts: confCounts }, null, 2));
});

splitter.on("error", (e) => {
  console.error("Stream error:", e);
  process.exit(1);
});
