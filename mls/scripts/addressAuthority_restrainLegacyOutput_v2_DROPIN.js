/**
 * addressAuthority_restrainLegacyOutput_v2_DROPIN.js (ESM)
 *
 * Fix vs v1:
 *  - Detects LEGACY changes by ANY difference (not only "baseline missing").
 *    This is critical when you want to re-validate the full v28->v29 (120m) patch set.
 *
 * What it does:
 *  - Reads BASE and LEG in lockstep (same row order; produced by your streaming pipeline).
 *  - Finds rows where LEG changed street_no / street_name / zip vs BASE.
 *  - For those changed rows, recomputes nearest MAD point (from tiles) and applies guards + confidence scoring.
 *  - Outcome:
 *      AUTO_ACCEPT -> keep LEG values
 *      QUARANTINE  -> keep LEG values, but flagged
 *      REVERT      -> revert ONLY the changed fields back to BASE values
 *
 * Guards:
 *  - maxDistM: maximum allowed nearest distance
 *  - farZipGuardM: if dist > this, require ZIP match (row zip vs MAD zip)
 *  - farStreetGuardM: if dist > this, require street name match (normalized compare)
 *
 * Typical use for your case:
 *  - Revalidate v28->v29 with maxDistM=120 and far guards at 60:
 *      maxDistM=120, farZipGuardM=60, farStreetGuardM=60
 *
 * Usage:
 *   node .\mls\scripts\addressAuthority_restrainLegacyOutput_v2_DROPIN.js `
 *     --baseline "C:\...\v28_addrAuthority_NEAREST.ndjson" `
 *     --legacy   "C:\...\v29_addrAuthority_NEAREST.ndjson" `
 *     --tilesDir "C:\...\mad_tiles_0p01" `
 *     --out      "C:\...\v29r_revalidated_120m.ndjson" `
 *     --report   "C:\...\v29r_revalidated_120m_report.json" `
 *     --maxDistM 120 `
 *     --farZipGuardM 60 `
 *     --farStreetGuardM 60
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
  constructor(maxBufChars = 32 * 1024 * 1024) {
    super({ readableObjectMode: true });
    this._buf = "";
    this._max = maxBufChars;
  }

  _transform(chunk, enc, cb) {
    // Convert chunk once
    const add = chunk.toString("utf8");

    // Guard BEFORE concatenation (prevents RangeError)
    if (this._buf.length + add.length > this._max) {
      cb(
        new Error(
          `LineSplitter buffer would exceed ${this._max.toLocaleString()} chars. ` +
          `Likely NOT NDJSON or line endings are unusual/corrupted (e.g., lone \\r).`
        )
      );
      return;
    }

    this._buf += add;

    // Split on \r\n, \n, OR lone \r
    const parts = this._buf.split(/\r\n|\n|\r/);
    this._buf = parts.pop() ?? "";

    for (const p of parts) {
      const t = p.trim();
      if (t) this.push(t);
    }

    cb();
  }

  _flush(cb) {
    const t = this._buf.trim();
    if (t) this.push(t);
    this._buf = "";
    cb();
  }
}


function ensureDir(p) { fs.mkdirSync(p, { recursive: true }); }

function pickField(obj, candidates) {
  for (const c of candidates) {
    if (obj[c] != null && String(obj[c]).trim() !== "") return c;
  }
  return null;
}

function normZip5(z) {
  const s = String(z ?? "").trim();
  const m9 = s.match(/^(\\d{5})-\\d{4}$/);
  if (m9) return m9[1];
  if (/^\\d{5}$/.test(s)) return s;
  return null;
}

function getLonLat(row) {
  const lon = row.lon ?? row.longitude ?? row.LON ?? row.LONGITUDE ?? row.x ?? row.X ?? row.lng ?? row.LNG;
  const lat = row.lat ?? row.latitude ?? row.LAT ?? row.LATITUDE ?? row.y ?? row.Y ?? row.lat_y ?? row.LAT_Y;
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

// MAD getters (best-effort)
function getMadLonLat(p) {
  const lon = p.lon ?? p.longitude ?? p.x ?? p.X ?? p.LON ?? p.Longitude ?? p.LONGITUDE;
  const lat = p.lat ?? p.latitude ?? p.y ?? p.Y ?? p.LAT ?? p.Latitude ?? p.LATITUDE;
  if (lon == null || lat == null) return null;
  const lo = Number(lon), la = Number(lat);
  if (!Number.isFinite(lo) || !Number.isFinite(la)) return null;
  return [lo, la];
}
function getMadZip(p) {
  const f = pickField(p, ["ZIP", "zip", "POSTCODE", "postcode"]);
  return f ? p[f] : null;
}
function getMadStreetName(p) {
  const f = pickField(p, ["FULL_STREET_NAME", "full_street_name", "STREETNAME", "street_name", "STREET", "street"]);
  return f ? p[f] : null;
}

function isMissingName(v) {
  return v == null || String(v).trim() === "";
}
function isMissingNo(v) {
  if (v == null) return true;
  const s = String(v).trim();
  if (!s) return true;
  if (/^0+$/.test(s)) return true;
  return false;
}

function normalizeStreetNameForCompare(v) {
  if (v == null) return null;
  let s = String(v).toUpperCase().trim();
  if (!s) return null;
  s = s.replace(/[^A-Z0-9\\s]/g, " ").replace(/\\s+/g, " ").trim();
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
  return Math.max(0, Math.min(100, Math.round(score)));
}

function scoreBucket(score) {
  if (score >= 85) return "HIGH";
  if (score >= 70) return "MED_HIGH";
  if (score >= 55) return "MED";
  if (score >= 40) return "LOW";
  return "REJECT";
}

// --- Tile cache ---
class LRUCache {
  constructor(limit = 60) { this.limit = limit; this.map = new Map(); }
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
    const lines = txt.split(/\\r?\\n/);
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

buildTileMapBySampling

function loadTilePoints(fp) {
  const raw = fs.readFileSync(fp, "utf8");
  const pts = [];
  const lines = raw.split(/\\r?\\n/);
  for (const line of lines) {
    const t = line.trim();
    if (!t) continue;
    try { pts.push(JSON.parse(t)); } catch {}
  }
  return pts;
}

function strEq(a, b) { return String(a ?? "") === String(b ?? ""); }

const args = parseArgs(process.argv);
const baselinePath = args.baseline;
const legacyPath = args.legacy;
const tilesDir = args.tilesDir;
const outPath = args.out;
const reportPath = args.report;

const outQuarantinePath = args.outQuarantine || null;
const outRevertedPath = args.outReverted || null;

const maxDistM = Number(args.maxDistM ?? 120);
const tileSize = Number(args.tileSize ?? 0.01);
const cacheLimit = Number(args.cacheLimit ?? 60);

const farZipGuardM = Number(args.farZipGuardM ?? 60);
const farStreetGuardM = Number(args.farStreetGuardM ?? 60);
const nearAmbiguityM = Number(args.nearAmbiguityM ?? 20);

const acceptScore = Number(args.acceptScore ?? 70);
const quarantineScore = Number(args.quarantineScore ?? 40);

if (!baselinePath || !legacyPath || !tilesDir || !outPath || !reportPath) {
  console.error("Missing args. Required: --baseline --legacy --tilesDir --out --report");
  process.exit(1);
}
for (const p of [baselinePath, legacyPath]) {
  if (!fs.existsSync(p)) { console.error("Input not found:", p); process.exit(1); }
}
if (!fs.existsSync(tilesDir)) { console.error("tilesDir not found:", tilesDir); process.exit(1); }

ensureDir(path.dirname(outPath));
ensureDir(path.dirname(reportPath));
if (outQuarantinePath) ensureDir(path.dirname(outQuarantinePath));
if (outRevertedPath) ensureDir(path.dirname(outRevertedPath));

console.log("====================================================");
console.log(" Restrict LEGACY output under guards (v2 any-change detect)");
console.log("====================================================");
console.log("BASE:", baselinePath);
console.log("LEG :", legacyPath);
console.log("TIL :", tilesDir);
console.log("OUT :", outPath);
console.log("maxDistM:", maxDistM, "farZipGuardM:", farZipGuardM, "farStreetGuardM:", farStreetGuardM);

console.log("Building tile map by sampling...");
const { map: tileMap, stats: tileStats } = buildTileMapBySampling(tilesDir, tileSize);
console.log("Tile map stats:", tileStats);

const tileCache = new LRUCache(cacheLimit);
function getTilePointsByKey(key) {
  const cached = tileCache.get(key);
  if (cached) return cached;
  const fp = tileMap.get(key);
  if (!fp) { tileCache.set(key, []); return []; }
  const pts = loadTilePoints(fp);
  tileCache.set(key, pts);
  return pts;
}

const out = fs.createWriteStream(outPath, { encoding: "utf8" });
const outQ = outQuarantinePath ? fs.createWriteStream(outQuarantinePath, { encoding: "utf8" }) : null;
const outR = outRevertedPath ? fs.createWriteStream(outRevertedPath, { encoding: "utf8" }) : null;

// lockstep reading
const baseRS = fs.createReadStream(baselinePath);
const legRS = fs.createReadStream(legacyPath);
const baseSplit = new LineSplitter();
const legSplit = new LineSplitter();
baseRS.pipe(baseSplit);
legRS.pipe(legSplit);

let baseQueue = [];
let legQueue = [];

let total = 0;
let parseErrBase = 0, parseErrLegacy = 0;

let detectedLegacyPatches = 0;
let kept_auto = 0;
let kept_quarantine = 0;
let reverted = 0;
let noCandidateWithinDist = 0;
let rejectedZipMismatch = 0;
let rejectedStreetMismatch = 0;

const distBandCounts = { A_0_30m: 0, B_30_60m: 0, C_60_120m: 0, D_120_180m: 0, E_OVER_180m: 0 };
const confCounts = { HIGH: 0, MED_HIGH: 0, MED: 0, LOW: 0, REJECT: 0 };
const outcomeCounts = { AUTO_ACCEPT: 0, QUARANTINE: 0, REVERT: 0 };

function processPairs() {
  while (baseQueue.length && legQueue.length) {
    const baseLine = baseQueue.shift();
    const legLine = legQueue.shift();
    total++;

    let baseRow, legRow;
    try { baseRow = JSON.parse(baseLine); } catch { parseErrBase++; continue; }
    try { legRow = JSON.parse(legLine); } catch { parseErrLegacy++; continue; }

    const baseNo = baseRow.street_no ?? baseRow.streetNo ?? baseRow.STREET_NO ?? null;
    const baseName = baseRow.street_name ?? baseRow.streetName ?? baseRow.STREET_NAME ?? null;
    const baseZip = baseRow.zip ?? baseRow.ZIP ?? null;

    const legNo = legRow.street_no ?? legRow.streetNo ?? legRow.STREET_NO ?? null;
    const legName = legRow.street_name ?? legRow.streetName ?? legRow.STREET_NAME ?? null;
    const legZip = legRow.zip ?? legRow.ZIP ?? null;

    // Any-change detection (legacy differs and legacy has something non-empty)
    const changedNo = (!strEq(baseNo, legNo)) && !isMissingNo(legNo);
    const changedName = (!strEq(baseName, legName)) && !isMissingName(legName);
    const changedZip = (!strEq(baseZip, legZip)) && (normZip5(legZip) != null);

    const looksLikeLegacyPatch = changedNo || changedName || changedZip;

    if (!looksLikeLegacyPatch) {
      out.write(JSON.stringify(legRow) + "\\n");
      continue;
    }

    detectedLegacyPatches++;

    const ll = getLonLat(legRow) || getLonLat(baseRow);
    if (!ll) {
      // safest: revert changed fields
      if (changedNo) legRow.street_no = baseNo;
      if (changedName) legRow.street_name = baseName;
      if (changedZip) legRow.zip = baseZip;

      legRow.addr_authority_revalidate = { outcome: "REVERT", reason: "NO_COORDS" };
      outcomeCounts.REVERT++;
      reverted++;
      out.write(JSON.stringify(legRow) + "\\n");
      if (outR) outR.write(JSON.stringify(legRow) + "\\n");
      continue;
    }

    const [lon, lat] = ll;
    const t = tileIndex(lon, lat, tileSize);
    const keys = neighborKeys(t.ix, t.iy);

    let candidates = [];
    for (const k of keys) {
      const pts = getTilePointsByKey(k);
      if (pts.length) candidates = candidates.concat(pts);
    }

    const rowZip = normZip5(legZip) || normZip5(baseZip);
    const rowStreetCmp = normalizeStreetNameForCompare(legName || baseName);

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
      const streetMismatch = (rowStreetCmp && pStreetCmp) ? (rowStreetCmp !== pStreetCmp) : false;
      if (d > farStreetGuardM && streetMismatch) continue;

      if (d <= nearAmbiguityM) closeWithin++;

      if (d < bestD) { bestD = d; best = p; }
    }

    if (!best) {
      // count why (guards)
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
        const streetMismatch = (rowStreetCmp && pStreetCmp) ? (rowStreetCmp !== pStreetCmp) : false;
        if (d > farStreetGuardM && streetMismatch) { hadStreetMismatch = true; continue; }
      }
      if (hadZipMismatch) rejectedZipMismatch++;
      if (hadStreetMismatch) rejectedStreetMismatch++;
      noCandidateWithinDist++;

      // revert changed fields
      if (changedNo) legRow.street_no = baseNo;
      if (changedName) legRow.street_name = baseName;
      if (changedZip) legRow.zip = baseZip;

      legRow.addr_authority_revalidate = {
        outcome: "REVERT",
        reason: "NO_VALID_CANDIDATE_UNDER_GUARDS",
        hadZipMismatch,
        hadStreetMismatch,
      };
      outcomeCounts.REVERT++;
      reverted++;
      out.write(JSON.stringify(legRow) + "\\n");
      if (outR) outR.write(JSON.stringify(legRow) + "\\n");
      continue;
    }

    const pZip = normZip5(getMadZip(best));
    const pStreetCmp = normalizeStreetNameForCompare(getMadStreetName(best));
    const zipMismatchSoft = (rowZip && pZip) ? (rowZip !== pZip) : false;
    const streetMismatchSoft = (rowStreetCmp && pStreetCmp) ? (rowStreetCmp !== pStreetCmp) : false;

    const multiClose = closeWithin >= 2;
    const score = confidenceScore({
      distM: bestD,
      zipMismatch: (bestD > farZipGuardM) ? zipMismatchSoft : false,
      streetMismatch: (bestD > farStreetGuardM) ? streetMismatchSoft : false,
      multiClose,
    });
    const scoreBand = scoreBucket(score);
    const distBand = distanceBucket(bestD);

    let outcome = "REVERT";
    if (score >= acceptScore) outcome = "AUTO_ACCEPT";
    else if (score >= quarantineScore) outcome = "QUARANTINE";

    if (outcome === "REVERT") {
      if (changedNo) legRow.street_no = baseNo;
      if (changedName) legRow.street_name = baseName;
      if (changedZip) legRow.zip = baseZip;
      reverted++;
      outcomeCounts.REVERT++;
      if (outR) outR.write(JSON.stringify(legRow) + "\\n");
    } else if (outcome === "AUTO_ACCEPT") {
      kept_auto++;
      outcomeCounts.AUTO_ACCEPT++;
    } else {
      kept_quarantine++;
      outcomeCounts.QUARANTINE++;
      if (outQ) outQ.write(JSON.stringify(legRow) + "\\n");
    }

    distBandCounts[distBand] = (distBandCounts[distBand] ?? 0) + 1;
    confCounts[scoreBand] = (confCounts[scoreBand] ?? 0) + 1;

    legRow.addr_authority_revalidate = {
      outcome,
      dist_m: Math.round(bestD * 10) / 10,
      distance_bucket: distBand,
      confidence_score: score,
      confidence_bucket: scoreBand,
      notes: { changedNo, changedName, changedZip, multiClose },
    };

    out.write(JSON.stringify(legRow) + "\\n");

    if (total % 250000 === 0) {
      console.log(`...processed ${total.toLocaleString()} detected=${detectedLegacyPatches.toLocaleString()} auto=${kept_auto.toLocaleString()} q=${kept_quarantine.toLocaleString()} rev=${reverted.toLocaleString()}`);
    }
  }
}

let baseEnded = false, legEnded = false;
function maybeFinish() {
  if (!baseEnded || !legEnded) return;
  out.end();
  if (outQ) outQ.end();
  if (outR) outR.end();

  const report = {
    baselinePath, legacyPath, tilesDir, outPath,
    outQuarantinePath, outRevertedPath,
    params: { maxDistM, tileSize, cacheLimit, farZipGuardM, farStreetGuardM, nearAmbiguityM, acceptScore, quarantineScore },
    tileMapStats: tileStats,
    counts: {
      total,
      parseErrBase, parseErrLegacy,
      detectedLegacyPatches,
      kept_auto,
      kept_quarantine,
      reverted,
      noCandidateWithinDist,
      rejectedZipMismatch,
      rejectedStreetMismatch,
      outcomeCounts,
      distance_band_counts: distBandCounts,
      confidence_bucket_counts: confCounts,
    },
    timestamp: new Date().toISOString(),
  };
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
  console.log("DONE.");
  console.log(JSON.stringify(report.counts, null, 2));
}

baseSplit.on("data", (line) => { baseQueue.push(line); processPairs(); });
legSplit.on("data", (line) => { legQueue.push(line); processPairs(); });

baseSplit.on("end", () => { baseEnded = true; maybeFinish(); });
legSplit.on("end", () => { legEnded = true; maybeFinish(); });

baseSplit.on("error", (e) => { console.error("Baseline stream error:", e); process.exit(1); });
legSplit.on("error", (e) => { console.error("Legacy stream error:", e); process.exit(1); });
