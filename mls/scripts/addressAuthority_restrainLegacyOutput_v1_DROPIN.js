/**
 * addressAuthority_restrainLegacyOutput_v1_DROPIN.js (ESM)
 *
 * What it does:
 *   You have a "legacy" MAD-nearest output produced with minimal/no restraints (higher patched counts).
 *   You want to KEEP those wins, but re-validate them under institutional restraints.
 *
 * This script:
 *   - Reads BASELINE and LEGACY files in lockstep (same line order expected; both should have same total rows).
 *   - Detects rows where LEGACY filled/changed street_no / street_name / zip while BASELINE was missing/invalid.
 *   - Recomputes nearest MAD point from tiles and applies ZIP+street guards + confidence scoring.
 *   - Outcomes for each detected "legacy patch":
 *       * AUTO_ACCEPT  (score >= acceptScore)      -> keep legacy patched fields + add audit fields
 *       * QUARANTINE   (score in [quarantineScore, acceptScore)) -> keep patch but mark for later verification
 *       * REVERT       (score < quarantineScore OR no candidate under guards) -> revert patched fields back to baseline
 *
 * Outputs:
 *   --out            : full restrained dataset (same row count as inputs)
 *   --outQuarantine  : (optional) only QUARANTINE rows (subset)
 *   --outReverted    : (optional) only REVERTED rows (subset)
 *   --report         : JSON summary counts
 *
 * Usage:
 *   node .\mls\scripts\addressAuthority_restrainLegacyOutput_v1_DROPIN.js `
 *     --baseline "C:\...\properties_statewide_geo_zip_district_v28_addrAuthority_NEAREST.ndjson" `
 *     --legacy   "C:\...\properties_statewide_geo_zip_district_v29_addrAuthority_NEAREST.ndjson" `
 *     --tilesDir "C:\...\mad_tiles_0p01" `
 *     --out      "C:\...\v29_restrained_from_legacy.ndjson" `
 *     --report   "C:\...\v29_restrained_from_legacy_report.json" `
 *     --maxDistM 60
 *
 * Recommended:
 *   First do maxDistM=60 (match yesterday's run), then optionally re-run with 120 on the same baseline+legacy pair.
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

// MAD resolvers
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
  if (/^\d+[A-Z]$/.test(s2)) return s2;      // 12A
  if (/^\d+1\/2$/.test(s.replace(/\s+/g, ""))) return s.replace(/\s+/g, "");  // 121/2
  if (/^\d+\s*-\s*\d+$/.test(s)) return s.replace(/\s+/g, "");               // 12-14
  return null;
}

function normalizeStreetNameBasic(v) {
  if (v == null) return null;
  let s = String(v).trim();
  if (!s) return null;
  return s.replace(/\s+/g, " ").trim() || null;
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
  let sampled = 0, skipped = 0, collisions = 0;
  for (const f of files) {
    const fp = path.join(tilesDir, f);
    const ll = sampleFirstPointLonLat(fp);
    if (!ll) { skipped++; continue; }
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
    try { pts.push(JSON.parse(t)); } catch {}
  }
  return pts;
}

// patch detection (baseline missing/invalid -> legacy filled/changed)
function isMissingNo(v) {
  if (v == null) return true;
  const s = String(v).trim();
  if (!s) return true;
  if (/^0+$/.test(s)) return true;
  return false;
}
function isMissingName(v) {
  if (v == null) return true;
  return String(v).trim() === "";
}
function strEq(a, b) {
  return String(a ?? "") === String(b ?? "");
}

const args = parseArgs(process.argv);
const baselinePath = args.baseline;
const legacyPath = args.legacy;
const tilesDir = args.tilesDir;
const outPath = args.out;
const reportPath = args.report;

const outQuarantinePath = args.outQuarantine || null;
const outRevertedPath = args.outReverted || null;

const maxDistM = Number(args.maxDistM ?? 60);
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
console.log(" Restrict LEGACY MAD-nearest output under guards");
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

    const baseNeedsNo = isMissingNo(baseNo);
    const baseNeedsName = isMissingName(baseName);
    const baseNeedsZip = (normZip5(baseZip) == null);

    const changedNo = baseNeedsNo && !isMissingNo(legNo) && !strEq(baseNo, legNo);
    const changedName = baseNeedsName && !isMissingName(legName) && !strEq(baseName, legName);
    const changedZip = baseNeedsZip && (normZip5(legZip) != null) && !strEq(baseZip, legZip);

    const looksLikeLegacyPatch = changedNo || changedName || changedZip;

    if (!looksLikeLegacyPatch) {
      out.write(JSON.stringify(legRow) + "\n");
      continue;
    }

    detectedLegacyPatches++;

    const ll = getLonLat(legRow) || getLonLat(baseRow);
    if (!ll) {
      // can't revalidate: safest is revert
      if (changedNo) legRow.street_no = baseNo;
      if (changedName) legRow.street_name = baseName;
      if (changedZip) legRow.zip = baseZip;

      legRow.addr_authority_revalidate = {
        outcome: "REVERT",
        reason: "NO_COORDS",
      };
      outcomeCounts.REVERT++;
      reverted++;
      out.write(JSON.stringify(legRow) + "\n");
      if (outR) outR.write(JSON.stringify(legRow) + "\n");
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

      if (d < bestD) {
        bestD = d;
        best = p;
      }
    }

    if (!best) {
      // determine why (zip/street guards)
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

      // revert patched fields
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
      out.write(JSON.stringify(legRow) + "\n");
      if (outR) outR.write(JSON.stringify(legRow) + "\n");
      continue;
    }

    const pZip = normZip5(getMadZip(best));
    const pStreetCmp = normalizeStreetNameForCompare(getMadStreetName(best));
    const zipMismatchSoft = (rowZip && pZip) ? (rowZip !== pZip) : false;
    const streetMismatchSoft = (rowStreetCmp && pStreetCmp) ? (rowStreetCmp !== pStreetCmp) : false;

    const multiClose = closeWithin >= 2;
    const score = confidenceScore({
      distM: bestD,
      zipMismatch: (bestD <= farZipGuardM) ? zipMismatchSoft : false,
      streetMismatch: (bestD <= farStreetGuardM) ? streetMismatchSoft : false,
      multiClose,
    });
    const scoreBand = scoreBucket(score);
    const distBand = distanceBucket(bestD);

    // strict accept/quarantine/revert
    let outcome = "REVERT";
    if (score >= acceptScore) outcome = "AUTO_ACCEPT";
    else if (score >= quarantineScore) outcome = "QUARANTINE";

    if (outcome === "REVERT") {
      if (changedNo) legRow.street_no = baseNo;
      if (changedName) legRow.street_name = baseName;
      if (changedZip) legRow.zip = baseZip;
      reverted++;
      outcomeCounts.REVERT++;
      if (outR) outR.write(JSON.stringify(legRow) + "\n");
    } else if (outcome === "AUTO_ACCEPT") {
      kept_auto++;
      outcomeCounts.AUTO_ACCEPT++;
    } else {
      kept_quarantine++;
      outcomeCounts.QUARANTINE++;
      if (outQ) outQ.write(JSON.stringify(legRow) + "\n");
    }

    distBandCounts[distBand] = (distBandCounts[distBand] ?? 0) + 1;
    confCounts[scoreBand] = (confCounts[scoreBand] ?? 0) + 1;

    legRow.addr_authority_revalidate = {
      outcome,
      dist_m: Math.round(bestD * 10) / 10,
      distance_bucket: distBand,
      confidence_score: score,
      confidence_bucket: scoreBand,
      notes: {
        changedNo, changedName, changedZip,
        multiClose,
      },
    };

    out.write(JSON.stringify(legRow) + "\n");

    if (total % 250000 === 0) {
      console.log(`...processed ${total.toLocaleString()} detectedLegacyPatches=${detectedLegacyPatches.toLocaleString()} kept_auto=${kept_auto.toLocaleString()} quarantine=${kept_quarantine.toLocaleString()} reverted=${reverted.toLocaleString()}`);
    }
  }
}

let baseEnded = false, legEnded = false;
function maybeFinish() {
  if (!baseEnded || !legEnded) return;
  // Drain any mismatch
  if (baseQueue.length || legQueue.length) {
    console.warn("WARNING: input line counts differ (queue leftovers). baselineLeft=", baseQueue.length, "legacyLeft=", legQueue.length);
  }
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
    note: "If you see leftovers warning, the baseline and legacy files are not in the same row order. In that case we must join by parcel_id (slower/more complex).",
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
