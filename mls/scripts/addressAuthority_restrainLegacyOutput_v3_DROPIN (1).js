/**
 * addressAuthority_restrainLegacyOutput_v3_DROPIN.js (ESM)
 * -------------------------------------------------------------------
 * Purpose:
 *   Re-validate the *legacy* MAD-nearest patches (v29) against the
 *   baseline (v28) under strict guards + confidence buckets.
 *
 * Fixes vs your v2:
 *   - No missing functions (buildTileMapBySampling included)
 *   - Robust NDJSON line splitting (\n, \r\n, lone \r) + buffer guard
 *   - Robust tile sampling (reads enough bytes to parse first NDJSON line)
 *
 * What it does:
 *   For each row pair (baseline, legacy) in lockstep:
 *     - If legacy did NOT change street_no/street_name/zip -> write legacy row unchanged
 *     - If legacy DID change -> attempt to find a MAD point near the row coords:
 *         - Must be within --maxDistM
 *         - If dist > --farZipGuardM, require ZIP match
 *         - If dist > --farStreetGuardM, require street-name match (normalized)
 *       -> outcome:
 *         AUTO_ACCEPT   (score >= --acceptScore) : keep legacy patch
 *         QUARANTINE    (score >= --quarantineScore) : keep legacy patch BUT flag
 *         REVERT        : restore baseline street fields
 *
 * Outputs:
 *   --out            (required) full dataset (same row count as input)
 *   --outQuarantine  (optional) only QUARANTINE rows
 *   --outReverted    (optional) only REVERT rows
 *   --report         (required) JSON report (counts + params)
 *
 * Usage:
 *   node .\mls\scripts\addressAuthority_restrainLegacyOutput_v3_DROPIN.js `
 *     --baseline "C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v28_addrAuthority_NEAREST.ndjson" `
 *     --legacy   "C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v29_addrAuthority_NEAREST.ndjson" `
 *     --tilesDir "C:\seller-app\backend\publicData\addresses\mad_tiles_0p01" `
 *     --out      "C:\seller-app\backend\publicData\properties\v29r_revalidated_120m_v3.ndjson" `
 *     --report   "C:\seller-app\backend\publicData\properties\v29r_revalidated_120m_v3_report.json" `
 *     --outQuarantine "C:\seller-app\backend\publicData\properties\v29r_revalidated_120m_v3_QUARANTINE.ndjson" `
 *     --outReverted   "C:\seller-app\backend\publicData\properties\v29r_revalidated_120m_v3_REVERTED.ndjson" `
 *     --maxDistM 120 `
 *     --farZipGuardM 60 `
 *     --farStreetGuardM 60 `
 *     --acceptScore 70 `
 *     --quarantineScore 40 `
 *     --tileSize 0.01 `
 *     --cacheLimit 250
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

class NDJSONLineSplitter extends Transform {
  constructor(maxBufChars = 32 * 1024 * 1024) {
    super({ readableObjectMode: true });
    this.buf = "";
    this.max = maxBufChars;
  }
  _transform(chunk, enc, cb) {
    const add = chunk.toString("utf8");
    if (this.buf.length + add.length > this.max) {
      cb(
        new Error(
          `NDJSON splitter buffer would exceed ${this.max.toLocaleString()} chars. ` +
            `This typically means the input is not true NDJSON or has unusual line endings/corruption.`
        )
      );
      return;
    }
    this.buf += add;

    let start = 0;
    for (let i = 0; i < this.buf.length; i++) {
      const ch = this.buf.charCodeAt(i);
      if (ch === 10 /*\n*/ || ch === 13 /*\r*/) {
        const line = this.buf.slice(start, i).trim();
        if (line) this.push(line);
        if (ch === 13 && i + 1 < this.buf.length && this.buf.charCodeAt(i + 1) === 10) i++; // \r\n
        start = i + 1;
      }
    }
    this.buf = this.buf.slice(start);
    cb();
  }
  _flush(cb) {
    const line = this.buf.trim();
    if (line) this.push(line);
    this.buf = "";
    cb();
  }
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function strEq(a, b) {
  return String(a ?? "") === String(b ?? "");
}

function normZip5(z) {
  const s = String(z ?? "").trim();
  const m9 = s.match(/^(\d{5})-\d{4}$/);
  if (m9) return m9[1];
  if (/^\d{5}$/.test(s)) return s;
  return null;
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

function isMissingNo(v) {
  if (v == null) return true;
  const s = String(v).trim();
  if (!s) return true;
  if (/^0+$/.test(s)) return true;
  return false;
}
function isMissingName(v) {
  return v == null || String(v).trim() === "";
}

function getLonLat(row) {
  const lon = row.lon ?? row.lng ?? row.longitude ?? row.LON ?? row.LNG ?? row.LONGITUDE ?? row.x ?? row.X;
  const lat = row.lat ?? row.latitude ?? row.LAT ?? row.LATITUDE ?? row.y ?? row.Y;
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

function pickField(obj, candidates) {
  for (const c of candidates) {
    if (obj[c] != null && String(obj[c]).trim() !== "") return c;
  }
  return null;
}

function madLonLat(p) {
  const lon = p.lon ?? p.lng ?? p.longitude ?? p.x ?? p.X ?? p.LON ?? p.LNG ?? p.LONGITUDE;
  const lat = p.lat ?? p.latitude ?? p.y ?? p.Y ?? p.LAT ?? p.LATITUDE;
  if (lon == null || lat == null) return null;
  const lo = Number(lon), la = Number(lat);
  if (!Number.isFinite(lo) || !Number.isFinite(la)) return null;
  return [lo, la];
}
function madZip(p) {
  const f = pickField(p, ["ZIP", "zip", "POSTCODE", "postcode"]);
  return f ? p[f] : null;
}
function madStreet(p) {
  const f = pickField(p, ["FULL_STREET_NAME", "full_street_name", "STREETNAME", "street_name", "STREET", "street"]);
  return f ? p[f] : null;
}

function distanceBand(d) {
  if (d <= 30) return "A_0_30m";
  if (d <= 60) return "B_30_60m";
  if (d <= 120) return "C_60_120m";
  if (d <= 180) return "D_120_180m";
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

class LRUCache {
  constructor(limit = 120) {
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

function neighborTileKeys(ix, iy) {
  const keys = [];
  for (let dx = -1; dx <= 1; dx++) {
    for (let dy = -1; dy <= 1; dy++) {
      keys.push(`${ix + dx},${iy + dy}`);
    }
  }
  return keys;
}

function readFirstNDJSONLine(filePath, maxBytes = 1024 * 256) {
  // Read chunks until we hit \n or \r, up to maxBytes
  const fd = fs.openSync(filePath, "r");
  try {
    const step = 64 * 1024;
    let pos = 0;
    let acc = Buffer.alloc(0);

    while (acc.length < maxBytes) {
      const buf = Buffer.alloc(step);
      const n = fs.readSync(fd, buf, 0, buf.length, pos);
      if (!n) break;
      pos += n;

      acc = Buffer.concat([acc, buf.slice(0, n)]);
      const idxN = acc.indexOf(0x0A); // \n
      const idxR = acc.indexOf(0x0D); // \r
      let cut = -1;
      if (idxN >= 0 && idxR >= 0) cut = Math.min(idxN, idxR);
      else cut = Math.max(idxN, idxR);

      if (cut >= 0) {
        const line = acc.slice(0, cut).toString("utf8").trim();
        return line || null;
      }
    }

    // fallback: treat whole buffer as text; take first non-empty line
    const txt = acc.toString("utf8").trim();
    if (!txt) return null;
    const lines = txt.split(/\r\n|\n|\r/);
    for (const l of lines) {
      const t = l.trim();
      if (t) return t;
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
    const firstLine = readFirstNDJSONLine(fp);
    if (!firstLine) {
      skipped++;
      continue;
    }
    try {
      const obj = JSON.parse(firstLine);
      const ll = madLonLat(obj);
      if (!ll) {
        skipped++;
        continue;
      }
      sampled++;
      const t = tileIndex(ll[0], ll[1], tileSize);
      if (map.has(t.key)) collisions++;
      if (!map.has(t.key)) map.set(t.key, fp);
    } catch {
      skipped++;
    }
  }

  return { map, stats: { files: files.length, sampled, skipped, collisions } };
}

function loadTilePoints(fp) {
  const raw = fs.readFileSync(fp, "utf8");
  if (!raw) return [];
  const pts = [];
  // Robust split (some Windows writes can have lone \r)
  const lines = raw.split(/\r\n|\n|\r/);
  for (const line of lines) {
    const t = line.trim();
    if (!t) continue;
    try {
      pts.push(JSON.parse(t));
    } catch {
      // ignore bad line
    }
  }
  return pts;
}

// ---------------- main ----------------
const args = parseArgs(process.argv);

const baselinePath = args.baseline;
const legacyPath = args.legacy;
const tilesDir = args.tilesDir;
const outPath = args.out;
const reportPath = args.report;

const outQuarantinePath = args.outQuarantine || null;
const outRevertedPath = args.outReverted || null;

const maxDistM = Number(args.maxDistM ?? 120);
const farZipGuardM = Number(args.farZipGuardM ?? 60);
const farStreetGuardM = Number(args.farStreetGuardM ?? 60);
const acceptScore = Number(args.acceptScore ?? 70);
const quarantineScore = Number(args.quarantineScore ?? 40);

const tileSize = Number(args.tileSize ?? 0.01);
const cacheLimit = Number(args.cacheLimit ?? 250);
const maxBufChars = Number(args.maxBufChars ?? (32 * 1024 * 1024));
const nearAmbiguityM = Number(args.nearAmbiguityM ?? 20);

if (!baselinePath || !legacyPath || !tilesDir || !outPath || !reportPath) {
  console.error("Missing required args: --baseline --legacy --tilesDir --out --report");
  process.exit(1);
}
for (const p of [baselinePath, legacyPath]) {
  if (!fs.existsSync(p)) {
    console.error("Input not found:", p);
    process.exit(1);
  }
}
if (!fs.existsSync(tilesDir)) {
  console.error("tilesDir not found:", tilesDir);
  process.exit(1);
}

ensureDir(path.dirname(outPath));
ensureDir(path.dirname(reportPath));
if (outQuarantinePath) ensureDir(path.dirname(outQuarantinePath));
if (outRevertedPath) ensureDir(path.dirname(outRevertedPath));

console.log("====================================================");
console.log(" Restrict LEGACY output under guards (v3)");
console.log("====================================================");
console.log("BASE:", baselinePath);
console.log("LEG :", legacyPath);
console.log("TIL :", tilesDir);
console.log("OUT :", outPath);
console.log("maxDistM:", maxDistM, "farZipGuardM:", farZipGuardM, "farStreetGuardM:", farStreetGuardM);

console.log("Building tile map by sampling...");
const { map: tileMap, stats: tileStats } = buildTileMapBySampling(tilesDir, tileSize);
console.log("Tile map stats:", tileStats);

if (tileStats.sampled < Math.max(1000, tileStats.files * 0.5)) {
  console.warn(
    "[warn] Tile sampling looks low. If sampled << files, your tiles may not be NDJSON or the first line lacks lon/lat."
  );
}

const tileCache = new LRUCache(cacheLimit);
function tilePointsForKey(key) {
  const hit = tileCache.get(key);
  if (hit) return hit;
  const fp = tileMap.get(key);
  if (!fp) {
    tileCache.set(key, []);
    return [];
  }
  const pts = loadTilePoints(fp);
  tileCache.set(key, pts);
  return pts;
}

const out = fs.createWriteStream(outPath, { encoding: "utf8" });
const outQ = outQuarantinePath ? fs.createWriteStream(outQuarantinePath, { encoding: "utf8" }) : null;
const outR = outRevertedPath ? fs.createWriteStream(outRevertedPath, { encoding: "utf8" }) : null;

const baseRS = fs.createReadStream(baselinePath);
const legRS = fs.createReadStream(legacyPath);

const baseSplit = new NDJSONLineSplitter(maxBufChars);
const legSplit = new NDJSONLineSplitter(maxBufChars);

baseRS.pipe(baseSplit);
legRS.pipe(legSplit);

let baseQueue = [];
let legQueue = [];

let total = 0;
let parseErrBase = 0, parseErrLegacy = 0;
let idMismatch = 0;

let detectedLegacyPatches = 0;
let kept_auto = 0;
let kept_quarantine = 0;
let reverted = 0;

let noCandidateWithinDist = 0;

const outcomeCounts = { AUTO_ACCEPT: 0, QUARANTINE: 0, REVERT: 0 };
const distBandCounts = { A_0_30m: 0, B_30_60m: 0, C_60_120m: 0, D_120_180m: 0, E_OVER_180m: 0 };
const confCounts = { HIGH: 0, MED_HIGH: 0, MED: 0, LOW: 0, REJECT: 0 };

function processPairs() {
  while (baseQueue.length && legQueue.length) {
    const baseLine = baseQueue.shift();
    const legLine = legQueue.shift();
    total++;

    let baseRow, legRow;
    try { baseRow = JSON.parse(baseLine); } catch { parseErrBase++; continue; }
    try { legRow = JSON.parse(legLine); } catch { parseErrLegacy++; continue; }

    const baseId = baseRow.property_id ?? baseRow.parcel_id ?? null;
    const legId = legRow.property_id ?? legRow.parcel_id ?? null;
    if (baseId && legId && baseId !== legId) idMismatch++;

    const baseNo = baseRow.street_no ?? baseRow.streetNo ?? baseRow.STREET_NO ?? null;
    const baseName = baseRow.street_name ?? baseRow.streetName ?? baseRow.STREET_NAME ?? null;
    const baseZipRaw = baseRow.zip ?? baseRow.ZIP ?? null;

    const legNo = legRow.street_no ?? legRow.streetNo ?? legRow.STREET_NO ?? null;
    const legName = legRow.street_name ?? legRow.streetName ?? legRow.STREET_NAME ?? null;
    const legZipRaw = legRow.zip ?? legRow.ZIP ?? null;

    const changedNo = (!strEq(baseNo, legNo)) && !isMissingNo(legNo);
    const changedName = (!strEq(baseName, legName)) && !isMissingName(legName);
    const changedZip = (!strEq(baseZipRaw, legZipRaw)) && (normZip5(legZipRaw) != null);

    const looksLikeLegacyPatch = changedNo || changedName || changedZip;

    if (!looksLikeLegacyPatch) {
      out.write(JSON.stringify(legRow) + "\n");
      continue;
    }

    detectedLegacyPatches++;

    const ll = getLonLat(legRow) || getLonLat(baseRow);
    if (!ll) {
      // Cannot validate — revert
      if (changedNo) legRow.street_no = baseNo;
      if (changedName) legRow.street_name = baseName;
      if (changedZip) legRow.zip = baseZipRaw;

      legRow.addr_authority_revalidate = { outcome: "REVERT", reason: "NO_COORDS" };
      reverted++; outcomeCounts.REVERT++;
      out.write(JSON.stringify(legRow) + "\n");
      if (outR) outR.write(JSON.stringify(legRow) + "\n");
      continue;
    }

    const [lon, lat] = ll;
    const t = tileIndex(lon, lat, tileSize);
    const neighborKeys = neighborTileKeys(t.ix, t.iy);

    const rowZip = normZip5(legZipRaw) || normZip5(baseZipRaw);
    const rowStreetCmp = normalizeStreetNameForCompare(legName || baseName);

    let best = null;
    let bestD = Infinity;
    let closeWithin = 0;

    for (const key of neighborKeys) {
      const pts = tilePointsForKey(key);
      for (const p of pts) {
        const pll = madLonLat(p);
        if (!pll) continue;
        const d = haversineMeters(lon, lat, pll[0], pll[1]);
        if (d > maxDistM) continue;

        const pZip = normZip5(madZip(p));
        const zipMismatch = (rowZip && pZip) ? (rowZip !== pZip) : false;
        if (d > farZipGuardM && zipMismatch) continue;

        const pStreetCmp = normalizeStreetNameForCompare(madStreet(p));
        const streetMismatch = (rowStreetCmp && pStreetCmp) ? (rowStreetCmp !== pStreetCmp) : false;
        if (d > farStreetGuardM && streetMismatch) continue;

        if (d <= nearAmbiguityM) closeWithin++;
        if (d < bestD) { bestD = d; best = p; }
      }
    }

    if (!best) {
      // No candidate under guards -> revert
      noCandidateWithinDist++;

      if (changedNo) legRow.street_no = baseNo;
      if (changedName) legRow.street_name = baseName;
      if (changedZip) legRow.zip = baseZipRaw;

      legRow.addr_authority_revalidate = { outcome: "REVERT", reason: "NO_VALID_CANDIDATE_UNDER_GUARDS" };
      reverted++; outcomeCounts.REVERT++;
      out.write(JSON.stringify(legRow) + "\n");
      if (outR) outR.write(JSON.stringify(legRow) + "\n");
      continue;
    }

    const pZip = normZip5(madZip(best));
    const pStreetCmp = normalizeStreetNameForCompare(madStreet(best));
    const zipMismatchSoft = (rowZip && pZip) ? (rowZip !== pZip) : false;
    const streetMismatchSoft = (rowStreetCmp && pStreetCmp) ? (rowStreetCmp !== pStreetCmp) : false;

    const multiClose = closeWithin >= 2;
    const zipMismatchForScore = (bestD > farZipGuardM) ? zipMismatchSoft : false;
    const streetMismatchForScore = (bestD > farStreetGuardM) ? streetMismatchSoft : false;

    const score = confidenceScore({
      distM: bestD,
      zipMismatch: zipMismatchForScore,
      streetMismatch: streetMismatchForScore,
      multiClose,
    });

    const band = distanceBand(bestD);
    const sb = scoreBucket(score);

    distBandCounts[band] = (distBandCounts[band] ?? 0) + 1;
    confCounts[sb] = (confCounts[sb] ?? 0) + 1;

    let outcome = "REVERT";
    if (score >= acceptScore) outcome = "AUTO_ACCEPT";
    else if (score >= quarantineScore) outcome = "QUARANTINE";

    if (outcome === "REVERT") {
      if (changedNo) legRow.street_no = baseNo;
      if (changedName) legRow.street_name = baseName;
      if (changedZip) legRow.zip = baseZipRaw;

      reverted++; outcomeCounts.REVERT++;
      if (outR) outR.write(JSON.stringify(legRow) + "\n");
    } else if (outcome === "AUTO_ACCEPT") {
      kept_auto++; outcomeCounts.AUTO_ACCEPT++;
    } else {
      kept_quarantine++; outcomeCounts.QUARANTINE++;
      if (outQ) outQ.write(JSON.stringify(legRow) + "\n");
    }

    legRow.addr_authority_revalidate = {
      outcome,
      dist_m: Math.round(bestD * 10) / 10,
      distance_bucket: band,
      confidence_score: score,
      confidence_bucket: sb,
      notes: { changedNo, changedName, changedZip, multiClose },
    };

    out.write(JSON.stringify(legRow) + "\n");

    if (total % 250000 === 0) {
      console.log(
        `...processed ${total.toLocaleString()} | detected=${detectedLegacyPatches.toLocaleString()} auto=${kept_auto.toLocaleString()} q=${kept_quarantine.toLocaleString()} rev=${reverted.toLocaleString()}`
      );
    }
  }
}

let baseEnded = false;
let legEnded = false;

function finishIfDone() {
  if (!baseEnded || !legEnded) return;

  out.end();
  if (outQ) outQ.end();
  if (outR) outR.end();

  const report = {
    baselinePath,
    legacyPath,
    tilesDir,
    outPath,
    outQuarantinePath,
    outRevertedPath,
    params: {
      maxDistM,
      farZipGuardM,
      farStreetGuardM,
      acceptScore,
      quarantineScore,
      tileSize,
      cacheLimit,
      nearAmbiguityM,
      maxBufChars,
    },
    tileMapStats: tileStats,
    counts: {
      total,
      parseErrBase,
      parseErrLegacy,
      idMismatch,
      detectedLegacyPatches,
      kept_auto,
      kept_quarantine,
      reverted,
      noCandidateWithinDist,
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

baseSplit.on("end", () => { baseEnded = true; finishIfDone(); });
legSplit.on("end", () => { legEnded = true; finishIfDone(); });

baseSplit.on("error", (e) => {
  console.error("Baseline stream error:", e?.message || e);
  process.exit(1);
});
legSplit.on("error", (e) => {
  console.error("Legacy stream error:", e?.message || e);
  process.exit(1);
});
