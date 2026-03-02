/**
 * addressAuthority_promoteTierC_madNearest_v1_DROPIN.js (ESM)
 * ------------------------------------------------------------
 * Tier C-only MAD-nearest promotion (institutional-safe):
 *   - Reads a full statewide NDJSON
 *   - Only targets rows with address_tier === "C"
 *   - Finds nearest MAD point using mad_tiles_0p01 (0.01° tiles)
 *   - Applies ONLY "AUTO_ACCEPT" updates to the actual address fields
 *   - Writes QUARANTINE and UNRESOLVED splits for review / later verification
 *
 * AUTO_ACCEPT policy (default):
 *   - Candidate within maxDistM (default 60m)
 *   - If dist > farZipGuardM (default 60): require ZIP match when a base ZIP exists
 *   - If dist > farStreetGuardM (default 60): require street_name match when a base street exists
 *   - Town mismatch => QUARANTINE (never auto-overwrite)
 *
 * QUARANTINE:
 *   - Candidate within maxDistM but missing base fields for guards, or medium confidence
 *   - We DO NOT overwrite primary fields; we attach mad_suggest_* fields.
 *
 * UNRESOLVED:
 *   - No coords, out-of-bounds, no tile candidates, or no candidate within maxDistM, or guard reject
 *
 * Usage (PowerShell):
 *   cd C:\seller-app\backend
 *   node .\mls\scripts\addressAuthority_promoteTierC_madNearest_v1_DROPIN.js `
 *     --in  "C:\seller-app\backend\publicData\properties\v33_tierB_categorized.ndjson" `
 *     --tilesDir "C:\seller-app\backend\publicData\addresses\mad_tiles_0p01" `
 *     --out "C:\seller-app\backend\publicData\properties\v34_tierC_madPromoted_60m.ndjson" `
 *     --report "C:\seller-app\backend\publicData\properties\v34_tierC_madPromoted_60m_report.json" `
 *     --maxDistM 60 `
 *     --farZipGuardM 60 `
 *     --farStreetGuardM 60 `
 *     --tileCacheN 250 `
 *     --outAuto "C:\seller-app\backend\publicData\properties\v34_tierC_madPromoted_60m_AUTO.ndjson" `
 *     --outQuarantine "C:\seller-app\backend\publicData\properties\v34_tierC_madPromoted_60m_QUARANTINE.ndjson" `
 *     --outUnresolved "C:\seller-app\backend\publicData\properties\v34_tierC_madPromoted_60m_UNRESOLVED.ndjson"
 */

import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

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

const args = parseArgs(process.argv);

const inPath = args.in;
const tilesDir = args.tilesDir;
const outPath = args.out;
const reportPath = args.report;

const outAutoPath = args.outAuto || null;
const outQuarantinePath = args.outQuarantine || null;
const outUnresolvedPath = args.outUnresolved || null;

const maxDistM = Number(args.maxDistM ?? 60);
const farZipGuardM = Number(args.farZipGuardM ?? 60);
const farStreetGuardM = Number(args.farStreetGuardM ?? 60);
const tileCacheN = Number(args.tileCacheN ?? 250);

const TILE_SIZE_DEG = 0.01; // matches mad_tiles_0p01
const NOW = new Date().toISOString();
const VERSION = "tierC_madNearest_v1";

if (!inPath || !tilesDir || !outPath || !reportPath) {
  console.error("Missing required args: --in --tilesDir --out --report");
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

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.mkdirSync(path.dirname(reportPath), { recursive: true });
if (outAutoPath) fs.mkdirSync(path.dirname(outAutoPath), { recursive: true });
if (outQuarantinePath) fs.mkdirSync(path.dirname(outQuarantinePath), { recursive: true });
if (outUnresolvedPath) fs.mkdirSync(path.dirname(outUnresolvedPath), { recursive: true });

function inc(obj, k, n = 1) { obj[k] = (obj[k] ?? 0) + n; }
function pct(n, d) { return d ? Number((100 * n / d).toFixed(3)) : 0; }

function zip5(z) {
  const s = String(z ?? "").trim();
  if (!s) return "";
  const m = s.match(/^(\d{5})/);
  return m ? m[1] : "";
}
function townNorm(t) { return String(t ?? "").trim().toUpperCase(); }

const STREET_MAP = new Map([
  ["STREET", "ST"], ["ST", "ST"], ["STR", "ST"],
  ["ROAD", "RD"], ["RD", "RD"],
  ["AVENUE", "AVE"], ["AVE", "AVE"],
  ["BOULEVARD", "BLVD"], ["BLVD", "BLVD"],
  ["DRIVE", "DR"], ["DR", "DR"],
  ["LANE", "LN"], ["LN", "LN"],
  ["COURT", "CT"], ["CT", "CT"],
  ["PLACE", "PL"], ["PL", "PL"],
  ["TERRACE", "TER"], ["TER", "TER"],
  ["CIRCLE", "CIR"], ["CIR", "CIR"],
  ["HIGHWAY", "HWY"], ["HWY", "HWY"],
  ["PARKWAY", "PKWY"], ["PKWY", "PKWY"],
  ["TURNPIKE", "TPKE"], ["TPKE", "TPKE"],
  ["NORTH", "N"], ["SOUTH", "S"], ["EAST", "E"], ["WEST", "W"],
]);

function streetNorm(s) {
  const raw = String(s ?? "").trim().toUpperCase();
  if (!raw) return "";
  const cleaned = raw.replace(/[.,]/g, " ").replace(/\s+/g, " ").trim();
  const tokens = cleaned.split(" ").map(tok => STREET_MAP.get(tok) ?? tok);
  return tokens.join(" ");
}

function inMABounds(lat, lon) {
  if (typeof lat !== "number" || typeof lon !== "number") return false;
  return lat >= 41.0 && lat <= 43.7 && lon >= -73.8 && lon <= -69.0;
}

function distM(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const toRad = (x) => (x * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function confidenceBucket(d, guards) {
  let score = 0;
  if (d <= 30) score = 90;
  else if (d <= 60) score = 75;
  else if (d <= 120) score = 55;
  else if (d <= 180) score = 40;
  else score = 0;

  if (guards.zipMatch) score += 10;
  if (guards.streetMatch) score += 10;
  if (guards.townMatch) score += 5;

  let bucket = "REJECT";
  if (score >= 90) bucket = "HIGH";
  else if (score >= 75) bucket = "MED_HIGH";
  else if (score >= 55) bucket = "MED";
  else if (score >= 40) bucket = "LOW";

  return { score, bucket };
}

function distanceBand(d) {
  if (d <= 30) return "A_0_30m";
  if (d <= 60) return "B_30_60m";
  if (d <= 120) return "C_60_120m";
  if (d <= 180) return "D_120_180m";
  return "E_OVER_180m";
}

function pickField(obj, keys) {
  for (const k of keys) {
    if (obj && obj[k] != null && String(obj[k]).trim() !== "") return obj[k];
  }
  return null;
}

function parseMadPoint(p) {
  const lat = Number(p.lat ?? p.latitude ?? p.y ?? p.Y ?? p.LAT ?? p.Lat);
  const lon = Number(p.lng ?? p.lon ?? p.longitude ?? p.x ?? p.X ?? p.LON ?? p.Lon);
  const street_no = pickField(p, ["street_no", "ADDR_NUM", "ADDRNUM", "HOUSE_NO", "STNUM", "NUMBER", "NUM"]);
  const street_name = pickField(p, ["street_name", "STREET_NAME", "STREETNAME", "ST_NAME", "FULL_ST_NAM", "STREET", "ROADNAME"]);
  const town = pickField(p, ["town", "MUNICIPALITY", "CITY", "TOWN", "COMMUNITY", "MUNI"]);
  const zip = pickField(p, ["zip", "ZIP", "ZIPCODE", "POSTCODE", "POST_CODE"]);

  return {
    lat,
    lon,
    street_no: street_no == null ? "" : String(street_no).trim(),
    street_name: street_name == null ? "" : String(street_name).trim(),
    town: town == null ? "" : String(town).trim(),
    zip: zip == null ? "" : String(zip).trim(),
  };
}

// ------------------------------------------------------------
// Build tile map + auto-detect order
// ------------------------------------------------------------
const tileFiles = fs.readdirSync(tilesDir).filter(f => /^tile_-?\d+_-?\d+\.ndjson$/i.test(f));
if (tileFiles.length === 0) {
  console.error("No tile_*.ndjson files found in tilesDir:", tilesDir);
  process.exit(1);
}

let order = "LON_LAT";
{
  let lonLatVotes = 0;
  let latLonVotes = 0;
  const sampleN = Math.min(200, tileFiles.length);
  for (let i = 0; i < sampleN; i++) {
    const f = tileFiles[i];
    const m = f.match(/^tile_(-?\d+)_(-?\d+)\.ndjson$/i);
    if (!m) continue;
    const a = Number(m[1]);
    const b = Number(m[2]);
    const aLooksLon = a < 0 && Math.abs(a) > 6000;
    const bLooksLon = b < 0 && Math.abs(b) > 6000;
    const aLooksLat = a > 0 && a < 6000;
    const bLooksLat = b > 0 && b < 6000;
    if (aLooksLon && bLooksLat) lonLatVotes++;
    if (aLooksLat && bLooksLon) latLonVotes++;
  }
  order = lonLatVotes >= latLonVotes ? "LON_LAT" : "LAT_LON";
}

const tileMap = new Map(); // "a,b" -> full path
for (const f of tileFiles) {
  const m = f.match(/^tile_(-?\d+)_(-?\d+)\.ndjson$/i);
  if (!m) continue;
  tileMap.set(`${m[1]},${m[2]}`, path.join(tilesDir, f));
}

function idxFor(lat, lon) {
  const latIdx = Math.floor(lat / TILE_SIZE_DEG);
  const lonIdx = Math.floor(lon / TILE_SIZE_DEG);
  if (order === "LON_LAT") return { latIdx, lonIdx, a: lonIdx, b: latIdx };
  return { latIdx, lonIdx, a: latIdx, b: lonIdx };
}

function neighborTilePaths(lat, lon) {
  const { latIdx, lonIdx } = idxFor(lat, lon);
  const paths = [];
  for (const dLat of [-1, 0, 1]) {
    for (const dLon of [-1, 0, 1]) {
      const nLat = latIdx + dLat;
      const nLon = lonIdx + dLon;
      const a = order === "LON_LAT" ? nLon : nLat;
      const b = order === "LON_LAT" ? nLat : nLon;
      const p = tileMap.get(`${a},${b}`);
      if (p) paths.push(p);
    }
  }
  return paths;
}

// ------------------------------------------------------------
// Tile cache
// ------------------------------------------------------------
const tileCache = new Map(); // path -> points[]
async function loadTilePoints(tilePath) {
  if (tileCache.has(tilePath)) {
    const v = tileCache.get(tilePath);
    tileCache.delete(tilePath);
    tileCache.set(tilePath, v);
    return v;
  }

  const pts = [];
  const rl = readline.createInterface({
    input: fs.createReadStream(tilePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    let obj;
    try { obj = JSON.parse(t); } catch { continue; }
    const p = parseMadPoint(obj);
    if (!Number.isFinite(p.lat) || !Number.isFinite(p.lon)) continue;
    pts.push(p);
  }

  tileCache.set(tilePath, pts);
  if (tileCache.size > tileCacheN) {
    const firstKey = tileCache.keys().next().value;
    tileCache.delete(firstKey);
  }
  return pts;
}

// ------------------------------------------------------------
// Report + output streams
// ------------------------------------------------------------
const report = {
  version: VERSION,
  timestamp: NOW,
  in: inPath,
  tilesDir,
  out: outPath,
  report: reportPath,
  params: { maxDistM, farZipGuardM, farStreetGuardM, tileCacheN, tileSizeDeg: TILE_SIZE_DEG, tileOrder: order },
  counts: {
    total: 0,
    skipped_non_target: 0,
    targets_tierC: 0,
    auto_accept: 0,
    quarantine: 0,
    unresolved: 0,
    parseErr: 0,
    noCoords: 0,
    outOfBounds: 0,
    noTileCandidates: 0,
    noCandidateWithinDist: 0,
    townMismatch_quarantine: 0,
    zipMismatch_reject: 0,
    streetMismatch_reject: 0,
    missingBaseZip_quarantine: 0,
    missingBaseStreet_quarantine: 0,
  },
  distance_band_counts: {},
  confidence_bucket_counts: {},
};

const out = fs.createWriteStream(outPath, { encoding: "utf8" });
const outAuto = outAutoPath ? fs.createWriteStream(outAutoPath, { encoding: "utf8" }) : null;
const outQ = outQuarantinePath ? fs.createWriteStream(outQuarantinePath, { encoding: "utf8" }) : null;
const outU = outUnresolvedPath ? fs.createWriteStream(outUnresolvedPath, { encoding: "utf8" }) : null;

const rlIn = readline.createInterface({
  input: fs.createReadStream(inPath, { encoding: "utf8" }),
  crlfDelay: Infinity,
});

for await (const line of rlIn) {
  const t = line.trim();
  if (!t) continue;

  let row;
  try { row = JSON.parse(t); }
  catch {
    report.counts.parseErr++;
    continue;
  }

  report.counts.total++;

  const tier = row.address_tier ?? "UNKNOWN";
  if (tier !== "C") {
    report.counts.skipped_non_target++;
    out.write(JSON.stringify(row) + "\n");
    continue;
  }

  report.counts.targets_tierC++;

  const lat = Number(row.lat ?? row.latitude);
  const lon = Number(row.lng ?? row.lon ?? row.longitude);

  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    report.counts.noCoords++;
    row.mad_nearest = { version: VERSION, outcome: "UNRESOLVED", reason: "NO_COORDS", at: NOW };
    const s = JSON.stringify(row) + "\n";
    out.write(s);
    if (outU) outU.write(s);
    report.counts.unresolved++;
    continue;
  }

  if (!inMABounds(lat, lon)) {
    report.counts.outOfBounds++;
    row.mad_nearest = { version: VERSION, outcome: "UNRESOLVED", reason: "OUT_OF_BOUNDS", at: NOW };
    const s = JSON.stringify(row) + "\n";
    out.write(s);
    if (outU) outU.write(s);
    report.counts.unresolved++;
    continue;
  }

  const tilePaths = neighborTilePaths(lat, lon);
  if (!tilePaths.length) {
    report.counts.noTileCandidates++;
    row.mad_nearest = { version: VERSION, outcome: "UNRESOLVED", reason: "NO_TILE_CANDIDATES", at: NOW };
    const s = JSON.stringify(row) + "\n";
    out.write(s);
    if (outU) outU.write(s);
    report.counts.unresolved++;
    continue;
  }

  let best = null;
  let bestDist = Infinity;

  for (const tp of tilePaths) {
    const pts = await loadTilePoints(tp);
    for (const p of pts) {
      const d = distM(lat, lon, p.lat, p.lon);
      if (d < bestDist) {
        bestDist = d;
        best = p;
      }
    }
  }

  if (!best || !Number.isFinite(bestDist) || bestDist > maxDistM) {
    report.counts.noCandidateWithinDist++;
    row.mad_nearest = { version: VERSION, outcome: "UNRESOLVED", reason: "NO_CANDIDATE_WITHIN_DIST", at: NOW, maxDistM };
    const s = JSON.stringify(row) + "\n";
    out.write(s);
    if (outU) outU.write(s);
    report.counts.unresolved++;
    continue;
  }

  const baseZip = zip5(row.zip);
  const candZip = zip5(best.zip);

  const baseStreet = streetNorm(row.street_name);
  const candStreet = streetNorm(best.street_name);

  const baseTown = townNorm(row.town);
  const candTown = townNorm(best.town);

  const guards = {
    zipMatch: !!baseZip && !!candZip && baseZip === candZip,
    streetMatch: !!baseStreet && !!candStreet && baseStreet === candStreet,
    townMatch: !!baseTown && !!candTown && baseTown === candTown,
  };

  inc(report.distance_band_counts, distanceBand(bestDist));

  let outcome = "QUARANTINE";
  let reason = "DEFAULT_QUARANTINE";

  // Town mismatch => quarantine always
  if (baseTown && candTown && baseTown !== candTown) {
    outcome = "QUARANTINE";
    reason = "TOWN_MISMATCH";
    report.counts.townMismatch_quarantine++;
  } else {
    // ZIP guard when far AND base zip exists
    if (bestDist > farZipGuardM) {
      if (baseZip) {
        if (!guards.zipMatch) {
          outcome = "UNRESOLVED";
          reason = "ZIP_MISMATCH_GUARD";
          report.counts.zipMismatch_reject++;
        }
      } else {
        outcome = "QUARANTINE";
        reason = "MISSING_BASE_ZIP_FAR";
        report.counts.missingBaseZip_quarantine++;
      }
    }

    // Street guard when far AND base street exists
    if (outcome !== "UNRESOLVED" && bestDist > farStreetGuardM) {
      if (baseStreet) {
        if (!guards.streetMatch) {
          outcome = "UNRESOLVED";
          reason = "STREET_MISMATCH_GUARD";
          report.counts.streetMismatch_reject++;
        }
      } else {
        outcome = "QUARANTINE";
        reason = "MISSING_BASE_STREET_FAR";
        report.counts.missingBaseStreet_quarantine++;
      }
    }

    // Confidence decision (only if not rejected)
    if (outcome !== "UNRESOLVED") {
      const { score, bucket } = confidenceBucket(bestDist, guards);
      inc(report.confidence_bucket_counts, bucket);

      const highEnough = (bucket === "HIGH" || bucket === "MED_HIGH");
      if (highEnough && bestDist <= maxDistM && bestDist <= 60) {
        outcome = "AUTO_ACCEPT";
        reason = "HIGH_CONF_WITHIN_POLICY";
      } else {
        outcome = "QUARANTINE";
        reason = highEnough ? "HIGH_CONF_BUT_POLICY_BLOCK" : "MED_OR_LOW_CONF";
      }

      row.mad_nearest = {
        version: VERSION,
        outcome,
        reason,
        at: NOW,
        distM: Number(bestDist.toFixed(2)),
        confidence_bucket: bucket,
        confidence_score: score,
        guards,
      };
    }
  }

  if (outcome === "AUTO_ACCEPT") {
    const prev = {
      street_no: row.street_no ?? null,
      street_name: row.street_name ?? null,
      town: row.town ?? null,
      zip: row.zip ?? null,
    };

    const sn = String(row.street_no ?? "").trim();
    const snZero = sn && /^0+$/.test(sn);
    const needNo = !sn || snZero;
    if (needNo && best.street_no) row.street_no = best.street_no;

    if (!row.street_name && best.street_name) row.street_name = best.street_name;
    if (!row.town && best.town) row.town = best.town;
    if (zip5(row.zip) === "" && candZip) row.zip = candZip;

    row.address_authority = {
      ...(row.address_authority ?? {}),
      mad_nearest: {
        version: VERSION,
        applied: true,
        at: NOW,
        distM: Number(bestDist.toFixed(2)),
        confidence_bucket: row.mad_nearest?.confidence_bucket,
        confidence_score: row.mad_nearest?.confidence_score,
      },
    };

    row.mad_candidate = {
      street_no: best.street_no || null,
      street_name: best.street_name || null,
      town: best.town || null,
      zip: candZip || null,
    };

    const changed =
      prev.street_no !== row.street_no ||
      prev.street_name !== row.street_name ||
      prev.town !== row.town ||
      prev.zip !== row.zip;
    if (changed) row.mad_prev = prev;

    const s = JSON.stringify(row) + "\n";
    out.write(s);
    if (outAuto) outAuto.write(s);
    report.counts.auto_accept++;
  } else if (outcome === "QUARANTINE") {
    row.mad_suggest = {
      street_no: best.street_no || null,
      street_name: best.street_name || null,
      town: best.town || null,
      zip: candZip || null,
      distM: Number(bestDist.toFixed(2)),
      note: "Suggestion only; not applied (quarantine)",
    };
    const s = JSON.stringify(row) + "\n";
    out.write(s);
    if (outQ) outQ.write(s);
    report.counts.quarantine++;
  } else {
    row.mad_nearest = {
      ...(row.mad_nearest ?? {}),
      version: VERSION,
      outcome: "UNRESOLVED",
      reason,
      at: NOW,
      distM: Number(bestDist.toFixed(2)),
      guards,
    };
    const s = JSON.stringify(row) + "\n";
    out.write(s);
    if (outU) outU.write(s);
    report.counts.unresolved++;
  }

  if (report.counts.total % 500000 === 0) {
    console.log(`...processed ${report.counts.total.toLocaleString()} rows | tierC=${report.counts.targets_tierC.toLocaleString()} | auto=${report.counts.auto_accept.toLocaleString()} | q=${report.counts.quarantine.toLocaleString()} | u=${report.counts.unresolved.toLocaleString()}`);
  }
}

out.end();
if (outAuto) outAuto.end();
if (outQ) outQ.end();
if (outU) outU.end();

report.percents = {
  tierC_target_rate: pct(report.counts.targets_tierC, report.counts.total),
  auto_accept_rate_of_total: pct(report.counts.auto_accept, report.counts.total),
  auto_accept_rate_of_tierC: pct(report.counts.auto_accept, report.counts.targets_tierC),
  quarantine_rate_of_tierC: pct(report.counts.quarantine, report.counts.targets_tierC),
  unresolved_rate_of_tierC: pct(report.counts.unresolved, report.counts.targets_tierC),
};

fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");

console.log("DONE.");
console.log(JSON.stringify({
  counts: report.counts,
  percents: report.percents,
  distance_band_counts: report.distance_band_counts,
  confidence_bucket_counts: report.confidence_bucket_counts,
  tileOrder: order
}, null, 2));
