/**
 * reportAddressTiersAndPercent_v2_DROPIN.js (ESM)
 *
 * Streams a NDJSON file and writes a summary JSON:
 *  - Total rows
 *  - Buckets: missZip, missName, missNo, badNo, noCoords
 *  - Mail-like % (strict + tolerant)
 *  - Street number kind breakdown
 *  - Authority breakdowns (distance/confidence/accept) if present
 *  - Tier/source breakdown if a tier-like field is detected
 *
 * Usage:
 *   node .\mls\scripts\reportAddressTiersAndPercent_v2_DROPIN.js `
 *     --in  "C:\path\file.ndjson" `
 *     --out "C:\path\report.json"
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

function inc(map, key) {
  map[key] = (map[key] ?? 0) + 1;
}

function normZip5(z) {
  const s = String(z ?? "").trim();
  const m9 = s.match(/^(\d{5})-\d{4}$/);
  if (m9) return m9[1];
  if (/^\d{5}$/.test(s)) return s;
  return null;
}

function isMissingName(v) {
  return v == null || String(v).trim() === "";
}

function streetNoKind(v) {
  // returns: MISSING | SIMPLE | ALNUM | FRACTION | RANGE | OTHER
  if (v == null) return "MISSING";
  const s0 = String(v).trim().toUpperCase();
  if (!s0) return "MISSING";
  if (/^0+$/.test(s0)) return "MISSING";

  const s = s0.replace(/\s+/g, "");
  if (/^\d+$/.test(s)) return "SIMPLE";
  if (/^\d+[A-Z]$/.test(s)) return "ALNUM";
  if (/^\d+1\/2$/.test(s)) return "FRACTION";
  if (/^\d+-\d+$/.test(s)) return "RANGE";
  return "OTHER";
}

function hasCoords(row) {
  const lon =
    row.lon ?? row.longitude ?? row.LON ?? row.LONGITUDE ?? row.x ?? row.X ?? row.lng ?? row.LNG;
  const lat =
    row.lat ?? row.latitude ?? row.LAT ?? row.LATITUDE ?? row.y ?? row.Y ?? row.lat_y ?? row.LAT_Y;
  if (lon == null || lat == null) return false;
  const lo = Number(lon), la = Number(lat);
  return Number.isFinite(lo) && Number.isFinite(la);
}

function discoverTierField(row) {
  const candidates = [
    "coords_tier", "coordsTier", "coord_tier", "coordTier", "geocode_tier", "geo_tier",
    "coord_src", "coords_src", "coordSource", "coordsSource", "geocode_src", "latlng_src", "xy_src",
    "tier", "Tier", "TIER"
  ];
  for (const k of candidates) {
    if (row[k] != null && String(row[k]).trim() !== "") return k;
  }
  return null;
}

const args = parseArgs(process.argv);
const inPath = args.in;
const outPath = args.out;

if (!inPath || !outPath) {
  console.error("Missing args. Required: --in --out");
  process.exit(1);
}
if (!fs.existsSync(inPath)) {
  console.error("Input not found:", inPath);
  process.exit(1);
}

ensureDir(path.dirname(outPath));

const rs = fs.createReadStream(inPath);
const splitter = new LineSplitter();
rs.pipe(splitter);

let total = 0;
let parseErr = 0;

let noCoords = 0;

let zipPresent = 0;
let missZip = 0;

let streetNamePresent = 0;
let missName = 0;

const noKindCounts = { MISSING: 0, SIMPLE: 0, ALNUM: 0, FRACTION: 0, RANGE: 0, OTHER: 0 };

let mailLikeStrict = 0;   // zip5 + street_name + (SIMPLE|ALNUM|FRACTION|RANGE)
let mailLikeTolerant = 0; // zip5 + street_name + not MISSING (includes OTHER)

const distanceBucketCounts = {};
const confidenceBucketCounts = {};
const acceptLevelCounts = {};
const authoritySrcCounts = {};

let tierField = null;
const tierCounts = {};

splitter.on("data", (line) => {
  total++;
  let row;
  try { row = JSON.parse(line); } catch { parseErr++; return; }

  if (!tierField) tierField = discoverTierField(row);

  if (!hasCoords(row)) noCoords++;

  const zip5 = normZip5(row.zip ?? row.ZIP ?? null);
  if (zip5) zipPresent++; else missZip++;

  const sname = row.street_name ?? row.streetName ?? row.STREET_NAME ?? null;
  if (!isMissingName(sname)) streetNamePresent++; else missName++;

  const sno = row.street_no ?? row.streetNo ?? row.STREET_NO ?? null;
  const kind = streetNoKind(sno);
  noKindCounts[kind]++;

  const strictNoOk = (kind === "SIMPLE" || kind === "ALNUM" || kind === "FRACTION" || kind === "RANGE");
  const tolerantNoOk = (kind !== "MISSING");

  if (zip5 && !isMissingName(sname) && strictNoOk) mailLikeStrict++;
  if (zip5 && !isMissingName(sname) && tolerantNoOk) mailLikeTolerant++;

  // authority breakdowns if present
  const distBucket =
    row.addr_authority_distance_bucket ??
    row.addrAuthorityDistanceBucket ??
    row.addr_authority_revalidate?.distance_bucket ??
    null;
  if (distBucket) inc(distanceBucketCounts, distBucket);

  const confBucket =
    row.addr_authority_confidence_bucket ??
    row.addrAuthorityConfidenceBucket ??
    row.addr_authority_revalidate?.confidence_bucket ??
    null;
  if (confBucket) inc(confidenceBucketCounts, confBucket);

  const acceptLevel =
    row.addr_authority_accept_level ??
    row.addrAuthorityAcceptLevel ??
    row.addr_authority_revalidate?.outcome ??
    null;
  if (acceptLevel) inc(acceptLevelCounts, acceptLevel);

  const src = row.addr_authority_src ?? row.addrAuthoritySrc ?? null;
  if (src) inc(authoritySrcCounts, src);

  if (tierField) {
    const v = String(row[tierField] ?? "").trim();
    if (v) inc(tierCounts, v);
  }
});

splitter.on("end", () => {
  const pct = (n) => total ? Math.round((n / total) * 10000) / 100 : 0;

  const missNo = noKindCounts.MISSING;
  const badNo = noKindCounts.OTHER;

  const report = {
    inPath,
    total,
    parseErr,
    buckets: {
      noCoords,
      missZip,
      missName,
      missNo,
      badNo,
    },
    streetNoKinds: noKindCounts,
    mailLike: {
      strict_count: mailLikeStrict,
      strict_percent: pct(mailLikeStrict),
      tolerant_count: mailLikeTolerant,
      tolerant_percent: pct(mailLikeTolerant),
      definition: {
        strict: "zip5 + street_name + street_no kind in {SIMPLE,ALNUM,FRACTION,RANGE}",
        tolerant: "zip5 + street_name + street_no not missing (includes OTHER)",
      },
    },
    authorityBreakdowns: {
      authoritySrcCounts,
      distanceBucketCounts,
      confidenceBucketCounts,
      acceptLevelCounts,
    },
    tier: {
      detectedTierField: tierField,
      tierCounts,
      note: tierField
        ? "Counts by detected tier/source field in your rows."
        : "No tier/source field detected. If you want coord tiers, ensure your dataset includes coords_tier/coord_src/etc.",
    },
    timestamp: new Date().toISOString(),
  };

  fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");
  console.log("DONE. Wrote:", outPath);
  console.log("mailLike.strict_percent:", report.mailLike.strict_percent);
  console.log("buckets:", report.buckets);
});

splitter.on("error", (e) => {
  console.error("Stream error:", e);
  process.exit(1);
});
