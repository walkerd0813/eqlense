// backend/mls/scripts/inspectListingSchema.js
// ============================================================
// STREAM INSPECT NDJSON SCHEMA (keys + coverage) without OOM
// Usage:
//   node mls/scripts/inspectListingSchema.js --input mls/normalized/listings.ndjson --max 50000
//   node mls/scripts/inspectListingSchema.js --input mls/normalized/listingsWithCoords_PASS2.ndjson --max 200000
// ============================================================

import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

function parseArgs(argv) {
  const out = { input: null, max: 50000, every: 1, includeRawRow: false };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--input") out.input = argv[++i];
    else if (a === "--max") out.max = Number(argv[++i] || 0) || out.max;
    else if (a === "--every") out.every = Number(argv[++i] || 0) || out.every; // sample every Nth row
    else if (a === "--includeRawRow") out.includeRawRow = true;
  }
  if (!out.input) {
    console.error("❌ Missing --input <path-to-ndjson>");
    process.exit(1);
  }
  return out;
}

function fileExists(p) {
  return fs.existsSync(p);
}

function getByPath(obj, dotPath) {
  const parts = dotPath.split(".");
  let cur = obj;
  for (const p of parts) {
    if (cur == null) return undefined;
    cur = cur[p];
  }
  return cur;
}

function isPresent(v) {
  if (v === null || v === undefined) return false;
  if (typeof v === "string") return v.trim().length > 0;
  return true;
}

function valType(v) {
  if (v === null) return "null";
  if (Array.isArray(v)) return "array";
  return typeof v;
}

function flattenKeys(obj, prefix = "", depth = 0, depthLimit = 4, exclude = () => false, out = []) {
  if (obj == null) return out;
  if (depth > depthLimit) return out;

  const t = valType(obj);
  if (t !== "object" && t !== "array") {
    out.push(prefix || "(root)");
    return out;
  }

  if (t === "array") {
    out.push(prefix || "(root_array)");
    // don’t walk huge arrays; just note it exists
    return out;
  }

  for (const k of Object.keys(obj)) {
    const p = prefix ? `${prefix}.${k}` : k;
    if (exclude(p)) continue;
    const v = obj[k];
    const vt = valType(v);
    if (vt === "object") flattenKeys(v, p, depth + 1, depthLimit, exclude, out);
    else out.push(p);
  }

  return out;
}

function nowStamp() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

// ---- REQUIRED FIELDS FOR DOWNSTREAM (Market Radar / Competitor / Zoning attach) ----
// Add/remove as you decide. This is the “contract” we’ll enforce.
const REQUIRED = [
  "listingId",
  "source",
  "schemaVersion",
  "propertyType",
  "status",
  "dates.listDate",

  "address.streetNumber",
  "address.streetName",
  "address.city",
  "address.state",
  "address.zip",

  "pricing.listPrice",
  "physical.sqft",

  "brokerage.listOffice",
  "brokerage.listAgent",

  // these should exist by the time we feed zoning/heatmaps
  "latitude",
  "longitude",
];

async function main() {
  const args = parseArgs(process.argv);
  const inputPath = path.resolve(process.cwd(), args.input);

  if (!fileExists(inputPath)) {
    console.error(`❌ Input not found: ${inputPath}`);
    process.exit(1);
  }

  console.log("====================================================");
  console.log(" LISTING SCHEMA INSPECTOR (STREAMING)");
  console.log("====================================================");
  console.log("Input:", inputPath);
  console.log("Max rows:", args.max.toLocaleString());
  console.log("Sample every:", args.every);
  console.log("Include raw.row keys:", args.includeRawRow);
  console.log("----------------------------------------------------");

  const topLevel = new Set();
  const keyCounts = new Map(); // path -> count
  const typeCounts = new Map(); // path -> {type: count}
  const requiredCounts = new Map(REQUIRED.map((k) => [k, 0]));

  let scanned = 0;
  let parsed = 0;
  let bad = 0;

  const rl = readline.createInterface({
    input: fs.createReadStream(inputPath),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if (scanned >= args.max) break;
    scanned++;

    if (args.every > 1 && (scanned % args.every) !== 0) continue;

    const t = line.trim();
    if (!t) continue;

    let obj;
    try {
      obj = JSON.parse(t);
    } catch {
      bad++;
      continue;
    }
    parsed++;

    // top-level keys
    for (const k of Object.keys(obj)) topLevel.add(k);

    // required coverage
    for (const req of REQUIRED) {
      const v = getByPath(obj, req);
      if (isPresent(v)) requiredCounts.set(req, (requiredCounts.get(req) || 0) + 1);
    }

    // collect normalized keys; exclude raw.row unless requested
    const exclude = (p) => {
      if (!args.includeRawRow && (p === "raw" || p.startsWith("raw.") || p.startsWith("raw.row"))) return true;
      // also skip huge text blobs for “key listing”
      if (p.endsWith("raw.row.REMARKS") || p.endsWith("raw.row.FIRM_RMK1")) return true;
      return false;
    };

    const leafKeys = flattenKeys(obj, "", 0, 4, exclude, []);
    for (const k of leafKeys) {
      keyCounts.set(k, (keyCounts.get(k) || 0) + 1);

      const v = getByPath(obj, k);
      const vt = valType(v);
      const tc = typeCounts.get(k) || {};
      tc[vt] = (tc[vt] || 0) + 1;
      typeCounts.set(k, tc);
    }

    if (parsed % 25000 === 0) {
      console.log(`[inspect] parsed=${parsed.toLocaleString()} bad=${bad.toLocaleString()}`);
    }
  }

  const total = parsed || 1;

  // Build a clean report
  const report = {
    input: inputPath,
    scannedLines: scanned,
    parsedRows: parsed,
    badJsonLines: bad,
    sampleEvery: args.every,
    topLevelKeys: Array.from(topLevel).sort(),

    requiredCoverage: REQUIRED.map((k) => ({
      key: k,
      present: requiredCounts.get(k) || 0,
      pct: Math.round(((requiredCounts.get(k) || 0) / total) * 10000) / 100,
    })),

    // most-common keys first
    keysByCoverage: Array.from(keyCounts.entries())
      .map(([k, c]) => ({
        key: k,
        present: c,
        pct: Math.round((c / total) * 10000) / 100,
        types: typeCounts.get(k) || {},
      }))
      .sort((a, b) => b.present - a.present),

    notes: {
      requiredList: "Edit REQUIRED[] in this script to match your canonical contract for MarketRadar/Competitor/Zoning.",
      rawRowIncluded: args.includeRawRow,
      depthLimit: 4,
    },
  };

  const base = path.basename(inputPath).replace(/\W+/g, "_");
  const outPath = path.resolve(process.cwd(), "mls/normalized", `schemaReport_${base}_${nowStamp()}.json`);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

  console.log("====================================================");
  console.log("✅ Schema report written:");
  console.log(outPath);
  console.log("----------------------------------------------------");
  console.log("Required coverage (key -> % present):");
  for (const r of report.requiredCoverage) {
    console.log(`  ${r.key} -> ${r.pct}%`);
  }
  console.log("====================================================");
}

main().catch((e) => {
  console.error("❌ inspect failed:", e?.stack || String(e));
  process.exit(1);
});
