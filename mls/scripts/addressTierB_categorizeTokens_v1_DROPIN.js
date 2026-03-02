/**
 * addressTierB_categorizeTokens_v1_DROPIN.js (ESM)
 * ------------------------------------------------------------
 * Categorize Tier B address "street_no" tokens (OFF, REAR, -SS, decimals, 0LOT, etc.)
 * WITHOUT guessing a numeric street number.
 *
 * Input is expected to already have address_tier computed (A/B/C) from addressTier_promoteBadge_*.
 *
 * Writes:
 *   --out    : full NDJSON with Tier B rows tagged with addr_token_* fields
 *   --report : JSON report with counts
 *
 * Optional:
 *   --outTierB : write Tier B-only NDJSON (tagged) for inspection
 *
 * Usage (PowerShell):
 *   cd C:\seller-app\backend
 *   node .\mls\scripts\addressTierB_categorizeTokens_v1_DROPIN.js `
 *     --in  "C:\seller-app\backend\publicData\properties\v32_addressTierBadged.ndjson" `
 *     --out "C:\seller-app\backend\publicData\properties\v33_tierB_categorized.ndjson" `
 *     --report "C:\seller-app\backend\publicData\properties\v33_tierB_categorized_report.json" `
 *     --outTierB "C:\seller-app\backend\publicData\properties\v33_tierB_only_tagged.ndjson"
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
const outPath = args.out;
const reportPath = args.report;
const outTierBPath = args.outTierB || null;

if (!inPath || !outPath || !reportPath) {
  console.error("Missing required args: --in --out --report");
  process.exit(1);
}
if (!fs.existsSync(inPath)) {
  console.error("Input not found:", inPath);
  process.exit(1);
}

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.mkdirSync(path.dirname(reportPath), { recursive: true });
if (outTierBPath) fs.mkdirSync(path.dirname(outTierBPath), { recursive: true });

const NOW = new Date().toISOString();
const VERSION = "tierB_tokens_v1";

function inc(obj, k, n = 1) { obj[k] = (obj[k] ?? 0) + n; }
function topN(mapObj, n = 25) {
  const arr = Object.entries(mapObj).sort((a, b) => b[1] - a[1]);
  return arr.slice(0, n).map(([value, count]) => ({ value, count }));
}

function categorizeStreetNoToken(raw) {
  const raw0 = raw == null ? "" : String(raw);
  const norm = raw0.trim().toUpperCase();

  if (!norm) return { cls: "EMPTY", sub: "EMPTY", norm, notes: "Blank token" };

  if (norm === "OFF" || norm === "-OFF") {
    return { cls: "NON_SITE_DESCRIPTOR", sub: "OFF", norm, notes: "Assessor/non-site descriptor token" };
  }
  if (norm === "REAR") {
    return { cls: "REAR_DESCRIPTOR", sub: "REAR", norm, notes: "Rear descriptor (not a numeric street number)" };
  }

  if (/^\-(SS|NS|WS|ES)$/.test(norm)) {
    return { cls: "DIRECTIONAL_SUFFIX", sub: norm.slice(1), norm, notes: "Directional side token (not a street number)" };
  }

  if (norm === "LOT" || norm === "TRACT" || norm === "PARCEL") {
    return { cls: "LOT_REFERENCE", sub: norm, norm, notes: "Lot/tract reference token" };
  }
  if (/^0LOT\b/.test(norm) || /\bLOT\b/.test(norm) || /\bTRACT\b/.test(norm) || /\bPARCEL\b/.test(norm)) {
    return { cls: "LOT_REFERENCE", sub: "CONTAINS_LOT", norm, notes: "Contains LOT/TRACT/PARCEL token" };
  }

  if (/^\d+\.\d+$/.test(norm)) {
    return { cls: "ASSESSOR_DECIMAL_SUFFIX", sub: "DECIMAL", norm, notes: "Decimal-like suffix often used by assessors" };
  }

  if (norm.includes("#")) {
    return { cls: "UNIT_OR_INTERNAL", sub: "HASH", norm, notes: "Contains # (unit/internal token)" };
  }
  if (norm.includes("/")) {
    return { cls: "FRACTION_OR_COMPOSITE", sub: "SLASH", norm, notes: "Contains / (fraction/composite token)" };
  }
  if (norm.includes("&")) {
    return { cls: "MULTI_OR_COMPOSITE", sub: "AMPERSAND", norm, notes: "Contains & (multi/composite token)" };
  }

  if (/^\d+\-\d+$/.test(norm)) {
    return { cls: "RANGE", sub: "DASH_RANGE", norm, notes: "Numeric range token (not accepted in strict A)" };
  }

  if (/^\d+[A-Z]{2,}$/.test(norm)) {
    return { cls: "ALPHANUMERIC", sub: "LONG_SUFFIX", norm, notes: "Alphanumeric long suffix (assessor style)" };
  }

  if (!/^\d/.test(norm)) {
    return { cls: "NON_NUMERIC_TOKEN", sub: "NON_NUMERIC_START", norm, notes: "Does not start with a digit" };
  }

  if (/[A-Z]/.test(norm)) {
    return { cls: "ALPHANUMERIC", sub: "HAS_LETTERS", norm, notes: "Contains letters; not strict-accepted" };
  }

  return { cls: "UNKNOWN_TOKEN", sub: "UNKNOWN", norm, notes: "Unclassified token pattern" };
}

const report = {
  in: inPath,
  out: outPath,
  report: reportPath,
  outTierB: outTierBPath,
  version: VERSION,
  timestamp: NOW,
  counts: { total: 0, tierA: 0, tierB: 0, tierC: 0, tierB_tagged: 0 },
  tierB_classes: {},
  tierB_subclasses: {},
  tierB_raw_tokens: {},
  tierB_top_raw_tokens: [],
};

const out = fs.createWriteStream(outPath, { encoding: "utf8" });
const outTierB = outTierBPath ? fs.createWriteStream(outTierBPath, { encoding: "utf8" }) : null;

const rl = readline.createInterface({
  input: fs.createReadStream(inPath, { encoding: "utf8" }),
  crlfDelay: Infinity,
});

for await (const line of rl) {
  const t = line.trim();
  if (!t) continue;

  let row;
  try { row = JSON.parse(t); } catch { continue; }

  report.counts.total++;

  const tier = row.address_tier ?? "UNKNOWN";
  if (tier === "A") report.counts.tierA++;
  else if (tier === "B") report.counts.tierB++;
  else if (tier === "C") report.counts.tierC++;

  if (tier === "B") {
    const rawKey = (row.street_no == null ? "" : String(row.street_no)).trim().toUpperCase();
    if (rawKey) inc(report.tierB_raw_tokens, rawKey);

    const { cls, sub, norm, notes } = categorizeStreetNoToken(row.street_no);

    row.addr_token_raw = row.street_no == null ? null : String(row.street_no);
    row.addr_token_norm = norm;
    row.addr_token_class = cls;
    row.addr_token_subclass = sub;
    row.addr_token_notes = notes;
    row.addr_token_version = VERSION;
    row.addr_token_tagged_at = NOW;

    report.counts.tierB_tagged++;
    inc(report.tierB_classes, cls);
    inc(report.tierB_subclasses, `${cls}|${sub}`);
  }

  const s = JSON.stringify(row) + "\n";
  out.write(s);
  if (tier === "B" && outTierB) outTierB.write(s);

  if (report.counts.total % 500000 === 0) {
    console.log(`...processed ${report.counts.total.toLocaleString()} rows`);
  }
}

out.end();
if (outTierB) outTierB.end();

report.tierB_top_raw_tokens = topN(report.tierB_raw_tokens, 25);

fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");

console.log("DONE.");
console.log(JSON.stringify({
  counts: report.counts,
  tierB_classes: report.tierB_classes,
  topTierBRawTokens: report.tierB_top_raw_tokens.slice(0, 10),
}, null, 2));
