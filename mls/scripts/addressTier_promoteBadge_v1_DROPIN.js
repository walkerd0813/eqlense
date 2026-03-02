/**
 * addressTier_promoteBadge_v1_DROPIN.js (ESM)
 * ------------------------------------------------------------
 * Simple "promotion badge" pass:
 *   - Computes strict mail-like validity vs "populated" validity
 *   - Assigns address_tier = A / B / C accordingly
 *
 * Tier rules (institutional-safe):
 *   A = strict_mail_like (deliverable-grade)
 *   B = populated_triplet (has non-zero street_no + street_name + ZIP 5/9), but NOT strict
 *   C = everything else
 *
 * Writes:
 *   --out    : full NDJSON with updated tier fields
 *   --report : JSON summary (counts + percents + overlap masks)
 *
 * Usage (PowerShell):
 *   cd C:\seller-app\backend
 *   node .\mls\scripts\addressTier_promoteBadge_v1_DROPIN.js `
 *     --in  "C:\seller-app\backend\publicData\properties\v29r_revalidated_120m_v3.ndjson" `
 *     --out "C:\seller-app\backend\publicData\properties\v30_addressTierBadged.ndjson" `
 *     --report "C:\seller-app\backend\publicData\properties\v30_addressTierBadged_report.json"
 *
 * Optional:
 *   --outA "...\v30_tierA_only.ndjson"
 *   --outB "...\v30_tierB_only.ndjson"
 *   --outC "...\v30_tierC_only.ndjson"
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

const outAPath = args.outA || null;
const outBPath = args.outB || null;
const outCPath = args.outC || null;

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
if (outAPath) fs.mkdirSync(path.dirname(outAPath), { recursive: true });
if (outBPath) fs.mkdirSync(path.dirname(outBPath), { recursive: true });
if (outCPath) fs.mkdirSync(path.dirname(outCPath), { recursive: true });

const isMissing = (v) => v == null || String(v).trim() === "";
const isZip5 = (z) => /^\d{5}$/.test(String(z ?? "").trim());
const isZip5or9 = (z) => {
  const s = String(z ?? "").trim();
  return /^\d{5}$/.test(s) || /^\d{5}-\d{4}$/.test(s);
};

// Your strict rules (same as quickAddressStats)
const isValidStreetNoStrict = (v) => {
  const s = String(v ?? "").trim();
  if (!s) return false;
  if (/^0+$/.test(s)) return false;
  if (/^\d+$/.test(s)) return true;          // 12
  if (/^\d+[A-Za-z]$/.test(s)) return true;  // 12A
  if (/^\d+\s*1\/2$/.test(s)) return true;   // 12 1/2
  if (/^\d+\-\d+$/.test(s)) return true;     // 12-14
  return false;
};

const isPopulatedStreetNo = (v) => {
  const s = String(v ?? "").trim();
  if (!s) return false;
  if (/^0+$/.test(s)) return false; // treat 0/00/000 as NOT populated
  return true;
};

function pct(n, d) {
  return d ? Number((100 * n / d).toFixed(3)) : 0;
}

function inc(obj, k, n = 1) {
  obj[k] = (obj[k] ?? 0) + n;
}

const report = {
  in: inPath,
  out: outPath,
  report: reportPath,
  params: {
    tierA: "strict_mail_like",
    tierB: "populated_triplet_not_strict",
    tierC: "otherwise",
    version: "address_tier_strict_v1",
  },
  counts: {
    total: 0,
    tierA: 0,
    tierB: 0,
    tierC: 0,
    strict_mail_like: 0,
    populated_triplet: 0,
    overlap_masks: {},  // for strict-fail only: missNo/badNo/missName/missZip combinations
  },
  percents: {},
  timestamp: new Date().toISOString(),
};

const out = fs.createWriteStream(outPath, { encoding: "utf8" });
const outA = outAPath ? fs.createWriteStream(outAPath, { encoding: "utf8" }) : null;
const outB = outBPath ? fs.createWriteStream(outBPath, { encoding: "utf8" }) : null;
const outC = outCPath ? fs.createWriteStream(outCPath, { encoding: "utf8" }) : null;

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

  const street_no = row.street_no;
  const street_name = row.street_name;
  const zip = row.zip;

  const strictMailLike =
    isValidStreetNoStrict(street_no) &&
    !isMissing(street_name) &&
    isZip5(zip);

  const populatedTriplet =
    isPopulatedStreetNo(street_no) &&
    !isMissing(street_name) &&
    isZip5or9(zip);

  if (strictMailLike) report.counts.strict_mail_like++;
  if (populatedTriplet) report.counts.populated_triplet++;

  let tier, reason;
  if (strictMailLike) {
    tier = "A";
    reason = "A_strict_mail_like";
    report.counts.tierA++;
  } else if (populatedTriplet) {
    tier = "B";
    reason = "B_populated_not_strict";
    report.counts.tierB++;
  } else {
    tier = "C";
    reason = "C_missing_or_non_mailable";
    report.counts.tierC++;
  }

  // For strict-fail overlap masks (diagnostic; doesn't bloat rows)
  if (!strictMailLike) {
    const missNo = isMissing(street_no) || /^0+$/.test(String(street_no ?? "").trim());
    const badNo = !missNo && !isValidStreetNoStrict(street_no);
    const missName = isMissing(street_name);
    const missZip = !isZip5(zip);

    const parts = [];
    if (missNo) parts.push("missNo");
    if (badNo) parts.push("badNo");
    if (missName) parts.push("missName");
    if (missZip) parts.push("missZip");
    const mask = parts.length ? parts.join("|") : "other";
    inc(report.counts.overlap_masks, mask);
  }

  // Apply badge fields (minimal + audit-friendly)
  row.address_tier = tier;
  row.address_tier_reason = reason;
  row.address_tier_version = "strict_v1";
  row.address_tier_badged_at = report.timestamp;

  const s = JSON.stringify(row) + "\n";
  out.write(s);
  if (tier === "A" && outA) outA.write(s);
  if (tier === "B" && outB) outB.write(s);
  if (tier === "C" && outC) outC.write(s);

  if (report.counts.total % 500000 === 0) {
    console.log(`...badged ${report.counts.total.toLocaleString()} rows`);
  }
}

out.end();
if (outA) outA.end();
if (outB) outB.end();
if (outC) outC.end();

report.percents = {
  tierA: pct(report.counts.tierA, report.counts.total),
  tierB: pct(report.counts.tierB, report.counts.total),
  tierC: pct(report.counts.tierC, report.counts.total),
  strict_mail_like: pct(report.counts.strict_mail_like, report.counts.total),
  populated_triplet: pct(report.counts.populated_triplet, report.counts.total),
};

fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");

console.log("DONE.");
console.log(JSON.stringify({ counts: report.counts, percents: report.percents }, null, 2));
