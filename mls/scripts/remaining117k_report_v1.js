#!/usr/bin/env node
/**
 * remaining117k_report_v1.js
 * -----------------------------------------
 * Streams an NDJSON properties file and produces a "remaining / fail" report:
 * - strict_mail_like (Tier A) count + %
 * - strict_fail count + %
 * - populated_triplet count + %
 * - fail breakdown (non-exclusive) + overlap masks
 * - fail by tier (A/B/C)
 * - badNo sub-buckets + top raw tokens
 *
 * Usage (PowerShell):
 *   node .\mls\scripts\remaining117k_report_v1.js --in "C:\\path\\file.ndjson" --out "C:\\path\\report.json"
 *
 * Notes:
 * - Designed for ESM projects (EquityLens backend uses ESM).
 * - Prints progress every 500k rows and always exits after DONE.
 */

import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
    args[k] = v;
  }
  return args;
}

function ensureDirForFile(fp) {
  const dir = path.dirname(fp);
  fs.mkdirSync(dir, { recursive: true });
}

function asStr(v) {
  if (v === null || v === undefined) return "";
  return String(v);
}

function normZip(zip) {
  const z = asStr(zip).trim();
  if (!z) return "";
  // keep first 5 digits if 9-digit exists
  const m = z.match(/^(\d{5})/);
  return m ? m[1] : z;
}

function hasValue(v) {
  const s = asStr(v).trim();
  return s.length > 0;
}

/**
 * Strict "mail-like" definition:
 * - street_no: digits only, not 0, no leading zeros
 * - street_name: present
 * - zip: 5 digits
 */
function isStrictStreetNo(streetNo) {
  const s = asStr(streetNo).trim();
  if (!s) return false;
  if (!/^\d+$/.test(s)) return false;
  if (s === "0") return false;
  if (s.length > 1 && s.startsWith("0")) return false;
  return true;
}

function isStrictZip(zip) {
  const z = normZip(zip);
  return /^\d{5}$/.test(z);
}

function classifyBadNo(rawIn) {
  const raw = asStr(rawIn).trim().toUpperCase();
  const out = {
    decimal_like: false,
    non_numeric_start: false,
    has_letters: false,
    has_space: false,
    maybe_fixable_by_compaction: false,
    alpha_suffix_long: false,
    has_dash: false,
    rear_like: false,
    has_hash: false,
    lot_like: false,
    has_slash: false,
    has_amp: false,
    unit_like: false
  };

  if (!raw) return out;

  out.decimal_like = /^\d+\.\d+$/.test(raw);
  out.non_numeric_start = !/^\d/.test(raw);
  out.has_letters = /[A-Z]/.test(raw);
  out.has_space = /\s/.test(raw);
  out.has_dash = raw.includes("-");
  out.has_hash = raw.includes("#");
  out.has_slash = raw.includes("/");
  out.has_amp = raw.includes("&");
  out.lot_like = raw.includes("LOT");
  out.rear_like = raw === "REAR" || raw.includes(" REAR") || raw.startsWith("REAR") || raw.includes("RR") || raw.includes("RREAR");
  out.unit_like = /\b(UNIT|APT|APARTMENT|STE|SUITE|ROOM|RM)\b/.test(raw);

  const compact = raw.replace(/\s+/g, "");
  out.maybe_fixable_by_compaction = compact !== raw && /^\d+[A-Z]?$/.test(compact);

  // e.g., 12AB, 120REAR (long alpha suffix)
  const m = raw.match(/^(\d+)([A-Z]{2,})$/);
  out.alpha_suffix_long = !!m;

  return out;
}

function makeMask(flags) {
  const order = ["badNo", "missNo", "missName", "missZip"];
  const parts = order.filter((k) => flags[k]);
  return parts.join("|") || "(none)";
}

async function main() {
  const args = parseArgs(process.argv);
  const inPath = args["in"];
  const outPath = args["out"];

  if (!inPath || !outPath) {
    console.error("Usage: node remaining117k_report_v1.js --in <input.ndjson> --out <report.json>");
    process.exit(1);
  }
  if (!fs.existsSync(inPath)) {
    console.error(`Input not found: ${inPath}`);
    process.exit(1);
  }

  ensureDirForFile(outPath);

  let total = 0;
  let parseErr = 0;

  let strict_mail_like = 0;
  let populated_triplet = 0;

  const failCounts = {
    missNo: 0,
    badNo: 0,
    missName: 0,
    missZip: 0
  };

  const overlap_mask_counts = Object.create(null);
  const tierFailCounts = { A: 0, B: 0, C: 0 };

  const badNoBuckets = {
    decimal_like: 0,
    non_numeric_start: 0,
    has_letters: 0,
    has_space: 0,
    maybe_fixable_by_compaction: 0,
    alpha_suffix_long: 0,
    has_dash: 0,
    rear_like: 0,
    has_hash: 0,
    lot_like: 0,
    has_slash: 0,
    has_amp: 0,
    unit_like: 0
  };

  const topBadNo = new Map();

  const rl = readline.createInterface({
    input: fs.createReadStream(inPath, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    if (!line) continue;
    total++;

    let row;
    try {
      row = JSON.parse(line);
    } catch {
      parseErr++;
      continue;
    }

    const streetNoRaw = row.street_no ?? row.streetNo ?? "";
    const streetNameRaw = row.street_name ?? row.streetName ?? "";
    const zipRaw = row.zip ?? "";

    const hasNo = hasValue(streetNoRaw);
    const hasName = hasValue(streetNameRaw);
    const zip5 = normZip(zipRaw);
    const hasZip = hasValue(zip5);

    const strictNo = isStrictStreetNo(streetNoRaw);
    const strictZip = isStrictZip(zip5);

    const strict = strictNo && hasName && strictZip;
    if (strict) strict_mail_like++;

    if (hasNo && hasName && hasZip) populated_triplet++;

    const flags = {
      missNo: !hasNo,
      badNo: hasNo && !strictNo,
      missName: !hasName,
      missZip: !strictZip
    };

    if (!strict) {
      if (flags.missNo) failCounts.missNo++;
      if (flags.badNo) failCounts.badNo++;
      if (flags.missName) failCounts.missName++;
      if (flags.missZip) failCounts.missZip++;

      const mask = makeMask(flags);
      overlap_mask_counts[mask] = (overlap_mask_counts[mask] ?? 0) + 1;

      const tier = row.address_tier ?? null;
      if (tier === "A" || tier === "B" || tier === "C") {
        tierFailCounts[tier]++;
      } else {
        const derivedTier = strict ? "A" : (hasNo && hasName && hasZip ? "B" : "C");
        tierFailCounts[derivedTier]++;
      }
    }

    if (hasNo && !strictNo) {
      const raw = asStr(streetNoRaw).trim().toUpperCase();
      topBadNo.set(raw, (topBadNo.get(raw) ?? 0) + 1);

      const b = classifyBadNo(raw);
      for (const k of Object.keys(badNoBuckets)) {
        if (b[k]) badNoBuckets[k]++;
      }
    }

    if (total % 500000 === 0) {
      console.log(`...scanned ${total.toLocaleString()} rows`);
    }
  }

  const strict_fail = total - strict_mail_like;

  const top10 = Array.from(topBadNo.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([value, count]) => ({ value, count }));

  const report = {
    in: inPath,
    out: outPath,
    created_at: new Date().toISOString(),
    total_rows: total,
    parseErr,
    strict_mail_like: {
      count: strict_mail_like,
      pct: total ? +(strict_mail_like * 100 / total).toFixed(3) : 0
    },
    strict_fail: {
      count: strict_fail,
      pct: total ? +(strict_fail * 100 / total).toFixed(3) : 0
    },
    populated_triplet: {
      count: populated_triplet,
      pct: total ? +(populated_triplet * 100 / total).toFixed(3) : 0
    },
    fail_breakdown_non_exclusive: {
      ...failCounts,
      overlap_mask_counts
    },
    fail_by_tier: tierFailCounts,
    badNo_sub_buckets: badNoBuckets,
    top10_badNo_raw_values: top10
  };

  fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

  console.log("DONE.");
  console.log(JSON.stringify({
    total_rows: total,
    parseErr,
    strict_mail_like: `${strict_mail_like} (${report.strict_mail_like.pct}%)`,
    strict_fail: `${strict_fail} (${report.strict_fail.pct}%)`,
    populated_triplet: `${populated_triplet} (${report.populated_triplet.pct}%)`,
    out: outPath
  }, null, 2));

  rl.close();
}

main().catch((err) => {
  console.error("FATAL:", err);
  process.exit(1);
});
