#!/usr/bin/env node
/**
 * remaining117k_report_v2.mjs
 * -----------------------------------------
 * Report "remaining / fail" counts CONSISTENT with your current pipeline:
 * - If `address_tier` exists, Tier A/B/C counts are taken directly from it (matches addressTier_promoteBadge output).
 * - Also reports a "presence triplet" metric (street_no + street_name + zip present) for sanity.
 * - Includes Tier-B token class summary (to guide repair work).
 *
 * Usage (PowerShell):
 *   node .\mls\scripts\remaining117k_report_v2.mjs --in "C:\\path\\file.ndjson" --out "C:\\path\\report.json"
 *
 * This script ALWAYS prints DONE and exits.
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
  fs.mkdirSync(path.dirname(fp), { recursive: true });
}

function s(v) {
  if (v === null || v === undefined) return "";
  return String(v);
}

function normZip(zip) {
  const z = s(zip).trim();
  if (!z) return "";
  const m = z.match(/^(\d{5})/);
  return m ? m[1] : z;
}

function hasValue(v) {
  return s(v).trim().length > 0;
}

function isStrictStreetNo(streetNo) {
  const v = s(streetNo).trim();
  if (!v) return false;
  // Conservative numeric-only (do NOT enforce no-leading-zeros to avoid drifting from your tier badge logic).
  if (!/^\d+$/.test(v)) return false;
  if (v === "0") return false;
  return true;
}

function isStrictZip(zip) {
  const z = normZip(zip);
  return /^\d{5}$/.test(z);
}

function classifyToken(rawIn) {
  const raw = s(rawIn).trim().toUpperCase();
  if (!raw) return "EMPTY";
  if (/^\d+\.\d+$/.test(raw)) return "ASSESSOR_DECIMAL_SUFFIX";
  if (/^\d+\s*1\/2$/.test(raw) || raw.includes("1/2")) return "FRACTION_OR_COMPOSITE";
  if (raw.includes("&") || raw.includes(",")) return "MULTI_OR_COMPOSITE";
  if (raw.includes("LOT")) return "LOT_REFERENCE";
  if (raw === "REAR" || raw.includes("REAR")) return "REAR_DESCRIPTOR";
  if (raw === "OFF" || raw.includes(" OFF") || raw.endsWith("-OFF") || raw.startsWith("OFF")) return "NON_SITE_DESCRIPTOR";
  if (raw.startsWith("-") && raw.length <= 5) return "DIRECTIONAL_SUFFIX"; // e.g., -SS, -NS, -ES, -WS
  if (raw.includes("#")) return "UNIT_OR_INTERNAL";
  if (/^\d+[A-Z]+$/.test(raw)) return "ALPHANUMERIC";
  if (!/^\d/.test(raw)) return "NON_NUMERIC_TOKEN";
  return "UNKNOWN_TOKEN";
}

async function main() {
  const args = parseArgs(process.argv);
  const inPath = args["in"];
  const outPath = args["out"];

  if (!inPath || !outPath) {
    console.error("Usage: node remaining117k_report_v2.mjs --in <input.ndjson> --out <report.json>");
    process.exit(1);
  }
  if (!fs.existsSync(inPath)) {
    console.error(`Input not found: ${inPath}`);
    process.exit(1);
  }
  ensureDirForFile(outPath);

  let total = 0;
  let parseErr = 0;

  // Tier counts (preferred)
  const tierCounts = { A: 0, B: 0, C: 0, missing_or_other: 0 };

  // Sanity presence metric
  let present_triplet = 0;

  // Recomputed strict (debug)
  let recomputed_strict_mail_like = 0;

  // Strict-fail flags (non-exclusive)
  const failCounts = { missNo: 0, badNo: 0, missName: 0, missZip: 0 };
  const overlap = Object.create(null);

  // Tier B token classes
  const tierBTokenClasses = Object.create(null);

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

    const tier = row.address_tier;
    if (tier === "A" || tier === "B" || tier === "C") tierCounts[tier]++;
    else tierCounts.missing_or_other++;

    const streetNo = row.street_no ?? row.streetNo ?? "";
    const streetName = row.street_name ?? row.streetName ?? "";
    const zipRaw = row.zip ?? "";

    const hasNo = hasValue(streetNo);
    const hasName = hasValue(streetName);
    const zip5 = normZip(zipRaw);
    const hasZipAny = hasValue(zip5);

    if (hasNo && hasName && hasZipAny) present_triplet++;

    const strictNo = isStrictStreetNo(streetNo);
    const strictZip = isStrictZip(zip5);
    const strict = strictNo && hasName && strictZip;
    if (strict) recomputed_strict_mail_like++;

    const flags = {
      missNo: !hasNo,
      badNo: hasNo && !strictNo,
      missName: !hasName,
      missZip: !strictZip
    };
    const anyFail = flags.missNo || flags.badNo || flags.missName || flags.missZip;
    if (anyFail) {
      if (flags.missNo) failCounts.missNo++;
      if (flags.badNo) failCounts.badNo++;
      if (flags.missName) failCounts.missName++;
      if (flags.missZip) failCounts.missZip++;

      const key = ["badNo","missNo","missName","missZip"].filter(k => flags[k]).join("|") || "(none)";
      overlap[key] = (overlap[key] ?? 0) + 1;
    }

    if (tier === "B") {
      const cls = classifyToken(streetNo);
      tierBTokenClasses[cls] = (tierBTokenClasses[cls] ?? 0) + 1;
    }

    if (total % 500000 === 0) {
      console.log(`...scanned ${total.toLocaleString()} rows`);
    }
  }

  const pct = (n) => total ? +(n * 100 / total).toFixed(3) : 0;

  const report = {
    in: inPath,
    out: outPath,
    created_at: new Date().toISOString(),
    total_rows: total,
    parseErr,

    tier_counts: tierCounts,
    tier_percents: {
      A: pct(tierCounts.A),
      B: pct(tierCounts.B),
      C: pct(tierCounts.C),
      missing_or_other: pct(tierCounts.missing_or_other)
    },

    present_triplet: { count: present_triplet, pct: pct(present_triplet) },
    recomputed_strict_mail_like: { count: recomputed_strict_mail_like, pct: pct(recomputed_strict_mail_like) },

    strict_fail_breakdown: {
      ...failCounts,
      overlap_mask_counts: overlap
    },

    tierB_token_classes: tierBTokenClasses
  };

  fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

  console.log("DONE.");
  console.log(JSON.stringify({
    total_rows: total,
    parseErr,
    tierA: `${tierCounts.A} (${pct(tierCounts.A)}%)`,
    tierB: `${tierCounts.B} (${pct(tierCounts.B)}%)`,
    tierC: `${tierCounts.C} (${pct(tierCounts.C)}%)`,
    present_triplet: `${present_triplet} (${pct(present_triplet)}%)`,
    recomputed_strict_mail_like: `${recomputed_strict_mail_like} (${pct(recomputed_strict_mail_like)}%)`,
    out: outPath
  }, null, 2));

  rl.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("FATAL:", err);
  process.exit(1);
});
