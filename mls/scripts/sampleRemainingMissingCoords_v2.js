#!/usr/bin/env node
/**
 * sampleRemainingMissingCoords_v2.js
 *
 * Purpose:
 *   Scan a properties NDJSON and surface a *representative* set of examples that still lack lat/lng.
 *   Buckets the problematic address patterns so we can design the next normalization patch.
 *
 * Works with ESM (Equity Lens codebase is ESM).
 *
 * Usage:
 *   node --max-old-space-size=8192 .\mls\scripts\sampleRemainingMissingCoords_v2.js
 *
 * Options:
 *   --in <path>         Input NDJSON (default: publicData/properties/properties_statewide_geo_zip_district_v6_coords.ndjson)
 *   --max <n>           Max samples to print per bucket (default: 20)
 *   --seed <n>          Deterministic sampling seed (default: 42)
 *   --topTowns <n>      Top N towns to print (default: 20)
 *   --topSuffix <n>     Top N "last token" values to print (default: 40)
 *   --progress <n>      Print progress every N lines (default: 500000)
 */

import fs from "fs";
import readline from "readline";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1];
    if (v == null || v.startsWith("--")) out[k] = true;
    else {
      out[k] = v;
      i++;
    }
  }
  return out;
}

// Deterministic RNG (mulberry32)
function mulberry32(seed) {
  let a = seed >>> 0;
  return function rand() {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function toInt(v, dflt) {
  const n = Number(v);
  return Number.isFinite(n) ? n : dflt;
}

function up(s) {
  return String(s ?? "").toUpperCase().trim();
}

function hasCoords(r) {
  return Number.isFinite(r?.lat) && Number.isFinite(r?.lng);
}

/**
 * Bucket classifier for remaining "missing coords" rows.
 * Keep this conservative: one primary bucket per record.
 */
function classify(r) {
  const streetNoRaw = String(r.street_no ?? "").trim();
  const streetName = up(r.street_name);
  const fullAddress = up(r.full_address);

  const hasStreetName = streetName.length > 0;
  const hasStreetNo = streetNoRaw.length > 0 && streetNoRaw !== "null" && streetNoRaw !== "undefined";
  const streetNoIsZero = hasStreetNo && (/^0+$/.test(streetNoRaw) || streetNoRaw === "0");

  // If we literally have no street name, we can't build keys
  if (!hasStreetName) return "noStreetName";

  // Land/lot patterns (often non-mailable)
  if (/\bLOT\b/.test(streetName) || /^\s*LOT\b/.test(streetName) || /^\s*LT\d*/.test(streetName) || /^\s*LT\b/.test(streetName))
    return "lotLike";

  // "REAR / OFF / LINE" & similar
  if (/\b(REAR|OFF|LINE|E OF|W OF|N OF|S OF|BEHIND)\b/.test(streetName) || /\b(REAR|OFF|LINE)\b/.test(fullAddress))
    return "rearOffLine";

  // Contains commas or parenthetical locality (", HYANNIS", "(R) BLDG 5")
  if (/[(),]/.test(streetName) || /[(),]/.test(fullAddress)) return "commaOrParenTail";

  // Missing or embedded street number inside street_name like "59-61 GORDON TERR"
  if (!hasStreetNo) {
    if (/\d/.test(streetName)) return "noStreetNo_but_hasNumsInName";
    return "noStreetNo_streetOnly";
  }

  // StreetNo = 0 cases
  if (streetNoIsZero) return "streetNoZero";

  // Unit or condo codes appended to street name: "D-121", "PS-84", "#21/1", "301 P2 P10", "U:E209", etc.
  if (
    /\b(UNIT|APT|APARTMENT|STE|SUITE|BLDG|BLD|FL|FLOOR)\b/.test(streetName) ||
    /#\s*[\w\-\/]+/.test(streetName) ||
    /\b[PDSA]-\d+\b/.test(streetName) || // D-121, PS-84, A-404
    /\bPS-\d+\b/.test(streetName) ||
    /\bU:?[A-Z0-9\-]+\b/.test(streetName) ||
    /\b[A-Z]\d{1,4}\b/.test(streetName) || // E209, B108
    /\/\d+/.test(streetName) || // #21/1
    /\bP\d+\b/.test(streetName) // P2 P10
  ) {
    return "unitCodeTail";
  }

  // Multi-address / ranges: "33 &35", "1459 1461", "91 93 MAXWELL", "477 479 E THIRD"
  if (/\b(&|AND)\b/.test(streetName) || /\b\d+\s*-\s*\d+\b/.test(fullAddress) || /\b\d+\s+\d+\b/.test(streetName))
    return "multiAddressOrRange";

  // Abbreviation / suffix weirdness
  // (We want to see a lot of these to expand SUFFIX_EXPAND in the next patch)
  const lastTok = streetName.split(/\s+/).slice(-1)[0] || "";
  if (["AV", "BV", "CR", "CI", "WY", "TR", "TNPK", "HWY", "PK", "LNDG", "BL", "TER", "TERR", "PW", "RD", "ST", "AVE", "BLVD", "DR", "LN", "CT", "PL"].includes(lastTok))
    return "suffixOrAbbrev";

  return "other";
}

// Reservoir sampler per bucket
function reservoirPush(bucketState, item, k, rand) {
  bucketState.count++;
  const n = bucketState.count;
  if (bucketState.samples.length < k) {
    bucketState.samples.push(item);
    return;
  }
  // Replace with probability k/n
  const j = Math.floor(rand() * n);
  if (j < k) bucketState.samples[j] = item;
}

function safeJsonParse(line) {
  try {
    return JSON.parse(line);
  } catch {
    return null;
  }
}

function pickLastToken(streetNameUpper) {
  const s = streetNameUpper.trim();
  if (!s) return "";
  const parts = s.split(/\s+/).filter(Boolean);
  if (!parts.length) return "";
  // If the last token is like "ST" but previous token is "MAIN", return "MAIN ST" as a unit
  const last = parts[parts.length - 1];
  const prev = parts[parts.length - 2] ?? "";
  if (["ST", "RD", "AVE", "AV", "DR", "LN", "CT", "PL", "HWY", "BLVD", "PKWY", "PK", "TER", "TERR", "CI", "CR", "WY"].includes(last) && prev) {
    return `${prev} ${last}`;
  }
  return last;
}

async function main() {
  const args = parseArgs(process.argv);

  const IN =
    args.in ??
    "publicData/properties/properties_statewide_geo_zip_district_v6_coords.ndjson";
  const MAX = toInt(args.max, 20);
  const SEED = toInt(args.seed, 42);
  const TOP_TOWNS = toInt(args.topTowns, 20);
  const TOP_SUFFIX = toInt(args.topSuffix, 40);
  const PROGRESS_EVERY = toInt(args.progress, 500000);

  if (!fs.existsSync(IN)) {
    console.error(`❌ Input not found: ${IN}`);
    process.exit(1);
  }

  console.log("====================================================");
  console.log(" SAMPLE REMAINING MISSING COORDS — v2");
  console.log("====================================================");
  console.log("IN:", IN);
  console.log("maxSamplesPerBucket:", MAX, "seed:", SEED);
  console.log("----------------------------------------------------");

  const rand = mulberry32(SEED);

  const buckets = new Map();
  const bucketNames = [
    "noStreetName",
    "noStreetNo_streetOnly",
    "noStreetNo_but_hasNumsInName",
    "streetNoZero",
    "lotLike",
    "rearOffLine",
    "commaOrParenTail",
    "unitCodeTail",
    "multiAddressOrRange",
    "suffixOrAbbrev",
    "other",
  ];
  for (const b of bucketNames) buckets.set(b, { count: 0, samples: [] });

  const townCounts = new Map();
  const suffixCounts = new Map();

  let total = 0;
  let missing = 0;

  const rl = readline.createInterface({ input: fs.createReadStream(IN) });

  for await (const line of rl) {
    if (!line.trim()) continue;
    total++;
    const r = safeJsonParse(line);
    if (!r) continue;

    if (hasCoords(r)) {
      if (total % PROGRESS_EVERY === 0) {
        console.log(`[progress] scanned=${total.toLocaleString()} missing=${missing.toLocaleString()}`);
      }
      continue;
    }

    missing++;

    const town = up(r.town);
    if (town) townCounts.set(town, (townCounts.get(town) ?? 0) + 1);

    const streetNameUpper = up(r.street_name);
    if (streetNameUpper) {
      const tok = pickLastToken(streetNameUpper);
      if (tok) suffixCounts.set(tok, (suffixCounts.get(tok) ?? 0) + 1);
    }

    const b = classify(r);
    const st = buckets.get(b) ?? buckets.get("other");
    reservoirPush(st, r, MAX, rand);

    if (total % PROGRESS_EVERY === 0) {
      console.log(`[progress] scanned=${total.toLocaleString()} missing=${missing.toLocaleString()}`);
    }
  }

  // Summary
  console.log("====================================================");
  console.log("[done]");
  console.log({
    total,
    missing,
    pctMissing: total ? (missing / total * 100).toFixed(4) + "%" : null,
  });

  const bucketSummary = {};
  for (const [k, v] of buckets.entries()) bucketSummary[k] = v.count;
  console.log("bucketCounts:", bucketSummary);

  const topTowns = [...townCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, TOP_TOWNS);
  console.log("topMissingByTown:", topTowns);

  const topSuffix = [...suffixCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, TOP_SUFFIX);
  console.log("topStreetNameLastToken:", topSuffix);

  // Examples per bucket
  for (const name of bucketNames) {
    const b = buckets.get(name);
    if (!b) continue;
    console.log(`\n--- ${name} (count=${b.count.toLocaleString()}) ---`);
    for (const r of b.samples) {
      console.log({
        parcel_id: r.parcel_id,
        town: r.town,
        zip: r.zip,
        full_address: r.full_address,
        street_no: r.street_no,
        street_name: r.street_name,
      });
    }
  }

  console.log("\n✅ Paste the output buckets you care about (or the whole output) and we’ll design the next cleanup patch.");
}

main().catch((err) => {
  console.error("❌ sampleRemainingMissingCoords_v2 failed:", err);
  process.exit(1);
});
