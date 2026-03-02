#!/usr/bin/env node
/**
 * Sample 10 examples per "issue bucket" from a properties NDJSON file.
 * Designed for very large files: streaming, stops early once all buckets are filled.
 *
 * Buckets (non-destructive, mutually-exclusive by priority):
 *  1) missingStreetName
 *  2) noStreetNo
 *  3) streetNoZero
 *  4) hasUnitCode
 *  5) rearOffLine
 *  6) lotLike
 *  7) hasCommaOrParen
 *  8) other
 *
 * By default we only sample rows that are STILL missing coords (lat/lng absent).
 *
 * Usage (PowerShell):
 *   node .\mls\scripts\sampleBucketsPerIssue_v1_DROPIN.js `
 *     --in C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v22_coords.ndjson `
 *     --limit 10
 *
 * Optional:
 *   --includeHasCoords true   (also sample rows that already have coords)
 *   --onlyBucket noStreetNo   (just one bucket)
 */

import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function getArg(name, fallback = null) {
  const argv = process.argv.slice(2);
  const key = `--${name}`;
  const idx = argv.findIndex((a) => a === key);
  if (idx === -1) return fallback;
  const v = argv[idx + 1];
  if (!v || v.startsWith("--")) return true;
  return v;
}

function toBool(v, def = false) {
  if (v === null || v === undefined) return def;
  if (typeof v === "boolean") return v;
  const s = String(v).trim().toLowerCase();
  if (["1", "true", "t", "yes", "y", "on"].includes(s)) return true;
  if (["0", "false", "f", "no", "n", "off"].includes(s)) return false;
  return def;
}

function resolveFromCwdOrAbs(p) {
  if (!p) return p;
  const raw = String(p).trim();
  if (path.isAbsolute(raw)) return raw;
  return path.resolve(process.cwd(), raw);
}

function hasCoords(rec) {
  // treat either lat/lng or lon/lat as "has coords"
  const lat = rec?.lat ?? rec?.latitude ?? null;
  const lng = rec?.lng ?? rec?.lon ?? rec?.longitude ?? null;
  return (
    typeof lat === "number" &&
    Number.isFinite(lat) &&
    typeof lng === "number" &&
    Number.isFinite(lng)
  );
}

function normStr(s) {
  return String(s ?? "")
    .replace(/\s+/g, " ")
    .trim();
}

function isStreetNoZero(streetNo) {
  if (streetNo === null || streetNo === undefined) return false;
  const s = String(streetNo).trim();
  return s === "0" || s === "00" || s === "000";
}

function detectUnitCode(text) {
  const t = normStr(text).toUpperCase();
  if (!t) return false;
  // common: "#", "UNIT", "APT", "SUITE", "PH", "U-###", "B###", "PS-##" etc
  if (t.includes("#")) return true;
  if (/\b(UNIT|APT|APARTMENT|SUITE|STE|PH|PENTHOUSE)\b/.test(t)) return true;
  if (/\b(U|B|D|C|A)-?\d{1,4}\b/.test(t)) return true; // U12, B003, D-12
  if (/\bPS-\d{1,4}\b/.test(t)) return true;
  if (/\bP\d{1,4}\b/.test(t)) return true; // parking like P14
  if (/\b\d{1,4}[A-Z]\b/.test(t)) return true; // 145A, 12B
  return false;
}

function detectRearOff(text) {
  const t = normStr(text).toUpperCase();
  if (!t) return false;
  return (
    /\bREAR\b/.test(t) ||
    /\bOFF\b/.test(t) ||
    /\b(S\/S|N\/S|E\/S|W\/S)\b/.test(t) || // side-of-street markers
    /\bRR\b/.test(t) // railroad shorthand
  );
}

function detectLotLike(text) {
  const t = normStr(text).toUpperCase();
  if (!t) return false;
  return (
    /\b(LOT|LT|LTS)\b/.test(t) ||
    /BEACH LOT/.test(t) ||
    /\b#LOT\b/.test(t)
  );
}

function detectCommaOrParen(text) {
  const s = String(text ?? "");
  return s.includes(",") || s.includes("(") || s.includes(")");
}

function pickBucket(rec) {
  const full = normStr(rec.full_address || "");
  const streetName = normStr(rec.street_name || "");
  const streetNo = rec.street_no;

  const combined = [full, streetName].filter(Boolean).join(" ");

  if (!streetName && !full) return "missingStreetName";
  if (!streetName && full) return "missingStreetName";

  if (streetNo === null || streetNo === undefined || String(streetNo).trim() === "")
    return "noStreetNo";

  if (isStreetNoZero(streetNo)) return "streetNoZero";

  if (detectUnitCode(combined)) return "hasUnitCode";
  if (detectRearOff(combined)) return "rearOffLine";
  if (detectLotLike(combined)) return "lotLike";
  if (detectCommaOrParen(combined)) return "hasCommaOrParen";

  return "other";
}

function slim(rec) {
  return {
    property_id: rec.property_id ?? null,
    parcel_id: rec.parcel_id ?? null,
    town: rec.town ?? null,
    zip: rec.zip ?? null,
    full_address: rec.full_address ?? null,
    street_no: rec.street_no ?? null,
    street_name: rec.street_name ?? null,
    unit: rec.unit ?? null,
  };
}

async function main() {
  const IN_DEFAULT =
    "../../publicData/properties/properties_statewide_geo_zip_district_v22_coords.ndjson";

  const IN = resolveFromCwdOrAbs(getArg("in", path.resolve(__dirname, IN_DEFAULT)));
  const LIMIT = parseInt(getArg("limit", "10"), 10) || 10;
  const includeHasCoords = toBool(getArg("includeHasCoords", "false"), false);
  const onlyBucket = getArg("onlyBucket", null);

  const BUCKETS = [
    "missingStreetName",
    "noStreetNo",
    "streetNoZero",
    "hasUnitCode",
    "rearOffLine",
    "lotLike",
    "hasCommaOrParen",
    "other",
  ];

  const targetBuckets = onlyBucket ? [String(onlyBucket)] : BUCKETS;

  const samples = {};
  for (const b of BUCKETS) samples[b] = [];

  if (!fs.existsSync(IN)) {
    console.error(`❌ Input not found: ${IN}`);
    process.exit(1);
  }

  console.log("====================================================");
  console.log(" SAMPLE 10 PER BUCKET (streaming)");
  console.log("====================================================");
  console.log(`IN    : ${IN}`);
  console.log(`LIMIT : ${LIMIT} per bucket`);
  console.log(`FILTER: ${includeHasCoords ? "include has coords" : "only missing coords"}`);
  if (onlyBucket) console.log(`ONLY  : ${onlyBucket}`);
  console.log("----------------------------------------------------");

  const rl = readline.createInterface({
    input: fs.createReadStream(IN, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let scanned = 0;
  let considered = 0;

  function doneAll() {
    for (const b of targetBuckets) {
      if (!samples[b] || samples[b].length < LIMIT) return false;
    }
    return true;
  }

  for await (const line of rl) {
    scanned++;
    if (!line || !line.trim()) continue;

    let rec;
    try {
      rec = JSON.parse(line);
    } catch {
      continue;
    }

    if (!includeHasCoords && hasCoords(rec)) continue;

    considered++;
    const bucket = pickBucket(rec);
    if (!samples[bucket]) continue;
    if (!targetBuckets.includes(bucket)) continue;

    if (samples[bucket].length < LIMIT) {
      samples[bucket].push(slim(rec));
    }

    if (scanned % 250000 === 0) {
      const status = targetBuckets
        .map((b) => `${b}:${samples[b]?.length ?? 0}/${LIMIT}`)
        .join("  ");
      console.log(`[progress] scanned ${scanned.toLocaleString()}  ${status}`);
    }

    if (doneAll()) break;
  }

  console.log("====================================================");
  console.log("[done]", {
    scanned,
    considered,
    buckets: Object.fromEntries(
      targetBuckets.map((b) => [b, samples[b]?.length ?? 0])
    ),
  });
  console.log("====================================================");

  for (const b of targetBuckets) {
    console.log(`\n--- ${b} (showing ${samples[b].length}/${LIMIT}) ---`);
    console.log(JSON.stringify(samples[b], null, 2));
  }
}

main().catch((e) => {
  console.error("❌ fatal:", e);
  process.exit(1);
});
