
/**
 * SAMPLE BUCKET RECORDS (NDJSON streaming) - v1 DROPIN
 * ----------------------------------------------------
 * Pull a random sample of records from a large NDJSON file by "bucket".
 *
 * Buckets implemented:
 *  - noStreetNo     : missing/blank street_no
 *  - streetNoZero   : street_no == 0
 *  - hasUnitCode    : unit present OR street_name/address contains unit-like tokens
 *  - rearOffLine    : address contains REAR/OFF
 *  - lotLike        : address contains LOT/LT
 *  - hasCommaOrParen: address contains comma or parentheses
 *
 * Default behavior is to sample ONLY records that are missing coords (lat/lon).
 *
 * Examples:
 *   node .\mls\scripts\sampleBucket_v1_DROPIN.js --in C:\path\file.ndjson --bucket noStreetNo --limit 20
 *   node .\mls\scripts\sampleBucket_v1_DROPIN.js --in C:\path\file.ndjson --bucket streetNoZero --limit 30 --missingOnly false
 */

import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function getArg(name, fallback = null) {
  const key = `--${name}`;
  const i = process.argv.findIndex((a) => a === key);
  if (i >= 0 && i + 1 < process.argv.length) return process.argv[i + 1];
  return fallback;
}

function toNum(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function hasCoords(o) {
  const lat = toNum(o.lat ?? o.latitude);
  const lon = toNum(o.lon ?? o.lng ?? o.longitude);
  if (lat == null || lon == null) return false;
  if (lat < 40.0 || lat > 46.0) return false;
  if (lon > -66.0 || lon < -76.0) return false;
  return true;
}

function s(v) {
  return String(v ?? "").trim();
}

function normUpper(v) {
  return s(v).toUpperCase();
}

function looksUnitish(text) {
  const t = normUpper(text);
  return (
    /\b(UNIT|APT|APARTMENT|STE|SUITE|PENTHOUSE|PH|FL|FLOOR|#)\b/.test(t) ||
    /\bU\d+\b/.test(t) ||
    /\bPS-[A-Z0-9]+\b/.test(t) ||
    /\b\d{1,4}[A-Z]\b/.test(t)
  );
}

function isBucket(o, bucket) {
  const full = s(o.full_address);
  const streetName = s(o.street_name);
  const streetNo = s(o.street_no);
  const unit = s(o.unit);

  const fullU = normUpper(full);
  const nameU = normUpper(streetName);

  switch (bucket) {
    case "noStreetNo":
      return streetNo === "";
    case "streetNoZero":
      return streetNo === "0";
    case "lotLike":
      return /\b(LOT|LT)\b/.test(fullU) || /\b(LOT|LT)\b/.test(nameU);
    case "rearOffLine":
      return /\b(REAR|OFF)\b/.test(fullU) || /\b(REAR|OFF)\b/.test(nameU);
    case "hasCommaOrParen":
      return /[(),]/.test(full);
    case "hasUnitCode":
      return unit !== "" || looksUnitish(full) || looksUnitish(streetName);
    case "any":
      return true;
    default:
      return false;
  }
}

function pickFields(o) {
  return {
    property_id: o.property_id ?? null,
    parcel_id: o.parcel_id ?? null,
    town: o.town ?? null,
    zip: o.zip ?? null,
    full_address: o.full_address ?? null,
    street_no: o.street_no ?? null,
    street_name: o.street_name ?? null,
    unit: o.unit ?? null,
  };
}

// Reservoir sampling for a uniform random sample while streaming.
function reservoirPush(sampleArr, limit, item, seenCount) {
  if (sampleArr.length < limit) {
    sampleArr.push(item);
    return;
  }
  const j = Math.floor(Math.random() * seenCount);
  if (j < limit) sampleArr[j] = item;
}

async function main() {
  const inPathArg = getArg("in", null);
  const bucket = getArg("bucket", "noStreetNo");
  const limit = Number(getArg("limit", "20"));
  const missingOnly = (getArg("missingOnly", "true") || "true").toLowerCase() !== "false";

  if (!inPathArg) {
    console.error("❌ Missing --in <path-to-ndjson>");
    process.exit(1);
  }

  const IN = path.isAbsolute(inPathArg) ? inPathArg : path.resolve(__dirname, inPathArg);
  if (!fs.existsSync(IN)) {
    console.error("❌ File not found:", IN);
    process.exit(1);
  }

  console.log("====================================================");
  console.log(" SAMPLE BUCKET RECORDS (NDJSON)");
  console.log("====================================================");
  console.log("IN         :", IN);
  console.log("BUCKET     :", bucket);
  console.log("LIMIT      :", limit);
  console.log("missingOnly:", missingOnly);
  console.log("----------------------------------------------------");

  const rl = readline.createInterface({
    input: fs.createReadStream(IN, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let scanned = 0;
  let matched = 0;
  const sample = [];

  for await (const line of rl) {
    scanned++;
    if (!line || line.length < 2) continue;

    let obj;
    try {
      obj = JSON.parse(line);
    } catch {
      continue;
    }

    if (missingOnly && hasCoords(obj)) continue;
    if (!isBucket(obj, bucket)) continue;

    matched++;
    reservoirPush(sample, limit, pickFields(obj), matched);

    if (scanned % 500000 === 0) {
      console.log(`[progress] scanned ${scanned.toLocaleString()} matched ${matched.toLocaleString()} sampleSize ${sample.length}`);
    }
  }

  console.log("----------------------------------------------------");
  console.log("[done]", { scanned, matched, sample: sample.length });
  console.log("----------------------------------------------------");
  console.log(JSON.stringify(sample, null, 2));
  console.log("====================================================");
}

main().catch((e) => {
  console.error("❌ sampleBucket failed:", e);
  process.exit(1);
});
