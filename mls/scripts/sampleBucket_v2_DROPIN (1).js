
/**
 * sampleBucket_v2_DROPIN.js
 * -------------------------
 * Stream NDJSON and sample a bucket (noStreetNo, streetNoZero, etc.)
 * with optional regex filters.
 *
 * Usage:
 *  node .\mls\scripts\sampleBucket_v2_DROPIN.js ^
 *    --in C:\...\properties_statewide_geo_zip_district_v17_coords.ndjson ^
 *    --bucket noStreetNo ^
 *    --limit 20 ^
 *    --parcelRegex "^\d{1,5}-[A-Z0-9]{1,8}$" ^
 *    --addrRegex "#|UNIT|APT"
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

const IN = path.resolve(__dirname, getArg("in"));
const BUCKET = getArg("bucket", "noStreetNo");
const LIMIT = Number(getArg("limit", "20"));

const parcelRegexStr = getArg("parcelRegex", null);
const addrRegexStr = getArg("addrRegex", null);

const parcelRe = parcelRegexStr ? new RegExp(parcelRegexStr, "i") : null;
const addrRe = addrRegexStr ? new RegExp(addrRegexStr, "i") : null;

function collapseSpaces(s) { return String(s ?? "").replace(/\s+/g, " ").trim(); }
function hasCoords(o) {
  const lat = Number(o.lat ?? o.latitude ?? NaN);
  const lon = Number(o.lon ?? o.lng ?? o.longitude ?? NaN);
  return Number.isFinite(lat) && Number.isFinite(lon);
}

function bucketOf(r) {
  const streetNo = collapseSpaces(r.street_no);
  const full = collapseSpaces(r.full_address).toUpperCase();
  const name = collapseSpaces(r.street_name).toUpperCase();
  const unit = collapseSpaces(r.unit).toUpperCase();

  const b = new Set();
  if (!streetNo) b.add("noStreetNo");
  if (streetNo === "0") b.add("streetNoZero");
  if (/\b(LOT|LT)\b/.test(full) || /\b(LOT|LT)\b/.test(name)) b.add("lotLike");
  if (/\b(REAR|OFF)\b/.test(full) || /\b(REAR|OFF)\b/.test(name)) b.add("rearOffLine");
  if (/[(),]/.test(r.full_address ?? "")) b.add("hasCommaOrParen");
  if (unit || /\b(UNIT|APT|APARTMENT|STE|SUITE|#)\b/.test(full) || /\b(UNIT|APT|APARTMENT|STE|SUITE|#)\b/.test(name)) b.add("hasUnitCode");
  return b;
}

async function main() {
  if (!IN || !fs.existsSync(IN)) throw new Error(`--in not found: ${IN}`);

  const rl = readline.createInterface({
    input: fs.createReadStream(IN, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  const picked = [];
  let scanned = 0;

  for await (const line of rl) {
    if (!line) continue;
    scanned++;
    let r;
    try { r = JSON.parse(line); } catch { continue; }

    if (hasCoords(r)) continue; // only missing coords

    const b = bucketOf(r);
    if (!b.has(BUCKET)) continue;

    const pid = String(r.parcel_id ?? "");
    const addr = String(r.full_address ?? r.street_name ?? "");

    if (parcelRe && !parcelRe.test(pid)) continue;
    if (addrRe && !addrRe.test(addr)) continue;

    picked.push({
      property_id: r.property_id,
      parcel_id: r.parcel_id,
      town: r.town,
      zip: r.zip,
      full_address: r.full_address,
      street_no: r.street_no,
      street_name: r.street_name,
      unit: r.unit,
    });

    if (picked.length >= LIMIT) break;
  }

  console.log(JSON.stringify(picked, null, 2));
  console.log(`\n[info] scanned=${scanned.toLocaleString()} picked=${picked.length} bucket=${BUCKET}`);
}

main().catch((e) => { console.error("❌ sampleBucket_v2 failed:", e); process.exit(1); });
