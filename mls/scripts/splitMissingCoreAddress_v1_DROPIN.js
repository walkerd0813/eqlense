// splitMissingCoreAddress_v1_DROPIN.js
// ESM, streaming NDJSON splitter
// Goal: move records with NO full_address, NO street_name, NO street_no into a separate file

import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function getArg(name, fallback = null) {
  const key = `--${name}`;
  const idx = process.argv.indexOf(key);
  if (idx !== -1 && process.argv[idx + 1]) return process.argv[idx + 1];
  return fallback;
}

function isBlank(v) {
  return v === null || v === undefined || (typeof v === "string" && v.trim() === "");
}

function looksMissingCoreAddress(rec) {
  // This matches your sample bucket:
  // full_address: null, street_no: null, street_name: null
  // Also treat "" as missing
  return isBlank(rec.full_address) && isBlank(rec.street_name) && isBlank(rec.street_no);
}

const IN = path.resolve(
  __dirname,
  getArg("in", "../../publicData/properties/properties_statewide_geo_zip_district_v22_coords.ndjson")
);

const OUT_KEEP = path.resolve(
  __dirname,
  getArg("outKeep", "../../publicData/properties/properties_statewide_geo_zip_district_v22_coords_KEEP.ndjson")
);

const OUT_MISSING = path.resolve(
  __dirname,
  getArg("outMissing", "../../publicData/properties/missingCoreAddress_v22.ndjson")
);

const OUT_META = path.resolve(
  __dirname,
  getArg("meta", OUT_MISSING.replace(/\.ndjson$/i, "_meta.json"))
);

if (!fs.existsSync(IN)) {
  console.error(`❌ Input not found: ${IN}`);
  process.exit(1);
}

console.log("====================================================");
console.log(" SPLIT MISSING CORE ADDRESS (streaming)");
console.log("====================================================");
console.log("IN        :", IN);
console.log("OUT_KEEP  :", OUT_KEEP);
console.log("OUT_MISS  :", OUT_MISSING);
console.log("META      :", OUT_META);
console.log("----------------------------------------------------");

const rs = fs.createReadStream(IN, { encoding: "utf8" });
const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });

const wsKeep = fs.createWriteStream(OUT_KEEP, { encoding: "utf8" });
const wsMiss = fs.createWriteStream(OUT_MISSING, { encoding: "utf8" });

const meta = {
  total: 0,
  keep: 0,
  missingCoreAddress: 0,
  parseErrors: 0,
  samplesMissing: [],
};

let lastLog = 0;

for await (const line of rl) {
  const s = line.trim();
  if (!s) continue;

  meta.total++;

  let rec;
  try {
    rec = JSON.parse(s);
  } catch {
    meta.parseErrors++;
    continue;
  }

  const isMissing = looksMissingCoreAddress(rec);

  if (isMissing) {
    meta.missingCoreAddress++;
    wsMiss.write(JSON.stringify(rec) + "\n");

    if (meta.samplesMissing.length < 20) {
      meta.samplesMissing.push({
        property_id: rec.property_id ?? null,
        parcel_id: rec.parcel_id ?? null,
        town: rec.town ?? null,
        zip: rec.zip ?? null,
      });
    }
  } else {
    meta.keep++;
    wsKeep.write(JSON.stringify(rec) + "\n");
  }

  if (meta.total - lastLog >= 250000) {
    lastLog = meta.total;
    console.log(`[progress] ${meta.total.toLocaleString()} lines... miss ${meta.missingCoreAddress.toLocaleString()}`);
  }
}

wsKeep.end();
wsMiss.end();

fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2));

console.log("====================================================");
console.log("[done]", {
  total: meta.total,
  keep: meta.keep,
  missingCoreAddress: meta.missingCoreAddress,
  parseErrors: meta.parseErrors,
});
console.log("META:", OUT_META);
console.log("====================================================");
