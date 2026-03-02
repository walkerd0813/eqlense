// backend/mls/scripts/ingestListingPhotos.js
// Extracts photo metadata from normalized listings and writes propertyPhotos.ndjson

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const MLS_ROOT = path.join(__dirname, "..");
const NORMALIZED_DIR = path.join(MLS_ROOT, "normalized");

const LISTINGS_NDJSON = path.join(NORMALIZED_DIR, "listings.ndjson");
const OUTPUT_PHOTOS_NDJSON = path.join(NORMALIZED_DIR, "propertyPhotos.ndjson");

// -----------------------------------
// Helpers
// -----------------------------------
function toNumber(val) {
  if (val == null || val === "") return null;
  const n = Number(val);
  return Number.isFinite(n) ? n : null;
}

// -----------------------------------
// Main ingestion function
// -----------------------------------
export async function ingestListingPhotos() {
  console.log("====================================================");
  console.log("     STARTING LISTING PHOTO EXTRACTION");
  console.log("====================================================");

  // Ensure listings exist
  try {
    await fsp.access(LISTINGS_NDJSON);
  } catch {
    console.error("[PHOTO INGEST] listings.ndjson not found. Run ingestion first.");
    return;
  }

  let totalListings = 0;
  let photoRecordsWritten = 0;

  const outputStream = fs.createWriteStream(OUTPUT_PHOTOS_NDJSON, {
    flags: "w",
    encoding: "utf8",
  });

  const rl = readline.createInterface({
    input: fs.createReadStream(LISTINGS_NDJSON, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    totalListings++;

    let listing;
    try {
      listing = JSON.parse(trimmed);
    } catch (err) {
      console.warn("[PHOTO INGEST] Failed to parse line:", err.message);
      continue;
    }

    const raw = listing.raw || {};
    const mls = listing.mlsNumber;

    const photoCount = toNumber(raw.PHOTO_COUNT);
    const photoDate = raw.PHOTO_DATE || null;
    const photoMask = raw.PHOTO_MASK || null;

    const record = {
      mlsNumber: mls,
      photoCount: photoCount ?? 0,
      photoDate,
      photoMask,
      sourceFile: listing.source || null,
      importedAt: new Date().toISOString(),
    };

    outputStream.write(JSON.stringify(record) + "\n");
    photoRecordsWritten++;
  }

  outputStream.end();

  console.log("");
  console.log("====================================================");
  console.log("   LISTING PHOTO INGEST COMPLETE");
  console.log("====================================================");
  console.log(`Listings scanned:      ${totalListings}`);
  console.log(`Photo records written: ${photoRecordsWritten}`);
  console.log(`Output: ${OUTPUT_PHOTOS_NDJSON}`);
  console.log("====================================================");
}

// Allow direct execution
if (import.meta.url === `file://${process.argv[1]}`) {
  ingestListingPhotos().catch((err) => {
    console.error("Photo ingestion failed:", err);
    process.exit(1);
  });
}
