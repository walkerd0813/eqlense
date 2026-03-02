// backend/mls/scripts/buildParcelIndex.js
// ============================================================
// Build Parcel Index
// - Reads MLS normalized listings.ndjson
// - Normalizes addresses for stable keys
// - Enriches with assessor data (parcelId, lot size, yearBuilt)
// - (Later) will attach zoning + coordinates
// - Writes parcelCache.json
// ============================================================

import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

// Resolve __dirname in ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// -------------------------
// IMPORT ASSESSOR ENRICHER
// -------------------------
import { enrichWithAssessorData } from "../../publicData/assessors/assessorEnrichment.js";

// -------------------------
// Input + Output Paths
// -------------------------
const LISTINGS_PATH = path.resolve(
  __dirname,
  "../../mls/normalized/listings.ndjson"
);

const PARCEL_CACHE_PATH = path.resolve(
  __dirname,
  "../../publicData/parcels/parcelCache.json"
);

// -------------------------
// Utility: Normalize address
// -------------------------
function normalizeAddress(streetNumber, streetName, unitNumber) {
  const parts = [];

  if (streetNumber) parts.push(String(streetNumber).trim());
  if (streetName) parts.push(String(streetName).trim());
  if (unitNumber) parts.push(String(unitNumber).trim());

  return parts.join(" ").replace(/\s+/g, " ").trim();
}

// -------------------------
// Main function
// -------------------------
async function buildParcelIndex() {
  console.log("=================================================");
  console.log("       BUILDING PARCEL → LISTING INDEX");
  console.log("=================================================");
  console.log("📄 Reading:", LISTINGS_PATH);

  if (!fs.existsSync(LISTINGS_PATH)) {
    console.error("❌ listings.ndjson not found. Run ingestion first.");
    process.exit(1);
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(LISTINGS_PATH),
    crlfDelay: Infinity,
  });

  /** @type {Object[]} */
  const parcelRecords = [];
  let count = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;

    let listing;
    try {
      listing = JSON.parse(line);
    } catch (e) {
      console.warn("⚠️ Skipping invalid JSON line:", line.slice(0, 50));
      continue;
    }

    const {
      mlsNumber,
      streetNumber,
      streetName,
      unitNumber,
      zipCode,
      yearBuilt,
      lotSizeSqFt,
      acres,
    } = listing;

    const normalizedAddress = normalizeAddress(
      streetNumber,
      streetName,
      unitNumber
    );

    // ------------------------------------
    // ENRICH WITH ASSESSOR DATA
    // ------------------------------------
    const assessorData =
      enrichWithAssessorData({
        address: normalizedAddress,
        zip: zipCode,
        rawSqft: lotSizeSqFt,
        rawAcres: acres,
        yearBuilt,
      }) || {};

    const record = {
      mlsNumber,
      propertyKey: listing.propertyKey,
      address: normalizedAddress,
      zip: zipCode,
      townCode: listing.townCode,

      // Parcel
      parcelId: assessorData.parcelId ?? null,

      // Land
      lotSizeSqFt: assessorData.lotSizeSqFt ?? lotSizeSqFt ?? null,
      acres: assessorData.acres ?? acres ?? null,

      // Year Built
      yearBuilt: assessorData.yearBuilt ?? yearBuilt ?? null,

      // Zoning (placeholder until we integrate zoning engine)
      zoningDistrict: null,
      zoningSource: null,

      // Coordinates (placeholder until geocoder or parcel polygons arrive)
      lat: assessorData.lat ?? null,
      lng: assessorData.lng ?? null,
    };

    parcelRecords.push(record);
    count++;
  }

  // ------------------------------------
  // Write parcelCache.json
  // ------------------------------------
  console.log("📦 Writing parcel cache…", PARCEL_CACHE_PATH);

  fs.writeFileSync(
    PARCEL_CACHE_PATH,
    JSON.stringify(parcelRecords, null, 2),
    "utf8"
  );

  console.log("-------------------------------------------------");
  console.log(`   ✔️ Parcel index built with ${count} records`);
  console.log("-------------------------------------------------");
}

buildParcelIndex().catch((err) => {
  console.error("❌ Unhandled error in buildParcelIndex:", err);
  process.exit(1);
});
