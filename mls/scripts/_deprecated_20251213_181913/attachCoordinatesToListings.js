// backend/mls/scripts/attachCoordinatesFromAddressPoints.js
// -------------------------------------------------------
// Attach coordinates to MLS listings using the parcel
// centroid index built from MassGIS L3 parcel data.
//
// INPUT:
//   backend/mls/normalized/listings.ndjson
//   backend/publicData/parcels/parcelCentroidIndex.json
//
// OUTPUT:
//   backend/mls/normalized/listingsWithCoords.ndjson
//
// Run with:
//   node mls/scripts/attachCoordinatesFromAddressPoints.js
// -------------------------------------------------------

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const MLS_DIR = path.resolve(__dirname, "../../mls/normalized");
const INPUT_PATH = path.join(MLS_DIR, "listings.ndjson");
const OUTPUT_PATH = path.join(MLS_DIR, "listingsWithCoords.ndjson");

const PARCELS_DIR = path.resolve(__dirname, "../../publicData/parcels");
const PARCEL_INDEX_PATH = path.join(PARCELS_DIR, "parcelCentroidIndex.json");

// -------------------------------------------------------
// Helpers
// -------------------------------------------------------

function cleanString(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s.length ? s : null;
}

function normalizeZip(zip) {
  if (!zip) return null;
  const digits = String(zip).replace(/\D/g, "");
  if (!digits) return null;
  return digits.slice(0, 5).padStart(5, "0");
}

function normalizeStreetName(name) {
  if (!name) return null;
  let up = String(name).trim().toUpperCase();
  const replacements = [
    { pattern: /\bSTREET\b/g, repl: "ST" },
    { pattern: /\bST\.\b/g, repl: "ST" },
    { pattern: /\bROAD\b/g, repl: "RD" },
    { pattern: /\bRD\.\b/g, repl: "RD" },
    { pattern: /\bAVENUE\b/g, repl: "AVE" },
    { pattern: /\bAVE\.\b/g, repl: "AVE" },
    { pattern: /\bBOULEVARD\b/g, repl: "BLVD" },
    { pattern: /\bBLVD\.\b/g, repl: "BLVD" },
    { pattern: /\bDRIVE\b/g, repl: "DR" },
    { pattern: /\bDR\.\b/g, repl: "DR" },
    { pattern: /\bLANE\b/g, repl: "LN" },
    { pattern: /\bLN\.\b/g, repl: "LN" },
    { pattern: /\bCOURT\b/g, repl: "CT" },
    { pattern: /\bCT\.\b/g, repl: "CT" },
  ];
  for (const { pattern, repl } of replacements) {
    up = up.replace(pattern, repl);
  }
  return up;
}

function buildAddressKey({ number, street, city, zip }) {
  const parts = [];
  if (number) parts.push(String(number).trim().toUpperCase());
  if (street) parts.push(normalizeStreetName(street));
  if (city) parts.push(String(city).trim().toUpperCase());
  if (zip) parts.push(normalizeZip(zip));
  if (!parts.length) return null;
  return parts.join(" ");
}

// Extract address fields from a listing record with flexible fallbacks
function extractListingAddressParts(listing) {
  // listings.ndjson has an outer object and a nested "raw" with MLS fields
  const src =
    listing && typeof listing.raw === "object" && listing.raw !== null
      ? listing.raw
      : listing;

  const number =
    cleanString(src.STREET_NO) ||
    cleanString(src.streetNumber) ||
    cleanString(src.street_no) ||
    cleanString(listing.STREET_NO) || // fallback to top-level
    null;

  const street =
    cleanString(src.STREET_NAME) ||
    cleanString(src.streetName) ||
    cleanString(src.street) ||
    cleanString(src.ST_NAME) ||
    cleanString(listing.STREET_NAME) ||
    null;

  // most MLS rows don't have CITY name, just town code + ZIP; CITY is optional
  const city =
    cleanString(src.CITY) ||
    cleanString(src.TOWN) ||
    cleanString(src.MUNICIPALITY) ||
    cleanString(listing.CITY) ||
    null;

  const zip =
    normalizeZip(
      cleanString(src.ZIP_CODE) ||
        cleanString(src.zip) ||
        cleanString(src.ZIP) ||
        cleanString(listing.ZIP_CODE) ||
        cleanString(listing.zip) ||
        cleanString(listing.ZIP)
    ) || null;

  return { number, street, city, zip };
}

// -------------------------------------------------------
// Main
// -------------------------------------------------------

async function main() {
  console.log("====================================================");
  console.log("      ATTACHING COORDINATES TO MLS LISTINGS");
  console.log("====================================================");

  if (!fs.existsSync(INPUT_PATH)) {
    console.error("❌ Missing listings input file:", INPUT_PATH);
    process.exit(1);
  }

  if (!fs.existsSync(PARCEL_INDEX_PATH)) {
    console.error(
      "❌ Missing parcel index file:",
      PARCEL_INDEX_PATH,
      "\n   → Run buildParcelCentroidIndex.js first."
    );
    process.exit(1);
  }

  console.log("[attachCoords] Loading parcel centroid index…");
  const rawIndex = await fsp.readFile(PARCEL_INDEX_PATH, "utf8");
  const parcelIndex = JSON.parse(rawIndex);

  console.log(
    `[attachCoords] Loaded ${
      Object.keys(parcelIndex).length
    } address keys from parcel index.`
  );

  const rl = readline.createInterface({
    input: fs.createReadStream(INPUT_PATH, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  const outStream = fs.createWriteStream(OUTPUT_PATH, { encoding: "utf8" });

  const stats = {
    total: 0,
    matchedParcel: 0,
    noMatch: 0,
  };

  for await (const line of rl) {
    if (!line.trim()) continue;
    let listing;
    try {
      listing = JSON.parse(line);
    } catch {
      console.warn("[attachCoords] Skipping invalid JSON line");
      continue;
    }

    stats.total++;

    const addrParts = extractListingAddressParts(listing);
    const key = buildAddressKey(addrParts);

    let lat = null;
    let lng = null;
    let coordSource = null;

    if (key && parcelIndex[key]) {
      lat = parcelIndex[key].lat;
      lng = parcelIndex[key].lng;
      coordSource = "parcel_centroid";
      stats.matchedParcel++;
    } else {
      coordSource = "none";
      stats.noMatch++;
    }

    const enriched = {
      ...listing,
      latitude: lat,
      longitude: lng,
      coordSource,
    };

    outStream.write(JSON.stringify(enriched) + "\n");

    if (stats.total % 10000 === 0) {
      console.log(
        `  processed ${stats.total.toLocaleString()} listings… (matched ${
          stats.matchedParcel
        })`
      );
    }
  }

  outStream.end();

  console.log("----------------------------------------------------");
  console.log(`Listings processed:        ${stats.total.toLocaleString()}`);
  console.log(
    `Listings w/ parcel coords: ${stats.matchedParcel.toLocaleString()}`
  );
  console.log(`Listings w/ no match:      ${stats.noMatch.toLocaleString()}`);
  console.log("Output written →", OUTPUT_PATH);
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ Fatal error attaching coordinates:", err);
  process.exit(1);
});
