// backend/mls/scripts/buildParcelCentroidIndex.js
// -------------------------------------------------------
// Build a fast lookup index of parcel centroids keyed by
// normalized address (house # + street + city + ZIP AND
// an alternate key with just house # + street + ZIP).
//
// INPUT:
//   backend/publicData/parcels/L3_TAXPAR_POLY_ASSESS_EAST.shp
//   backend/publicData/parcels/L3_TAXPAR_POLY_ASSESS_WEST.shp
//
// OUTPUT:
//   backend/publicData/parcels/parcelCentroidIndex.json
//
// Run with:
//   node mls/scripts/buildParcelCentroidIndex.js
// -------------------------------------------------------

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import * as shapefile from "shapefile";
import { centroid as turfCentroid } from "@turf/turf";
import proj4 from "proj4";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PARCELS_DIR = path.resolve(__dirname, "../../publicData/parcels");
const OUTPUT_PATH = path.join(PARCELS_DIR, "parcelCentroidIndex.json");

// -------------------------------------------------------
// Coordinate systems
// MassGIS L3 parcels are in Massachusetts State Plane Mainland
// (NAD83, meters). EPSG:26986.
// We convert to WGS84 lat/lng (EPSG:4326).
// -------------------------------------------------------

const MASS_STATE_PLANE_MAINLAND =
  "+proj=lcc +lat_1=41.71666666666667 +lat_2=42.68333333333333 " +
  "+lat_0=41 +lon_0=-71.5 +x_0=200000 +y_0=750000 " +
  "+datum=NAD83 +units=m +no_defs";

const WGS84 =
  "+proj=longlat +datum=WGS84 +no_defs";

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

// Street normalization tuned for MassGIS + MLS PIN
function normalizeStreetName(name) {
  if (!name) return null;
  let up = String(name).trim().toUpperCase();

  const replacements = [
    // Common suffixes
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
    { pattern: /\bTERRACE\b/g, repl: "TER" },
    { pattern: /\bTER\.\b/g, repl: "TER" },
    { pattern: /\bCIRCLE\b/g, repl: "CIR" },
    { pattern: /\bCIR\.\b/g, repl: "CIR" },
    { pattern: /\bPARKWAY\b/g, repl: "PKWY" },
    { pattern: /\bPKWY\.\b/g, repl: "PKWY" },
    { pattern: /\bHIGHWAY\b/g, repl: "HWY" },
    { pattern: /\bHWY\.\b/g, repl: "HWY" },
    { pattern: /\bEXTENSION\b/g, repl: "EXT" },
    { pattern: /\bEXT\.\b/g, repl: "EXT" },

    // Boston specifics
    { pattern: /\bSQUARE\b/g, repl: "SQ" },  // LOUISBURG SQ
    { pattern: /\bSQ\.\b/g, repl: "SQ" },
    { pattern: /\bPLACE\b/g, repl: "PL" },   // WEST HILL PL
    { pattern: /\bPL\.\b/g, repl: "PL" },

    // MOUNT/MT normalisation (Beacon Hill, etc.)
    { pattern: /\bMOUNT\b/g, repl: "MT" },
    { pattern: /\bMT\.\b/g, repl: "MT" },
  ];

  for (const { pattern, repl } of replacements) {
    up = up.replace(pattern, repl);
  }

  // collapse multiple spaces
  up = up.replace(/\s+/g, " ").trim();

  return up;
}

function extractNumberAndStreetFromSiteAddr(siteAddrRaw) {
  const site = cleanString(siteAddrRaw);
  if (!site) return { number: null, street: null };
  const m = site.match(/^(\d+[A-Z\-]*)\s+(.+)$/i);
  if (!m) return { number: null, street: normalizeStreetName(site) };
  return {
    number: cleanString(m[1]),
    street: normalizeStreetName(m[2]),
  };
}

// Build normalized key: parts may or may not include city.
// With city: "123 MAIN ST BOSTON 02134"
// Without city: "123 MAIN ST 02134"
function buildAddressKey({ number, street, city, zip }) {
  const parts = [];
  if (number) parts.push(String(number).trim().toUpperCase());
  if (street) parts.push(normalizeStreetName(street));
  if (city) parts.push(String(city).trim().toUpperCase());
  if (zip) parts.push(normalizeZip(zip));
  if (!parts.length) return null;
  return parts.join(" ");
}

// Try to derive address parts from MassGIS parcel properties
function extractAddressParts(props) {
  const siteAddr =
    cleanString(props.SITE_ADDR) ||
    cleanString(props.SITE_ADR) ||
    cleanString(props.SITE_ADD);

  let number =
    cleanString(props.ADDR_NUM) ||
    cleanString(props.HOUSENUM) ||
    cleanString(props.HOUSENO) ||
    cleanString(props.HOUSE_NO) ||
    null;

  let street =
    normalizeStreetName(
      cleanString(props.ST_NAME) ||
        cleanString(props.STNAME) ||
        cleanString(props.STREET) ||
        cleanString(props.ST_NAME_F) ||
        cleanString(props.STREETNAME)
    ) || null;

  if ((!number || !street) && siteAddr) {
    const parsed = extractNumberAndStreetFromSiteAddr(siteAddr);
    if (!number) number = parsed.number;
    if (!street) street = parsed.street;
  }

  const city =
    cleanString(props.SITE_CITY) ||
    cleanString(props.CITY) ||
    cleanString(props.TOWN) ||
    cleanString(props.MUNI) ||
    null;

  const zip =
    normalizeZip(
      cleanString(props.SITE_ZIP) ||
        cleanString(props.ZIP) ||
        cleanString(props.ZIPCODE) ||
        cleanString(props.MAIL_ZIP)
    ) || null;

  return { number, street, city, zip };
}

// Compute centroid and project to WGS84 lat/lng
function computeCentroid(geometry) {
  try {
    const feature = { type: "Feature", geometry, properties: {} };
    const c = turfCentroid(feature);
    const [x, y] = c.geometry.coordinates;

    // x/y are in Mass State Plane (meters); convert to lon/lat
    const [lng, lat] = proj4(MASS_STATE_PLANE_MAINLAND, WGS84, [x, y]);

    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
    return { lat, lng };
  } catch {
    return null;
  }
}

// -------------------------------------------------------
// Main processing
// -------------------------------------------------------

async function processShapefile(shpPath, index, stats) {
  console.log(`\n[buildParcelCentroidIndex] Reading: ${shpPath}`);
  const source = await shapefile.open(shpPath);

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const { done, value } = await source.read();
    if (done) break;
    if (!value) continue;

    stats.totalParcels++;

    const { geometry, properties } = value;

    if (!geometry) {
      stats.noGeometry++;
      continue;
    }

    const centroid = computeCentroid(geometry);
    if (!centroid) {
      stats.badCentroid++;
      continue;
    }

    const parts = extractAddressParts(properties || {});
    const keyFull = buildAddressKey(parts);

    if (!keyFull) {
      stats.noAddress++;
      continue;
    }

    // Primary key: number + street + city + ZIP (when city exists)
    if (!index[keyFull]) {
      index[keyFull] = {
        lat: centroid.lat,
        lng: centroid.lng,
      };
      stats.indexedParcels++;
    } else {
      stats.duplicateKeys++;
    }

    // Secondary key: number + street + ZIP only (no city)
    const { number, street, zip } = parts;
    if (number && street && zip) {
      const keyNoCity = buildAddressKey({
        number,
        street,
        city: null,
        zip,
      });
      if (keyNoCity && !index[keyNoCity]) {
        index[keyNoCity] = {
          lat: centroid.lat,
          lng: centroid.lng,
        };
        stats.altKeys++;
      }
    }

    if (stats.totalParcels % 50000 === 0) {
      console.log(
        `  processed ${stats.totalParcels.toLocaleString()} parcels...`
      );
    }
  }
}

async function main() {
  console.log("====================================================");
  console.log("    BUILDING PARCEL CENTROID INDEX (STATEWIDE)");
  console.log("====================================================");

  const eastPath = path.join(
    PARCELS_DIR,
    "L3_TAXPAR_POLY_ASSESS_EAST.shp"
  );
  const westPath = path.join(
    PARCELS_DIR,
    "L3_TAXPAR_POLY_ASSESS_WEST.shp"
  );

  for (const p of [eastPath, westPath]) {
    if (!fs.existsSync(p)) {
      console.error(`❌ Missing shapefile: ${p}`);
      process.exit(1);
    }
  }

  const index = Object.create(null);
  const stats = {
    totalParcels: 0,
    indexedParcels: 0,
    noGeometry: 0,
    badCentroid: 0,
    noAddress: 0,
    duplicateKeys: 0,
    altKeys: 0,
  };

  await processShapefile(eastPath, index, stats);
  await processShapefile(westPath, index, stats);

  console.log("\n[buildParcelCentroidIndex] Writing index to disk…");
  await fsp.writeFile(OUTPUT_PATH, JSON.stringify(index), "utf8");

  const fileSizeMb = fs.statSync(OUTPUT_PATH).size / (1024 * 1024);

  console.log("----------------------------------------------------");
  console.log(
    `Total parcels processed:   ${stats.totalParcels.toLocaleString()}`
  );
  console.log(
    `Parcels indexed (primary): ${stats.indexedParcels.toLocaleString()}`
  );
  console.log(
    `Alt keys (no-city):        ${stats.altKeys.toLocaleString()}`
  );
  console.log(`Parcels w/ no geometry:    ${stats.noGeometry.toLocaleString()}`);
  console.log(`Parcels w/ bad centroid:   ${stats.badCentroid.toLocaleString()}`);
  console.log(`Parcels w/ no address:     ${stats.noAddress.toLocaleString()}`);
  console.log(`Duplicate address keys:    ${stats.duplicateKeys.toLocaleString()}`);
  console.log(`Index file size:           ${fileSizeMb.toFixed(1)} MB`);
  console.log("Output →", OUTPUT_PATH);
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ Fatal error building parcel centroid index:", err);
  process.exit(1);
});
