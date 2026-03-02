// backend/mls/scripts/attachCoordinatesFromAddressPoints.js
// -------------------------------------------------------
// Tiered coordinate attachment for MLS listings:
//
// Tier 1: Direct parcel match by NUMBER + STREET + ZIP
//         (using parcelCentroidIndex.json keys like "126 MARLBOROUGH ST 02116")
// Tier 2: Prefix match by NUMBER + STREET (ZIP/city-agnostic)
//         when prefix maps to exactly one parcel
// Tier 3: MassGIS Address Points index (addressIndex.json)
//         keys like "126|MARLBOROUGH STREET|02116"
//         → use address point lat/lon directly
// Tier 4: External geocoder + nearest parcel centroid search
//         using a spatial grid index over all parcel centroids.
//
// INPUT:
//   backend/mls/normalized/listings.ndjson
//   backend/publicData/parcels/parcelCentroidIndex.json
//   backend/publicData/addresses/addressIndex.json
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
import fetch from "node-fetch"; // npm install node-fetch

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const MLS_DIR = path.resolve(__dirname, "../../mls/normalized");
// Allow overriding input (argv[2]) for PASS 2
const INPUT_PATH = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.join(MLS_DIR, "listings.ndjson");

// Allow overriding output (argv[3])
let OUTPUT_OVERRIDE = null;
if (process.argv[3]) {
  OUTPUT_OVERRIDE = path.resolve(process.argv[3]);
}

// Use override output if provided
const OUTPUT_PATH = OUTPUT_OVERRIDE
  ? OUTPUT_OVERRIDE
  : path.join(MLS_DIR, "listingsWithCoords.ndjson");


const PARCELS_DIR = path.resolve(__dirname, "../../publicData/parcels");
const PARCEL_INDEX_PATH = path.join(PARCELS_DIR, "parcelCentroidIndex.json");

const ADDRESSES_DIR = path.resolve(__dirname, "../../publicData/addresses");
const ADDRESS_INDEX_PATH = path.join(ADDRESSES_DIR, "addressIndex.json");
const GEOCODE_CACHE_PATH = path.join(PARCELS_DIR, "geocodeCache.json");
const outStream = fs.createWriteStream(OUTPUT_PATH, { encoding: "utf8" });
// Collect unmatched listings for review
const unmatched = [];


// -------------------------------------------------------
// Geocoder config (Tier 4)
// -------------------------------------------------------

const MAX_GEOCODER_CALLS_PER_RUN = 500; // safety limit for a single run
const GEOCODER_BASE_URL = "https://nominatim.openstreetmap.org/search";
const GEOCODER_USER_AGENT =
  "EquityLens/1.0 (replace-with-your-email@example.com)";

// -------------------------------------------------------
// Shared helpers
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

// -------------------------------------------------------
// PARCEL key normalization (for parcelCentroidIndex.json)
// (kept as-is per Option C)
// -------------------------------------------------------

function normalizeParcelStreetName(name) {
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
    { pattern: /\bSQUARE\b/g, repl: "SQ" },
    { pattern: /\bSQ\.\b/g, repl: "SQ" },
    { pattern: /\bPLACE\b/g, repl: "PL" },
    { pattern: /\bPL\.\b/g, repl: "PL" },
    { pattern: /\bMOUNT\b/g, repl: "MT" },
    { pattern: /\bMT\.\b/g, repl: "MT" },
  ];

  for (const { pattern, repl } of replacements) {
    up = up.replace(pattern, repl);
  }

  up = up.replace(/\s+/g, " ").trim();

  return up;
}


// KEY FORMAT for parcel index: "123 MAIN ST 02134"
function buildParcelAddressKey({ number, street, zip }) {
  const parts = [];
  if (number) parts.push(String(number).trim().toUpperCase());
  if (street) parts.push(normalizeParcelStreetName(street));
  if (zip) parts.push(normalizeZip(zip));
  if (!parts.length) return null;
  return parts.join(" ");
}

// PREFIX FORMAT: "123 MAIN ST"
function buildParcelPrefix({ number, street }) {
  if (!number || !street) return null;
  const n = String(number).trim().toUpperCase();
  const s = normalizeParcelStreetName(street);
  return `${n} ${s}`;
}

const STREET_SUFFIX_TOKENS = new Set([
  "ST",
  "RD",
  "AVE",
  "BLVD",
  "DR",
  "LN",
  "CT",
  "TER",
  "CIR",
  "PKWY",
  "HWY",
  "SQ",
  "PL",
  "EXT",
]);

function computePrefixFromParcelKey(key) {
  if (!key) return null;
  const tokens = key.split(" ").filter(Boolean);
  if (tokens.length < 3) return null;

  let suffixIndex = -1;
  for (let i = 1; i < tokens.length; i++) {
    if (STREET_SUFFIX_TOKENS.has(tokens[i])) {
      suffixIndex = i;
    }
  }
  if (suffixIndex === -1) return null;

  return tokens.slice(0, suffixIndex + 1).join(" ");
}

// -------------------------------------------------------
// ADDRESS POINT key normalization (for addressIndex.json)
// Full-word suffix expansion to match MassGIS (Option C).
// -------------------------------------------------------

const ADDRESS_SUFFIX_MAP = {
  " ST": " STREET",
  " ST.": " STREET",
  " AVE": " AVENUE",
  " AVE.": " AVENUE",
  " RD": " ROAD",
  " RD.": " ROAD",
  " BLVD": " BOULEVARD",
  " BLVD.": " BOULEVARD",
  " DR": " DRIVE",
  " DR.": " DRIVE",
  " PL": " PLACE",
  " PL.": " PLACE",
  " CT": " COURT",
  " CT.": " COURT",
  " LN": " LANE",
  " LN.": " LANE",
  " HWY": " HIGHWAY",
  " HWY.": " HIGHWAY",
  " TER": " TERRACE",
  " TER.": " TERRACE",
  " CIR": " CIRCLE",
  " CIR.": " CIRCLE",
  " SQ": " SQUARE",
  " SQ.": " SQUARE",
  " PKWY": " PARKWAY",
  " PKWY.": " PARKWAY",
};

function normalizeHouseNumberForAddress(num) {
  if (!num) return "";
  let s = String(num).trim().toUpperCase();
  const dashIdx = s.indexOf("-");
  if (dashIdx > 0) {
    s = s.slice(0, dashIdx);
  }
  return s.replace(/\s+/g, " ");
}

function normalizeStreetNameForAddress(name) {
  if (!name) return "";
  let out = String(name).toUpperCase();

  

  // Remove periods and collapse whitespace first
  out = out.replace(/\./g, "").replace(/\s+/g, " ").trim();

  

  // Expand directional prefixes if they're the first token (W → WEST, etc.)
  const dirMap = {
    N: "NORTH",
    S: "SOUTH",
    E: "EAST",
    W: "WEST",
    NE: "NORTHEAST",
    NW: "NORTHWEST",
    SE: "SOUTHEAST",
    SW: "SOUTHWEST",
  };

  let tokens = out.split(" ").filter(Boolean);
  if (tokens.length > 1 && dirMap[tokens[0]]) {
    tokens[0] = dirMap[tokens[0]]; // e.g. "W RUTLAND SQ" → "WEST RUTLAND SQ"
    out = tokens.join(" ");
  }

  // Expand suffixes (PL → PLACE, SQ → SQUARE, AVE → AVENUE, etc.)
  for (const [abbr, full] of Object.entries(ADDRESS_SUFFIX_MAP)) {
    const simple = abbr.replace(/\./g, "").trim(); // e.g. "PL"
    if (out.endsWith(simple)) {
      out = out.slice(0, out.length - simple.length) + full;
      break;
    }
  }

  // Final cleanup: collapse any accidental double spaces
  out = out.replace(/\s+/g, " ").trim();

  return out;
}

function normalizeStreetNameForAddress(name) {
  if (!name) return "";
  let out = String(name).toUpperCase();

  // Remove periods and collapse whitespace first
  out = out.replace(/\./g, "").replace(/\s+/g, " ").trim();

  // Expand directional prefixes if they're the first token (W → WEST, etc.)
  const dirMap = {
    N: "NORTH",
    S: "SOUTH",
    E: "EAST",
    W: "WEST",
    NE: "NORTHEAST",
    NW: "NORTHWEST",
    SE: "SOUTHEAST",
    SW: "SOUTHWEST",
  };

  let tokens = out.split(" ").filter(Boolean);
  if (tokens.length > 1 && dirMap[tokens[0]]) {
    tokens[0] = dirMap[tokens[0]]; // e.g. "W RUTLAND SQ" → "WEST RUTLAND SQ"
    out = tokens.join(" ");
  }

  // Expand suffixes (PL → PLACE, SQ → SQUARE, AVE → AVENUE, etc.)
  for (const [abbr, full] of Object.entries(ADDRESS_SUFFIX_MAP)) {
    const simple = abbr.replace(/\./g, "").trim(); // e.g. "PL"
    if (out.endsWith(simple)) {
      out = out.slice(0, out.length - simple.length) + full;
      break;
    }
  }

  // Final cleanup: collapse any accidental double spaces
  out = out.replace(/\s+/g, " ").trim();

  return out;
}

// 👇👇👇 ADD THIS NEW HELPER RIGHT HERE 👇👇👇
function normStreetForIndex(street) {
  if (!street) return "";
  // Reuse the same logic your address index keys use
  return normalizeStreetNameForAddress(street);
}

function normalizeZipForAddress(zip) {
  if (!zip) return "";
  let z = String(zip).trim();
  
}


function normalizeZipForAddress(zip) {
  if (!zip) return "";
  let z = String(zip).trim();

  // Strip ZIP+4 (e.g. "02114-1234" -> "02114")
  const dashIndex = z.indexOf("-");
  if (dashIndex !== -1) {
    z = z.slice(0, dashIndex);
  }

  // Remove all non-digits
  z = z.replace(/\D/g, "");

  // Ensure 5-digit format
  if (z.length === 5) return z;

  // If it's too short/padded, we still return it safely
  if (z.length < 5) return z.padStart(5, "0");

  // If it's too long, truncate to 5
  return z.slice(0, 5);
}

// KEY FORMAT for address index: "123|MAIN STREET|02134"
function buildAddressIndexKey(number, street, zip) {
  if (!number || !street || !zip) return null;
  const num = normalizeHouseNumberForAddress(number);
  const st = normalizeStreetNameForAddress(street);
  const z = normalizeZipForAddress(zip);
  if (!num || !st || !z) return null;
  return `${num}|${st}|${z}`;
}

// -------------------------------------------------------
// Extract MLS listing address parts
// -------------------------------------------------------

function extractListingAddressParts(listing) {
  const addr = listing.address || {};

  const number =
    cleanString(listing.streetNumber) ||
    cleanString(addr.streetNumber) ||
    cleanString(listing.STREET_NO) ||
    cleanString(listing.houseNumber) ||
    null;

  const rawStreet =
    cleanString(listing.streetName) ||
    cleanString(addr.streetName) ||
    cleanString(listing.STREET_NAME) ||
    cleanString(listing.street) ||
    cleanString(listing.ST_NAME) ||
    null;

  let street = rawStreet;
  if (street) {
    street = street.replace(
      /\s+(UNIT|APT|APARTMENT|FLR|FLOOR|REAR|FRONT|PH)\b.*$/i,
      ""
    );
    street = street.replace(/\s+#\s*\S+$/i, "");
    street = street.trim();
  }

  const zip =
    normalizeZip(
      cleanString(listing.zipCode) ||
        cleanString(addr.zipCode) ||
        cleanString(listing.ZIP_CODE) ||
        cleanString(listing.zip) ||
        cleanString(listing.ZIP)
    ) || null;

  const county =
    cleanString(listing.county) ||
    cleanString(addr.county) ||
    null;

  const state =
    cleanString(listing.state) ||
    cleanString(addr.state) ||
    "MA";

  return { number, street, zip, county, state };
}

// -------------------------------------------------------
// Distance / geo helpers
// -------------------------------------------------------

function haversineMeters(lat1, lng1, lat2, lng2) {
  const R = 6371000;
  const toRad = (deg) => (deg * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) *
      Math.cos(toRad(lat2)) *
      Math.sin(dLng / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

// -------------------------------------------------------
// Geocoding cache + API (Tier 4)
// -------------------------------------------------------

async function loadGeocodeCache() {
  try {
    const txt = await fsp.readFile(GEOCODE_CACHE_PATH, "utf8");
    return JSON.parse(txt);
  } catch {
    return Object.create(null);
  }
}

async function saveGeocodeCache(cache) {
  try {
    await fsp.writeFile(
      GEOCODE_CACHE_PATH,
      JSON.stringify(cache, null, 2),
      "utf8"
    );
  } catch (err) {
    console.warn("[attachCoords] Failed to write geocode cache:", err.message);
  }
}

let geocodeCalls = 0;

async function geocodeAddressCached(addrKey, query) {
  if (!addrKey || !query) return null;

  if (globalThis.__geocodeCache?.[addrKey]) {
    return globalThis.__geocodeCache[addrKey];
  }

  if (geocodeCalls >= MAX_GEOCODER_CALLS_PER_RUN) {
    return null;
  }

  geocodeCalls++;

  const url = new URL(GEOCODER_BASE_URL);
  url.searchParams.set("q", query);
  url.searchParams.set("format", "json");
  url.searchParams.set("limit", "1");
  url.searchParams.set("addressdetails", "0");

  try {
    const res = await fetch(url.toString(), {
      headers: { "User-Agent": GEOCODER_USER_AGENT },
    });
    if (!res.ok) return null;

    const data = await res.json();
    if (!Array.isArray(data) || data.length === 0) return null;

    const best = data[0];
    const lat = parseFloat(best.lat);
    const lon = parseFloat(best.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;

    const result = { lat, lng: lon };
    globalThis.__geocodeCache[addrKey] = result;
    return result;
  } catch {
    return null;
  }
}

// -------------------------------------------------------
// Spatial grid index over parcel centroids (Tier 4 search)
// -------------------------------------------------------

function buildParcelGrid(parcelIndex) {
  const grid = Object.create(null);
  const cellSize = 0.01;

  for (const [key, pt] of Object.entries(parcelIndex)) {
    const { lat, lng } = pt;
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;

    const latIdx = Math.floor(lat / cellSize);
    const lngIdx = Math.floor(lng / cellSize);
    const cellKey = `${latIdx}:${lngIdx}`;

    if (!grid[cellKey]) grid[cellKey] = [];
    grid[cellKey].push({ lat, lng, key });
  }

  return { grid, cellSize };
}

function findNearestParcelInGrid(
  gridInfo,
  parcelIndex,
  lat,
  lng,
  maxDistanceMeters = 200
) {
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;

  const { grid, cellSize } = gridInfo;

  const latIdx = Math.floor(lat / cellSize);
  const lngIdx = Math.floor(lng / cellSize);

  let best = null;
  let bestDist = Infinity;

  for (let di = -1; di <= 1; di++) {
    for (let dj = -1; dj <= 1; dj++) {
      const cellKey = `${latIdx + di}:${lngIdx + dj}`;
      const bucket = grid[cellKey];
      if (!bucket) continue;

      for (const entry of bucket) {
        const d = haversineMeters(lat, lng, entry.lat, entry.lng);
        if (d < bestDist) {
          bestDist = d;
          best = entry;
        }
      }
    }
  }

  if (!best || bestDist > maxDistanceMeters) return null;

  const parcel = parcelIndex[best.key];
  if (!parcel) return null;

  return {
    lat: parcel.lat,
    lng: parcel.lng,
    key: best.key,
    distance: bestDist,
  };
}

function buildPrefixIndex(parcelIndex) {
  const prefixIndex = Object.create(null);

  for (const [key, value] of Object.entries(parcelIndex)) {
    const prefix = computePrefixFromParcelKey(key);
    if (!prefix) continue;

    const existing = prefixIndex[prefix];
    if (!existing) {
      prefixIndex[prefix] = {
        lat: value.lat,
        lng: value.lng,
        count: 1,
      };
    } else {
      existing.count++;
    }
  }

  return prefixIndex;
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

  if (!fs.existsSync(ADDRESS_INDEX_PATH)) {
    console.error(
      "❌ Missing address index file:",
      ADDRESS_INDEX_PATH,
      "\n   → Run publicData/addresses/buildAddressIndex.js first."
    );
    process.exit(1);
  }

  console.log("[attachCoords] Loading parcel centroid index…");
  const rawParcelIndex = await fsp.readFile(PARCEL_INDEX_PATH, "utf8");
  const parcelIndex = JSON.parse(rawParcelIndex);
  console.log(
    `[attachCoords] Loaded ${Object.keys(parcelIndex).length.toLocaleString()} parcel address keys.`
  );

  console.log("[attachCoords] Loading MassGIS address index (address points)…");
  const rawAddressIdx = await fsp.readFile(ADDRESS_INDEX_PATH, "utf8");
  const addrJson = JSON.parse(rawAddressIdx);
  const addressIndex = addrJson.index || addrJson;
  console.log(
    `[attachCoords] Loaded ${Object.keys(addressIndex).length.toLocaleString()} address-point keys.`
  );

  console.log("[attachCoords] Building prefix index (Tier 2)…");
  const prefixIndex = buildPrefixIndex(parcelIndex);
  console.log(
    `[attachCoords] Prefix index built with ${Object.keys(prefixIndex).length.toLocaleString()} prefixes.`
  );

  console.log("[attachCoords] Building parcel spatial grid (Tier 4)…");
  const gridInfo = buildParcelGrid(parcelIndex);
  console.log("[attachCoords] Parcel grid ready.");

  globalThis.__geocodeCache = await loadGeocodeCache();

  const rl = readline.createInterface({
    input: fs.createReadStream(INPUT_PATH, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  const outStream = fs.createWriteStream(OUTPUT_PATH, { encoding: "utf8" });

  const stats = {
    total: 0,
    matchedParcelDirect: 0,
    matchedParcelPrefix: 0,
    matchedAddressPoint: 0,
    matchedParcelGeocodeGrid: 0,
    noMatch: 0,
  };

  let debugLogged = 0;

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

    // ⭐ DO NOT overwrite existing coordinates
    if (
      listing.latitude != null &&
      listing.longitude != null &&
      Number.isFinite(listing.latitude) &&
      Number.isFinite(listing.longitude)
    ) {



      const enrichedExisting = {
        ...listing,
        coordSource: listing.coordSource || "existing",
      };
      outStream.write(JSON.stringify(enrichedExisting) + "\n");
      continue;
    }

    const addrParts = extractListingAddressParts(listing);
    const parcelKey = buildParcelAddressKey(addrParts);

    let lat = null;
    let lng = null;
    let coordSource = "none";

    // TIER 1: Direct parcel key
    if (parcelKey && parcelIndex[parcelKey]) {
      lat = parcelIndex[parcelKey].lat;
      lng = parcelIndex[parcelKey].lng;
      coordSource = "parcel_direct";
      stats.matchedParcelDirect++;
    } else {
      // TIER 2: Unique prefix
      const prefix = buildParcelPrefix(addrParts);
      const prefixEntry = prefix ? prefixIndex[prefix] : null;

      if (prefixEntry && prefixEntry.count === 1) {
        lat = prefixEntry.lat;
        lng = prefixEntry.lng;
        coordSource = "parcel_prefix";
        stats.matchedParcelPrefix++;
      } else {
        // -------------------------------------------------------------
        // TIER 3 — MassGIS Address Points index
        //          (exact match + letter-stripped house number)
        // -------------------------------------------------------------
        let addrKeyForIndex = null;

        if (addrParts.number && addrParts.street && addrParts.zip) {
          addrKeyForIndex = buildAddressIndexKey(
            addrParts.number,
            addrParts.street,
            addrParts.zip
          );
        }

        // If no direct match, try stripping a trailing letter from house number (e.g. 7A → 7)
        if (!addrKeyForIndex || !addressIndex[addrKeyForIndex]) {
          const numRaw = (addrParts.number || "").toString().toUpperCase();
          const letterMatch = /^([0-9]+)([A-Z])$/.exec(numRaw);

          if (letterMatch) {
            const baseNum = letterMatch[1]; // "7A" → "7"
            const fallbackKey = buildAddressIndexKey(
              baseNum,
              addrParts.street,
              addrParts.zip
            );

            if (fallbackKey && addressIndex[fallbackKey]) {
              if (debugLogged < 3) {
                console.log(
                  "[attachCoords] Tier3 letter-stripped match:",
                  fallbackKey
                );
                debugLogged++;
              }
              addrKeyForIndex = fallbackKey;
              // mark that this came from the letter-stripped path;
              // we'll keep this if nothing else overwrites coordSource
              coordSource = "address_point_letter_stripped";
            }
          }
        }

        const addrRecord =
          addrKeyForIndex && addressIndex[addrKeyForIndex]
            ? addressIndex[addrKeyForIndex]
            : null;

        if (addrRecord) {
          const apLat =
            typeof addrRecord.lat === "number"
              ? addrRecord.lat
              : addrRecord.latitude;
          const apLng =
            typeof addrRecord.lng === "number"
              ? addrRecord.lng
              : typeof addrRecord.lon === "number"
              ? addrRecord.lon
              : addrRecord.longitude;

          if (Number.isFinite(apLat) && Number.isFinite(apLng)) {
            lat = apLat;
            lng = apLng;

            // Only override coordSource if nothing set yet
            if (coordSource === "none" || coordSource === "address_point") {
              coordSource =
                coordSource === "address_point_letter_stripped"
                  ? "address_point_letter_stripped"
                  : "address_point";
            }

            stats.matchedAddressPoint++;

            if (debugLogged < 3) {
              console.log("[attachCoords] Tier3 address-point match:", {
                addrKeyForIndex,
                addrParts,
                apLat,
                apLng,
              });
              debugLogged++;
            }
          }
        }

        // TIER 3C — Street Name Variant Normalization (universal engine)
        // Handles things like:
        //  - "LAGRANGE" → "LAGRANGE STREET"
        //  - "E 2ND ST" → "EAST SECOND STREET"
        //  - "E 8TH ST" → "EAST EIGHTH STREET"
        //  - "63 BEECH GLN" → "63 BEECH GLEN WAY/ROAD/STREET/..."
        //  - "653-R E 3RD ST" → "653 EAST THIRD STREET"
        //  - abbreviations like GLN → GLEN, HWY → HIGHWAY, PL → PLACE, etc.
        if (
          !lat &&
          !lng &&
          addrParts &&
          addrParts.street &&
          addrParts.number &&
          addrParts.zip
        ) {
          const rawStreet = String(addrParts.street).trim();
          const rawNumber = String(addrParts.number).trim();
          const rawZip = String(addrParts.zip).trim();

          if (rawStreet && rawNumber && rawZip) {
            const DIR_MAP = {
              E: "EAST",
              W: "WEST",
              N: "NORTH",
              S: "SOUTH",
            };

            const ORDINAL_MAP = {
              "1ST": "FIRST",
              "2ND": "SECOND",
              "3RD": "THIRD",
              "4TH": "FOURTH",
              "5TH": "FIFTH",
              "6TH": "SIXTH",
              "7TH": "SEVENTH",
              "8TH": "EIGHTH",
              "9TH": "NINTH",
              "10TH": "TENTH",
              "11TH": "ELEVENTH",
              "12TH": "TWELFTH",
              "13TH": "THIRTEENTH",
            };

            // Common suffix words MassGIS uses
            const SUFFIXES = [
              "STREET",
              "ROAD",
              "AVENUE",
              "LANE",
              "DRIVE",
              "PLACE",
              "TERRACE",
              "COURT",
              "CIRCLE",
              "HIGHWAY",
              "WAY",
              "PATH",
            ];

            // Abbreviations → full words (for middle tokens)
            const ABBR_MAP = {
              HWY: "HIGHWAY",
              PL: "PLACE",
              GLN: "GLEN",
              CIR: "CIRCLE",
              TER: "TERRACE",
              SQ: "SQUARE",
              CTR: "CENTER",
            };

            // Short suffix forms → canonical suffix
            const SUFFIX_CANON = {
              ST: "STREET",
              "ST.": "STREET",
              RD: "ROAD",
              "RD.": "ROAD",
              AVE: "AVENUE",
              "AVE.": "AVENUE",
              PL: "PLACE",
              "PL.": "PLACE",
              LN: "LANE",
              "LN.": "LANE",
              DR: "DRIVE",
              "DR.": "DRIVE",
              HWY: "HIGHWAY",
              "HWY.": "HIGHWAY",
              CIR: "CIRCLE",
              "CIR.": "CIRCLE",
              TER: "TERRACE",
              "TER.": "TERRACE",
            };

            function tokenizeStreet(str) {
              return str
                .toUpperCase()
                .replace(/[.,]/g, " ")
                .replace(/\s+/g, " ")
                .trim()
                .split(" ")
                .filter(Boolean);
            }

            // 1) Normalize street tokens (direction, ordinals, abbreviations, suffix)
            let tokens = tokenizeStreet(rawStreet);
            if (tokens.length > 0) {
              // Direction at the front: E/W/N/S → full word
              const first = tokens[0];
              if (DIR_MAP[first]) {
                tokens[0] = DIR_MAP[first];
              }

              // Ordinal tokens (2ND → SECOND, 8TH → EIGHTH, etc.)
              tokens = tokens.map((t) => {
                const mapped = ORDINAL_MAP[t];
                return mapped ? mapped : t;
              });

              // Abbreviations and suffix canonicalization
              if (tokens.length > 0) {
                const lastIdx = tokens.length - 1;
                const last = tokens[lastIdx];

                // If last token is an abbreviation like GLN, HWY, PL, etc.
                if (ABBR_MAP[last]) {
                  tokens[lastIdx] = ABBR_MAP[last];
                }

                // Then normalize suffix (ST → STREET, RD → ROAD, etc.)
                const last2 = tokens[lastIdx];
                if (SUFFIX_CANON[last2]) {
                  tokens[lastIdx] = SUFFIX_CANON[last2];
                }
              }

              const baseStreet = tokens.join(" ");
              const normalizedZip = normalizeZipForAddress(rawZip);

              // 2) Strip trailing letters from house number (653-R → 653)
              let baseNum = rawNumber;
              const numMatch =
                /^(\d+)(?:[A-Z\-\/ ]+)?$/i.exec(rawNumber);
              if (numMatch) {
                baseNum = numMatch[1];
              }

              // 3) Build candidate street variants
              const candidateStreets = new Set();

              // Always include the normalized base street
              if (baseStreet) {
                candidateStreets.add(baseStreet);
              }

              const lastToken = tokens[tokens.length - 1];
              const hasSuffix = SUFFIXES.includes(lastToken);

              // If we don't see a real suffix at the end, try adding all common suffixes
              if (!hasSuffix) {
                SUFFIXES.forEach((suf) => {
                  candidateStreets.add(`${baseStreet} ${suf}`);
                });

                // Also try dropping the last token as if it was a pseudo-suffix:
                // e.g. "BEECH GLN" → try "BEECH GLEN STREET", "BEECH GLEN WAY", etc.
                if (tokens.length > 1) {
                  const noLast = tokens.slice(0, -1).join(" ");
                  SUFFIXES.forEach((suf) => {
                    candidateStreets.add(`${noLast} ${suf}`);
                  });
                }
              }

              let variantHit = null;

              let variantCheckCount = 0;


              // 4) Try variants against addressIndex (first with stripped number, then full)
for (const streetVariant of candidateStreets) {
  // ⛔ STOP FREEZE: cap variant attempts per listing
  variantCheckCount++;
  if (variantCheckCount > 50) {
    break; // too many variants → stop checking
  }

  const normalizedVariantStreet = normStreetForIndex(streetVariant);

  // First: base number (e.g. 653 from 653-R)
  const keyBase = buildAddressIndexKey(
    baseNum,
    normalizedVariantStreet,
    normalizedZip
  );
  const recBase = addressIndex[keyBase];

  if (recBase) {
    lat = recBase.lat;
    lng = recBase.lon;
    coordSource = "address_point_variant";
    variantHit = {
      originalStreet: addrParts.street,
      correctedStreet: streetVariant,
      variantKey: keyBase,
    };
    break;
  }

  // Second: full MLS number (e.g. 653-R) in case MassGIS stored it that way
  if (baseNum !== rawNumber) {
    const keyFull = buildAddressIndexKey(
      rawNumber,
      normalizedVariantStreet,
      normalizedZip
    );
    const recFull = addressIndex[keyFull];
    if (recFull) {
      lat = recFull.lat;
      lng = recFull.lon;
      coordSource = "address_point_variant";
      variantHit = {
        originalStreet: addrParts.street,
        correctedStreet: streetVariant,
        variantKey: keyFull,
      };
      break;
    }
  }
}


              if (variantHit) {
                stats.matchedAddressPoint++;

                if (debugLogged < 15) {
                  console.log(
                    "[attachCoords] Tier3C street-variant match:",
                    variantHit
                  );
                  debugLogged++;
                }

                // APPLY Tier 3C RESULT → Assign coordinates if variant exists
                if (
                  variantHit.variantKey &&
                  addressIndex[variantHit.variantKey]
                ) {
                  const v = addressIndex[variantHit.variantKey];

                  lat = v.lat;
                  lng = v.lon;
                  coordSource = "address_point_variant";

                  if (debugLogged < 15) {
                    console.log(
                      "[attachCoords] Tier3C variant coordinate applied:",
                      {
                        variantKey: variantHit.variantKey,
                        lat: v.lat,
                        lon: v.lon,
                      }
                    );
                    debugLogged++;
                  }
                }
              }
            }
          }
        }
        // -------------------------------------------------------------
// SAFETY SKIP: Avoid freezing on invalid or incomplete addresses
// -------------------------------------------------------------
const badNumber = !addrParts.number || /^[0\s]/.test(addrParts.number) || /LOT/i.test(addrParts.number);
const badStreet =
  !addrParts.street ||
  /LOT/i.test(addrParts.street) ||
  /UNIT/i.test(addrParts.street) ||
  /BLDG/i.test(addrParts.street) ||
  /BUILDING/i.test(addrParts.street) ||
  /CONDO/i.test(addrParts.street) ||
  /^[\s,]+$/.test(addrParts.street);

if (badNumber || badStreet) {
  // Skip Tier 4 entirely — prevent infinite geocode/grid loop
  if (debugLogged < 10) {
    console.log("[attachCoords] Skipping Tier 4 for malformed address:", {
      number: addrParts.number,
      street: addrParts.street,
      zip: addrParts.zip
    });
    debugLogged++;
  }

  stats.noMatch++;
  unmatched.push(listing);
  continue;
}


        // -------------------------------------------------------------
        // TIER 4: Geocode + nearest parcel grid
        // -------------------------------------------------------------
        if (coordSource === "none") {
          const baseStreet = normalizeStreetNameForAddress(addrParts.street);
          if (addrParts.number && baseStreet) {
            const stableKey = buildAddressIndexKey(
              addrParts.number,
              baseStreet,
              addrParts.zip
            );

            const queryParts = [];
            queryParts.push(`${addrParts.number} ${baseStreet}`);
            if (addrParts.zip) queryParts.push(addrParts.zip);
            if (addrParts.state) queryParts.push(addrParts.state);
            if (addrParts.county)
              queryParts.push(`${addrParts.county} County`);
            const query = queryParts.join(", ");

            const geocoded = await geocodeAddressCached(stableKey, query);

            if (geocoded) {
              const nearest = findNearestParcelInGrid(
                gridInfo,
                parcelIndex,
                geocoded.lat,
                geocoded.lng,
                200
              );
              if (nearest) {
                lat = nearest.lat;
                lng = nearest.lng;
                coordSource = "parcel_geocode_grid";
                stats.matchedParcelGeocodeGrid++;
              }
            }
          }

          // ⭐ RESCUE BLOCK – ONLY if Tier4 didn't get us coords
          if (!lat && !lng) {
            function rescueAddressParts(parts) {
              let { number, street, zip } = parts;

              if (street) {
                street = street.replace(/,/g, "").trim();

                if (/\s+EXT$/i.test(street)) {
                  street = street.replace(/\s+EXT$/i, " EXTENSION");

                  
                }

                const rescueSuffixes = {
                  CIR: "CIRCLE",
                  "CIR.": "CIRCLE",
                  LN: "LANE",
                  "LN.": "LANE",
                  SQ: "SQUARE",
                  "SQ.": "SQUARE",
                  BLVD: "BOULEVARD",
                  "BLVD.": "BOULEVARD",
                };

                const tokens = street.toUpperCase().split(/\s+/).filter(Boolean);
                if (tokens.length) {
                  const last = tokens[tokens.length - 1];
                  if (rescueSuffixes[last]) {
                    tokens[tokens.length - 1] = rescueSuffixes[last];
                    street = tokens.join(" ");
                  }
                }
              }

              if (number) {
                const m = String(number).toUpperCase().match(/\d+/);
                if (m) number = m[0];
              }

              const rescuedKey =
                number && street && zip
                  ? `${number}|${street.toUpperCase()}|${zip}`
                  : null;

              return { rescuedKey, number, street };
            }

            const rescue = rescueAddressParts(addrParts);
            if (rescue.rescuedKey && addressIndex[rescue.rescuedKey]) {
              const hit = addressIndex[rescue.rescuedKey];
              if (Number.isFinite(hit.lat) && Number.isFinite(hit.lng)) {
                lat = hit.lat;
                lng = hit.lng;
                coordSource = "address_point_rescue";
                stats.matchedAddressPoint++;
              }
            }

            if (!lat && !lng) {
              stats.noMatch++;

              unmatched.push({
                listingId: listing.LIST_NO || listing.id || null,
                address: listing.address || null,
                cleaned: addrParts,
                parcelKey,
                rescueAttemptKey: rescue.rescuedKey || null
});

              if (debugLogged < 1) {
                console.log(
                  "[attachCoords] No match after Tier 4 + RESCUE:",
                  {
                    parcelKey,
                    prefix,
                    addrKeyForIndex,
                    addrParts,
                    rescueAttemptKey: rescue.rescuedKey,
                  }
                );
                debugLogged++;
              }
            }
          }
        }
      }
    }
    // ⭐ FINAL SAFE GEOCODE-ONLY FALLBACK (using your real geocoder)
if (!lat && !lng) {
  const qparts = [];
  if (addrParts.number && addrParts.street)
    qparts.push(`${addrParts.number} ${addrParts.street}`);
  if (addrParts.zip) qparts.push(addrParts.zip);
  qparts.push("MA");

  const query = qparts.join(", ");
  const stableKey = `${addrParts.number}|${addrParts.street}|${addrParts.zip}`;

  // ✔ use your real geocoder
  const geo = await geocodeAddressCached(stableKey, query);

  if (geo) {
    lat = geo.lat;
    lng = geo.lng;
    coordSource = "geocode_only";
  }
}


    const enriched = {
      ...listing,
      latitude: lat,
      longitude: lng,
      coordSource,
    };

    outStream.write(JSON.stringify(enriched) + "\n");

    if (stats.total % 10000 === 0) {
      const totalMatched =
        stats.matchedParcelDirect +
        stats.matchedParcelPrefix +
        stats.matchedAddressPoint +
        stats.matchedParcelGeocodeGrid;

      console.log(
        `  processed ${stats.total.toLocaleString()} listings… (matched ${totalMatched.toLocaleString()})`
      );
    }
  }

  outStream.end();

  // Save unmatched addresses
try {
  const unmatchedPath = path.join(MLS_DIR, "unmatched_addresses.json");
  await fsp.writeFile(
    unmatchedPath,
    JSON.stringify(unmatched, null, 2),
    "utf8"
  );
  console.log(`[attachCoords] Unmatched written → ${unmatchedPath}`);
} catch (err) {
  console.error("[attachCoords] Failed to write unmatched list:", err.message);
}


  await saveGeocodeCache(globalThis.__geocodeCache);

  const totalMatched =
    stats.matchedParcelDirect +
    stats.matchedParcelPrefix +
    stats.matchedAddressPoint +
    stats.matchedParcelGeocodeGrid;

  console.log("----------------------------------------------------");
  console.log(
    `Listings processed:                      ${stats.total.toLocaleString()}`
  );
  console.log(
    `Listings w/ parcel coords (direct):      ${stats.matchedParcelDirect.toLocaleString()}`
  );
  console.log(
    `Listings w/ parcel coords (prefix):      ${stats.matchedParcelPrefix.toLocaleString()}`
  );
  console.log(
    `Listings w/ coords (address points):     ${stats.matchedAddressPoint.toLocaleString()}`
  );
  console.log(
    `Listings w/ parcel coords (geocode+grid): ${stats.matchedParcelGeocodeGrid.toLocaleString()}`
  );
  console.log(
    `Total listings w/ coords:                ${totalMatched.toLocaleString()}`
  );
  console.log(
    `Listings w/ no match:                    ${stats.noMatch.toLocaleString()}`
  );
  console.log(
    `Geocoder calls this run:                 ${geocodeCalls.toLocaleString()}`
  );
  console.log("Output written →", OUTPUT_PATH);
  console.log("Geocode cache →", GEOCODE_CACHE_PATH);
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ Fatal error attaching coordinates:", err);
  process.exit(1);
});
