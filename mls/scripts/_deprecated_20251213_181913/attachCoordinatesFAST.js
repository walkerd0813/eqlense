// backend/mls/scripts/attachCoordinates_FAST.js
// -------------------------------------------------------
// FAST coordinate attachment for MLS listings
//
// Uses ONLY:
//   - Tier 1: Direct parcel key match (parcelCentroidIndex.json)
//   - Tier 2: Unique prefix match (NUMBER + STREET → one parcel)
//   - Tier 3: MassGIS Address Points (addressIndex.json)
//
// Skips ALL slow logic:
//   - No Tier 3C universal variant engine
//   - No Tier 4 geocoder
//   - No parcel spatial grid
//
// IMPORTANT:
//   - Reuses the SAME normalization rules as your main engine
//     for parcel keys and address keys, so it lines up with
//     parcelCentroidIndex.json and addressIndex.json exactly.
//   - Does NOT overwrite any listings that already have
//     latitude/longitude — it just passes them through.
//
// INPUT:
//   backend/mls/normalized/listings.ndjson
//   backend/publicData/parcels/parcelCentroidIndex.json
//   backend/publicData/addresses/addressIndex.json
//
// OUTPUT:
//   backend/mls/normalized/listingsWithCoords_FAST.ndjson   (matched)
//   backend/mls/normalized/unmatched_FAST.ndjson           (no match yet)
//
// -------------------------------------------------------

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ================= PATHS =================
const INPUT_PATH = path.join(
  __dirname,
  "../../mls/normalized/listings.ndjson"
);

const OUTPUT_MATCHED = path.join(
  __dirname,
  "../../mls/normalized/listingsWithCoords_FAST.ndjson"
);

const OUTPUT_UNMATCHED
 = path.join(
  __dirname,
  "../../mls/normalized/unmatched_FAST.ndjson"
);


if (!fs.existsSync(INPUT_PATH)) {
  console.error("❌ FAST attach aborted — canonical listings.ndjson not found");
  process.exit(1);
}

// ================= STREAMS =================
const outMatched = fs.createWriteStream(OUTPUT_MATCHED, { flags: "w" });
const outUnmatched = fs.createWriteStream(OUTPUT_UNMATCHED, { flags: "w" });

// ================= LOOKUP DATA =================
const PARCELS_DIR = path.resolve(__dirname, "../../publicData/parcels");
const PARCEL_INDEX_PATH = path.join(PARCELS_DIR, "parcelCentroidIndex.json");

const ADDRESSES_DIR = path.resolve(__dirname, "../../publicData/addresses");
const ADDRESS_INDEX_PATH = path.join(ADDRESSES_DIR, "addressIndex.json");

// -------------------------------------------------------
// Shared helpers (copied from your main script)
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
// ADDRESS POINT key normalization (for addressIndex.json)
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
// Extract basic MLS listing address parts (FAST version)
// -------------------------------------------------------

function extractListingAddressParts(listing) {
  const addr = listing.address || {};

  const number =
    cleanString(addr.streetNumber) ||
    cleanString(listing?.raw?.row?.STREET_NUM) ||
    null;

  let street =
    cleanString(addr.streetName) ||
    cleanString(listing?.raw?.row?.STREET_NAME) ||
    null;

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
      cleanString(addr.zip) ||
        cleanString(listing?.raw?.row?.ZIP_CODE)
    ) || null;

  const county = cleanString(addr.county) || null;
  const state = cleanString(addr.state) || "MA";

  return { number, street, zip, county, state };
}


// -------------------------------------------------------
// MAIN FAST ATTACH
// -------------------------------------------------------

async function main() {
  console.log("====================================================");
  console.log("  FAST ATTACH COORDINATES (Tier1–Tier3 ONLY)");
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

  console.log("[FAST] Loading parcel centroid index…");
  const rawParcelIndex = await fsp.readFile(PARCEL_INDEX_PATH, "utf8");
  const parcelIndex = JSON.parse(rawParcelIndex);
  console.log(
    `[FAST] Loaded ${Object.keys(parcelIndex).length.toLocaleString()} parcel address keys.`
  );

  console.log("[FAST] Building prefix index (Tier 2)...");
  const prefixIndex = buildPrefixIndex(parcelIndex);
  console.log(
    `[FAST] Prefix index built with ${Object.keys(prefixIndex).length.toLocaleString()} prefixes.`
  );

  console.log("[FAST] Loading MassGIS address index (address points)…");
  const rawAddressIdx = await fsp.readFile(ADDRESS_INDEX_PATH, "utf8");
  const addrJson = JSON.parse(rawAddressIdx);
  const addressIndex = addrJson.index || addrJson;
  console.log(
    `[FAST] Loaded ${Object.keys(addressIndex).length.toLocaleString()} address-point keys.`
  );

  const rl = readline.createInterface({
    input: fs.createReadStream(INPUT_PATH),
    crlfDelay: Infinity,
  });

  const outGood = fs.createWriteStream(OUTPUT_MATCHED, { flags: "w" });
  const outBad = fs.createWriteStream(OUTPUT_UNMATCHED, { flags: "w" });


  const stats = {
    total: 0,
    parseErrors: 0,
    alreadyGeocoded: 0,
    matchedParcelDirect: 0,
    matchedParcelPrefix: 0,
    matchedAddressPoint: 0,
    matched: 0,
    noMatch: 0,
  };

  for await (const line of rl) {
    if (!line.trim()) continue;
    let listing;
    try {
      listing = JSON.parse(line);
    } catch {
      stats.parseErrors++;
      continue;
    }

    stats.total++;

    // If this listing already has coordinates, keep them (do not overwrite)
    const hasExistingCoords =
      typeof listing.latitude === "number" &&
      typeof listing.longitude === "number";

    if (hasExistingCoords) {
      stats.alreadyGeocoded++;
      outGood.write(JSON.stringify(listing) + "\n");
      continue;
    }

    const addrParts = extractListingAddressParts(listing);
    let lat = null;
    let lng = null;
    let coordSource = null;

    const parcelKey = buildParcelAddressKey(addrParts);

    // -------------------------
    // Tier 1: Direct parcel key
    // -------------------------
    if (parcelKey && parcelIndex[parcelKey]) {
      lat = parcelIndex[parcelKey].lat;
      lng = parcelIndex[parcelKey].lng;
      coordSource = "parcel_direct";
      stats.matchedParcelDirect++;
    } else {
      // -------------------------
      // Tier 2: Unique parcel prefix
      // -------------------------
      const prefix = buildParcelPrefix(addrParts);
      const prefixEntry = prefix ? prefixIndex[prefix] : null;

      if (prefixEntry && prefixEntry.count === 1) {
        lat = prefixEntry.lat;
        lng = prefixEntry.lng;
        coordSource = "parcel_prefix";
        stats.matchedParcelPrefix++;
      } else {
        // -------------------------
        // Tier 3: Address points
        // -------------------------
        let addrKeyForIndex = null;

        if (addrParts.number && addrParts.street && addrParts.zip) {
          addrKeyForIndex = buildAddressIndexKey(
            addrParts.number,
            addrParts.street,
            addrParts.zip
          );

          // If no direct address-point match, try stripping trailing letter from house number (7A → 7)
          if (!addrKeyForIndex || !addressIndex[addrKeyForIndex]) {
            const numRaw = (addrParts.number || "")
              .toString()
              .toUpperCase();
            const letterMatch = /^([0-9]+)([A-Z])$/.exec(numRaw);

            if (letterMatch) {
              const baseNum = letterMatch[1]; // "7A" → "7"
              const fallbackKey = buildAddressIndexKey(
                baseNum,
                addrParts.street,
                addrParts.zip
              );

              if (fallbackKey && addressIndex[fallbackKey]) {
                addrKeyForIndex = fallbackKey;
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

            if (
              typeof apLat === "number" &&
              typeof apLng === "number"
            ) {
              lat = apLat;
              lng = apLng;
              if (!coordSource) {
                coordSource = "address_point";
              }
              stats.matchedAddressPoint++;
            }
          }
        }
      }
    }

    if (lat != null && lng != null) {
      stats.matched++;
      const enriched = {
        ...listing,
        latitude: lat,
        longitude: lng,
        coordSource,
      };
      outGood.write(JSON.stringify(enriched) + "\n");
    } else {
      stats.noMatch++;
      outBad.write(JSON.stringify(listing) + "\n");
    }

    if (stats.total % 5000 === 0) {
      console.log(
        `[FAST] Processed ${stats.total.toLocaleString()} listings... ` +
          `${stats.matched.toLocaleString()} matched, ` +
          `${stats.noMatch.toLocaleString()} unmatched`
      );
    }
  }

  outGood.end();
  outBad.end();

  console.log("====================================================");
  console.log(" FAST ATTACH COMPLETE");
  console.log(
    `  Total listings:           ${stats.total.toLocaleString()}`
  );
  console.log(
    `  Already had coords:       ${stats.alreadyGeocoded.toLocaleString()}`
  );
  console.log(
    `  Tier1 parcel_direct:      ${stats.matchedParcelDirect.toLocaleString()}`
  );
  console.log(
    `  Tier2 parcel_prefix:      ${stats.matchedParcelPrefix.toLocaleString()}`
  );
  console.log(
    `  Tier3 address_point:      ${stats.matchedAddressPoint.toLocaleString()}`
  );
  console.log(
    `  Matched (any fast tier):  ${stats.matched.toLocaleString()}`
  );
  console.log(
    `  Unmatched (FAST only):    ${stats.noMatch.toLocaleString()}`
  );
  console.log("  Output (matched):        ", (OUTPUT_MATCHED)
);
  console.log("  Output (unmatched):      ", (OUTPUT_UNMATCHED)
);
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ Fatal error in FAST attach:", err);
  process.exit(1);
});
