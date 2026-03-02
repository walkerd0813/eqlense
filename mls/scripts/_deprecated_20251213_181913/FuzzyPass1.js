// backend/mls/scripts/FuzzyPass1.js
// ============================================================
// FUZZY PASS 1 (RELAXED) — internal-only recovery for unmatched_FAST
// Uses: parcelCentroidIndex.json + addressIndex.json
// Output:
//  - mls/normalized/pass1_newMatches.ndjson (new matches only)
//  - mls/normalized/unmatched_PASS1.ndjson (still unmatched)
//  - mls/normalized/listingsWithCoords_PASS1.ndjson (FAST matched + new matches)
// ============================================================

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const NORM_DIR = path.resolve(__dirname, "../normalized");

const FAST_MATCHED = path.join(NORM_DIR, "listingsWithCoords_FAST.ndjson");
const FAST_UNMATCHED = path.join(NORM_DIR, "unmatched_FAST.ndjson");

const PASS1_MATCHED = path.join(NORM_DIR, "listingsWithCoords_PASS1.ndjson");
const PASS1_UNMATCHED = path.join(NORM_DIR, "unmatched_PASS1.ndjson");
const PASS1_NEW = path.join(NORM_DIR, "pass1_newMatches.ndjson");

const PARCEL_INDEX_PATH = path.resolve(
  __dirname,
  "../../publicData/parcels/parcelCentroidIndex.json"
);
const ADDRESS_INDEX_PATH = path.resolve(
  __dirname,
  "../../publicData/addresses/addressIndex.json"
);

function die(msg) {
  console.error(`❌ ${msg}`);
  process.exit(1);
}

function fileExists(p) {
  return fs.existsSync(p);
}

function normalizeZip5(z) {
  if (!z) return "";
  const digits = String(z).replace(/\D/g, "");
  if (!digits) return "";
  if (digits.length >= 5) return digits.slice(0, 5);
  return digits.padStart(5, "0");
}

function stripUnit(s) {
  return String(s || "")
    .replace(/\bU:\s*[A-Z0-9\-]+\b/gi, "")
    .replace(/\b(APT|UNIT|STE|SUITE)\s*[A-Z0-9\-]+\b/gi, "")
    .replace(/\s+#\s*[A-Z0-9\-]+\b/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

function extractFromRawAddress(rawAddress) {
  const a = String(rawAddress || "").trim();
  // "800 Bearses Way U:1NE" -> num=800, street="Bearses Way"
  const m = a.match(/^(\d+)\s+(.*)$/);
  if (!m) return { number: "", street: "" };
  const number = m[1];
  let street = m[2] || "";
  street = street.replace(/\s+U:.*$/i, "").trim();
  street = stripUnit(street);
  return { number, street };
}

function getListingNumberStreetZip(listing) {
  const addr = listing?.address || {};
  const raw = listing?.raw?.row || {};

  let number =
    addr.streetNumber ??
    raw.STREET_NUM ??
    raw.STREET_NO ??
    raw.ADDR_NUM ??
    "";

  let street =
    addr.streetName ??
    raw.STREET_NAME ??
    raw.FULL_STR ??
    raw.SITE_ADDR ??
    "";

  // last resort: parse raw ADDRESS / combinedAddress
  if (!number || !street) {
    const rawAddr = listing?.raw?.combinedAddress ?? raw.ADDRESS ?? "";
    const parsed = extractFromRawAddress(rawAddr);
    if (!number) number = parsed.number;
    if (!street) street = parsed.street;
  }

  const zip =
    addr.zip ??
    raw.ZIP_CODE ??
    raw.ZIP ??
    "";

  number = String(number || "").trim();
  street = String(street || "").trim();
  return { number, street, zip: normalizeZip5(zip) };
}

// Parcel street normalization (abbrev style: "LANE"->"LN", "STREET"->"ST")
function normalizeParcelStreetName(street) {
  if (!street) return "";
  let s = String(street).toUpperCase();
  s = stripUnit(s).toUpperCase();
  s = s.replace(/\./g, "").replace(/[^A-Z0-9\s]/g, " ").replace(/\s+/g, " ").trim();

  const reps = [
    [/\bSTREET\b/g, "ST"],
    [/\bROAD\b/g, "RD"],
    [/\bAVENUE\b/g, "AVE"],
    [/\bBOULEVARD\b/g, "BLVD"],
    [/\bDRIVE\b/g, "DR"],
    [/\bLANE\b/g, "LN"],
    [/\bCOURT\b/g, "CT"],
    [/\bPLACE\b/g, "PL"],
    [/\bTERRACE\b/g, "TER"],
    [/\bCIRCLE\b/g, "CIR"],
    [/\bPARKWAY\b/g, "PKWY"],
    [/\bHIGHWAY\b/g, "HWY"],
    [/\bSQUARE\b/g, "SQ"],
  ];
  for (const [re, to] of reps) s = s.replace(re, to);

  return s.replace(/\s+/g, " ").trim();
}

function buildParcelKey(number, street, zip5) {
  if (!number || !street || !zip5) return null;
  const n = String(number).trim().toUpperCase();
  const st = normalizeParcelStreetName(street);
  const z = normalizeZip5(zip5);
  if (!n || !st || !z) return null;
  return `${n} ${st} ${z}`;
}

function buildParcelPrefix(number, street) {
  if (!number || !street) return null;
  const n = String(number).trim().toUpperCase();
  const st = normalizeParcelStreetName(street);
  if (!n || !st) return null;
  return `${n} ${st}`;
}

// Address-point street normalization (expanded style: "LN"->"LANE", "ST"->"STREET")
function normalizeStreetForAddressIndex(street) {
  if (!street) return "";
  let s = String(street).toUpperCase();
  s = stripUnit(s).toUpperCase();
  s = s.replace(/\./g, "").replace(/[^A-Z0-9\s]/g, " ").replace(/\s+/g, " ").trim();

  // expand leading direction token if abbreviated
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
  const tokens = s.split(" ").filter(Boolean);
  if (tokens.length > 1 && dirMap[tokens[0]]) tokens[0] = dirMap[tokens[0]];
  s = tokens.join(" ");

  // expand suffix abbreviations
  const suffix = {
    ST: "STREET",
    RD: "ROAD",
    AVE: "AVENUE",
    BLVD: "BOULEVARD",
    DR: "DRIVE",
    LN: "LANE",
    CT: "COURT",
    PL: "PLACE",
    TER: "TERRACE",
    CIR: "CIRCLE",
    PKWY: "PARKWAY",
    HWY: "HIGHWAY",
    SQ: "SQUARE",
  };
  const parts = s.split(" ").filter(Boolean);
  if (parts.length >= 2) {
    const last = parts[parts.length - 1];
    if (suffix[last]) parts[parts.length - 1] = suffix[last];
  }
  return parts.join(" ");
}

function normalizeHouseNumberForAddressIndex(num) {
  if (!num) return "";
  let s = String(num).trim().toUpperCase();
  const dashIdx = s.indexOf("-");
  if (dashIdx > 0) s = s.slice(0, dashIdx);
  return s.replace(/\s+/g, " ");
}

function buildAddressIndexKey(number, street, zip5) {
  if (!number || !street || !zip5) return null;
  const n = normalizeHouseNumberForAddressIndex(number);
  const st = normalizeStreetForAddressIndex(street);
  const z = normalizeZip5(zip5);
  if (!n || !st || !z) return null;
  return `${n}|${st}|${z}`;
}

function extractLatLon(v) {
  if (!v) return null;

  // object forms
  if (typeof v === "object" && !Array.isArray(v)) {
    const lat = v.lat ?? v.latitude ?? v.y ?? v.Y;
    const lon = v.lon ?? v.lng ?? v.longitude ?? v.x ?? v.X;
    if (Number.isFinite(+lat) && Number.isFinite(+lon)) {
      return { latitude: +lat, longitude: +lon };
    }
  }

  // array forms: [lon,lat] OR [lat,lon]
  if (Array.isArray(v) && v.length >= 2) {
    const a = +v[0];
    const b = +v[1];
    if (Number.isFinite(a) && Number.isFinite(b)) {
      // heuristic
      const aIsLon = Math.abs(a) <= 180 && Math.abs(a) > 90;
      const bIsLat = Math.abs(b) <= 90;
      if (aIsLon && bIsLat) return { latitude: b, longitude: a };
      const bIsLon = Math.abs(b) <= 180 && Math.abs(b) > 90;
      const aIsLat = Math.abs(a) <= 90;
      if (bIsLon && aIsLat) return { latitude: a, longitude: b };
      // fallback assume [lon,lat]
      return { latitude: b, longitude: a };
    }
  }

  return null;
}

function confidence(tier, taxonomy) {
  // simple, defensible scoring (tune later)
  let c =
    tier === "T1_RELAXED" ? 0.93 :
    tier === "T2_RELAXED" ? 0.90 :
    tier === "T3_RELAXED" ? 0.88 : 0.80;

  if (taxonomy === "as_is") c += 0.03;
  if (taxonomy === "unit_stripped") c += 0.02;
  if (taxonomy === "cleaned") c += 0.01;

  c = Math.max(0.60, Math.min(0.99, c));
  return Math.round(c * 100) / 100;
}

function generateStreetVariants(street) {
  const base = String(street || "").trim();
  const v = [];
  if (base) v.push({ street: base, taxonomy: "as_is" });

  const noUnit = stripUnit(base);
  if (noUnit && noUnit !== base) v.push({ street: noUnit, taxonomy: "unit_stripped" });

  const cleaned = noUnit.replace(/\./g, "").replace(/\s+/g, " ").trim();
  if (cleaned && cleaned !== noUnit) v.push({ street: cleaned, taxonomy: "cleaned" });

  return v;
}

function buildPrefixIndex(parcelIndex) {
  const prefixIndex = Object.create(null);
  for (const k of Object.keys(parcelIndex)) {
    const parts = String(k).split(" ").filter(Boolean);
    if (parts.length < 3) continue;
    const last = parts[parts.length - 1];
    if (!/^\d{5}$/.test(last)) continue;
    const prefix = parts.slice(0, -1).join(" ");
    if (!prefixIndex[prefix]) prefixIndex[prefix] = parcelIndex[k];
  }
  return prefixIndex;
}

async function appendFileToStream(src, out) {
  if (!fileExists(src)) return 0;
  const rl = readline.createInterface({
    input: fs.createReadStream(src),
    crlfDelay: Infinity,
  });
  let count = 0;
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    out.write(t + "\n");
    count++;
  }
  return count;
}

async function main() {
  console.log("====================================================");
  console.log(" FUZZY PASS 1 — RELAXED INTERNAL RECOVERY");
  console.log("====================================================");

  if (!fileExists(FAST_UNMATCHED)) die(`Missing input: ${FAST_UNMATCHED}`);
  if (!fileExists(FAST_MATCHED)) die(`Missing input: ${FAST_MATCHED}`);
  if (!fileExists(PARCEL_INDEX_PATH)) die(`Missing: ${PARCEL_INDEX_PATH}`);
  if (!fileExists(ADDRESS_INDEX_PATH)) die(`Missing: ${ADDRESS_INDEX_PATH}`);

  console.log("[pass1] Loading parcel centroid index...");
  const parcelIndex = JSON.parse(await fsp.readFile(PARCEL_INDEX_PATH, "utf8"));
  console.log(`[pass1] parcel keys: ${Object.keys(parcelIndex).length.toLocaleString()}`);

  console.log("[pass1] Building parcel prefix index...");
  const prefixIndex = buildPrefixIndex(parcelIndex);
  console.log(`[pass1] prefix keys: ${Object.keys(prefixIndex).length.toLocaleString()}`);

  console.log("[pass1] Loading address-point index...");
  const addressIndex = JSON.parse(await fsp.readFile(ADDRESS_INDEX_PATH, "utf8"));
  console.log(`[pass1] address keys: ${Object.keys(addressIndex).length.toLocaleString()}`);

  const outPass1Matched = fs.createWriteStream(PASS1_MATCHED, { flags: "w" });
  const outPass1New = fs.createWriteStream(PASS1_NEW, { flags: "w" });
  const outPass1Unmatched = fs.createWriteStream(PASS1_UNMATCHED, { flags: "w" });

  console.log("[pass1] Seeding PASS1 matched with FAST matched...");
  const seeded = await appendFileToStream(FAST_MATCHED, outPass1Matched);
  console.log(`[pass1] seeded: ${seeded.toLocaleString()}`);

  const rl = readline.createInterface({
    input: fs.createReadStream(FAST_UNMATCHED),
    crlfDelay: Infinity,
  });

  let scanned = 0;
  let newMatches = 0;
  let still = 0;

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    scanned++;

    let listing;
    try {
      listing = JSON.parse(t);
    } catch {
      still++;
      outPass1Unmatched.write(t + "\n");
      continue;
    }

    const { number, street, zip } = getListingNumberStreetZip(listing);

    if (!number || !street || !zip) {
      still++;
      outPass1Unmatched.write(JSON.stringify(listing) + "\n");
      continue;
    }

    let matched = null;

    for (const v of generateStreetVariants(street)) {
      // Tier 1 relaxed (parcel direct)
      const parcelKey = buildParcelKey(number, v.street, zip);
      if (parcelKey && parcelIndex[parcelKey]) {
        const ll = extractLatLon(parcelIndex[parcelKey]);
        if (ll) {
          matched = {
            tier: "T1_RELAXED",
            source: "parcel_centroid",
            taxonomy: v.taxonomy,
            ll,
          };
          break;
        }
      }

      // Tier 2 relaxed (parcel prefix)
      const prefix = buildParcelPrefix(number, v.street);
      if (prefix && prefixIndex[prefix]) {
        const ll = extractLatLon(prefixIndex[prefix]);
        if (ll) {
          matched = {
            tier: "T2_RELAXED",
            source: "parcel_centroid_prefix",
            taxonomy: v.taxonomy,
            ll,
          };
          break;
        }
      }

      // Tier 3 relaxed (address point)
      const addrKey = buildAddressIndexKey(number, v.street, zip);
      if (addrKey && addressIndex[addrKey]) {
        const ll = extractLatLon(addressIndex[addrKey]);
        if (ll) {
          matched = {
            tier: "T3_RELAXED",
            source: "address_point",
            taxonomy: v.taxonomy,
            ll,
          };
          break;
        }
      }
    }

    if (matched) {
      listing.latitude = matched.ll.latitude;
      listing.longitude = matched.ll.longitude;
      listing.coordTier = matched.tier;
      listing.coordSource = matched.source;
      listing.coordTaxonomy = matched.taxonomy;
      listing.coordConfidence = confidence(matched.tier, matched.taxonomy);

      const outLine = JSON.stringify(listing);
      outPass1New.write(outLine + "\n");
      outPass1Matched.write(outLine + "\n");
      newMatches++;
    } else {
      outPass1Unmatched.write(JSON.stringify(listing) + "\n");
      still++;
    }

    if (scanned % 10000 === 0) {
      console.log(
        `[pass1] scanned=${scanned.toLocaleString()} | newMatches=${newMatches.toLocaleString()} | still=${still.toLocaleString()}`
      );
    }
  }

  outPass1Matched.end();
  outPass1New.end();
  outPass1Unmatched.end();

  console.log("====================================================");
  console.log("PASS 1 SUMMARY");
  console.log("----------------------------------------------------");
  console.log("FAST matched seeded:      ", seeded.toLocaleString());
  console.log("unmatched_FAST scanned:   ", scanned.toLocaleString());
  console.log("NEW matches (PASS1):      ", newMatches.toLocaleString());
  console.log("STILL unmatched (PASS1):  ", still.toLocaleString());
  console.log("Outputs:");
  console.log("  ", PASS1_MATCHED);
  console.log("  ", PASS1_NEW);
  console.log("  ", PASS1_UNMATCHED);
  console.log("====================================================");
}

main().catch((e) => die(e?.stack || String(e)));
