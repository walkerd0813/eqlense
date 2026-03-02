// mlsNormalize.js — ES MODULE VERSION
// Compatible with: import { normalizeListing, listingToPropertyDoc } from "../lib/mlsNormalize.js";

// -----------------------------------------------------------
// INTERNAL HELPERS (REPLACEMENT FOR utils.js THAT NEVER EXISTED)
// These match the behavior of the old normalize utilities.
// -----------------------------------------------------------

function safeTrim(value) {
  if (value === undefined || value === null) return null;
  const trimmed = String(value).trim();
  return trimmed === "" ? null : trimmed;
}

function toInt(value) {
  const n = parseInt(value, 10);
  return Number.isFinite(n) ? n : null;
}

function toNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

// Normalizes ZIP codes like: "2184", "02184-1234", "2184     ", etc.
function normalizeZip(zip) {
  if (!zip) return null;
  const cleaned = String(zip).trim().slice(0, 5);
  const n = Number(cleaned);
  if (!Number.isFinite(n)) return null;
  return cleaned.padStart(5, "0");
}

// -----------------------------------------------------------
// normalizeListing
// -----------------------------------------------------------

export function normalizeListing(rawRow, { source }) {
  const mlsNumber = safeTrim(rawRow.LIST_NO);
  const propertyType = safeTrim(rawRow.PROP_TYPE);
  const propTypeRaw = propertyType;

  const statusCode = safeTrim(rawRow.STATUS);
  const statusGroup = deriveStatusGroup(statusCode);

  // --- ADDRESS FIELDS ---
  const streetNumber = safeTrim(rawRow.STREET_NO);
  const streetName = safeTrim(rawRow.STREET_NAME);
  const unitNumber = safeTrim(rawRow.UNIT_NO);
  const townCode = safeTrim(rawRow.TOWN_NUM);
  const areaCode = safeTrim(rawRow.AREA);
  const zipCode = normalizeZip(rawRow.ZIP_CODE);
  const county = safeTrim(rawRow.COUNTY);
  const state = safeTrim(rawRow.STATE);

  const address = {
    streetNumber,
    streetName,
    unitNumber,
    townCode,
    areaCode,
    zipCode,
    county,
    state,
  };

  // --- PRICES ---
  const listPrice = toNumber(rawRow.LIST_PRICE);
  
  const salePrice =
  toNumber(rawRow.SALE_PRICE) ??
  toNumber(rawRow.SOLD_PRICE) ??
  toNumber(rawRow.CLOSING_PRICE) ??
  null;


  // --- DATES ---
  const listDate = safeTrim(rawRow.LIST_DATE) || null;
  // --- SOLD DATE EXTRACTION (MLS PIN uses multiple possible field names) ---
const saleDate =
  safeTrim(rawRow.SALE_DATE) ??
  safeTrim(rawRow.SETTLED_DATE) ??
  safeTrim(rawRow.CLOSE_DATE) ??
  safeTrim(rawRow.CLOSED_DATE) ??
  safeTrim(rawRow.SOLD_DATE) ??
  safeTrim(rawRow.OFFMARKET_DATE) ??
  null;

// Aliases for compatibility with verification & analytics
const soldPrice = salePrice;
const soldDate = saleDate;


  // --- SIZE ---
  let sqft =
    toInt(rawRow.SQUARE_FEET) ??
    toInt(rawRow.AboveGradeFinishedArea) ??
    null;

  if (sqft !== null) {
    if (sqft <= 0) sqft = null;
    if (sqft >= 99000) sqft = null;
  }

  let lotSizeSqFt = toNumber(rawRow.LOT_SIZE);
  let acres = toNumber(rawRow.ACRE);

  if (!lotSizeSqFt && acres) {
    lotSizeSqFt = Math.round(acres * 43560);
  } else if (!acres && lotSizeSqFt) {
    acres = lotSizeSqFt / 43560;
  }

  // If both values exist, check for mismatch
if (lotSizeSqFt !== null && acres !== null) {
  const expected = acres * 43560;
  const diffRatio = Math.abs(expected - lotSizeSqFt) / expected;

  // Mark acres invalid if wildly inconsistent (>10% mismatch)
  if (diffRatio > 0.10) {
    acres = null;
  }
}

// Normalize zero/invalid land values
if (lotSizeSqFt !== null && lotSizeSqFt <= 0) {
  lotSizeSqFt = null;
}

if (acres !== null && acres <= 0) {
  acres = null;
}


  let yearBuilt =
    toInt(rawRow.YEAR_BUILT) ??
    toInt(rawRow.YEAR_BUILT_DESCRP) ??
    null;

  if (yearBuilt !== null) {
    const currentYear = new Date().getFullYear();
    if (yearBuilt < 1700 || yearBuilt > currentYear + 2) {
      yearBuilt = null;
    }
  }

  // --- AGENTS/OFFICES ---
  const listAgentId = safeTrim(rawRow.LIST_AGENT);
  const listOfficeId = safeTrim(rawRow.LIST_OFFICE);
  const saleAgentId = safeTrim(rawRow.SALE_AGENT);
  const saleOfficeId = safeTrim(rawRow.SALE_OFFICE);

  // --- PROPERTY KEY ---
  const propertyKey = buildPropertyKey({
    streetNumber,
    streetName,
    unitNumber,
    zipCode,
  });

  // --- META ---
  const lastUpdate = safeTrim(rawRow.LAST_UPDATE) || null;

  // --- FINAL RECORD ---
  return {
    mlsNumber,
    propertyType,
    propTypeRaw,
    statusCode,
    statusGroup,

    streetNumber,
    streetName,
    unitNumber,
    townCode,
    areaCode,
    zipCode,
    county,
    state,

      address,

  listPrice,
  salePrice,
  soldPrice,

  listDate,
  saleDate,
  soldDate,

  sqft,
  lotSizeSqFt,
  acres,
  yearBuilt,


    listAgentId,
    listOfficeId,
    saleAgentId,
    saleOfficeId,

    propertyKey,

    source,
    lastUpdate,

    raw: rawRow,
  };
}

// -----------------------------------------------------------
// listingToPropertyDoc
// -----------------------------------------------------------

export function listingToPropertyDoc(listing) {
  if (!listing) return null;

  return {
    propertyKey: listing.propertyKey,
    streetNumber: listing.streetNumber,
    streetName: listing.streetName,
    unitNumber: listing.unitNumber,
    townCode: listing.townCode,
    areaCode: listing.areaCode,
    zipCode: listing.zipCode,
    county: listing.county,
    state: listing.state,

    sqft: listing.sqft,
    lotSizeSqFt: listing.lotSizeSqFt,
    acres: listing.acres,
    yearBuilt: listing.yearBuilt,

    address: listing.address,

    mlsNumber: listing.mlsNumber,
    source: listing.source,
  };
}

// -----------------------------------------------------------
// Helpers
// -----------------------------------------------------------

function deriveStatusGroup(code) {
  if (!code) return "unknown";
  const c = code.toUpperCase();

  if (["ACT", "NEW", "BOM", "PCG", "RAC"].includes(c)) return "active";
  if (["CTG", "UAG"].includes(c)) return "under_agreement";
  if (["SLD"].includes(c)) return "sold";
  if (["EXP", "CAN", "EXT", "WDN", "WITH", "WTH"].includes(c)) return "withdrawn";

  return "unknown";
}

function buildPropertyKey({ streetNumber, streetName, unitNumber, zipCode }) {
  const parts = [streetNumber, streetName, unitNumber, zipCode]
    .map((p) => (p ? p.toString().trim().toUpperCase() : ""))
    .filter(Boolean);

  return parts.join("|");
}