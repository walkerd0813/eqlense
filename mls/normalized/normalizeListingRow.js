// backend/mls/normalized/normalizeListingRow.js
// Canonical MLS Listing Normalizer (FROZEN)
// ESM + default export
//
// Input:  rawRow (object from csv-parser), context { propertyType, status, sourceFile, sourcePath }
// Output: canonical listing schema for mls/normalized/listings.ndjson

import crypto from "node:crypto";

export const SCHEMA_VERSION = "mls_listing_v1";
export const SOURCE = "MLS_PIN";

function cleanStr(v) {
  if (v === undefined || v === null) return null;
  const s = String(v).replace(/^"|"$/g, "").trim();
  return s === "" ? null : s;
}

function toNum(v) {
  const s = cleanStr(v);
  if (!s) return null;
  const n = Number(s.replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : null;
}

function toInt(v) {
  const n = toNum(v);
  if (n === null) return null;
  const i = Math.trunc(n);
  return Number.isFinite(i) ? i : null;
}

function parseDate(v) {
  const s = cleanStr(v);
  if (!s) return null;

  // Common MLS formats: "MM/DD/YYYY", "MM/DD/YYYY HH:MM", ISO-like
  const d = new Date(s);
  if (!Number.isNaN(d.getTime())) return d.toISOString();

  // Fallback: try MM/DD/YYYY manually
  const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
  if (m) {
    const mm = String(m[1]).padStart(2, "0");
    const dd = String(m[2]).padStart(2, "0");
    let yyyy = m[3];
    if (yyyy.length === 2) yyyy = `20${yyyy}`;
    const d2 = new Date(`${yyyy}-${mm}-${dd}T00:00:00Z`);
    if (!Number.isNaN(d2.getTime())) return d2.toISOString();
  }

  return null;
}

function pick(row, keys) {
  for (const k of keys) {
    const val = row?.[k];
    const s = cleanStr(val);
    if (s !== null) return s;
  }
  return null;
}

function normalizeZip(zip) {
  if (!zip) return null;
  const digits = zip.replace(/\D/g, "");
  if (digits.length === 0) return null;
  return digits.padStart(5, "0").slice(0, 5);
}

function normalizePropertyType(raw, contextType) {
  const ctx = cleanStr(contextType);
  if (ctx) return ctx; // expected: single_family|condo|multi_family|land

  const t = (cleanStr(raw) || "").toUpperCase();
  if (t === "SF" || t === "SINGLE FAMILY" || t === "SINGLE_FAMILY") return "single_family";
  if (t === "CC" || t === "CONDO" || t === "CONDOMINIUM") return "condo";
  if (t === "MF" || t === "MULTI FAMILY" || t === "MULTI_FAMILY") return "multi_family";
  if (t === "LD" || t === "LAND") return "land";
  return "other";
}

function normalizeStatus(raw, contextStatus) {
  const ctx = cleanStr(contextStatus);
  if (ctx) return ctx;

  // If your STATUS is already human-ish, pass it; else map codes.
  const s = (cleanStr(raw) || "").toUpperCase();

  if (s === "SLD" || s === "SOLD") return "sold";
  if (s === "ACT" || s === "ACTIVE" || s === "NEW" || s === "PCG" || s === "BOM") return "active";
  if (s === "UAG" || s === "UNDER AGREEMENT" || s === "UNDER_AGREEMENT") return "under_agreement";
  if (s === "PEND" || s === "PENDING") return "pending";
  if (s === "WTH" || s === "WITHDRAWN") return "withdrawn";
  if (s === "CAN" || s === "CANCELED") return "canceled";
  if (s === "EXP" || s === "EXPIRED") return "expired";
  if (s === "OFF" || s === "OFF MARKET" || s === "OFF_MARKET") return "off_market";

  return "other";
}

function normalizeBaths(row) {
  // Your file shows BTH_DESC (string), and NO_HALF_BATHS exists.
  // If NO_FULL_BATHS / NO_BATHS are absent, we at least compute totalBaths from beds/bth_desc heuristics later.
  const fullBaths =
    toInt(pick(row, ["NO_FULL_BATHS", "FULL_BATHS", "NO_FULL_BATH"])) ?? null;

  const halfBaths = toInt(pick(row, ["NO_HALF_BATHS", "HALF_BATHS", "NO_HALF_BATH"])) ?? null;

  // If there is a direct total baths column, use it.
  const totalBathsDirect =
    toNum(pick(row, ["NO_BATHS", "BATHS_TOTAL", "TOTAL_BATHS"])) ?? null;

  // If total not present but full/half exist, compute.
  const computed =
    totalBathsDirect !== null
      ? totalBathsDirect
      : fullBaths !== null || halfBaths !== null
        ? (fullBaths ?? 0) + (halfBaths ?? 0) * 0.5
        : null;

  return { fullBaths, halfBaths, totalBaths: computed };
}

function makeListingId({ mlsNumber, standardizedKey }) {
  if (mlsNumber) return `${SOURCE}:${mlsNumber}`;
  // stable hash fallback
  const h = crypto.createHash("sha1").update(standardizedKey || crypto.randomUUID()).digest("hex");
  return `${SOURCE}:H:${h}`;
}

export function normalizeListingRow(rawRow, context = {}) {
  if (!rawRow || typeof rawRow !== "object") return null;

  // --- Raw header reads (your MapMyMLS headers + common variants)
  const mlsNumber = pick(rawRow, ["LIST_NO", "MLSNUMBER", "MLS_NUMBER", "MLSNUM"]);

  const propTypeRaw = pick(rawRow, ["PROP_TYPE", "PROP_TYP", "PROPERTY_TYPE"]);
  const statusRaw = pick(rawRow, ["STATUS", "ACTIVE_STATUS", "STATUS_CODE"]);

  const streetNumber = pick(rawRow, ["STREET_NUM", "STREET_NO", "ST_NO", "HOUSE_NO"]);
  const streetName = pick(rawRow, ["STREET_NAME", "ST_NAME"]);
  const unit = pick(rawRow, ["UNIT_NO", "UNIT", "APT_NO"]);

  // Some exports have combined "ADDRESS"
  const combinedAddress = pick(rawRow, ["ADDRESS", "FULL_ADDRESS"]);

  const city = pick(rawRow, ["TOWN_DESC", "CITY", "TOWN"]);
  const county = pick(rawRow, ["COUNTY"]);
  const zip = normalizeZip(pick(rawRow, ["ZIP_CODE", "ZIP", "ZIPCODE"]));
  const zip4 = pick(rawRow, ["ZIP_CODE_4", "ZIP4"]);

  const listPrice = toNum(pick(rawRow, ["LIST_PRICE", "LISTPRICE"]));
  const salePrice = toNum(pick(rawRow, ["SALE_PRICE", "SOLD_PRICE", "SOLDPRICE"]));
  const pricePerSqft = toNum(pick(rawRow, ["PRICE_PER_SQFT", "PPSF"]));

  const taxes = toNum(pick(rawRow, ["TAXES"]));
  const taxYear = toInt(pick(rawRow, ["TAX_YEAR"]));

  const assessedLand = toNum(pick(rawRow, ["ASSESSED_VALUE_LAND_CI"]));
  const assessedBldg = toNum(pick(rawRow, ["ASSESSED_VALUE_BLDG_CI"]));
  const assessedTotal = toNum(pick(rawRow, ["TOTAL_ASSESSED_VALUE_CI"]));
  const assessments = toNum(pick(rawRow, ["ASSESSMENTS"]));

  const sqft =
    toInt(pick(rawRow, ["SQUARE_FEET", "TOTAL_BLDG_SF_CI", "TOTAL_BLDG_SF"])) ?? null;

  const lotSizeSqft = toNum(pick(rawRow, ["LOT_SIZE"]));
  const acres = toNum(pick(rawRow, ["ACRE", "ACRES"]));
  const yearBuilt = toInt(pick(rawRow, ["YEAR_BUILT"]));

  const beds = toInt(pick(rawRow, ["NO_BEDROOMS", "BEDROOMS", "BEDS"]));

  const { fullBaths, halfBaths, totalBaths } = normalizeBaths(rawRow);

  const units = toInt(pick(rawRow, ["RSU_UNITS_CI", "UNITS", "NO_UNITS"]));

  const parkingSpaces =
    toInt(pick(rawRow, ["TOTAL_PARKING_SF", "TOTAL_PARKING_MF", "TOTAL_PARKING_CC", "TOTAL_PARKING", "PARKING_SPACES_SF", "PARKING_SPACES_MF", "PARKING_SPACES_CC"])) ?? null;

  const garageSpaces =
    toInt(pick(rawRow, ["GARAGE_SPACES_SF", "GARAGE_SPACES_CC", "GARAGE_SPACES_MF", "GARAGE_SPACES"])) ?? null;

  const listDate =
    parseDate(pick(rawRow, ["LIST_DATE", "CSO_ListDate"])) ?? null;

  const saleDate =
    parseDate(pick(rawRow, ["SETTLED_DATE", "SOLD_DATE"])) ?? null;

  const offMarketDate = parseDate(pick(rawRow, ["OFF_MKT_DATE"])) ?? null;
  const contractDate = parseDate(pick(rawRow, ["OFFER_DATE"])) ?? null;

  const statusDate =
    parseDate(pick(rawRow, ["STATUS_DATE", "ACTIVE_STATUS_FLAG_DATE"])) ?? null;

  const daysOnMarket =
    toInt(pick(rawRow, ["MARKET_TIME", "DOM", "DAYS_ON_MARKET"])) ?? null;

  const listOffice = pick(rawRow, ["LIST_OFFICE"]);
  const listAgent = pick(rawRow, ["LIST_AGENT"]);
  const saleOffice = pick(rawRow, ["SALE_OFFICE", "MAIN_SO"]);
  const saleAgent = pick(rawRow, ["SALE_AGENT"]);

  // --- Determine canonical type/status (context wins)
  const propertyType = normalizePropertyType(propTypeRaw, context.propertyType);
  const status = normalizeStatus(statusRaw, context.status);

  // --- Build address
  const address = {
    streetNumber: streetNumber ?? null,
    streetName: streetName ?? null,
    unit: unit ?? null,
    city: city ?? null,
    state: "MA",
    zip: zip ?? null,
    zip4: zip4 ?? null,
    county: county ?? null,
  };

  // If STREET fields are missing but ADDRESS is present, keep it in raw only.
  // We do NOT parse/guess combined address here (that’s for later enrichment).
  const standardizedKey = `${address.streetNumber || ""}|${address.streetName || ""}|${address.unit || ""}|${address.city || ""}|${address.zip || ""}|${listDate || ""}|${listPrice || ""}|${propertyType || ""}`;

  const listingId = makeListingId({ mlsNumber, standardizedKey });

  return {
    listingId,
    source: SOURCE,
    sourceFile: cleanStr(context.sourceFile) ?? null,
    ingestedAt: new Date().toISOString(),
    schemaVersion: SCHEMA_VERSION,

    propertyType,
    status,
    statusDate,

    address,

    physical: {
      beds,
      fullBaths,
      halfBaths,
      totalBaths,
      sqft,
      lotSizeSqft,
      acres,
      yearBuilt,
      units,
      parkingSpaces,
      garageSpaces,
    },

    pricing: {
      listPrice,
      salePrice,
      pricePerSqft,
      taxes,
      taxYear,
      assessments: {
        land: assessedLand,
        building: assessedBldg,
        total: assessedTotal,
      },
      // some exports have a generic "ASSESSMENTS" too
      assessmentsMisc: assessments,
    },

    dates: {
      listDate,
      contractDate,
      saleDate,
      offMarketDate,
      daysOnMarket,
    },

    brokerage: {
      listOffice,
      listAgent,
      saleOffice,
      saleAgent,
    },

    // lawsuit-proof safety net (never used for logic)
    raw: {
      row: rawRow,
      combinedAddress: combinedAddress ?? null,
      sourcePath: cleanStr(context.sourcePath) ?? null,
    },
  };
}

export default normalizeListingRow;
