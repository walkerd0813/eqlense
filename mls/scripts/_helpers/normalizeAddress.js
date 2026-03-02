// backend/mls/scripts/_helpers/normalizeAddress.js

// ============================================================================
// Canonical street suffixes
// ============================================================================
const SUFFIX_MAP = {
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
  " TER": " TERRACE",
  " TER.": " TERRACE",
  " HWY": " HIGHWAY",
  " HWY.": " HIGHWAY",
  " PKWY": " PARKWAY",
  " PKWY.": " PARKWAY",
};

// ============================================================================
// BASIC CLEANERS
// ============================================================================
function normalizeZip(zip) {
  if (!zip) return "";
  const z = String(zip).trim();
  return z.slice(0, 5); // remove ZIP+4
}

function normalizeHouseNumber(num) {
  if (!num) return "";

  let s = String(num).trim().toUpperCase();

  // Example: 126-128 → 126
  const dash = s.indexOf("-");
  if (dash > 0) s = s.slice(0, dash);

  return s.replace(/\s+/g, " ");
}

function normalizeStreetName(name) {
  if (!name) return "";

  let out = String(name).toUpperCase();

  // Remove periods; collapse whitespace
  out = out.replace(/\./g, "").replace(/\s+/g, " ").trim();

  // Replace common abbreviations
  for (const [abbr, full] of Object.entries(SUFFIX_MAP)) {
    const simple = abbr.replace(/\./g, "");
    if (out.endsWith(simple)) {
      out = out.slice(0, out.length - simple.length) + full;
      break;
    }
  }
  return out;
}

// ============================================================================
// KEY BUILDERS — MUST MATCH THE INDEX BUILDER
// ============================================================================
function buildAddressKey(number, street, zip) {
  const num = normalizeHouseNumber(number);
  const st = normalizeStreetName(street);
  const z = normalizeZip(zip);
  return `${num}|${st}|${z}`;
}

function buildNumStreetKey(number, street) {
  const num = normalizeHouseNumber(number);
  const st = normalizeStreetName(street);
  return `${num}|${st}`;
}

function buildStreetZipKey(street, zip) {
  const st = normalizeStreetName(street);
  const z = normalizeZip(zip);
  return `${st}|${z}`;
}

// ============================================================================
// EXTRACTOR: Read MLS listing address robustly
// ============================================================================
function extractListingAddress(listing) {
  const a = listing.address || listing;

  const number =
    a.streetNumber ??
    a.street_no ??
    a.STREET_NO ??
    listing.streetNumber ??
    listing.street_no ??
    listing.STREET_NO ??
    "";

  const street =
    a.streetName ??
    a.street_name ??
    a.STREET_NAME ??
    listing.streetName ??
    listing.street_name ??
    listing.STREET_NAME ??
    "";

  const zip =
    a.zipCode ??
    a.zip_code ??
    a.ZIP_CODE ??
    listing.zipCode ??
    listing.zip_code ??
    listing.ZIP_CODE ??
    "";

  const city =
    a.city ??
    a.town ??
    a.CITY ??
    a.TOWN ??
    listing.city ??
    listing.town ??
    listing.CITY ??
    listing.TOWN ??
    "";

  return { number, street, zip, city };
}

// ============================================================================
// EXPORTS
// ============================================================================
export {
  normalizeZip,
  normalizeHouseNumber,
  normalizeStreetName,
  buildAddressKey,
  buildNumStreetKey,
  buildStreetZipKey,
  extractListingAddress,
};
