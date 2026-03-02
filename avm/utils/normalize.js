// utils/normalize.js
// ------------------
// Normalize subject + comp records into a consistent shape
// without crashing if fields are missing.

function toNumber(value, fallback = null) {
  if (value === undefined || value === null || value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toDate(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

/**
 * Try to infer property "class" from flags.
 * Returns: "singleFamily" | "multiFamily" | "condo" | "other"
 */
function inferPropertyClass(raw = {}) {
  const type = (raw.propertyType || raw.type || raw.useCode || "")
    .toString()
    .toLowerCase();

  if (type.includes("condo")) return "condo";
  if (
    type.includes("multi") ||
    type.includes("2 family") ||
    type.includes("3 family")
  ) {
    return "multiFamily";
  }
  if (type.includes("single") || type.includes("sf")) return "singleFamily";

  const units =
    toNumber(raw.units) ||
    toNumber(raw.unitCount) ||
    toNumber(raw.residentialUnits);

  if (units && units >= 2 && units <= 4) return "multiFamily";

  // Default for single family
  return "singleFamily";
}

/**
 * Normalize a property record (subject or comp)
 */
function normalizeProperty(raw = {}) {
  // Coordinates
  const lat = toNumber(raw.lat ?? raw.latitude);
  const lng = toNumber(raw.lng ?? raw.longitude ?? raw.lon);

  // Areas
  const sqft =
    toNumber(
      raw.sqft ??
        raw.livingArea ??
        raw.grossArea ??
        raw.buildingArea ??
        raw.BuildingArea
    ) || null;

  const lotSqft =
    toNumber(
      raw.lotSqft ??
        raw.lot_size ??
        raw.lotSizeSqFt ??
        raw.LotSqFt ??
        raw.LotSize
    ) || null;

  // Prices + dates
  const salePrice =
    toNumber(
      raw.salePrice ??
        raw.soldPrice ??
        raw.closePrice ??
        raw.price ??
        raw.sale_price ??
        raw.SalePrice
    ) || null;

  const saleDate =
    toDate(
      raw.saleDate ??
        raw.closeDate ??
        raw.closingDate ??
        raw.recordingDate ??
        raw.sale_date ??
        raw.SaleDate
    ) || null;

  // Year built
  const yearBuilt =
    toNumber(
      raw.yearBuilt ??
        raw.YearBuilt ??
        raw.yr_built ??
        raw.Built ??
        raw.BuiltYear
    ) || null;

  // Address string (avoid mixing ?? and || fallbacks)
  const addressFromParts = [raw.street, raw.city, raw.state, raw.zip]
    .filter(Boolean)
    .join(", ");

  const address =
    raw.address ||
    raw.Address ||
    raw.fullAddress ||
    addressFromParts ||
    null;

  // Remarks / condition text (MLS-style)
  const remarks =
    raw.remarks ||
    raw.Remarks ||
    raw.publicRemarks ||
    raw.PublicRemarks ||
    raw.agentRemarks ||
    raw.AgentRemarks ||
    raw.officeRemarks ||
    raw.OfficeRemarks ||
    null;

  const conditionText =
    raw.conditionText ||
    raw.ConditionText ||
    raw.conditionDescription ||
    raw.ConditionDescription ||
    remarks ||
    null;

  return {
    // IDs
    id:
      raw.id ||
      raw.propertyId ||
      raw.PropertyID ||
      raw.PropertyId ||
      null,

    // Location
    address,
    city: raw.city || raw.City || null,
    state: raw.state || raw.State || null,
    zip:
      raw.zip ||
      raw.Zip ||
      raw.zipCode ||
      raw.ZipCode ||
      raw.postalCode ||
      null,

    lat,
    lng,

    // Structure
    beds:
      toNumber(
        raw.beds ??
          raw.Beds ??
          raw.bedrooms ??
          raw.Bedrooms ??
          raw.bed ??
          raw.Bed
      ) || 0,
    baths:
      toNumber(
        raw.baths ??
          raw.Baths ??
          raw.bathrooms ??
          raw.Bathrooms ??
          raw.bath ??
          raw.Bath
      ) || 0,
    sqft,
    lotSqft,
    yearBuilt,

    // Units / property class
    units:
      toNumber(
        raw.units ??
          raw.unitCount ??
          raw.residentialUnits ??
          raw.Units ??
          raw.UnitCount
      ) || 1,

    propertyClass: raw.propertyClass || inferPropertyClass(raw),
    neighborhood: raw.neighborhood || raw.Neighborhood || null,

    // Transaction
    salePrice,
    saleDate,

    // Condition-related text (for future ML / scoring)
    remarks,
    conditionText,

    // Keep original for debugging / future ML / calibration
    raw,
  };
}

module.exports = {
  normalizeProperty,
  inferPropertyClass,
};

