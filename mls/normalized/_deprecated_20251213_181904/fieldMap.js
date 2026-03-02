// fieldMap.js
// Canonical MLS → Normalized Listing Schema (v1.0)

export const FIELD_MAP = {

  // ─────────────────────────────────────────────
  // Identity & Status
  // ─────────────────────────────────────────────
  mlsNumber: null, // not present in current feed
  propertyType: ["PROP_TYPE"],
  rawStatus: ["STATUS"],

  // ─────────────────────────────────────────────
  // Address & Location
  // ─────────────────────────────────────────────
  address: {
    full: ["ADDRESS"],
    streetNumber: ["STREET_NUM"],
    streetName: ["STREET_NAME"],
    unit: ["UNIT_NO"],
    city: ["TOWN_DESC"],
    zipCode: ["ZIP_CODE"],
    zip4: ["ZIP_CODE_4"],
    county: ["COUNTY"]
  },

  // ─────────────────────────────────────────────
  // Lifecycle & Timing (CRITICAL)
  // ─────────────────────────────────────────────
  listDate: ["LIST_DATE", "CSO_ListDate"],
  statusDate: ["STATUS_DATE"],
  activeStatusFlagDate: ["ACTIVE_STATUS_FLAG_DATE"],
  offerDate: ["OFFER_DATE"],
  offMarketDate: ["OFF_MKT_DATE"],
  contingencyExpiryDate: ["CTG_EXPIRY_DATE"],
  soldDate: ["SETTLED_DATE"],
  marketTimeDays: ["MARKET_TIME"],

  // ─────────────────────────────────────────────
  // Pricing
  // ─────────────────────────────────────────────
  listPrice: ["LIST_PRICE"],
  salePrice: ["SALE_PRICE"],
  pricePerSqFt: ["PRICE_PER_SQFT"],
  originalListPrice: null, // not available in this feed

  // ─────────────────────────────────────────────
  // Physical Characteristics (AVM CORE)
  // ─────────────────────────────────────────────
  beds: ["NO_BEDROOMS"],
  fullBaths: ["BTH_DESC"], // parsed later
  halfBaths: ["NO_HALF_BATHS"],
  sqft: ["SQUARE_FEET"],
  totalBuildingSqft: ["TOTAL_BLDG_SF_CI"],
  lotSizeSqft: ["LOT_SIZE"],
  yearBuilt: ["YEAR_BUILT"],
  style: ["STYLE_SF"],

  // ─────────────────────────────────────────────
  // Parking
  // ─────────────────────────────────────────────
  garageSpaces: [
    "GARAGE_SPACES_SF",
    "GARAGE_SPACES_CC"
  ],

  parkingSpaces: [
    "PARKING_SPACES_SF",
    "PARKING_SPACES_MF",
    "PARKING_SPACES_CC"
  ],

  totalParking: [
    "TOTAL_PARKING_SF",
    "TOTAL_PARKING_MF",
    "TOTAL_PARKING_CC"
  ],

  // ─────────────────────────────────────────────
  // Heating / Cooling / Construction
  // ─────────────────────────────────────────────
  heating: [
    "HEATING_SF",
    "HEATING_CC",
    "HEA_COMMON_MF",
    "HTE_MF"
  ],

  cooling: [
    "COOLING_SF",
    "COOLING_CC",
    "COL_COMMON_MF"
  ],

  construction: [
    "CONSTRUCTION_SF",
    "CONSTRUCTION_MF",
    "CONSTRUCTION_CC"
  ],

  siteCondition: ["SITE_CONDITION_CI"],

  // ─────────────────────────────────────────────
  // Brokerage & Agent Intelligence
  // ─────────────────────────────────────────────
  listAgentId: ["LIST_AGENT"],
  saleAgentId: ["SALE_AGENT"],
  listOfficeId: ["LIST_OFFICE"],
  saleOfficeId: ["SALE_OFFICE"],
  mainSellingOffice: ["MAIN_SO"],
  contingencyType: ["CONTINGENCY_TYPE"],

  // ─────────────────────────────────────────────
  // Financials & Assessments
  // ─────────────────────────────────────────────
  taxes: ["TAXES"],
  taxYear: ["TAX_YEAR"],

  assessedBuildingValue: ["ASSESSED_VALUE_BLDG_CI"],
  assessedLandValue: ["ASSESSED_VALUE_LAND_CI"],
  assessedTotalValue: ["TOTAL_ASSESSED_VALUE_CI"],

  hoaRequiredSF: ["REQD_OWN_ASSOCIATION_SF"],
  hoaRequiredMH: ["REQD_OWN_ASSOCIATION_MH"],
  hoaRequiredLD: ["REQD_OWN_ASSOCIATION_LD"],

  // ─────────────────────────────────────────────
  // Media & Remarks
  // ─────────────────────────────────────────────
  photoDate: ["PHOTO_DATE"],

  remarks: [
    "REMARKS",
    "FIRM_RMK1"
  ],

  interiorFeaturesSF: ["INTERIOR_FEATURES_SF"],
  interiorFeaturesMF: ["IFE_COMMON_MF"],
  interiorFeaturesCC: ["INTERIOR_FEATURES_CC"]
};
