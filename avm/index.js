// backend/avm/index.js
// ------------------------------------------------------
// AVM v1+v2 merged engine
// - Loads the correct comps dataset for the subject
// - Runs NeighborhoodFilter (if available)
// - Runs CompSelector (strict + fallback + sparse-mode)
// - Runs pricingEngine (v1+v2 "X") to produce estimate + range
// - Optionally looks up hazard data
// ------------------------------------------------------

const path = require("path");
const { normalizeProperty } = require("./utils/normalize");
const { selectComps } = require("./selection/CompSelector");
const { priceProperty } = require("./pricing/pricingEngine");
const { findSubjectByAddress } = require("./lookupSubject");

// --------------------------------------------
// Optional hazard loader
// --------------------------------------------
let fetchHazardData = null;

try {
  ({ fetchHazardData } = require("../publicData/hazards/hazardLookup"));
} catch (err) {
  console.warn(
    "[AVM] Hazard module not found — running without hazard adjustments"
  );
}

// --------------------------------------------
// Optional Neighborhood Filter
// --------------------------------------------
let filterCompsByNeighborhood = null;

try {
  // NOTE: casing must exactly match filename: minNeighborhoodFilter.js
  ({ filterCompsByNeighborhood } = require("./selection/minNeighborhoodFilter"));
  console.log("[AVM] Neighborhood filter active");
} catch (err) {
  console.warn("[AVM] Neighborhood filter NOT found — proceeding without it");
  console.error("[AVM] Neighborhood filter load error:", err.message);
}

// --------------------------------------------
// Load comps dataset based on property type
// --------------------------------------------
function loadCompsForSubject(subject) {
  const propertyType =
    subject.propertyClass ||
    subject.propertyType ||
    subject.type ||
    subject.buildingType ||
    null;

  let fileName;

  switch (propertyType) {
    case "singleFamily":
    case "sf":
    case "single":
      fileName = "singleFamily.json";
      break;

    case "multiFamily":
    case "multi":
    case "2-4":
      fileName = "multiFamily.json";
      break;

    case "condo":
    case "condominium":
      fileName = "condos.json";
      break;

    default:
      fileName = "singleFamily.json";
      break;
  }

  const filePath = path.join(__dirname, "selection", "comps", fileName);
  const comps = require(filePath);

  return { comps, fileName, propertyType };
}

// --------------------------------------------
// MAIN ENTRY
// --------------------------------------------
async function runAVM(subjectRaw = {}, options = {}) {
  const { debug = false, selector: selectorOptions = {} } = options;

  // 1) Normalize subject
  const subjectRecord = findSubjectByAddress(subjectRaw);
const subject = normalizeProperty({
    ...subjectRecord,
    ...subjectRaw
});
  // 2) Load dataset
  const { comps: allComps, fileName, propertyType } =
    loadCompsForSubject(subject);

  if (debug) {
    console.log(
      "[AVM] Dataset:",
      fileName,
      "| total rows:",
      Array.isArray(allComps) ? allComps.length : "NOT ARRAY"
    );
  }

  // --------------------------------------------
  // 3) Neighborhood pre-filter (if module exists)
  // --------------------------------------------
  let compsForSelection = allComps;
  let neighborhoodDebug = null;

  if (filterCompsByNeighborhood) {
    try {
      const result = filterCompsByNeighborhood(subject, allComps);
      compsForSelection = result.comps;
      neighborhoodDebug = result;

      if (debug) {
        console.log(
          `[AVM] Neighborhood filter reduced ${allComps.length} → ${compsForSelection.length}`
        );
      }
    } catch (err) {
      console.warn("[AVM] Neighborhood filter failed:", err.message);
      // fall back to full dataset
      compsForSelection = allComps;
    }
  }

  // --------------------------------------------
  // 4) Standard CMA comp selection (strict + fallback + sparse)
// --------------------------------------------
  const selection = selectComps(subject, compsForSelection, {
    maxRadiusMiles: selectorOptions.maxRadiusMiles ?? 2,
    maxAgeMonths: selectorOptions.maxAgeMonths ?? 18,
    targetCompCount: selectorOptions.targetCompCount ?? 12,
  });

  const comps = selection.comps || [];

  if (debug) {
    console.log("[AVM] Selected comps:", comps.length);
  }

  // --------------------------------------------
  // 5) Hazard lookup (optional)
// --------------------------------------------
  let hazard = null;

  if (fetchHazardData && subject.lat && subject.lng) {
    try {
      hazard = await fetchHazardData({
        lat: subject.lat,
        lng: subject.lng,
        address: subject.address,
        zip: subject.zip,
      });
    } catch (err) {
      console.warn("[AVM] Hazard lookup failed:", err.message);
    }
  }

  // --------------------------------------------
  // 6) Run pricing engine
  // --------------------------------------------
  const pricing = priceProperty({ subject, comps, hazard });

  // --------------------------------------------
  // 7) Final response
  // --------------------------------------------
  const result = {
    ok: pricing.estimate != null,
    version: "avm-v1+v2",
    subject,
    comps,
    estimate: pricing.estimate,
    range: {
      low: pricing.low,
      high: pricing.high,
    },
    method: pricing.method,
    meta: {
      dataset: fileName,
      propertyType,
      compCount: comps.length,
      confidence: pricing.confidence,
      spreadPercent: pricing.spreadPercent,
      hazardScore: pricing.hazardScore,
      hazardAdjPct: pricing.hazardAdjPct,
      // Condition starter outputs
      conditionAdjPct: pricing.conditionAdjPct,
      conditionScoreSubject: pricing.conditionScoreSubject,
      conditionScoreMedian: pricing.conditionScoreMedian,
      conditionLabel: pricing.conditionLabel,
      // Selector + neighborhood + market diagnostics
      selector: selection.debug,
      neighborhoodFilter: neighborhoodDebug,
      market: pricing.market, // volatility / absorption / liquidity live here
    },
  };

  // --------------------------------------------
  // 8) Debug report (verbose)
  // --------------------------------------------
  if (debug) {
    const { buildDebugReport } = require("./DebugReport");
    result.debug = buildDebugReport({
      subject,
      selection,
      pricing,
    });
  }

  return result;
}

module.exports = {
runAVM,
};