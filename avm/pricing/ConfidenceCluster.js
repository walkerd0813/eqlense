
// backend/avm/pricing/ConfidenceCluster.js
// ========================================
// Pure "confidence + spread" engine based ONLY on how the comps
// cluster around price-per-square-foot.
//
// This DOES NOT know about hazards or condition adjustments.
// Those live in pricingEngine.js. This module gives you a clean,
// re-usable object you can use in DebugReport or the UI to explain
// *why* an estimate is high- vs low-confidence.

//
// Helpers
// --------------------------------------

function safeNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function median(arr) {
  const nums = arr
    .map((v) => safeNumber(v, null))
    .filter((v) => v !== null)
    .sort((a, b) => a - b);

  if (!nums.length) return null;
  const mid = Math.floor(nums.length / 2);
  return nums.length % 2 ? nums[mid] : (nums[mid - 1] + nums[mid]) / 2;
}

function percentile(sortedAsc, p) {
  if (!sortedAsc.length) return null;
  if (p <= 0) return sortedAsc[0];
  if (p >= 1) return sortedAsc[sortedAsc.length - 1];

  const idx = (sortedAsc.length - 1) * p;
  const lower = Math.floor(idx);
  const upper = Math.ceil(idx);

  if (lower === upper) return sortedAsc[lower];

  const weight = idx - lower;
  return sortedAsc[lower] * (1 - weight) + sortedAsc[upper] * weight;
}

function clamp(value, min, max) {
  const n = safeNumber(value, null);
  if (n === null) return value;
  if (n < min) return min;
  if (n > max) return max;
  return n;
}

// Local rule bounds for this module. These mirror the defaults
// from pricingEngine.js so behavior is consistent.
const LOCAL_RULES = {
  SPREAD_MIN_PCT: 0.03, // 3%
  SPREAD_MAX_PCT: 0.25, // 25%
  CONF_MIN: 0,
  CONF_MAX: 95,
};

/**
 * computeConfidenceAndSpread
 * --------------------------
 * @param {Object} params
 * - subject: normalized subject (needs sqft)
 * - comps: array of normalized comps (needs salePrice + sqft)
 *
 * Returns:
 * {
 * confidenceBase, // 0–95, before hazard/condition
 * spreadFactorBase, // e.g. 0.10 = ±10% band
 * compCount,
 * subjectSqft,
 * baseValue, // central PPSF * subjectSqft
 * centralPpsf,
 * dispersion, // (P90 - P10) / median
 * clusterLabel, // human readable text
 * outlierCount,
 * coreCoverage, // fraction of comps in 5–95 band
 * percentiles: { p05, p10, p25, p50, p60, p75, p90, p95 },
 * diagnostics: { ...same fields for easy logging }
 * }
 */
function computeConfidenceAndSpread(params = {}) {
  const { subject = {}, comps = [] } = params;

  const usable = (comps || []).filter(
    (c) =>
      safeNumber(c.salePrice, null) !== null &&
      safeNumber(c.sqft, null) !== null &&
      c.sqft > 200
  );

  const compCount = usable.length;

  if (!compCount) {
    return {
      confidenceBase: 0,
      spreadFactorBase: null,
      compCount: 0,
      subjectSqft: subject?.sqft || null,
      baseValue: null,
      centralPpsf: null,
      dispersion: null,
      clusterLabel: "no-usable-comps",
      outlierCount: 0,
      coreCoverage: 0,
      percentiles: {
        p05: null,
        p10: null,
        p25: null,
        p50: null,
        p60: null,
        p75: null,
        p90: null,
        p95: null,
      },
      diagnostics: {
        reason: "No usable comps (need salePrice + sqft > 200).",
      },
    };
  }

  // PPSF vector
  const ppsf = usable
    .map((c) => {
      const price = safeNumber(c.salePrice, null);
      const sqft = safeNumber(c.sqft, null);
      if (price === null || sqft === null || sqft <= 0) return null;
      return price / sqft;
    })
    .filter((v) => v !== null);

  if (!ppsf.length) {
    return {
      confidenceBase: 0,
      spreadFactorBase: null,
      compCount,
      subjectSqft: subject?.sqft || usable[0]?.sqft || null,
      baseValue: null,
      centralPpsf: null,
      dispersion: null,
      clusterLabel: "invalid-ppsf",
      outlierCount: 0,
      coreCoverage: 0,
      percentiles: {
        p05: null,
        p10: null,
        p25: null,
        p50: null,
        p60: null,
        p75: null,
        p90: null,
        p95: null,
      },
      diagnostics: {
        reason: "Could not compute PPSF for any comps.",
      },
    };
  }

  const sorted = [...ppsf].sort((a, b) => a - b);
  const n = sorted.length;

  const p05 = percentile(sorted, 0.05);
  const p10 = percentile(sorted, 0.10);
  const p25 = percentile(sorted, 0.25);
  const p50 = percentile(sorted, 0.50);
  const p60 = percentile(sorted, 0.60);
  const p75 = percentile(sorted, 0.75);
  const p90 = percentile(sorted, 0.90);
  const p95 = percentile(sorted, 0.95);

  const trimmed = sorted.filter((v) => v >= p05 && v <= p95);
  const outlierCount = n - trimmed.length;
  const coreCoverage = n ? trimmed.length / n : 0;

  // --- Base central PPSF: upper-middle, but not wild ---
  let upperMiddlePpsf;
  if (trimmed.length && p60 !== null && p75 !== null) {
    upperMiddlePpsf = p60 * 0.5 + p75 * 0.5;
  } else if (p50 !== null) {
    upperMiddlePpsf = p50;
  } else {
    upperMiddlePpsf = median(sorted) || sorted[Math.floor(n / 2)];
  }

  // Safety cap: don't let upperMiddle get more than ±15% away from median
  const capPct = 0.15;
  let centralPpsf = upperMiddlePpsf;
  if (p50 !== null && p50 > 0) {
    const rawRatio = upperMiddlePpsf / p50;
    const cappedRatio = Math.min(1 + capPct, Math.max(1 - capPct, rawRatio));
    centralPpsf = p50 * cappedRatio;
  }

  const subjectSqft = subject.sqft || usable[0].sqft;
  const baseValue = centralPpsf * subjectSqft;

  // Dispersion: how wide is the distribution?
  let dispersion;
  const med = p50 || centralPpsf;
  if (med && p10 !== null && p90 !== null) {
    dispersion = (p90 - p10) / med;
  } else {
    dispersion = 0.3; // default if unknown
  }

  // Base confidence from comp count + dispersion
  let baseConfidence;
  if (compCount >= 20) baseConfidence = 88;
  else if (compCount >= 15) baseConfidence = 85;
  else if (compCount >= 10) baseConfidence = 78;
  else if (compCount >= 6) baseConfidence = 70;
  else if (compCount >= 3) baseConfidence = 60;
  else baseConfidence = 45;

  if (dispersion < 0.15) baseConfidence += 8;
  else if (dispersion < 0.25) baseConfidence += 4;
  else if (dispersion > 0.45) baseConfidence -= 6;

  baseConfidence = clamp(
    baseConfidence,
    LOCAL_RULES.CONF_MIN,
    LOCAL_RULES.CONF_MAX
  );

  // Spread factor from base confidence
  let spreadFactor;
  if (baseConfidence >= 90) spreadFactor = 0.05;
  else if (baseConfidence >= 80) spreadFactor = 0.07;
  else if (baseConfidence >= 70) spreadFactor = 0.10;
  else if (baseConfidence >= 60) spreadFactor = 0.13;
  else spreadFactor = 0.18;

  // Refine based on dispersion
  if (dispersion < 0.15) {
    spreadFactor *= 0.7; // tight cluster → narrower band
  } else if (dispersion > 0.5) {
    spreadFactor *= 1.2; // very noisy → wider band
  }

  spreadFactor = clamp(
    spreadFactor,
    LOCAL_RULES.SPREAD_MIN_PCT,
    LOCAL_RULES.SPREAD_MAX_PCT
  );

  // Human-readable cluster label
  let clusterLabel;
  if (compCount >= 12 && dispersion < 0.15) {
    clusterLabel = "Very tight cluster (high agreement)";
  } else if (dispersion < 0.25) {
    clusterLabel = "Tight cluster";
  } else if (dispersion < 0.4) {
    clusterLabel = "Normal spread";
  } else if (dispersion < 0.6) {
    clusterLabel = "Wide cluster (mixed comps)";
  } else {
    clusterLabel = "Very noisy cluster";
  }

  const result = {
    confidenceBase: baseConfidence,
    // Alias for convenience if you just want "confidence"
    confidence: baseConfidence,

    spreadFactorBase: spreadFactor,
    spreadFactor,

    compCount,
    subjectSqft,
    baseValue,
    centralPpsf,
    dispersion,
    clusterLabel,
    outlierCount,
    coreCoverage,
    percentiles: {
      p05,
      p10,
      p25,
      p50,
      p60,
      p75,
      p90,
      p95,
    },
  };

  return {
    ...result,
    diagnostics: {
      ...result,
      // extra hints specifically for logging / UI if you want
      note:
        "Hazard + condition adjustments still applied separately in pricingEngine.",
    },
  };
}

/**
 * computeConfidenceCluster
 * ------------------------
 * Multi-factor wrapper used by pricingEngine.
 * Takes summary stats + extra signals (condition, hazard, neighborhood)
 * and produces a final confidence score, while keeping the original
 * PPSF-cluster logic available for debugging/UI.
 *
 * @param {Object} params
 * - compCount
 * - dispersion
 * - conditionAdjPct (±% due to condition, e.g. +5 or -7)
 * - hazardScore (0–1, from pricingEngine)
 * - hasNeighborhoodMatch (boolean)
 * - p10, p50, p90 (optional, for future nuance)
 */
function computeConfidenceCluster(params = {}) {
  const {
    compCount,
    dispersion,
    conditionAdjPct = 0,
    hazardScore = null,
    hasNeighborhoodMatch = false,
    p10,
    p50,
    p90,
  } = params;

  const n = safeNumber(compCount, 0) || 0;
  const d = safeNumber(dispersion, null);
  const disp = d === null ? 0.3 : d;

  // --- Base confidence from count + dispersion (mirrors engine) ---
  let baseConfidence;
  if (n >= 20) baseConfidence = 88;
  else if (n >= 15) baseConfidence = 85;
  else if (n >= 10) baseConfidence = 78;
  else if (n >= 6) baseConfidence = 70;
  else if (n >= 3) baseConfidence = 60;
  else baseConfidence = 45;

  if (disp < 0.15) baseConfidence += 8;
  else if (disp < 0.25) baseConfidence += 4;
  else if (disp > 0.45) baseConfidence -= 6;

  baseConfidence = clamp(
    baseConfidence,
    LOCAL_RULES.CONF_MIN,
    LOCAL_RULES.CONF_MAX
  );

  // --- Neighborhood boost ---
  let neighborhoodBoost = 0;
  if (hasNeighborhoodMatch && n >= 3) {
    neighborhoodBoost = 3; // same-neighborhood comps → slightly higher confidence
  }

  // --- Condition penalty ---
  const absCond = Math.abs(safeNumber(conditionAdjPct, 0));
  let conditionPenalty = 0;
  // thresholds in %-points (e.g. 8 = ±8%)
  if (absCond > 12) conditionPenalty = 4;
  else if (absCond > 8) conditionPenalty = 2;

  // --- Hazard penalty ---
  let hazardPenalty = 0;
  const hs = safeNumber(hazardScore, null);
  if (hs !== null && hs >= 0) {
    // up to -10 points when hazardScore ≈ 1
    hazardPenalty = Math.round(10 * Math.min(hs, 1));
  }

  let finalConfidence =
    baseConfidence + neighborhoodBoost - conditionPenalty - hazardPenalty;

  finalConfidence = clamp(
    finalConfidence,
    LOCAL_RULES.CONF_MIN,
    LOCAL_RULES.CONF_MAX
  );

  return {
    finalConfidence,
    baseConfidence,
    hazardPenalty,
    conditionPenalty,
    neighborhoodBoost,
    inputs: {
      compCount: n,
      dispersion: disp,
      conditionAdjPct,
      hazardScore: hs,
      hasNeighborhoodMatch,
      p10,
      p50,
      p90,
    },
  };
}

module.exports = {
  computeConfidenceAndSpread,
  computeConfidenceCluster,
};