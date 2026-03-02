// backend/avm/pricing/pricingEngine.js
// ===================================
// Core pricing engine for AVM v1+v2 "X"
// - Uses trimmed PPSF from comps
// - Stronger tilt toward upper-middle PPSF, but with safety caps
// (optimistic but not crazy → "X" behavior")
// - Confidence + spread come from a cluster-based engine
// (comp count, dispersion, condition, hazard, neighborhood match)
// - Optional hazard adjustment hook (downside only)
// - Delegates market metrics (volatility / absorption / liquidity)
// to backend/avm/market/MarketMetrics.js
// - Condition-scoring adjustment (subject vs comps)
// - All public outputs are rounded (no long decimals)

const { computeMarketMetrics } = require("../market/MarketMetrics");
const { computeConfidenceCluster } = require("./ConfidenceCluster");

// -----------------------
// Helpers
// -----------------------
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

// -----------------------
// Rule clamping helpers
// -----------------------
function clamp(value, min, max) {
  const n = safeNumber(value, null);
  if (n === null) return value;
  if (n < min) return min;
  if (n > max) return max;
  return n;
}

function clampFromEnv(envKey, defaultValue, min, max) {
  const raw = process.env[envKey];
  const base =
    raw === undefined || raw === null
      ? defaultValue
      : safeNumber(raw, defaultValue);
  return clamp(base, min, max);
}

// Central place for tunable rule values (with hard safety bounds)
const RULES = {
  // Max downside % for hazards (e.g. flood) – defaults to 8%, clamps 2–20%
  HAZARD_MAX_PCT: clampFromEnv("AVM_HAZARD_MAX_PCT", 0.08, 0.02, 0.2),

  // Max absolute % for condition adjustment – defaults to 12%, clamps 4–25%
  CONDITION_MAX_PCT: clampFromEnv("AVM_CONDITION_MAX_PCT", 0.12, 0.04, 0.25),

  // Spread min/max – defaults 3–25%, clamps 1–10% min and 10–40% max
  SPREAD_MIN_PCT: clampFromEnv("AVM_SPREAD_MIN_PCT", 0.03, 0.01, 0.1),
  SPREAD_MAX_PCT: clampFromEnv("AVM_SPREAD_MAX_PCT", 0.25, 0.1, 0.4),

  // Confidence bounds – default 0–95, clamps inside 0–100
  CONFIDENCE_MIN: clampFromEnv("AVM_CONFIDENCE_MIN", 0, 0, 100),
  CONFIDENCE_MAX: clampFromEnv("AVM_CONFIDENCE_MAX", 95, 50, 100),
};

// -----------------------
// Hazard helpers (optional)
// -----------------------
function deriveHazardScore(hazard) {
  if (!hazard || !hazard.data) return null;
  const data = hazard.data;

  // Prefer 0–1 composite if present
  const composite = safeNumber(data.compositeRisk, null);
  if (composite !== null && composite >= 0 && composite <= 1) {
    return composite;
  }

  // Fallback to floodRisk category, if that's all we have
  if (typeof data.floodRisk === "string") {
    const r = data.floodRisk.toLowerCase();
    if (r.includes("very_high") || r.includes("extreme")) return 1.0;
    if (r.includes("high")) return 0.8;
    if (r.includes("moderate") || r.includes("medium")) return 0.5;
    if (r.includes("low")) return 0.2;
  }

  return null;
}

function applyHazardAdjustment(baseValue, hazard) {
  const score = deriveHazardScore(hazard);
  if (score === null) {
    return {
      adjustedValue: baseValue,
      hazardAdj: 0,
      hazardAdjPct: 0,
      hazardScore: null,
    };
  }

  // Up to -X% impact in worst case (X from RULES)
  const maxPct = RULES.HAZARD_MAX_PCT;
  const pct = -maxPct * score;
  const delta = baseValue * pct;

  return {
    adjustedValue: baseValue + delta,
    hazardAdj: delta,
    hazardAdjPct: pct * 100,
    hazardScore: score,
  };
}

// -----------------------
// Condition scoring
// -----------------------
function getConditionScore(entity) {
  if (!entity) return null;

  const candidates = [
    entity.conditionScore,
    entity.condition,
    entity.overallCondition,
    entity.quality,
    entity.Quality,
    entity.raw && entity.raw.conditionScore,
    entity.raw && entity.raw.condition,
    entity.raw && entity.raw.overallCondition,
  ];

  for (const c of candidates) {
    if (c == null) continue;

    // Text descriptors (MLS-style)
    if (typeof c === "string") {
      const lower = c.toLowerCase();
      if (lower.includes("excellent") || lower.includes("new")) return 95;
      if (lower.includes("good") || lower.includes("updated")) return 80;
      if (lower.includes("average") || lower.includes("typical")) return 60;
      if (lower.includes("fair")) return 45;
      if (lower.includes("poor") || lower.includes("tear")) return 25;
    }

    const n = safeNumber(c, null);
    if (n === null) continue;

    // 0–1 scale
    if (n >= 0 && n <= 1) return n * 100;

    // 1–5 or 1–10 scale
    if (n >= 1 && n <= 5) return n * 20; // 1→20, 5→100
    if (n > 5 && n <= 10) return n * 10; // 6→60, 10→100

    // 0–100 scale already
    if (n > 10 && n <= 100) return n;
  }

  return null;
}

function applyConditionAdjustment(baseValue, subject, comps) {
  const subjectScore = getConditionScore(subject);
  const compScores = (comps || [])
    .map((c) => getConditionScore(c))
    .filter((s) => s !== null);

  if (subjectScore === null || compScores.length < 3) {
    return {
      adjustedValue: baseValue,
      conditionAdjPct: 0,
      conditionScoreSubject: subjectScore,
      conditionScoreMedian: compScores.length ? median(compScores) : null,
      conditionLabel: null,
    };
  }

  const compMedian = median(compScores);
  if (compMedian === null) {
    return {
      adjustedValue: baseValue,
      conditionAdjPct: 0,
      conditionScoreSubject: subjectScore,
      conditionScoreMedian: null,
      conditionLabel: null,
    };
  }

  const delta = subjectScore - compMedian; // positive = nicer than comps
  const maxPct = RULES.CONDITION_MAX_PCT; // up to ±X%
  const clampedDelta = Math.max(-40, Math.min(40, delta)); // ±40 points → full effect
  const normalized = clampedDelta / 40; // -1 → +1

  const adjPct = maxPct * normalized; // -maxPct → +maxPct
  const adjustedValue = baseValue * (1 + adjPct);

  let conditionLabel;
  if (adjPct > maxPct * 0.5) conditionLabel = "Above-market condition";
  else if (adjPct > maxPct * 0.17)
    conditionLabel = "Slightly above-market condition";
  else if (adjPct < -maxPct * 0.5) conditionLabel = "Below-market condition";
  else if (adjPct < -maxPct * 0.17)
    conditionLabel = "Slightly below-market condition";
  else conditionLabel = "Similar condition to comps";

  return {
    adjustedValue,
    conditionAdjPct: adjPct * 100, // percent, e.g. +5 = +5%
    conditionScoreSubject: Math.round(subjectScore),
    conditionScoreMedian: Math.round(compMedian),
    conditionLabel,
  };
}

// -----------------------
// Core pricing function
// -----------------------
/**
 * priceProperty
 * -------------
 * @param {Object} params
 * - subject: normalized subject property (from CompSelector)
 * - comps: array of normalized comps
 * - hazard: optional hazard object from hazardLookup
 */
function priceProperty({ subject, comps, hazard = null }) {
  if (!subject || !Array.isArray(comps) || !comps.length) {
    return {
      estimate: null,
      low: null,
      high: null,
      method: "no-comps",
      subjectSqft: subject?.sqft || null,
      compCount: 0,
      confidence: 0,
      spreadPercent: null,
      hazardScore: null,
      hazardAdjPct: 0,
      conditionAdjPct: 0,
      conditionScoreSubject: null,
      conditionScoreMedian: null,
      conditionLabel: null,
      market: null,
    };
  }

  // 1) Filter to usable comps
  const usable = comps.filter(
    (c) =>
      safeNumber(c.salePrice, null) &&
      safeNumber(c.sqft, null) &&
      c.sqft > 200
  );

  if (!usable.length) {
    return {
      estimate: null,
      low: null,
      high: null,
      method: "no-usable-comps",
      subjectSqft: subject?.sqft || null,
      compCount: 0,
      confidence: 0,
      spreadPercent: null,
      hazardScore: null,
      hazardAdjPct: 0,
      conditionAdjPct: 0,
      conditionScoreSubject: null,
      conditionScoreMedian: null,
      conditionLabel: null,
      market: null,
    };
  }

  const compCount = usable.length;

  // 2) PPSF vector
  const ppsf = usable.map((c) => c.salePrice / c.sqft);

  // 3) Distribution stats + "X" style center
  const sorted = [...ppsf].sort((a, b) => a - b);
  const n = sorted.length;

  // Slightly gentler trim: 5–95 instead of 10–90 (keeps a bit more signal)
  const p05 = percentile(sorted, 0.05);
  const p95 = percentile(sorted, 0.95);

  const p10 = percentile(sorted, 0.1);
  const p25 = percentile(sorted, 0.25);
  const p50 = percentile(sorted, 0.5); // median
  const p60 = percentile(sorted, 0.6);
  const p75 = percentile(sorted, 0.75);
  const p90 = percentile(sorted, 0.9);

  const trimmed = sorted.filter((v) => v >= p05 && v <= p95);

  // --- Base central PPSF: upper-middle, but not wild ---
  // Use a blend of 60th and 75th percentile (optimistic)
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
  const baseEstimate = centralPpsf * subjectSqft;

  // 4) Hazard adjustment (downside only)
  const {
    adjustedValue: hazardAdjustedValue,
    hazardAdjPct,
    hazardScore,
  } = applyHazardAdjustment(baseEstimate, hazard);

  // 5) Condition adjustment
  const {
    adjustedValue: conditionAdjustedValue,
    conditionAdjPct,
    conditionScoreSubject,
    conditionScoreMedian,
    conditionLabel,
  } = applyConditionAdjustment(hazardAdjustedValue, subject, usable);

  // 6) Confidence + spread via cluster engine

  // Rough dispersion metric: (P90 - P10) / median
  let dispersion;
  const med = p50 || centralPpsf;
  if (med && p10 !== null && p90 !== null) {
    dispersion = (p90 - p10) / med;
  } else {
    dispersion = 0.3; // baseline if unknown
  }

  // ------------------------------------------------------
  // CONFIDENCE CLUSTER ENGINE (multi-factor)
  // ------------------------------------------------------
  const cluster = computeConfidenceCluster({
    compCount,
    dispersion,
    conditionAdjPct, // already in %-points, e.g. +5 / -7
    hazardScore,
    hasNeighborhoodMatch: subject._neighborhoodMatch === true,
    p10,
    p50,
    p90,
  });

  let confidence = cluster.finalConfidence;

  // Spread factor from confidence (base relationship)
  let spreadFactor;
  if (confidence >= 90) spreadFactor = 0.05;
  else if (confidence >= 80) spreadFactor = 0.07;
  else if (confidence >= 70) spreadFactor = 0.10;
  else if (confidence >= 60) spreadFactor = 0.13;
  else spreadFactor = 0.18;

  // X-style refinement
  if (dispersion < 0.15) spreadFactor *= 0.7;
  else if (dispersion > 0.5) spreadFactor *= 1.2;

  // Clamp using RULES
  spreadFactor = clamp(
    spreadFactor,
    RULES.SPREAD_MIN_PCT,
    RULES.SPREAD_MAX_PCT
  );

  // 7) Market metrics from comps
  const marketMetrics = computeMarketMetrics(usable, dispersion);

  // 8) Final rounded outputs
  const rawEstimate = conditionAdjustedValue;
  const rawLow = rawEstimate * (1 - spreadFactor);
  const rawHigh = rawEstimate * (1 + spreadFactor);

  const estimate = Math.round(rawEstimate);
  const low = Math.round(rawLow);
  const high = Math.round(rawHigh);

  return {
    estimate,
    low,
    high,
    method: "ppsf-upper-middle-vX",
    subjectSqft,
    compCount,
    confidence: Math.round(confidence),
    spreadPercent: Math.round(spreadFactor * 100),
    hazardScore,
    hazardAdjPct,
    conditionAdjPct, // +/- % due to condition
    conditionScoreSubject, // 0–100 scale
    conditionScoreMedian, // 0–100 scale
    conditionLabel, // human-readable
    market: marketMetrics,
    // Extra internals for future ML / calibration / debug UI
    _stats: {
      p05,
      p10,
      p25,
      p50,
      p60,
      p75,
      p90,
      p95,
      dispersion,
      centralPpsf,
      rules: RULES,
      confidenceDiagnostics: cluster,
    },
  };
}

module.exports = {
  priceProperty,
};
