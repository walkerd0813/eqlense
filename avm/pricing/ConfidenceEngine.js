
// backend/avm/pricing/ConfidenceEngine.js
// =======================================
// Confidence + spread "clustering" engine
// ---------------------------------------
// Takes the PPSF distribution from comps and computes:
// - baseConfidence: 0–95 (before hazard / condition penalties)
// - baseSpreadFactor: 0.03–0.25 (low/high band as ±% around estimate)
// - diagnostics: detailed clustering metrics for debug / tuning
//
// This file is intentionally PURE: no logging, no side-effects.

function safeNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
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

// Core API
// --------
// params = {
// compCount: number,
// dispersion: number | null, // (P90 - P10) / median
// ppsf: number[], // raw PPSF values (one per usable comp)
// centralPpsf: number | null // central PPSF used for pricing
// }
function computeConfidenceAndSpread(params = {}) {
  const {
    compCount = 0,
    dispersion = null,
    ppsf = [],
    centralPpsf = null,
  } = params;

  const validPpsf = (ppsf || [])
    .map((v) => safeNumber(v, null))
    .filter((v) => v !== null && v > 0);

  const n = validPpsf.length;

  // If we somehow get no usable PPSF, fall back to very low confidence.
  if (!n || compCount === 0) {
    return {
      baseConfidence: 40,
      baseSpreadFactor: 0.20,
      diagnostics: {
        compCount,
        usedCount: n,
        meanPpsf: null,
        stdPpsf: null,
        coeffVar: null,
        clusterCoverage: null,
        outlierCount: null,
        p10: null,
        p25: null,
        p50: null,
        p75: null,
        p90: null,
        dispersion,
        centralPpsf,
      },
    };
  }

  const sorted = [...validPpsf].sort((a, b) => a - b);

  const p10 = percentile(sorted, 0.10);
  const p25 = percentile(sorted, 0.25);
  const p50 = percentile(sorted, 0.50);
  const p75 = percentile(sorted, 0.75);
  const p90 = percentile(sorted, 0.90);

  // Simple distribution stats
  const mean =
    sorted.reduce((sum, v) => sum + v, 0) / sorted.length;

  let variance = 0;
  for (const v of sorted) {
    const diff = v - mean;
    variance += diff * diff;
  }
  variance /= sorted.length;
  const stdDev = Math.sqrt(variance);
  const coeffVar = mean > 0 ? stdDev / mean : null; // relative dispersion

  // IQR-based "central cluster"
  let clusterCoverage = null;
  let outlierCount = null;

  if (p25 !== null && p75 !== null) {
    const iqr = p75 - p25;
    const lowerFence = p25 - 1.5 * iqr;
    const upperFence = p75 + 1.5 * iqr;

    let inCluster = 0;
    let outCluster = 0;

    for (const v of sorted) {
      if (v >= lowerFence && v <= upperFence) inCluster++;
      else outCluster++;
    }

    clusterCoverage = inCluster / sorted.length;
    outlierCount = outCluster;
  }

  // ------------------------------
  // 1) Base confidence from count
  // ------------------------------
  let baseConfidence;
  if (compCount >= 20) baseConfidence = 88;
  else if (compCount >= 15) baseConfidence = 85;
  else if (compCount >= 10) baseConfidence = 78;
  else if (compCount >= 6) baseConfidence = 70;
  else if (compCount >= 3) baseConfidence = 60;
  else baseConfidence = 45;

  // ------------------------------
  // 2) Adjust for dispersion
  // ------------------------------
  const d = safeNumber(dispersion, null);

  if (d !== null) {
    if (d < 0.15) baseConfidence += 8;
    else if (d < 0.25) baseConfidence += 4;
    else if (d > 0.45) baseConfidence -= 6;
  }

  // ------------------------------
  // 3) Cluster quality adjustments
  // ------------------------------
  if (coeffVar !== null && clusterCoverage !== null) {
    // Very tight, clean cluster → confidence boost
    if (clusterCoverage > 0.9 && coeffVar < 0.20) {
      baseConfidence += 4;
    } else if (clusterCoverage > 0.8 && coeffVar < 0.25) {
      baseConfidence += 2;
    }

    // Messy / multi-cluster / noisy → penalty
    if (clusterCoverage < 0.7 || coeffVar > 0.55) {
      baseConfidence -= 4;
    } else if (clusterCoverage < 0.8 || coeffVar > 0.45) {
      baseConfidence -= 2;
    }
  }

  // Final clamp for baseConfidence (before hazard/condition)
  baseConfidence = Math.max(0, Math.min(95, baseConfidence));

  // ------------------------------
  // 4) Base spread factor
  // ------------------------------
  let spreadFactor;

  // Map confidence bands to base spread
  if (baseConfidence >= 90) spreadFactor = 0.05;
  else if (baseConfidence >= 80) spreadFactor = 0.07;
  else if (baseConfidence >= 70) spreadFactor = 0.10;
  else if (baseConfidence >= 60) spreadFactor = 0.13;
  else spreadFactor = 0.18;

  // Refine spread using dispersion + cluster tightness
  if (d !== null && coeffVar !== null) {
    if (d < 0.15 && coeffVar < 0.25) {
      // very tight → shrink range ~30%
      spreadFactor *= 0.7;
    } else if (d > 0.5 || coeffVar > 0.6) {
      // messy → widen range ~30%
      spreadFactor *= 1.3;
    } else if (clusterCoverage !== null && clusterCoverage > 0.9) {
      // strong central cluster → small extra shrink
      spreadFactor *= 0.85;
    }
  }

  // Global safety clamp (what you requested)
  spreadFactor = Math.max(0.03, Math.min(0.25, spreadFactor));

  return {
    baseConfidence,
    baseSpreadFactor: spreadFactor,
    diagnostics: {
      compCount,
      usedCount: n,
      meanPpsf: mean,
      stdPpsf: stdDev,
      coeffVar,
      clusterCoverage,
      outlierCount,
      p10,
      p25,
      p50,
      p75,
      p90,
      dispersion: d,
      centralPpsf,
    },
  };
}

module.exports = {
  computeConfidenceAndSpread,
};