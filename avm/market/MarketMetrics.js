// backend/avm/market/MarketMetrics.js
// ------------------------------------
// Computes neighborhood-level market metrics from the comps:
// - Volatility (0–100)
// - Absorption (0–100)
// - Liquidity (0–100)
// Plus supporting stats: DOM median, sales per month, lookback window.

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

/**
 * computeMarketMetrics
 * --------------------
 * @param {Array} usable - usable comps (already filtered in pricingEngine)
 * @param {number} dispersion - PPSF dispersion (approx (P90 - P10) / median)
 *
 * Returns:
 * {
 * volatilityScore, volatilityLabel,
 * absorptionScore, absorptionLabel,
 * liquidityScore, liquidityLabel,
 * domMedian, salesPerMonth, lookbackMonths
 * }
 */
function computeMarketMetrics(usable = [], dispersion) {
  const MS_PER_DAY = 24 * 60 * 60 * 1000;
  const DAYS_PER_MONTH = 30.4375;

  const saleDates = [];
  const domValues = [];

  for (const c of usable) {
    let d = null;

    if (c.saleDate) d = new Date(c.saleDate);
    else if (c.settledDate) d = new Date(c.settledDate);
    else if (c.closeDate) d = new Date(c.closeDate);
    else if (c.closingDate) d = new Date(c.closingDate);
    else if (c.recordingDate) d = new Date(c.recordingDate);

    if (d && !Number.isNaN(d.getTime())) {
      saleDates.push(d);
    }

    const dom = safeNumber(
      c.marketTime ??
        c.daysOnMarket ??
        c.dom ??
        c.DOM ??
        c.cdom ??
        c.CDOM,
      null
    );
    if (dom !== null) {
      domValues.push(dom);
    }
  }

  // Lookback window in months, based on sale date range
  let lookbackMonths = 12;
  if (saleDates.length >= 2) {
    saleDates.sort((a, b) => a - b);
    const first = saleDates[0];
    const last = saleDates[saleDates.length - 1];
    const diffDays = Math.max(1, (last - first) / MS_PER_DAY);
    lookbackMonths = Math.max(1, diffDays / DAYS_PER_MONTH);
  } else if (saleDates.length === 1) {
    // Single recent sale → assume ~6 months of effective window
    lookbackMonths = 6;
  }

  const rawSalesPerMonth = lookbackMonths > 0 ? usable.length / lookbackMonths : 0;
  const salesPerMonth = Number(rawSalesPerMonth.toFixed(2));

  const domMedian = domValues.length ? median(domValues) : null;

  // --- Volatility (0–100, higher = more volatile) ---
  let volatilityScore = null;
  let volatilityLabel = null;

  if (typeof dispersion === "number" && Number.isFinite(dispersion)) {
    const normalized = Math.max(0, Math.min(1, dispersion / 0.6)); // ~0.6+ = highly volatile
    volatilityScore = Math.round(normalized * 100);

    if (volatilityScore < 20) volatilityLabel = "Very stable";
    else if (volatilityScore < 40) volatilityLabel = "Stable";
    else if (volatilityScore < 60) volatilityLabel = "Normal";
    else if (volatilityScore < 80) volatilityLabel = "Volatile";
    else volatilityLabel = "Highly volatile";
  }

  // --- Absorption (0–100, higher = faster absorption) ---
  let absorptionScore = 60; // baseline mid
  if (domMedian != null) {
    if (domMedian <= 15) absorptionScore = 92;
    else if (domMedian <= 30) absorptionScore = 85;
    else if (domMedian <= 45) absorptionScore = 78;
    else if (domMedian <= 60) absorptionScore = 70;
    else if (domMedian <= 90) absorptionScore = 60;
    else absorptionScore = 50;
  }

  // Adjust by sales velocity
  if (salesPerMonth >= 15) absorptionScore += 8;
  else if (salesPerMonth >= 8) absorptionScore += 4;
  else if (salesPerMonth <= 2) absorptionScore -= 6;

  absorptionScore = Math.max(0, Math.min(100, Math.round(absorptionScore)));

  let absorptionLabel;
  if (absorptionScore >= 85) absorptionLabel = "Very fast";
  else if (absorptionScore >= 70) absorptionLabel = "Fast";
  else if (absorptionScore >= 55) absorptionLabel = "Balanced";
  else absorptionLabel = "Slow";

  // --- Liquidity (0–100, ease of entering/exiting) ---
  let velocityScore = Math.max(0, Math.min(100, (salesPerMonth / 10) * 100));
  velocityScore = Math.round(velocityScore);

  let liquidityScore = Math.round(
    0.6 * absorptionScore + 0.4 * velocityScore
  );
  liquidityScore = Math.max(0, Math.min(100, liquidityScore));

  let liquidityLabel;
  if (liquidityScore >= 85) liquidityLabel = "High";
  else if (liquidityScore >= 65) liquidityLabel = "Good";
  else if (liquidityScore >= 45) liquidityLabel = "Moderate";
  else liquidityLabel = "Low";

  return {
    volatilityScore,
    volatilityLabel,
    absorptionScore,
    absorptionLabel,
    liquidityScore,
    liquidityLabel,
    domMedian: domMedian != null ? Math.round(domMedian) : null,
    salesPerMonth,
    lookbackMonths: Number(lookbackMonths.toFixed(1)),
  };
}

module.exports = {
  computeMarketMetrics,
};
