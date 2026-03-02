// backend/services/marketRadarService.js (ESM VERSION)

import Listing from "../models/Listing.js";
import MarketRadarHistory from "../models/MarketRadarHistory.js";

// Utility functions
function safeDivide(numerator, denominator) {
  if (!denominator || denominator === 0) return 0;
  return numerator / denominator;
}

function median(values) {
  if (!values || values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length * 0.5);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return sorted[mid];
}

function pctChange(newVal, oldVal) {
  if (!oldVal || oldVal === 0 || newVal == null || oldVal == null) return 0;
  return ((newVal - oldVal) / oldVal) * 100;
}

function buildSeasonalCurve(soldListings) {
  if (!soldListings || soldListings.length === 0) return {};
  const counts = new Array(12).fill(0);
  soldListings.forEach((l) => {
    if (l.closeDate) {
      const m = l.closeDate.getMonth();
      counts[m] += 1;
    }
  });
  const total = counts.reduce((sum, c) => sum + c, 0) || 1;
  const curve = {};
  counts.forEach((c, idx) => {
    const month = idx + 1;
    curve[month] = c / total;
  });
  return curve;
}

function computeSellNowIndex({ absorptionRate, priceTrendPct, inventoryChangePct }) {
  const absorptionScore = Math.max(0, Math.min(100, absorptionRate * 20));
  const priceScore = Math.max(0, Math.min(100, 50 + priceTrendPct));
  const inventoryScore = Math.max(0, Math.min(100, 50 - inventoryChangePct));

  const combined =
    absorptionScore * 0.4 +
    priceScore * 0.35 +
    inventoryScore * 0.25;

  return Math.round(combined);
}

export async function getMarketRadarForZip({ zip, propertyType }) {
  const now = new Date();

  const ninetyDaysAgo = new Date(now);
  ninetyDaysAgo.setDate(now.getDate() - 90);

  const sixtyDaysAgo = new Date(now);
  sixtyDaysAgo.setDate(now.getDate() - 60);

  const thirtyDaysAgo = new Date(now);
  thirtyDaysAgo.setDate(now.getDate() - 30);

  const twentyFourMonthsAgo = new Date(now);
  twentyFourMonthsAgo.setMonth(now.getMonth() - 24);

  const baseMatch = {
    "address.zip": zip,
  };

  if (propertyType) baseMatch.propertyType = propertyType;

  const [active, pending, recentSold] = await Promise.all([
    Listing.find({ ...baseMatch, status: "active" }).lean(),
    Listing.find({ ...baseMatch, status: "pending" }).lean(),
    Listing.find({
      ...baseMatch,
      status: "sold",
      closeDate: { $gte: ninetyDaysAgo },
    }).lean(),
  ]);

  const activeCount = active.length;
  const pendingCount = pending.length;
  const soldCount = recentSold.length;

  const absorptionPerMonth = safeDivide(soldCount, 3);

  const activeDomValues = active
    .map((l) => l.daysOnMarket)
    .filter((v) => typeof v === "number");
  const soldDomValues = recentSold
    .map((l) => l.daysOnMarket)
    .filter((v) => typeof v === "number");

  const medianActiveDom = median(activeDomValues);
  const medianSoldDom = median(soldDomValues);

  const soldLast30 = recentSold.filter((l) => l.closeDate >= thirtyDaysAgo);
  const soldPrev30 = recentSold.filter(
    (l) =>
      l.closeDate >= sixtyDaysAgo && l.closeDate < thirtyDaysAgo
  );

  const medianSoldLast30 = median(
    soldLast30.map((l) => l.closePrice).filter((v) => typeof v === "number")
  );
  const medianSoldPrev30 = median(
    soldPrev30.map((l) => l.closePrice).filter((v) => typeof v === "number")
  );

  const priceTrendPct = pctChange(medianSoldLast30, medianSoldPrev30);

  const activeSixtyDaysAgoCount = await Listing.countDocuments({
    ...baseMatch,
    status: "active",
    listDate: { $lte: sixtyDaysAgo },
  });

  const inventoryChangePct = pctChange(activeCount, activeSixtyDaysAgoCount);

  const seasonalSolds = await Listing.find({
    ...baseMatch,
    status: "sold",
    closeDate: { $gte: twentyFourMonthsAgo },
  })
    .select("closeDate")
    .lean();

  const seasonalCurve = buildSeasonalCurve(seasonalSolds);

  const sellNowIndex = computeSellNowIndex({
    absorptionRate: absorptionPerMonth,
    priceTrendPct,
    inventoryChangePct,
  });

  const weeklyHeatMeter = Math.max(
    0,
    Math.min(
      100,
      Math.round(
        sellNowIndex * 0.7 +
          safeDivide(soldLast30.length, 10) * 20 +
          safeDivide(pendingCount, 5) * 10
      )
    )
  );

  const metrics = {
    absorptionRate: absorptionPerMonth,
    avgDaysOnMarket: medianSoldDom || medianActiveDom,
    priceTrendPct,
    inventoryTrendPct: inventoryChangePct,
    demandLevel: Number((absorptionPerMonth / 5).toFixed(3)),
    activeCount,
    pendingCount,
    soldCount,
  };

  const existing = await MarketRadarHistory.findOne({ zip });
  let previous = null;
  let momentum = null;

  if (!existing) {
    await MarketRadarHistory.create({
      zip,
      currentMetrics: metrics,
      previousMetrics: null,
      momentum: null,
      lastUpdated: now,
    });
  } else {
    previous = existing.currentMetrics;
    momentum = previous
      ? Object.fromEntries(
          Object.keys(metrics).map((key) => [
            key,
            Number(((metrics[key] || 0) - (previous[key] || 0)).toFixed(3)),
          ])
        )
      : null;

    existing.previousMetrics = previous;
    existing.currentMetrics = metrics;
    existing.momentum = momentum;
    existing.lastUpdated = now;

    await existing.save();
  }

  return {
    zip,
    propertyType: propertyType || null,

    metrics,
    previousMetrics: previous,
    momentum,

    summary: {
      activeCount,
      pendingCount,
      soldLast90Count: soldCount,
      absorptionPerMonth,
      medianActiveDom,
      medianSoldDom,
    },

    trend: {
      medianSoldLast30,
      medianSoldPrev30,
      priceTrendPct,
      inventoryChangePct,
    },

    seasonalCurve,

    indices: {
      sellNowIndex,
      weeklyHeatMeter,
    },

    generatedAt: now,
  };
}
