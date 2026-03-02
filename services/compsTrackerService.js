import Listing from "../models/Listing.js";

/**
 * Returns comps near a given home based on:
 * - zip
 * - radius (via geolocation if present)
 * - propertyType
 */
export async function getCompetitorComps({ zip, propertyType }) {

  const now = new Date();
  const sixMonthsAgo = new Date(now);
  sixMonthsAgo.setMonth(now.getMonth() - 6);

  // Base filter
  const base = {
    "address.zip": zip,
    propertyType,
    listDate: { $gte: sixMonthsAgo },
  };

  // Active, pending, sold comps
  const [active, pending, sold] = await Promise.all([
    Listing.find({ ...base, status: "active" }).lean(),
    Listing.find({ ...base, status: "pending" }).lean(),
    Listing.find({ ...base, status: "sold" }).lean(),
  ]);

  // Trends: upgrades
  const upgradesTrend = {
    kitchen: sold.filter((l) => l.upgrades?.kitchenRemodel).length,
    bathroom: sold.filter((l) => l.upgrades?.bathroomRemodel).length,
    flooring: sold.filter((l) => l.upgrades?.flooring).length,
  };

  // Calculate competitive score for each comp
  function scoreComp(comp) {
    let score = 0;

    // Renovations boost score
    if (comp.upgrades?.kitchenRemodel) score += 12;
    if (comp.upgrades?.bathroomRemodel) score += 10;
    if (comp.upgrades?.flooring) score += 5;

    // Bigger home = slight boost
    if (comp.livingAreaSqft) score += Math.min(15, comp.livingAreaSqft / 200);

    // Recent sale = higher relevance
    if (comp.closeDate) score += 10;

    // DOM advantage
    if (comp.daysOnMarket && comp.daysOnMarket < 15) score += 10;

    return score;
  }

  // Build competitive scoring payload
  const scoredActive = active.map((c) => ({ ...c, score: scoreComp(c) }));
  const scoredPending = pending.map((c) => ({ ...c, score: scoreComp(c) }));
  const scoredSold = sold.map((c) => ({ ...c, score: scoreComp(c) }));

  // Sort by score (descending)
  scoredActive.sort((a, b) => b.score - a.score);
  scoredPending.sort((a, b) => b.score - a.score);
  scoredSold.sort((a, b) => b.score - a.score);

  return {
    zip,
    propertyType,
    totals: {
      active: active.length,
      pending: pending.length,
      sold: sold.length,
    },
    comps: {
      active: scoredActive,
      pending: scoredPending,
      sold: scoredSold,
    },
    trends: {
      last6Months: {
        upgrades: upgradesTrend,
      },
    },
    generatedAt: new Date(),
  };
}
