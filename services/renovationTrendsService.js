import Listing from "../models/Listing.js";

export async function getRenovationTrendsForZip(zip) {
  const listings = await Listing.find({
    "address.zip": zip,
    status: "sold",
    closeDate: { $gte: new Date(Date.now() - 365 * 24 * 60 * 60 * 1000) }, // last 12 mo
  }).lean();

  const trends = {
    upgradedCount: 0,
    totalListings: listings.length,
    upgradeFrequency: {
      kitchenRemodel: 0,
      bathroomRemodel: 0,
      roof: 0,
      hvac: 0,
      flooring: 0,
      exteriorSiding: 0,
      windows: 0,
    },
    renovationPremium: {}, // computed below
  };

  // Count upgrades
  for (const l of listings) {
    if (!l.upgrades) continue;

    for (const key of Object.keys(trends.upgradeFrequency)) {
      if (l.upgrades[key]) {
        trends.upgradeFrequency[key]++;
        trends.upgradedCount++;
      }
    }
  }

  // Compute price premium
  const renovatedSales = listings.filter((l) => l.upgrades && Object.values(l.upgrades).includes(true));
  const normalSales = listings.filter((l) => !l.upgrades || !Object.values(l.upgrades).includes(true));

  const avgRenovated = renovatedSales.reduce((a, b) => a + (b.closePrice || 0), 0) / (renovatedSales.length || 1);
  const avgNormal = normalSales.reduce((a, b) => a + (b.closePrice || 0), 0) / (normalSales.length || 1);

  trends.renovationPremium = {
    avgRenovated,
    avgNormal,
    premiumPct: avgNormal ? ((avgRenovated - avgNormal) / avgNormal) * 100 : 0,
  };

  return trends;
}
