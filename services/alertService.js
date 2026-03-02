import Listing from "../models/Listing.js";
import Alert from "../models/Alert.js";

// Main generator for listing-based alerts
export async function generateNeighborhoodAlerts(zip) {
  const now = new Date();
  const last15Min = new Date(now.getTime() - 15 * 60 * 1000);

  // -----------------------------
  // 1. NEW LISTINGS
  // -----------------------------
  const newListings = await Listing.find({
    "address.zip": zip,
    status: "active",
    createdAt: { $gte: last15Min },
  }).lean();

  for (const listing of newListings) {
    await Alert.create({
      zip,
      type: "new_listing",
      headline: "New Listing in Your Neighborhood",
      message: `${listing.beds} bed • ${listing.baths} bath • Listed at $${listing.listPrice?.toLocaleString()}`,
      propertyId: listing._id.toString(),
      meta: {
        listPrice: listing.listPrice,
        beds: listing.beds,
        baths: listing.baths,
      },
    });
  }

  // -----------------------------
  // 2. NEW PENDING
  // -----------------------------
  const pendings = await Listing.find({
    "address.zip": zip,
    status: "pending",
    pendingDate: { $gte: last15Min },
  }).lean();

  for (const listing of pendings) {
    await Alert.create({
      zip,
      type: "pending",
      headline: "Home Went Pending",
      message: `A nearby home just went pending.`,
      propertyId: listing._id.toString(),
      meta: {
        beds: listing.beds,
        baths: listing.baths,
      },
    });
  }

  // -----------------------------
  // 3. NEW SOLD
  // -----------------------------
  const solds = await Listing.find({
    "address.zip": zip,
    status: "sold",
    closeDate: { $gte: last15Min },
  }).lean();

  for (const listing of solds) {
    const overUnder =
      listing.closePrice && listing.listPrice
        ? listing.closePrice - listing.listPrice
        : null;

    await Alert.create({
      zip,
      type: "sold",
      headline: "Nearby Home Sold",
      message: overUnder
        ? `Sold for $${listing.closePrice?.toLocaleString()} (${overUnder >= 0 ? "+" : ""}${overUnder.toLocaleString()})`
        : `Home sold near you.`,
      propertyId: listing._id.toString(),
      meta: {
        listPrice: listing.listPrice,
        closePrice: listing.closePrice,
        overUnder,
      },
    });
  }

  // V1 returns true (future versions will return summary)
  return true;
}
