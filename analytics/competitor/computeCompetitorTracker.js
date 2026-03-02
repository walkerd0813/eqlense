// analytics/competitor/computeCompetitorTracker.js

import Listing from "../../models/Listing.js";
import { computeVelocity } from "../../publicData/liquidity/velocity.js";

/**
 * Field mapping so you can easily adjust to whatever your Listing schema uses.
 * If your Listing model uses different field names, update these in one place.
 */
const FIELD_MAP = {
  zip: "zip",              // e.g. "zip" or "postalCode"
  status: "status",
  listDate: "listDate",    // listing date
  closeDate: "closeDate",  // closing date
  officeId: "listOfficeMlsId",   // brokerage/office ID
  officeName: "listOfficeName",  // brokerage/office name
};

/**
 * Phase B competitor tracker engine.
 *
 * Core idea:
 * - Look at listings in the broker's ZIPs over a lookback window (e.g. 90 days)
 * - Group by brokerage/office
 * - Compute:
 *    - totalListings
 *    - activeListings
 *    - soldLast30
 *    - velocity
 *    - marketShare
 * - Mark which office is the "focus" (the logged-in broker) if we have an ID
 *
 * This is designed to power:
 * - Competitor leaderboard
 * - Territory risk / pressure indicators
 * - Broker's own standing vs competitors
 *
 * @param {Object} options
 * @param {string[]} [options.zips]               Array of ZIP codes to include (if empty => all zips)
 * @param {Date}     [options.asOfDate]          Anchor date for analysis (defaults to now)
 * @param {number}   [options.windowDays]        Lookback window in days (default 90)
 * @param {string}   [options.focusBrokerOfficeId] Current broker's office MLS ID (for highlighting)
 * @returns {Promise<Object>} structured competitor analytics
 */
export async function computeCompetitorTracker(options = {}) {
  const {
    zips = [],
    asOfDate = new Date(),
    windowDays = 90,
    focusBrokerOfficeId = null,
  } = options;

  const asOf = new Date(asOfDate);
  const windowStart = new Date(asOf);
  windowStart.setDate(windowStart.getDate() - windowDays);

  const soldCutoff = new Date(asOf);
  soldCutoff.setDate(soldCutoff.getDate() - 30);

  const { zip, status, listDate, closeDate, officeId, officeName } = FIELD_MAP;

  const matchStage = {
    [listDate]: { $gte: windowStart, $lte: asOf },
  };

  if (zips && zips.length > 0) {
    matchStage[zip] = { $in: zips };
  }

  // Adjust statuses here if your Listing model stores them differently
  const ACTIVE_STATUSES = ["ACTIVE", "UNDER AGREEMENT", "PENDING", "BACK ON MARKET"];
  const SOLD_STATUS = "SOLD";

  const pipeline = [
    { $match: matchStage },

    {
      $addFields: {
        _isActive: { $in: [`$${status}`, ACTIVE_STATUSES] },
        _isSoldRecent: {
          $and: [
            { $eq: [`$${status}`, SOLD_STATUS] },
            { $gte: [`$${closeDate}`, soldCutoff] },
          ],
        },
      },
    },

    {
      $group: {
        _id: `$${officeId}`,
        officeName: { $first: `$${officeName}` },
        totalListings: { $sum: 1 },
        activeListings: {
          $sum: {
            $cond: ["$_isActive", 1, 0],
          },
        },
        soldLast30: {
          $sum: {
            $cond: ["$_isSoldRecent", 1, 0],
          },
        },
        zips: { $addToSet: `$${zip}` },
      },
    },

    // Ignore entries with no office id (can be filtered if needed)
    {
      $match: {
        _id: { $ne: null },
      },
    },

    // Sort by total listing volume descending
    {
      $sort: {
        totalListings: -1,
      },
    },
  ];

  const rawOffices = await Listing.aggregate(pipeline).exec();

  const totalListings = rawOffices.reduce(
    (sum, o) => sum + (o.totalListings || 0),
    0
  );

  const offices = rawOffices.map((o) => {
    const velocity = computeVelocity({
      soldCount30: o.soldLast30 || 0,
      activeCount: o.activeListings || 0,
    });

    const share = totalListings
      ? (o.totalListings || 0) / totalListings
      : 0;

    return {
      brokerageId: o._id,
      name: o.officeName || "Unknown Office",
      totalListings: o.totalListings || 0,
      activeListings: o.activeListings || 0,
      soldLast30: o.soldLast30 || 0,
      velocity,
      share,
      zips: o.zips || [],
      isFocusBroker:
        focusBrokerOfficeId &&
        String(o._id) === String(focusBrokerOfficeId),
    };
  });

  const topBrokerages = offices.slice(0, 10);

  return {
    generatedAt: new Date(),
    windowDays,
    asOf,
    zipFilter: zips,
    totals: {
      totalListings,
      distinctBrokerages: offices.length,
    },
    brokerages: offices,
    topBrokerages,
  };
}
