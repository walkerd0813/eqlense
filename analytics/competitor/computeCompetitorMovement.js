// backend/analytics/competitor/computeCompetitorMovement.js

import Listing from "../../models/Listing.js";

/**
 * A single weekly point in an office's listing timeline.
 * @typedef {Object} OfficeTimelinePoint
 * @property {number} weekIndex    - index of week since windowStart (0 = first week)
 * @property {number} listings     - total listings matching filter in that week
 * @property {number} activeCount  - active listings in that week
 * @property {number} soldCount    - sold listings in that week
 */

/**
 * Aggregate listing counts for a given ZIP and office.
 * @typedef {Object} ZipImpact
 * @property {string} zip          - ZIP code
 * @property {number} listings     - total listings in that ZIP
 * @property {number} activeCount  - active listings in that ZIP
 * @property {number} soldCount    - sold listings in that ZIP
 */

/**
 * Movement data for a single brokerage office.
 * @typedef {Object} OfficeMovement
 * @property {string} officeId
 * @property {string} officeName
 * @property {OfficeTimelinePoint[]} timeline
 * @property {number} netChange      - net change in listings from first to last week
 * @property {"rising"|"falling"|"flat"} direction
 * @property {ZipImpact[]} zipImpact - ZIPs where this office is active
 */

/**
 * Result of the competitor movement engine.
 * @typedef {Object} CompetitorMovementResult
 * @property {Date} generatedAt
 * @property {Date} asOf
 * @property {number} windowWeeks
 * @property {string[]} zipFilter
 * @property {OfficeMovement[]} offices
 */

/**
 * Field mapping: adjust here if your Listing schema changes.
 */
const FIELD_MAP = {
  zip: "address.zip",
  status: "status",
  listDate: "listDate",
  closeDate: "closeDate",
  officeId: "listOfficeMlsId",
  officeName: "listOfficeName",
};

/**
 * Phase C – Competitor Movement Engine
 *
 * Goal:
 * - Look at activity per brokerage office by ZIP across a rolling window (e.g., last 8 weeks).
 * - Produce a simple "timeline" for each office.
 * - Compute net movement (rising / falling / flat) and ZIP impact.
 *
 * @param {Object} [options]
 * @param {string[]} [options.zips]         - ZIPs to restrict to (if empty => all)
 * @param {Date|string} [options.asOfDate]  - Anchor date (defaults to now)
 * @param {number} [options.weeks]          - Lookback in weeks (default 8)
 * @returns {Promise<CompetitorMovementResult>}
 */
export async function computeCompetitorMovement(options = {}) {
  const {
    zips = [],
    asOfDate = new Date(),
    weeks = 8,
  } = options;

  const asOf = new Date(asOfDate);
  const windowWeeks = Math.max(1, weeks);
  const windowStart = new Date(asOf);
  windowStart.setDate(windowStart.getDate() - windowWeeks * 7);

  const { zip, status, listDate, officeId, officeName } = FIELD_MAP;

  /** @type {Record<string, any>} */
  const matchStage = {
    [listDate]: { $gte: windowStart, $lte: asOf },
  };

  if (zips && zips.length > 0) {
    matchStage[zip] = { $in: zips };
  }

  // Listing status enums in Listing.js:
  // ["active", "pending", "sold", "expired", "withdrawn"]
  const ACTIVE_STATUSES = ["active", "pending"];

  const millisPerWeek = 1000 * 60 * 60 * 24 * 7;

  const pipeline = [
    { $match: matchStage },

    // derive weekIndex, active / sold flags
    {
      $addFields: {
        _weekIndex: {
          $floor: {
            $divide: [
              { $subtract: [`$${listDate}`, windowStart] },
              millisPerWeek,
            ],
          },
        },
        _isActive: {
          $in: [`$${status}`, ACTIVE_STATUSES],
        },
        _isSold: {
          $eq: [`$${status}`, "sold"],
        },
      },
    },

    {
      $group: {
        _id: {
          officeId: `$${officeId}`,
          officeName: `$${officeName}`,
          zip: `$${zip}`,
          weekIndex: "$_weekIndex",
        },
        listings: { $sum: 1 },
        activeCount: {
          $sum: {
            $cond: ["$_isActive", 1, 0],
          },
        },
        soldCount: {
          $sum: {
            $cond: ["$_isSold", 1, 0],
          },
        },
      },
    },

    // filter out rows with no office id
    {
      $match: {
        "_id.officeId": { $ne: null },
      },
    },

    {
      $sort: {
        "_id.officeId": 1,
        "_id.weekIndex": 1,
      },
    },
  ];

  /** @type {Array<{_id:{officeId:string,officeName:string,zip:string,weekIndex:number}, listings:number, activeCount:number, soldCount:number}>} */
  const raw = await Listing.aggregate(pipeline).exec();

  /** @type {Map<string, {officeId:string, officeName:string, timeline:OfficeTimelinePoint[], _zipAgg: Map<string, ZipImpact>}>} */
  const officesById = new Map();

  for (const row of raw) {
    const { officeId: oid, officeName: name, zip: zipVal, weekIndex } = row._id;

    if (!officesById.has(oid)) {
      officesById.set(oid, {
        officeId: oid,
        officeName: name || "Unknown Office",
        timeline: [],
        _zipAgg: new Map(),
      });
    }

    const office = officesById.get(oid);

    /** @type {OfficeTimelinePoint} */
    const point = {
      weekIndex,
      listings: row.listings || 0,
      activeCount: row.activeCount || 0,
      soldCount: row.soldCount || 0,
    };

    office.timeline.push(point);

    const zipKey = zipVal || "unknown";
    if (!office._zipAgg.has(zipKey)) {
      /** @type {ZipImpact} */
      const initialZip = {
        zip: zipKey,
        listings: 0,
        activeCount: 0,
        soldCount: 0,
      };
      office._zipAgg.set(zipKey, initialZip);
    }

    const zipStats = office._zipAgg.get(zipKey);
    zipStats.listings += row.listings || 0;
    zipStats.activeCount += row.activeCount || 0;
    zipStats.soldCount += row.soldCount || 0;
  }

  /** @type {OfficeMovement[]} */
  const offices = [];

  for (const office of officesById.values()) {
    // sort timeline in chronological order
    office.timeline.sort((a, b) => a.weekIndex - b.weekIndex);

    const first = office.timeline[0] || { listings: 0 };
    const last =
      office.timeline[office.timeline.length - 1] || { listings: 0 };

    const netChange = (last.listings || 0) - (first.listings || 0);

    /** @type {"rising"|"falling"|"flat"} */
    let direction = "flat";
    if (netChange > 0) direction = "rising";
    else if (netChange < 0) direction = "falling";

    // convert zipAgg map to ordered array (most listings first)
    const zipImpact = Array.from(office._zipAgg.values()).sort(
      (a, b) => (b.listings || 0) - (a.listings || 0)
    );

    offices.push({
      officeId: office.officeId,
      officeName: office.officeName,
      timeline: office.timeline,
      netChange,
      direction,
      zipImpact,
    });
  }

  // sort offices by netChange descending (biggest movers first)
  offices.sort((a, b) => (b.netChange || 0) - (a.netChange || 0));

  /** @type {CompetitorMovementResult} */
  const result = {
    generatedAt: new Date(),
    asOf,
    windowWeeks,
    zipFilter: zips,
    offices,
  };

  return result;
}

export default computeCompetitorMovement;
