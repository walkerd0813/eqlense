// @ts-nocheck
// backend/analytics/competitor/computeCompetitorMomentum.js

import { computeCompetitorMovement } from "./computeCompetitorMovement.js";



/**
 * Reuse types from movement module for stronger safety.
 * These imports exist only at type-check time (JSDoc, not runtime).
 *
 * @typedef {import("./computeCompetitorMovement.js").OfficeMovement} OfficeMovement
 * @typedef {import("./computeCompetitorMovement.js").CompetitorMovementResult} CompetitorMovementResult
 */

/**
 * Point in the dominance timeline (for charts).
 * @typedef {Object} DominanceTimelinePoint
 * @property {string} weekLabel
 * @property {number} dominance
 */

/**
 * Strategy alert describing an opportunity or risk.
 * @typedef {Object} StrategyAlert
 * @property {"high"|"medium"|"low"} severity
 * @property {string} title
 * @property {string} detail
 * @property {string} timeAgo
 */

/**
 * Compute a momentum score for a single brokerage office.
 *
 * @param {OfficeMovement} office
 * @returns {number}
 */
function computeOfficeMomentumScore(office) {
  const timeline = office.timeline || [];
  if (timeline.length < 2) return 0;

  const sorted = [...timeline].sort((a, b) => a.weekIndex - b.weekIndex);

  /** @type {number[]} */
  const deltas = [];
  for (let i = 1; i < sorted.length; i++) {
    const prev = sorted[i - 1];
    const curr = sorted[i];
    deltas.push((curr.listings || 0) - (prev.listings || 0));
  }

  if (deltas.length === 0) return 0;

  const recent = deltas.slice(-3);
  const recentAvg = recent.reduce((a, b) => a + b, 0) / recent.length;

  const firstListings = sorted[0].listings || 0;
  const lastListings = sorted[sorted.length - 1].listings || 0;
  const overallDelta = lastListings - firstListings;

  const volumeBias = lastListings * 0.2;

  return recentAvg * 0.6 + overallDelta * 0.4 + volumeBias;
}

/**
 * Build a simple dominance timeline for charting.
 *
 * @param {CompetitorMovementResult} movementResult
 * @returns {DominanceTimelinePoint[]}
 */
function buildDominanceTimeline(movementResult) {
  const offices = movementResult.offices || [];

  /** @type {Map<number, {weekIndex:number, dominance:number}>} */
  const weekMap = new Map();

  for (const office of offices) {
    for (const t of office.timeline || []) {
      if (!weekMap.has(t.weekIndex)) {
        weekMap.set(t.weekIndex, {
          weekIndex: t.weekIndex,
          dominance: 0,
        });
      }
      const bucket = weekMap.get(t.weekIndex);
      bucket.dominance += t.listings || 0;
    }
  }

  return Array.from(weekMap.values())
    .sort((a, b) => a.weekIndex - b.weekIndex)
    .map(
      /** 
       * @param {{weekIndex:number, dominance:number}} w 
       * @returns {DominanceTimelinePoint}
       */
      (w) => ({
        weekLabel: `Week ${w.weekIndex + 1}`,
        dominance: w.dominance,
      })
    );
}

/**
 * Convert movement data into actionable strategy alerts.
 *
 * @param {Array<OfficeMovement & {momentumScore:number}>} offices
 * @returns {StrategyAlert[]}
 */
function buildMovementAlerts(offices) {
  /** @type {StrategyAlert[]} */
  const alerts = [];

  const surging = offices.filter((o) => o.momentumScore > 1 && o.netChange > 0);
  const falling = offices.filter((o) => o.momentumScore < -1 && o.netChange < 0);

  for (const office of surging.slice(0, 3)) {
    const z = office.zipImpact?.[0]?.zip || "your ZIPs";
    alerts.push({
      severity: "high",
      title: `${office.officeName} is surging`,
      detail: `${office.officeName} is rapidly gaining listings, especially in ${z}.`,
      timeAgo: "Recent weeks",
    });
  }

  for (const office of falling.slice(0, 2)) {
    const z = office.zipImpact?.[0]?.zip || "your ZIPs";
    alerts.push({
      severity: "medium",
      title: `${office.officeName} is losing ground`,
      detail: `${office.officeName} is losing share in ${z}. Potential expansion opportunity.`,
      timeAgo: "Recent weeks",
    });
  }

  return alerts;
}

/**
 * Phase C – Competitor Momentum Engine
 *
 * @param {Object} [options]
 * @param {string[]} [options.zips]
 * @param {Date|string} [options.asOfDate]
 * @param {number} [options.weeks]
 * @returns {Promise<
 *   CompetitorMovementResult & {
 *     offices: Array<OfficeMovement & {momentumScore:number}>;
 *     topMomentumOffices: Array<OfficeMovement & {momentumScore:number}>;
 *     dominanceTimeline: DominanceTimelinePoint[];
 *     strategyAlerts: StrategyAlert[];
 *   }
 * >}
 */
export async function computeCompetitorMomentum(options = {}) {
  const movement = await computeCompetitorMovement(options);

  /** @type {Array<OfficeMovement & {momentumScore:number}>} */
  const officesWithMomentum = (movement.offices || []).map(
    /**
     * @param {OfficeMovement} office
     * @returns {OfficeMovement & {momentumScore:number}}
     */
    (office) => ({
      ...office,
      momentumScore: computeOfficeMomentumScore(office),
    })
  );

  officesWithMomentum.sort(
    (a, b) => (b.momentumScore || 0) - (a.momentumScore || 0)
  );

  const dominanceTimeline = buildDominanceTimeline({
    ...movement,
    offices: officesWithMomentum,
  });

  const strategyAlerts = buildMovementAlerts(officesWithMomentum);

  return {
    ...movement,
    offices: officesWithMomentum,
    topMomentumOffices: officesWithMomentum.slice(0, 10),
    dominanceTimeline,
    strategyAlerts,
  };
}

export default computeCompetitorMomentum;
