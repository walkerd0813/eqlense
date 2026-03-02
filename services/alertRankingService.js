/**
 * ALERT RANKING + PRIORITIZATION ENGINE
 *
 * Scores each alert based on:
 * - severity
 * - type importance
 * - recency
 * - value impact
 * - duplicates
 */

export function rankAlerts(alerts) {
  const now = Date.now();

  return alerts
    .map((alert) => {
      let score = 0;

      // --------------------------------------------------
      // 1. BASE SCORE BY ALERT TYPE
      // --------------------------------------------------
      const typeScores = {
        sold: 90,
        pending: 70,
        new_listing: 80,
        price_cut: 75,
        upgrade_detected: 65,

        local_business: 40,
        utility: 55,
        safety: 60,
        event: 30,
      };

      score += typeScores[alert.type] || 10;

      // --------------------------------------------------
      // 2. RECENCY BOOST
      // --------------------------------------------------
      const ageMinutes = (now - new Date(alert.createdAt).getTime()) / (1000 * 60);

      if (ageMinutes < 60) score += 40;              // last hour = hot
      else if (ageMinutes < 6 * 60) score += 25;     // last 6 hours
      else if (ageMinutes < 24 * 60) score += 10;    // last day
      else score -= 10;                              // older = less relevant

      // --------------------------------------------------
      // 3. VALUE IMPACT BOOST (for sold/pending)
      // --------------------------------------------------
      if (alert.meta?.overUnder) {
        const impact = Math.abs(alert.meta.overUnder);
        score += Math.min(impact / 2000, 50);
      }

      // --------------------------------------------------
      // 4. SEVERITY BOOST (utility, safety)
      // --------------------------------------------------
      if (alert.meta?.severity) {
        const sevScores = { low: 5, medium: 15, high: 30 };
        score += sevScores[alert.meta.severity] || 0;
      }

      return { ...alert, score };
    })
    .sort((a, b) => b.score - a.score);
}
