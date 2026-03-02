// backend/services/alertGroupingService.js

/**
 * ALERT CATEGORY GROUPING
 * -----------------------
 * This module groups alerts into meaningful categories:
 * - real_estate_activity
 * - neighborhood_insights
 * - safety_utilities
 *
 * Each returned category contains an array of alerts.
 */

export function groupAlerts(rankedAlerts) {
  const groups = {
    real_estate_activity: [],
    neighborhood_insights: [],
    safety_utilities: [],
  };

  for (const alert of rankedAlerts) {
    switch (alert.type) {
      // ----------------------------------------------
      // HIGH VALUE REAL ESTATE ACTIVITY
      // ----------------------------------------------
      case "sold":
      case "pending":
      case "new_listing":
      case "price_cut":
      case "upgrade_detected":
        groups.real_estate_activity.push(alert);
        break;

      // ----------------------------------------------
      // NEIGHBORHOOD INSIGHTS (Businesses, Events, School)
      // ----------------------------------------------
      case "local_business":
      case "event":     // school + community events
        groups.neighborhood_insights.push(alert);
        break;

      // ----------------------------------------------
      // SAFETY & UTILITIES
      // ----------------------------------------------
      case "utility":
      case "safety":
        groups.safety_utilities.push(alert);
        break;

      // ----------------------------------------------
      // DEFAULT (fallback)
      // ----------------------------------------------
      default:
        groups.neighborhood_insights.push(alert);
    }
  }

  return groups;
}
