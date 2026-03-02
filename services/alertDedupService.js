// backend/services/alertDedupService.js

/**
 * DEDUPLICATION ENGINE
 *
 * Goal:
 * - Collapse noisy, repeated alerts into a single useful one.
 * - Example: 6 "new_listing" alerts within 60 minutes -> 1 alert with mergedCount = 6
 *
 * Strategy:
 * - Group by (type + propertyId) when propertyId exists (listings).
 * - For non-property alerts (business, utility, events), group by (type + headline).
 * - Only deduplicate within a rolling time window (e.g. 60 minutes).
 */

const DEDUP_WINDOW_MINUTES = 60;

function makeKey(alert) {
  if (alert.propertyId) {
    return `${alert.type}|property|${alert.propertyId}`;
  }
  const headline = (alert.headline || "").trim().toLowerCase();
  return `${alert.type}|headline|${headline}`;
}

export function deduplicateAlerts(alerts) {
  if (!Array.isArray(alerts) || alerts.length === 0) return [];

  const now = Date.now();
  const windowMs = DEDUP_WINDOW_MINUTES * 60 * 1000;

  const byKey = new Map();
  const result = [];

  for (const alert of alerts) {
    const key = makeKey(alert);
    const createdAt = alert.createdAt ? new Date(alert.createdAt).getTime() : now;

    const existing = byKey.get(key);

    if (!existing) {
      // First time seeing this key – keep this alert
      const copy = {
        ...alert,
        mergedCount: 1,
      };
      byKey.set(key, { alert: copy, latestTime: createdAt });
      result.push(copy);
      continue;
    }

    // If within dedup window, merge into existing instead of adding a new alert
    const diff = Math.abs(existing.latestTime - createdAt);
    if (diff <= windowMs) {
      existing.alert.mergedCount = (existing.alert.mergedCount || 1) + 1;

      // Optionally, we could tweak the message when multiple are merged – leave as-is for now
      // existing.alert.message = ...

      // Update latestTime if this alert is newer
      if (createdAt > existing.latestTime) {
        existing.latestTime = createdAt;
        existing.alert.createdAt = alert.createdAt;
      }
    } else {
      // Outside dedup window: treat as a separate alert
      const copy = {
        ...alert,
        mergedCount: 1,
      };
      byKey.set(key, { alert: copy, latestTime: createdAt });
      result.push(copy);
    }
  }

  return result;
}
