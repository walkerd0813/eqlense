import { Router } from "express";
import Alert from "../models/Alert.js";
import { rankAlerts } from "../services/alertRankingService.js";
import { deduplicateAlerts } from "../services/alertDedupService.js";
import { groupAlerts } from "../services/alertGroupingService.js";

const router = Router();

/**
 * GET /api/alerts/:zip
 * Fetch latest alerts for a ZIP code
 * - Fetches most recent alerts
 * - Deduplicates noisy/duplicate alerts
 * - Ranks & prioritizes them
 * - Groups by category
 */
router.get("/:zip", async (req, res) => {
  try {
    const { zip } = req.params;

    if (!zip) {
      return res.status(400).json({
        ok: false,
        error: "ZIP code is required",
      });
    }

    // 1) Fetch raw alerts
    const rawAlerts = await Alert.find({ zip })
      .sort({ createdAt: -1 })
      .limit(100)
      .lean();

    // 2) Deduplicate noisy alerts
    const dedupedAlerts = deduplicateAlerts(rawAlerts);

    // 3) Rank alerts by importance
    const rankedAlerts = rankAlerts(dedupedAlerts);

    // 4) Group alerts by category
    const groupedAlerts = groupAlerts(rankedAlerts);

    return res.json({
      ok: true,
      zip,
      alerts: groupedAlerts,
    });
  } catch (err) {
    console.error("❌ Error fetching alerts:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to load alerts",
    });
  }
});

export default router;
