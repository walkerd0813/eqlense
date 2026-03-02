// backend/controllers/competitorController.js

import { computeCompetitorTracker } from "../analytics/competitor/computeCompetitorTracker.js";
import { computeCompetitorMovement } from "../analytics/competitor/computeCompetitorMovement.js";
import { computeCompetitorMomentum } from "../analytics/competitor/computeCompetitorMomentum.js";

/**
 * GET /competitors/summary
 * Example: /api/competitors/summary?zips=02118,02119&windowDays=90
 *
 * @param {import("express").Request} req
 * @param {import("express").Response} res
 * @returns {Promise<void>}
 */
export async function getCompetitorSummary(req, res) {
  try {
    const zips = req.query.zips
      ? String(req.query.zips).split(",").map((z) => z.trim())
      : [];

    const windowDays = req.query.windowDays
      ? Number(req.query.windowDays)
      : 90;

    const focusBrokerOfficeId = req.query.officeId
      ? String(req.query.officeId)
      : null;

    const result = await computeCompetitorTracker({
      zips,
      windowDays,
      focusBrokerOfficeId,
    });

    res.json({
      ok: true,
      data: result,
    });
  } catch (error) {
    console.error("Competitor Summary error:", error);
    res.status(500).json({
      ok: false,
      error: error.message || "Internal Server Error",
    });
  }
}

/**
 * GET /competitors/movement
 * ZIP-level competitor drift + timeline deltas.
 *
 * Example: /api/competitors/movement?zips=02118,02119&weeks=8
 *
 * @param {import("express").Request} req
 * @param {import("express").Response} res
 * @returns {Promise<void>}
 */
export async function getCompetitorMovement(req, res) {
  try {
    const zips = req.query.zips
      ? String(req.query.zips).split(",").map((z) => z.trim())
      : [];

    const weeks = req.query.weeks ? Number(req.query.weeks) : 8;

    const result = await computeCompetitorMovement({
      zips,
      weeks,
    });

    res.json({
      ok: true,
      data: result,
    });
  } catch (error) {
    console.error("Competitor Movement error:", error);
    res.status(500).json({
      ok: false,
      error: error.message || "Internal Server Error",
    });
  }
}

/**
 * GET /competitors/momentum
 * High-level competitor momentum scores + strategy alerts.
 *
 * Example: /api/competitors/momentum?zips=02118,02119&weeks=8
 *
 * @param {import("express").Request} req
 * @param {import("express").Response} res
 * @returns {Promise<void>}
 */
export async function getCompetitorMomentum(req, res) {
  try {
    const zips = req.query.zips
      ? String(req.query.zips).split(",").map((z) => z.trim())
      : [];

    const weeks = req.query.weeks ? Number(req.query.weeks) : 8;

    const result = await computeCompetitorMomentum({
      zips,
      weeks,
    });

    res.json({
      ok: true,
      data: result,
    });
  } catch (error) {
    console.error("Competitor Momentum error:", error);
    res.status(500).json({
      ok: false,
      error: error.message || "Internal Server Error",
    });
  }
}

/**
 * GET /competitors/zip/:zip
 * Gives competitive breakdown for a single ZIP.
 *
 * @param {import("express").Request} req
 * @param {import("express").Response} res
 * @returns {Promise<void>}
 */
export async function getCompetitorByZip(req, res) {
  try {
    const zip = req.params.zip;
    if (!zip) {
      res.status(400).json({ ok: false, error: "ZIP required" });
      return;
    }

    const result = await computeCompetitorTracker({
      zips: [zip],
      windowDays: 90,
    });

    res.json({ ok: true, data: result });
  } catch (error) {
    console.error("Competitor ZIP error:", error);
    res.status(500).json({ ok: false, error: error.message });
  }
}
