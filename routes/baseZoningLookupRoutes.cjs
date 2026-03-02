"use strict";

const express = require("express");
const router = express.Router();
const { lookupBaseZoningByPropertyId } = require("../services/baseZoningIndex.cjs");

// GET /api/zoning/base/:propertyId?includeMeta=1
router.get("/base/:propertyId", async (req, res) => {
  try {
    const propertyId = String(req.params.propertyId || "").trim();
    if (!propertyId) return res.status(400).json({ ok: false, error: "Missing propertyId" });

    const includeMeta = req.query.includeMeta === "1" || req.query.includeMeta === "true";
    const out = await lookupBaseZoningByPropertyId(propertyId, { includeMeta });

    return res.json({ ok: true, ...out });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e && e.message ? e.message : e) });
  }
});

// GET /api/zoning/base?propertyId=...
router.get("/base", async (req, res) => {
  try {
    const propertyId = String(req.query.propertyId || "").trim();
    if (!propertyId) return res.status(400).json({ ok: false, error: "Missing propertyId" });

    const includeMeta = req.query.includeMeta === "1" || req.query.includeMeta === "true";
    const out = await lookupBaseZoningByPropertyId(propertyId, { includeMeta });

    return res.json({ ok: true, ...out });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e && e.message ? e.message : e) });
  }
});

module.exports = router;
