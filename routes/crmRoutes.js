import express from "express";

import {
  createLead,
  listLeads,
  getLeadById,
  updateLead,
  updateLeadStage
} from "../services/crmService.js";

const router = express.Router();

// Create a new lead
router.post("/", (req, res, next) => {
  try {
    const lead = createLead(req.body);
    res.json({ ok: true, lead });
  } catch (err) {
    next(err);
  }
});

// List all leads or filter by owner
router.get("/", (req, res, next) => {
  try {
    const ownerId = req.query.ownerId || null;
    const leads = listLeads({ ownerId });
    res.json({ ok: true, leads });
  } catch (err) {
    next(err);
  }
});

// Get one lead
router.get("/:id", (req, res, next) => {
  try {
    const lead = getLeadById(req.params.id);
    if (!lead) {
      return res.status(404).json({ ok: false, error: "Lead not found." });
    }
    res.json({ ok: true, lead });
  } catch (err) {
    next(err);
  }
});

// Update a lead
router.patch("/:id", (req, res, next) => {
  try {
    const updated = updateLead(req.params.id, req.body);
    res.json({ ok: true, lead: updated });
  } catch (err) {
    next(err);
  }
});

// Update lead stage
router.patch("/:id/stage", (req, res, next) => {
  try {
    const { stage } = req.body;
    const updated = updateLeadStage(req.params.id, stage);
    res.json({ ok: true, lead: updated });
  } catch (err) {
    next(err);
  }
});

export default router;
