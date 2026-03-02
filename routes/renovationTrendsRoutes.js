import express from "express";
import { getRenovationTrendsForZip } from "../services/renovationTrendsService.js";

const router = express.Router();

router.get("/:zip", async (req, res) => {
  try {
    const { zip } = req.params;
    const data = await getRenovationTrendsForZip(zip);
    return res.json({ ok: true, zip, trends: data });
  } catch (err) {
    console.error("Renovation trends error:", err);
    return res.status(500).json({ ok: false, error: "Failed to load renovation trends" });
  }
});

export default router;
