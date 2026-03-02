import express from "express";
import { getZoningContext } from "../publicData/zoning/zoningEngine.js";

const router = express.Router();

// POST /api/pro/zoning/context
router.post("/context", async (req, res) => {
  try {
    const { lat, lng } = req.body;

    if (typeof lat !== "number" || typeof lng !== "number") {
      return res.status(400).json({
        ok: false,
        error: "lat and lng must be numbers",
      });
    }

    const data = getZoningContext(lat, lng);
    return res.json(data);

  } catch (err) {
    console.error("🔥 Zoning Error:", err);
    return res.status(500).json({
      ok: false,
      error: "Internal zoning error",
    });
  }
});

export default router;
