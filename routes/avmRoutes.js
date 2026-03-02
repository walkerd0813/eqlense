import express from "express";

const router = express.Router();

// -------------------------------------------------------
// POST /api/avm/estimate — TEMP STUB
// -------------------------------------------------------
router.post("/estimate", async (req, res) => {
  try {
    const { address, zip } = req.body || {};

    if (!address || !zip) {
      return res.status(400).json({
        ok: false,
        error: "Missing required fields: address or zip",
      });
    }

    // Stub response so frontend doesn't break
    return res.json({
      ok: true,
      estimate: null,
      range: { low: null, high: null },
      comps: [],
      subject: { address, zip },
      debug: { stub: true },
    });
  } catch (err) {
    console.error("AVM stub route error:", err);
    return res.status(500).json({
      ok: false,
      error: "AVM route error",
    });
  }
});

export default router;
