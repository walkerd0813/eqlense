import express from "express";
import Homeowner from "../../models/Homeowner.js";

const router = express.Router();

/**
 *  GET /api/homeowner/:email
 *  Returns dashboard snapshot ONLY.
 */
router.get("/:email", async (req, res) => {
  try {
    const { email } = req.params;
    const homeowner = await Homeowner.findOne({ email });

    if (!homeowner) {
      return res.status(404).json({ error: "Homeowner not found" });
    }

    return res.json({
      ok: true,
      homeowner,
    });
  } catch (err) {
    console.error("Dashboard fetch error:", err);
    res.status(500).json({ error: "Server error fetching dashboard" });
  }
});

export default router;
