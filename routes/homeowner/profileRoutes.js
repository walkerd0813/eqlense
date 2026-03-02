import express from "express";
import Homeowner from "../../models/Homeowner.js";

const router = express.Router();

/**
 *  GET profile
 */
router.get("/:email", async (req, res) => {
  try {
    const homeowner = await Homeowner.findOne({ email: req.params.email });
    if (!homeowner) return res.status(404).json({ error: "Profile not found" });

    res.json({ ok: true, profile: homeowner });
  } catch (err) {
    console.error("Profile fetch error:", err);
    res.status(500).json({ error: "Server error fetching profile" });
  }
});

/**
 * UPDATE profile
 */
router.put("/:email", async (req, res) => {
  try {
    const updated = await Homeowner.findOneAndUpdate(
      { email: req.params.email },
      req.body,
      { new: true }
    );

    res.json({ ok: true, profile: updated });
  } catch (err) {
    console.error("Profile update error:", err);
    res.status(500).json({ error: "Server error updating profile" });
  }
});

export default router;
