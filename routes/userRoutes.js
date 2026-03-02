import express from "express";

const router = express.Router();

// Example placeholder route
router.get("/status", (req, res) => {
  res.json({ ok: true, message: "User routes active" });
});

export default router;
