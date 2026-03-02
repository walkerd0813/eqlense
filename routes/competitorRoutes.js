// routes/competitorRoutes.js

import express from "express";
import {
  getCompetitorSummary,
  getCompetitorMovement,
  getCompetitorMomentum,
  getCompetitorByZip,
} from "../controllers/competitorController.js";

const router = express.Router();

router.get("/summary", getCompetitorSummary);

// NEW Phase C Endpoints
router.get("/movement", getCompetitorMovement);
router.get("/momentum", getCompetitorMomentum);

router.get("/zip/:zip", getCompetitorByZip);

export default router;
