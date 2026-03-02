// ------------------------------------------------------------
//  SERVER.JS â€” FULL ESM VERSION WITH MONGODB CONNECTION
// ------------------------------------------------------------
import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";
import mongoose from "mongoose";

// Load environment variables
dotenv.config();

// Validate environment variables early
if (!process.env.MONGO_URI) {
  console.error("âŒ ERROR: MONGO_URI is missing in your .env file.");
  process.exit(1);
}

// ESM dirname fix
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ------------------------------------------------------------
//  CONNECT TO MONGODB (ESSENTIAL FOR FIXING TIMEOUT ERROR)
// ------------------------------------------------------------
async function connectDB() {
  try {
    await mongoose.connect(process.env.MONGO_URI);

    console.log("âœ… MongoDB connected successfully");
  } catch (err) {
    console.error("âŒ MongoDB connection failed:", err.message);
    process.exit(1);
  }
}


connectDB();

// Log additional connection status
mongoose.connection.on("error", (err) => {
  console.error("ðŸ”¥ MongoDB runtime error:", err);
});

// ------------------------------------------------------------
//  IMPORT ROUTERS
// ------------------------------------------------------------
import homeRoutes from "./routes/homeowner/homeRoutes.js";
import profileRoutes from "./routes/homeowner/profileRoutes.js";
import photoRoutes from "./routes/homeowner/photoRoutes.js";
import feedRoutes from "./routes/feed/feedRoutes.js";
import avmRoutes from "./routes/avmRoutes.js";
import marketRadarRoutes from "./routes/marketRadarRoutes.js";
import competitorRoutes from "./routes/competitorRoutes.js";

import baseZoningRoutes from "./routes/zoning/baseZoningRoutes.js";
//import { runAlertEngine } from "./services/alertMasterEngine.js";
import alertRoutes from "./routes/alertRoutes.js";
import renovationTrendsRoutes from "./routes/renovationTrendsRoutes.js";
// ------------------------------------------------------------
//  APP INIT
// ------------------------------------------------------------
const app = express();
const PORT = process.env.PORT || 4000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Static folders (serve uploaded images)
app.use("/uploads", express.static(path.join(__dirname, "uploads")));
app.use("/publicData", express.static(path.join(__dirname, "publicData")));

// ------------------------------------------------------------
//  ROUTES
// ------------------------------------------------------------
app.use("/api/homeowner", homeRoutes);
app.use("/api/homeowner/profile", profileRoutes);
app.use("/api/homeowner/photos", photoRoutes);
app.use("/api/feed", feedRoutes);
app.use("/api/avm", avmRoutes);

app.use("/api/zoning", baseZoningRoutes);
app.use("/api/market-radar", marketRadarRoutes);
app.use("/api/competitors", competitorRoutes);
app.use("/api/alerts", alertRoutes);
app.use("/api/renovation-trends", renovationTrendsRoutes);
// ------------------------------------------------------------
//  ROOT TEST ROUTE
// ------------------------------------------------------------
app.get("/", (req, res) => {
  res.send("EquityLens Backend Running (ESM Mode + MongoDB Connected)");
});

// ------------------------------------------------------------
//  START SERVER
// ------------------------------------------------------------
app.listen(PORT, () => {
  console.log(`ðŸš€ Backend server running on port ${PORT}`);

  // Neighborhood Alerts Worker
  setInterval(async () => {
    try {
      const zipsToMonitor = ["02124", "02125"]; // Temporary
      await runAlertEngine(zipsToMonitor);

      console.log("ðŸ”” Alert engine cycle complete");
    } catch (err) {
      console.error("âŒ Alert engine error:", err);
    }
  }, 15 * 60 * 1000);
});



