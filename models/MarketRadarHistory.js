// backend/models/MarketRadarHistory.js
// ES Module — Momentum Snapshot Storage for Market Radar V2

import mongoose from "mongoose";

const MetricSnapshotSchema = new mongoose.Schema(
  {
    absorptionRate: Number,
    absorptionScore: Number,

    avgDaysOnMarket: Number,
    domScore: Number,

    priceTrendPct: Number,
    priceStabilityScore: Number,

    liquidityScore: Number,
    marketVelocityScore: Number,

    inventoryTrendPct: Number,

    activeCount: Number,
    pendingCount: Number,
    soldCount: Number
  },
  { _id: false }
);

const MarketRadarHistorySchema = new mongoose.Schema({
  zip: {
    type: String,
    required: true,
    index: true,
    trim: true
  },

  lastUpdated: {
    type: Date,
    default: Date.now
  },

  // The metrics from the *most recent* radar computation
  currentMetrics: {
    type: MetricSnapshotSchema,
    required: true
  },

  // The metrics from the *previous* radar computation
  previousMetrics: {
    type: MetricSnapshotSchema,
    default: null
  },

  // Delta between previousMetrics and currentMetrics
  momentum: {
    type: mongoose.Schema.Types.Mixed,
    default: null
  }
});

export default mongoose.model("MarketRadarHistory", MarketRadarHistorySchema);
