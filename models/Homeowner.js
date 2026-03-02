import mongoose from "mongoose";

const HomeownerSchema = new mongoose.Schema(
  {
    // Basic info
    fullName: { type: String, required: true },
    email: { type: String, required: true, unique: true },
    phone: { type: String, required: true },

    // Home info
    address: { type: String, required: true },
    unit: { type: String },
    zip: { type: String, required: true },

    // -----------------------------
    // AVM RESULTS
    // -----------------------------
    lastEstimateValue: { type: Number, default: null },
    lastEstimateLow: { type: Number, default: null },
    lastEstimateHigh: { type: Number, default: null },

    estimateStatus: {
      type: String,
      enum: ["pending", "ready", "error"],
      default: "pending",
    },

    lastRun: { type: Date, default: null },

    // Subject home details from AVM
    subjectDetails: {
      photos: [
        {
          filename: String,
          path: String,
          size: Number,
          uploadedAt: Date,
        },
      ],
    },

    // Comps used in last AVM run
    compsUsed: { type: Array, default: [] },

    // User verification
    isVerified: { type: Boolean, default: false },

    // Broker / Agent linking
    agentId: { type: String, default: null },
    brokerId: { type: String, default: null },

    // Dashboard metadata
    createdAt: { type: Date, default: Date.now },
    lastLogin: { type: Date },
  },
  {
    timestamps: true,
  }
);

const Homeowner = mongoose.model("Homeowner", HomeownerSchema);

export default Homeowner;