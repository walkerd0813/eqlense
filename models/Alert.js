import mongoose from "mongoose";

const AlertSchema = new mongoose.Schema(
  {
    zip: { type: String, index: true },

    type: {
      type: String,
      enum: [
        "new_listing",
        "pending",
        "sold",
        "price_cut",
        "upgrade_detected",
        "local_business",
        "utility",
        "safety",
        "event",
      ],
      required: true,
      index: true,
    },

    headline: String,
    message: String,

    // Listing-related events can reference a property
    propertyId: String,

    // Who should see this alert (supports multi-role dashboards)
    // Examples:
    // homeowner, broker, agent, investor, developer,
    // contractor_res, contractor_com, mortgage_broker
    audience: {
      type: [String],
      default: ["homeowner"],   // Always safe to default to homeowners
    },

    // Extra structured data for scoring, ranking, UI logic
    meta: mongoose.Schema.Types.Mixed,

    // Explicit timestamp for alert occurrence
    timestamp: { type: Date, default: Date.now },
  },
  { timestamps: true }
);

const Alert = mongoose.model("Alert", AlertSchema);

export default Alert;
