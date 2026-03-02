// backend/models/Listing.js (ES MODULE VERSION)

import mongoose from "mongoose";

const ListingSchema = new mongoose.Schema(
  {
    mlsId: { type: String, index: true },

    address: {
      line1: String,
      line2: String,
      city: String,
      state: String,
      zip: { type: String, index: true },
    },

    location: {
      type: {
        type: String,
        enum: ["Point"],
        default: "Point",
      },
      coordinates: {
        type: [Number], // [lng, lat]
        index: "2dsphere",
      },
    },

    propertyType: {
      type: String,
      enum: ["single_family", "multi_family", "condo", "townhouse", "other"],
      default: "single_family",
      index: true,
    },

    beds: Number,
    baths: Number,
    livingAreaSqft: Number,
    lotSizeSqft: Number,
    yearBuilt: Number,

    listPrice: Number,
    closePrice: Number,
    listDate: Date,
    pendingDate: Date,
    closeDate: Date,

    status: {
      type: String,
      enum: ["active", "pending", "sold", "expired", "withdrawn"],
      index: true,
    },

    daysOnMarket: Number,

    priceHistory: [
      {
        price: Number,
        date: Date,
        note: String, // "list", "price_cut", "relist", etc.
      },
    ],

    upgrades: {
      kitchenRemodel: { type: Boolean, default: false },
      bathroomRemodel: { type: Boolean, default: false },
      roof: { type: Boolean, default: false },
      hvac: { type: Boolean, default: false },
      flooring: { type: Boolean, default: false },
      exteriorSiding: { type: Boolean, default: false },
      windows: { type: Boolean, default: false },
    },

    source: {
      type: String,
      enum: ["mls", "public_record", "manual_import", "other"],
      default: "mls",
    },
  },
  { timestamps: true }
);

// Auto-calc Days on Market
ListingSchema.pre("save", function (next) {
  if (this.listDate && this.closeDate) {
    const diffMs = this.closeDate.getTime() - this.listDate.getTime();
    this.daysOnMarket = Math.round(diffMs / (1000 * 60 * 60 * 24));
  }
  next();
});

// Compile model using ES Module export
const Listing = mongoose.model("Listing", ListingSchema);

// ⭐ IMPORTANT: Proper ESM export
export default Listing;
