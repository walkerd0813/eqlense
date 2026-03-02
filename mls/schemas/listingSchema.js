import mongoose from "mongoose";

const ListingSchema = new mongoose.Schema(
  {
    mlsId: { type: String, unique: true },
    propertyId: { type: String, index: true },
    propertyType: String,  // single_family, multi_family, condo, land
    status: String,        // active, sold

    listPrice: Number,
    originalListPrice: Number,
    closePrice: Number,

    listDate: Date,
    closeDate: Date,
    dom: Number,

    remarks: String,
    publicRemarks: String,

    brokerName: String,
    brokerId: String,
    agentName: String,
    agentId: String,

    photos: [String], // local file paths

    geo: {
      type: { type: String, enum: ["Point"], default: "Point" },
      coordinates: [Number], // [lon, lat]
    },

    raw: Object, // entire source row as backup
  },
  { timestamps: true }
);

ListingSchema.index({ geo: "2dsphere" });

export default mongoose.model("MLSListing", ListingSchema);
