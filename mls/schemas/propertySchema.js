import mongoose from "mongoose";

const PropertySchema = new mongoose.Schema(
  {
    propertyId: { type: String, index: true }, // fusion key (address+zip or MLS id)
    mlsId: { type: String, index: true },
    propertyType: String, // single_family, multi_family, condo, land

    address: {
      streetNumber: String,
      streetName: String,
      city: String,
      state: String,
      zip: String,
    },

    location: {
      type: { type: String, enum: ["Point"], default: "Point" },
      coordinates: [Number], // [lon, lat]
    },

    beds: Number,
    baths: Number,
    sqft: Number,
    lotSize: Number,
    yearBuilt: Number,
    raw: Object, // for extra mappings if needed
  },
  { timestamps: true }
);

PropertySchema.index({ location: "2dsphere" });

export default mongoose.model("MLSProperty", PropertySchema);
