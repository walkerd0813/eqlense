import mongoose from "mongoose";

const mediaSchema = new mongoose.Schema({
  filename: String,
  path: String,
  size: Number,
  uploadedAt: { type: Date, default: Date.now },
});

const FeedPostSchema = new mongoose.Schema(
  {
    email: { type: String, required: true },
    zip: { type: String, required: true },

    body: { type: String, default: "" },
    tags: { type: [String], default: [] },

    photos: { type: [mediaSchema], default: [] },
    videos: { type: [mediaSchema], default: [] },

    createdAt: { type: Date, default: Date.now },
  },
  { timestamps: true }
);

const FeedPost = mongoose.model("FeedPost", FeedPostSchema);
export default FeedPost;
