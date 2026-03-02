import express from "express";
import multer from "multer";
import path from "path";
import fs from "fs";

import FeedPost from "../../models/FeedPost.js";
import Homeowner from "../../models/Homeowner.js";

const router = express.Router();

// ----------------------------------
// Ensure uploads/ folder exists
// ----------------------------------
const uploadDir = path.join(process.cwd(), "uploads");
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir);
}

// ----------------------------------
// Multer: Storage + Filters
// ----------------------------------
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, "uploads/"),
  filename: (req, file, cb) =>
    cb(null, `${Date.now()}-${file.originalname.replace(/\s+/g, "_")}`),
});

const allowedPhotoTypes = ["image/jpeg", "image/png", "image/webp"];
const allowedVideoTypes = ["video/mp4", "video/quicktime"];

function fileFilter(req, file, cb) {
  if (allowedPhotoTypes.includes(file.mimetype)) return cb(null, true);
  if (allowedVideoTypes.includes(file.mimetype)) return cb(null, true);
  return cb(new Error("Unsupported file type"), false);
}

const upload = multer({
  storage,
  fileFilter,
  limits: { fileSize: 50 * 1024 * 1024 }, // 50MB max video
});

// ----------------------------------
// CREATE FEED POST
// ----------------------------------
router.post(
  "/post",
  upload.fields([
    { name: "photos", maxCount: 10 },
    { name: "videos", maxCount: 4 },
  ]),
  async (req, res) => {
    try {
      const { email, zip, body } = req.body;
      const tags = req.body.tags ? req.body.tags.split(",") : [];

      if (!email || !zip) {
        return res.status(400).json({
          ok: false,
          error: "Missing email or zip",
        });
      }

      const homeowner = await Homeowner.findOne({ email });
      if (!homeowner) {
        return res.status(404).json({ ok: false, error: "User not found" });
      }

      const photos = (req.files.photos || []).map((f) => ({
        filename: f.filename,
        path: `/uploads/${f.filename}`,
        size: f.size,
        uploadedAt: new Date(),
      }));

      const videos = (req.files.videos || []).map((f) => ({
        filename: f.filename,
        path: `/uploads/${f.filename}`,
        size: f.size,
        uploadedAt: new Date(),
      }));

      const newPost = await FeedPost.create({
        email,
        zip,
        body,
        tags,
        photos,
        videos,
        createdAt: new Date(),
      });

      res.json({ ok: true, post: newPost });
    } catch (err) {
      console.error("🔥 Feed post error:", err);
      return res.status(500).json({
        ok: false,
        error: "Failed to create feed post",
      });
    }
  }
);

// ----------------------------------
// FETCH FEED BY ZIP
// ----------------------------------
router.get("/zip/:zip", async (req, res) => {
  try {
    const feed = await FeedPost.find({ zip })
      .sort({ createdAt: -1 })
      .lean();

    return res.json({ ok: true, feed });
  } catch (err) {
    console.error("🔥 Feed fetch error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to fetch feed",
    });
  }
});

export default router;
