import express from "express";
import multer from "multer";
import fs from "fs";
import path from "path";
import Homeowner from "../../models/Homeowner.js";

const router = express.Router();

// Ensure uploads folder exists
const uploadDir = path.join(process.cwd(), "uploads");
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir);

// Multer storage
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, "uploads/"),
  filename: (req, file, cb) =>
    cb(null, `${Date.now()}-${file.originalname.replace(/\s+/g, "_")}`),
});

const upload = multer({ storage });

// -------------------------------------------
// LIMIT: MAX 20 GALLERY PHOTOS
// -------------------------------------------
function enforceGalleryLimit(homeowner) {
  const MAX = 20;
  if (homeowner.subjectDetails.photos.length > MAX) {
    homeowner.subjectDetails.photos =
      homeowner.subjectDetails.photos.slice(-MAX);
  }
}

// -------------------------------------------
// UPLOAD PHOTOS
// -------------------------------------------
router.post("/upload/:email", upload.array("photos"), async (req, res) => {
  try {
    const { email } = req.params;

    const homeowner = await Homeowner.findOne({ email });
    if (!homeowner)
      return res.status(404).json({ error: "Homeowner not found" });

    if (!homeowner.subjectDetails) homeowner.subjectDetails = {};
    if (!Array.isArray(homeowner.subjectDetails.photos))
      homeowner.subjectDetails.photos = [];

    // Add uploaded photos
    req.files.forEach((file) => {
      homeowner.subjectDetails.photos.push({
        filename: file.filename,
        path: `/uploads/${file.filename}`,
        size: file.size,
        uploadedAt: new Date(),
      });
    });

    enforceGalleryLimit(homeowner);

    await homeowner.save();

    return res.json({
      ok: true,
      photos: homeowner.subjectDetails.photos,
    });
  } catch (err) {
    console.error("🔥 Photo upload error:", err);
    return res.status(500).json({ error: "Photo upload failed" });
  }
});

// -------------------------------------------
// DELETE PHOTO
// -------------------------------------------
router.post("/delete", async (req, res) => {
  try {
    const { email, path: filePath } = req.body;

    const homeowner = await Homeowner.findOne({ email });
    if (!homeowner)
      return res.status(404).json({ error: "Homeowner not found" });

    homeowner.subjectDetails.photos =
      homeowner.subjectDetails.photos.filter((p) => p.path !== filePath);

    await homeowner.save();

    return res.json({ ok: true, photos: homeowner.subjectDetails.photos });
  } catch (err) {
    console.error("🔥 Delete photo error:", err);
    return res.status(500).json({ error: "Failed to delete photo" });
  }
});

export default router;
