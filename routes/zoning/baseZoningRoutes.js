import express from "express";
import path from "path";
import { execFile } from "child_process";
import { fileURLToPath } from "url";

const router = express.Router();

// ESM dirname fix
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Backend root is two levels up from routes/zoning/
const backendRoot = path.resolve(__dirname, "..", "..");

// CLI lookup script (already proven working)
const lookupScript = path.join(
  backendRoot,
  "mls",
  "scripts",
  "zoning",
  "ops",
  "lookup_base_zoning_by_property_id_v1.cjs"
);

router.get("/base/health", async (req, res) => {
  res.json({
    ok: true,
    backendRoot,
    lookupScript,
    note: "Base zoning lookup API is mounted. This is read-only and uses the frozen index pointer."
  });
});

// GET /api/zoning/base/:propertyId?includeMeta=1
router.get("/base/:propertyId", async (req, res) => {
  try {
    const propertyId = req.params.propertyId;
    if (!propertyId) return res.status(400).json({ ok: false, error: "missing propertyId" });

    const includeMeta = String(req.query.includeMeta || "").toLowerCase();
    const args = [lookupScript, "--propertyId", propertyId];
    if (includeMeta === "1" || includeMeta === "true" || includeMeta === "yes") {
      args.push("--includeMeta");
    }

    execFile(
      process.execPath,
      args,
      { cwd: backendRoot, timeout: 15000, maxBuffer: 5 * 1024 * 1024 },
      (err, stdout, stderr) => {
        if (err) {
          return res.status(500).json({
            ok: false,
            error: "lookup_failed",
            message: err.message,
            stderr: (stderr || "").slice(0, 2000)
          });
        }

        const text = (stdout || "").trim();
        try {
          const json = JSON.parse(text);
          return res.json(json);
        } catch (e) {
          return res.status(500).json({
            ok: false,
            error: "bad_json_from_lookup_script",
            parseError: e.message,
            stdoutPreview: text.slice(0, 2000),
            stderrPreview: (stderr || "").slice(0, 2000)
          });
        }
      }
    );
  } catch (e) {
    res.status(500).json({ ok: false, error: "server_error", message: e.message });
  }
});

export default router;
