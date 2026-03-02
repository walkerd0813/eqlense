import express from "express";
import fs from "fs";
import path from "path";
import readline from "readline";

const router = express.Router();

function stripBom(s) {
  if (!s || typeof s !== "string") return s;
  return s.charCodeAt(0) === 0xFEFF ? s.slice(1) : s;
}

function safeReadJson(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(stripBom(raw));
}

function resolveProjectRoot() {
  const here = path.dirname(new URL(import.meta.url).pathname);
  const norm = here.replace(/^\/([A-Za-z]:)/, "$1");
  const parts = norm.split(path.sep);
  const srcIdx = parts.lastIndexOf("src");
  if (srcIdx !== -1 && parts[srcIdx + 1] === "routes") return parts.slice(0, srcIdx).join(path.sep);
  return path.resolve(norm, "..");
}

function normalizeZip(z) {
  const s = String(z || "").trim();
  if (!s) return "";
  const d = s.replace(/\D/g, "");
  if (!d) return "";
  return d.padStart(5, "0").slice(0, 5);
}

async function findNdjsonRowByZip(filePath, zip) {
  if (!fs.existsSync(filePath)) return null;
  const input = fs.createReadStream(filePath, { encoding: "utf8" });
  const rl = readline.createInterface({ input, crlfDelay: Infinity });
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    try {
      const obj = JSON.parse(stripBom(t));
      const z = normalizeZip(obj.zip || obj.zip_code || obj.zipCode);
      if (z === zip) return obj;
    } catch {
      // ignore
    }
  }
  return null;
}

router.get("/track/:track/pointers", (req, res) => {
  try {
    const track = String(req.params.track || "").toUpperCase();
    const root = resolveProjectRoot();
    const fp = path.join(root, "publicData", "marketRadar", "CURRENT", `CURRENT_MARKET_RADAR_POINTERS__${track}.json`);
    if (!fs.existsSync(fp)) return res.status(404).json({ state: "UNKNOWN", reason: "POINTERS_NOT_FOUND", radar_track: track });
    return res.json(safeReadJson(fp));
  } catch (e) {
    return res.status(500).json({ state: "ERROR", reason: "POINTERS_READ_FAILED", message: String(e) });
  }
});

router.get("/track/:track/zip/:zip/summary", async (req, res) => {
  try {
    const track = String(req.params.track || "").toUpperCase();
    const zip = normalizeZip(req.params.zip);
    const root = resolveProjectRoot();

    const ptrPath = path.join(root, "publicData", "marketRadar", "CURRENT", `CURRENT_MARKET_RADAR_POINTERS__${track}.json`);
    if (!fs.existsSync(ptrPath)) return res.status(404).json({ state: "UNKNOWN", reason: "POINTERS_NOT_FOUND", radar_track: track });

    const ptr = safeReadJson(ptrPath);
    if (ptr.state === "UNKNOWN") return res.json(ptr);

    const out = {
      schema: "equity_lens.market_radar.zip_summary.v0_1",
      radar_track: track,
      zip,
      as_of_date: ptr.as_of_date || ptr.market_radar?.as_of_date || null,
      state: "READY",
      rows: {}
    };

    const mr = ptr.market_radar || ptr.marketRadar || {};
    const base = mr.market_radar ? mr.market_radar : mr;

    const keys = [
      "velocity_zip",
      "absorption_zip",
      "liquidity_p01_zip",
      "price_discovery_p01_zip",
      "regime_zip",
      "explainability_zip"
    ];

    for (const k of keys) {
      const v = base[k] || mr[k];
      const fp = v?.ndjson || v?.path;
      if (!fp) continue;
      const row = await findNdjsonRowByZip(fp, zip);
      if (row) out.rows[k] = row;
    }

    out.missing_keys = keys.filter(k => !out.rows[k]);
    return res.json(out);
  } catch (e) {
    return res.status(500).json({ state: "ERROR", reason: "ZIP_SUMMARY_FAILED", message: String(e) });
  }
});

export default router;