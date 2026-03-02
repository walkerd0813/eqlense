// mls/scripts/classifyIDX.js
// ------------------------------------------------------
// Classify new IDX files and route them into the MLS tree
// ------------------------------------------------------

import dotenvx from "@dotenvx/dotenvx";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Load MLS env only
dotenvx.config({
  path: path.join(path.dirname(fileURLToPath(import.meta.url)), "..", ".env.mls"),
  override: true,
  dotenvxPath: false
});

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// MLS folder roots
const MLS_ROOT = path.join(__dirname, "..");
const RAW_ROOT = path.join(MLS_ROOT, "raw");
const INCOMING_DIR = path.join(MLS_ROOT, "incoming");

// ------------------------------------------------------
// Utility: ensure directory exists
// ------------------------------------------------------
async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

// ------------------------------------------------------
// Utility: Windows-safe rename (handles EBUSY/EPERM locks)
// ------------------------------------------------------
async function safeRename(src, dest, retries = 20) {
  for (let i = 0; i < retries; i++) {
    try {
      await fs.rename(src, dest);
      return;
    } catch (err) {
      if (err.code === "EBUSY" || err.code === "EPERM") {
        // Wait 100ms and retry
        await new Promise(res => setTimeout(res, 100));
        continue;
      }
      throw err; // all other errors are real errors
    }
  }

  throw new Error(`safeRename failed after ${retries} retries: ${src} → ${dest}`);
}

// ------------------------------------------------------
// Read first header line
// ------------------------------------------------------
async function readHeaderLine(filePath) {
  try {
    const buf = await fs.readFile(filePath, { encoding: "utf8" });
    const first = buf.split(/\r?\n/)[0] || "";
    return first.trim();
  } catch (err) {
    console.warn("Warning: Could not read header from", filePath, err.message);
    return "";
  }
}

// ------------------------------------------------------
// Normalize header columns
// ------------------------------------------------------
function parseHeaderColumns(line) {
  if (!line) return [];
  return line
    .split("|")
    .map(x => x.trim())
    .filter(Boolean)
    .map(x => x.toUpperCase());
}

// ------------------------------------------------------
// Determination helpers
// ------------------------------------------------------
function looksLikeListingHeader(cols) {
  const markers = ["LIST_NO", "LIST_PRICE", "STATUS", "PROP_TYPE"];
  return markers.some(m => cols.includes(m));
}

function looksLikeAgentHeader(cols) {
  const markers = ["AGENT_ID", "AGENT_NAME", "LIST_AGENT"];
  return markers.some(m => cols.includes(m));
}

function looksLikeOfficeHeader(cols) {
  const markers = ["OFFICE_ID", "OFFICE_NAME", "LIST_OFFICE"];
  return markers.some(m => cols.includes(m));
}

function looksLikeReferenceHeader(cols) {
  const refMarkers = ["AREA", "ZIP", "TOWN_NUM", "DISPLAY_NAME", "FIELD_NAME"];
  const listingMarkers = ["LIST_NO", "LIST_PRICE"];
  return refMarkers.some(m => cols.includes(m)) && !listingMarkers.some(m => cols.includes(m));
}

// ------------------------------------------------------
// Infer property folder from filename
// ------------------------------------------------------
function inferPropFolderFromName(baseLower) {
  if (baseLower.startsWith("idx_sf")) return "single_family";
  if (baseLower.startsWith("idx_mf")) return "multi_family";
  if (baseLower.startsWith("idx_cc") || baseLower.startsWith("idx_cnd")) return "condo";
  if (baseLower.startsWith("idx_ld")) return "land";
  return "single_family"; // default fallback
}

// ------------------------------------------------------
// Infer status
// ------------------------------------------------------
function inferStatusFromName(baseLower) {
  if (baseLower.includes("sold") || baseLower.includes("_sld")) return "sold";
  if (baseLower.includes("pending")) return "pending";
  if (baseLower.includes("contingent")) return "contingent";
  if (baseLower.includes("coming_soon")) return "coming_soon";
  if (baseLower.includes("canceled")) return "canceled";
  if (baseLower.includes("expired")) return "expired";
  if (baseLower.includes("off_market")) return "off_market";
  if (baseLower.includes("temp_off_market")) return "temp_off_market";
  if (baseLower.includes("under_agreement")) return "under_agreement";
  if (baseLower.includes("withdrawn")) return "withdrawn";
  return "active";
}

// ------------------------------------------------------
// Move unknown files safely
// ------------------------------------------------------
async function moveToUnclassified(filePath, base, reason = "unknown") {
  const dir = path.join(INCOMING_DIR, "unclassified");
  await ensureDir(dir);
  const dest = path.join(dir, base);
  await safeRename(filePath, dest);
  return { domain: reason, destPath: dest, info: { base, reason } };
}

// ------------------------------------------------------
// MAIN: classify and route file
// ------------------------------------------------------
export async function classifyAndRoute(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const base = path.basename(filePath);
  const baseLower = base.toLowerCase();

  const isText = [".txt", ".csv", ".tsv"].includes(ext);
  const isJson = [".json", ".jsonl"].includes(ext);

  // ------------------------------------------------------
  // JSON files → agents / offices / reference
  // ------------------------------------------------------
  if (isJson) {
    if (baseLower.includes("agent")) {
      const destDir = path.join(RAW_ROOT, "agents");
      await ensureDir(destDir);
      const dest = path.join(destDir, base);
      await safeRename(filePath, dest);
      return { domain: "agents", destPath: dest, info: {} };
    }

    if (baseLower.includes("office")) {
      const destDir = path.join(RAW_ROOT, "offices");
      await ensureDir(destDir);
      const dest = path.join(destDir, base);
      await safeRename(filePath, dest);
      return { domain: "offices", destPath: dest, info: {} };
    }

    if (
      baseLower.includes("ref") ||
      baseLower.includes("lookup") ||
      baseLower.includes("tbl")
    ) {
      const destDir = path.join(RAW_ROOT, "reference");
      await ensureDir(destDir);
      const dest = path.join(destDir, base);
      await safeRename(filePath, dest);
      return { domain: "reference", destPath: dest, info: {} };
    }

    return await moveToUnclassified(filePath, base, "unknown_json");
  }

  // ------------------------------------------------------
  // TEXT-LIKE FILES (main IDX)
  // ------------------------------------------------------
  if (isText) {
    const header = await readHeaderLine(filePath);
    const cols = parseHeaderColumns(header);

    // --- LISTINGS ---
    if (looksLikeListingHeader(cols) || baseLower.startsWith("idx_")) {
      const propFolder = inferPropFolderFromName(baseLower);
      const statusFolder = inferStatusFromName(baseLower);

      const destDir = path.join(RAW_ROOT, propFolder, statusFolder);
      await ensureDir(destDir);

      const dest = path.join(destDir, base);
      await safeRename(filePath, dest);

      return {
        domain: "listing",
        destPath: dest,
        info: { propFolder, statusFolder }
      };
    }

    // --- AGENTS ---
    if (looksLikeAgentHeader(cols) || baseLower.includes("agent")) {
      const destDir = path.join(RAW_ROOT, "agents");
      await ensureDir(destDir);
      const dest = path.join(destDir, base);
      await safeRename(filePath, dest);
      return { domain: "agents", destPath: dest, info: {} };
    }

    // --- OFFICES ---
    if (looksLikeOfficeHeader(cols) || baseLower.includes("office")) {
      const destDir = path.join(RAW_ROOT, "offices");
      await ensureDir(destDir);
      const dest = path.join(destDir, base);
      await safeRename(filePath, dest);
      return { domain: "offices", destPath: dest, info: {} };
    }

    // --- REFERENCE TABLE ---
    if (looksLikeReferenceHeader(cols)) {
      const destDir = path.join(RAW_ROOT, "reference");
      await ensureDir(destDir);
      const dest = path.join(destDir, base);
      await safeRename(filePath, dest);
      return { domain: "reference", destPath: dest, info: {} };
    }

    return await moveToUnclassified(filePath, base, "unknown_text");
  }

  // ------------------------------------------------------
  // NON-TEXT / NON-JSON → unclassified
  // ------------------------------------------------------
  return await moveToUnclassified(filePath, base, "unknown_binary");
}

// ------------------------------------------------------
// DEBUG: run directly from CLI
// ------------------------------------------------------
if (import.meta.url === `file://${process.argv[1]}`) {
  const arg = process.argv[2];
  if (!arg) {
    console.error("Usage: node classifyIDX.js <file>");
    process.exit(1);
  }

  const abs = path.resolve(arg);
  classifyAndRoute(abs)
    .then(res => console.log("Classification result:", res))
    .catch(err => console.error(err));
}
