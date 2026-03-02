// mls/scripts/officesIngest.js
// Ingest offices.txt (pipe-delimited) → normalized/offices.ndjson + MongoDB

import dotenvx from "@dotenvx/dotenvx";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import mongoose from "mongoose";

// --------------------------------------------------
// Env + paths
// --------------------------------------------------
dotenvx.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const MLS_ROOT = path.join(__dirname, "..");
const RAW_DIR = path.join(MLS_ROOT, "raw", "offices");
const NORMALIZED_DIR = path.join(MLS_ROOT, "normalized");
const OFFICES_NDJSON = path.join(NORMALIZED_DIR, "offices.ndjson");

const MONGO_URI = process.env.MONGO_URI;

// --------------------------------------------------
// Mongo schema/model (text phone, dedupe by officeId)
// --------------------------------------------------
const OfficeSchema = new mongoose.Schema(
  {
    officeId: { type: String, required: true, index: true, unique: true },
    name: { type: String },
    // treat all as text
    phoneRaw: { type: String },
    source: { type: String, default: "mlspin_idx" },
    updatedAt: { type: Date, default: Date.now },
  },
  { collection: "mls_offices" }
);

let OfficeModel;
try {
  OfficeModel =
    mongoose.models.mls_offices || mongoose.model("mls_offices", OfficeSchema);
} catch {
  OfficeModel = mongoose.model("mls_offices", OfficeSchema);
}

// --------------------------------------------------
// Helpers
// --------------------------------------------------
async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

/**
 * Parse a single offices.txt row.
 *
 * Sample patterns from your uploads:
 *   AN5251|Morning Tide Financial Group, Inc.|781-3789760
 *   K95321|RE/MAX Associates|978-4228100
 *
 * We’ll ignore weird header/partial lines (e.g. starting with just a "|" or lacking ID).
 */
function parseOfficeLine(line) {
  const trimmed = line.trim();
  if (!trimmed) return null;

  const parts = trimmed.split("|");
  if (parts.length < 3) return null;

  const officeIdRaw = parts[0]?.trim();
  const nameRaw = parts[1]?.trim();
  const phoneRaw = parts[2]?.trim();

  // Skip lines with missing ID or name; that first " |508-7561203" style row gets ignored.
  if (!officeIdRaw || officeIdRaw.startsWith("|")) return null;
  if (!nameRaw) return null;

  const officeId = officeIdRaw;
  const name = nameRaw || "";
  const phone = phoneRaw || "";

  return {
    officeId,
    name,
    phoneRaw: phone,
    source: "mlspin_idx",
    updatedAt: new Date(),
  };
}

/**
 * Read all .txt-like files in raw/offices, parse, and return
 * a deduped list of office records, where the LAST row for each
 * officeId wins (your choice #3).
 */
async function loadAndDedupeOfficesFromRaw() {
  let files;
  try {
    files = await fs.readdir(RAW_DIR, { withFileTypes: true });
  } catch (err) {
    console.warn(
      "[officesIngest] No raw offices directory or error reading it:",
      err.message
    );
    return [];
  }

  const txtFiles = files
    .filter((d) => d.isFile())
    .map((d) => d.name)
    .filter((name) => name.toLowerCase().endsWith(".txt"));

  if (txtFiles.length === 0) {
    console.warn("[officesIngest] No .txt files found in raw/offices.");
    return [];
  }

  const officeMap = new Map(); // officeId -> office (LAST row wins)

  for (const fileName of txtFiles) {
    const fullPath = path.join(RAW_DIR, fileName);
    const content = await fs.readFile(fullPath, "utf8");
    const lines = content.split(/\r?\n/);

    for (const line of lines) {
      const office = parseOfficeLine(line);
      if (!office) continue;

      // LAST row for each ID wins
      officeMap.set(office.officeId, office);
    }
  }

  return [...officeMap.values()];
}

/**
 * Write offices.ndjson to normalized/ directory.
 */
async function writeOfficesNdjson(offices) {
  await ensureDir(NORMALIZED_DIR);

  const chunks = offices.map((o) => JSON.stringify(o));
  const data = chunks.join("\n") + (chunks.length ? "\n" : "");
  await fs.writeFile(OFFICES_NDJSON, data, "utf8");

  console.log(
    `[officesIngest] Wrote ${offices.length} offices to ${OFFICES_NDJSON}`
  );
}

/**
 * Store offices in MongoDB (replace collection with deduped set).
 */
async function storeOfficesInMongo(offices) {
  if (!MONGO_URI) {
    console.warn(
      "[officesIngest] No MONGO_URI set; skipping MongoDB storage for offices."
    );
    return;
  }

  await mongoose.connect(MONGO_URI, {
    serverSelectionTimeoutMS: 15000,
  });

  console.log(
    `[officesIngest] Connected to MongoDB. Replacing mls_offices with ${offices.length} documents…`
  );

  await OfficeModel.deleteMany({});
  if (offices.length > 0) {
    await OfficeModel.insertMany(offices, { ordered: false });
  }

  console.log("[officesIngest] MongoDB upsert complete.");

  await mongoose.disconnect();
}

/**
 * Main ingestion function – called from runManualIngestion.js
 */
export async function ingestOffices() {
  console.log("🏢 [officesIngest] Ingesting offices from raw/offices…");

  const offices = await loadAndDedupeOfficesFromRaw();
  console.log(
    `🏢 [officesIngest] Parsed ${offices.length} unique offices after dedupe-by-ID.`
  );

  await writeOfficesNdjson(offices);
  await storeOfficesInMongo(offices);

  console.log("✅ [officesIngest] Offices ingestion complete.\n");
}

// Allow running directly: node mls/scripts/officesIngest.js
if (import.meta.url === `file://${process.argv[1]}`) {
  ingestOffices().catch((err) => {
    console.error("[officesIngest] Fatal error:", err);
    process.exit(1);
  });
}
