import fs from "fs";
import path from "path";
import csv from "csv-parser";
import { fileURLToPath } from "url";
import { normalizeListingRow } from "../normalized/normalizeListingRow.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Paths
const RAW_ROOT = path.resolve(__dirname, "../raw");
const NORMALIZED_DIR = path.resolve(__dirname, "../normalized");
const OUTPUT_FILE = path.join(NORMALIZED_DIR, "listings.ndjson");

fs.mkdirSync(NORMALIZED_DIR, { recursive: true });

console.log("====================================================");
console.log("      INGEST FULL MLS LISTINGS (CANONICAL)");
console.log("====================================================");
console.log("RAW root:    ", RAW_ROOT);
console.log("Output file: ", OUTPUT_FILE);
console.log("====================================================");

let fileCount = 0;
let rowCount = 0;

const output = fs.createWriteStream(OUTPUT_FILE, { flags: "w" });

/**
 * Only accept FULL listing CSVs
 * Excludes event feeds
 */
function isListingCsv(filename) {
  const name = filename.toUpperCase();

  // hard exclusions (event / lifecycle feeds)
  if (
    name.includes("_BOM") ||
    name.includes("_CAN") ||
    name.includes("_CTG") ||
    name.includes("_EXP")
  ) {
    return false;
  }

  // true listings (ACTIVE / SOLD / OTHER)
  return (
    name.includes("ACTIVE") ||
    name.includes("SOLD") ||
    name.includes("OTHER")
  );
}


/**
 * Infer propertyType + status from folder path
 */
function extractContextFromPath(fullPath) {
  const parts = fullPath.toLowerCase().split(path.sep);

  const propertyTypes = ["single_family", "condo", "multi_family", "land"];
  const statuses = [
    "active",
    "sold",
    "pending",
    "expired",
    "off_market",
    "under_agreement",
    "withdrawn",
    "coming_soon",
    "canceled",
    "temp_off_market"
  ];

  return {
    propertyType: propertyTypes.find(p => parts.includes(p)) || null,
    status: statuses.find(s => parts.includes(s)) || null
  };
}

async function ingestCsv(filePath, context) {
  return new Promise((resolve) => {
    let rows = 0;

    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (raw) => {
        try {
          const normalized = normalizeListingRow(raw, context);
          if (!normalized) return;

          output.write(JSON.stringify(normalized) + "\n");
          rows++;
          rowCount++;
        } catch {
          // skip malformed rows
        }
      })
      .on("end", () => {
        fileCount++;
        console.log(`✔ ${rows.toLocaleString()} rows → ${path.basename(filePath)}`);
        resolve();
      });
  });
}

async function walk(dir) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);

    if (entry.isDirectory()) {
      await walk(fullPath);
      continue;
    }

    if (
  entry.isFile() &&
  entry.name.toLowerCase().endsWith(".csv") &&
  !entry.name.toLowerCase().includes("agent") &&
  !entry.name.toLowerCase().includes("office") &&
  !entry.name.toLowerCase().includes("reference")
) {
  const context = extractContextFromPath(fullPath);
  console.log("FOUND CSV:", fullPath, context);
  await ingestCsv(fullPath, context);
}

  }
}

(async function main() {
  await walk(RAW_ROOT);
  output.end();

  console.log("----------------------------------------------------");
  console.log("✅ LISTING INGEST COMPLETE");
  console.log("Files processed:", fileCount);
  console.log("Rows ingested:  ", rowCount.toLocaleString());
  console.log("Output:         ", OUTPUT_FILE);
  console.log("====================================================");
})();


