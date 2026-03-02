/*
  CANONICAL EVENT INGESTER
  ------------------------
  INPUT:  mls/raw/<property_type>/events/*.csv
  OUTPUT: mls/normalized/listings.ndjson

  This is the ONLY ingestion step for live inventory.
  Everything downstream reads listings.ndjson.
*/


import fs from "fs";
import path from "path";
import csv from "csv-parser";
import { fileURLToPath } from "url";

import { normalizeListingRow } from "../normalized/normalizeListingRow.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ================= CONFIG =================
const RAW_ROOT = path.resolve(__dirname, "../raw");
const OUTPUT_FILE = path.resolve(__dirname, "../normalized/listings.ndjson");

// =========================================
function extractContextFromPath(filePath) {
  const p = filePath.toLowerCase();

  let propertyType = null;
  if (p.includes("single_family")) propertyType = "single_family";
  else if (p.includes("multi_family")) propertyType = "multi_family";
  else if (p.includes("condo")) propertyType = "condo";
  else if (p.includes("land")) propertyType = "land";

  let status = "event";
  if (p.includes(`${path.sep}sold${path.sep}`)) status = "sold";
  else if (p.includes(`${path.sep}active${path.sep}`)) status = "active";

  return { propertyType, status };
}
let fileCount = 0;
let rowCount = 0;

fs.mkdirSync(path.dirname(OUTPUT_FILE), { recursive: true });
const output = fs.createWriteStream(OUTPUT_FILE, { flags: "w" });

console.log("====================================================");
console.log("      INGEST MLS EVENTS → CANONICAL LISTINGS");
console.log("====================================================");
console.log("RAW root:    ", RAW_ROOT);
console.log("Output file: ", OUTPUT_FILE);
console.log("====================================================");

/**
 * Only ingest CSVs that live inside an /events/ folder
 */
function isEventCsv(fullPath) {
  return (
    fullPath.toLowerCase().endsWith(".csv") &&
    fullPath.toLowerCase().includes(`${path.sep}events${path.sep}`)
  );
}

async function ingestCsv(filePath) {
  return new Promise((resolve) => {
    let rows = 0;
    const context = extractContextFromPath(filePath);

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

    if (entry.isFile() && isEventCsv(fullPath)) {
      console.log("FOUND EVENT CSV:", fullPath);
      await ingestCsv(fullPath);
    }
  }
}

// ================= RUN =================
await walk(RAW_ROOT);
output.end();

console.log("----------------------------------------------------");
console.log("✅ EVENT INGEST COMPLETE");
console.log("Files processed:", fileCount);
console.log("Rows ingested:  ", rowCount.toLocaleString());
console.log("Output:         ", OUTPUT_FILE);
console.log("====================================================");
