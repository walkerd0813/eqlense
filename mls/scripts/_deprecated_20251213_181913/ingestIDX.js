// backend/mls/scripts/ingestIDX.js
// Full IDX ingestion -> normalized/listings.ndjson + properties.ndjson

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";
import { normalizeListing, listingToPropertyDoc } from "../lib/mlsNormalize.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const MLS_ROOT = path.join(__dirname, "..");
const RAW_ROOT = path.join(MLS_ROOT, "raw");
const NORMALIZED_DIR = path.join(MLS_ROOT, "normalized");

const PROPERTY_FOLDERS = [
  { folder: "single_family" },
  { folder: "multi_family" },
  { folder: "condo" },
  { folder: "land" },
];

// -------------------------------------
// Helpers
// -------------------------------------
function log(msg) {
  console.log(msg);
}

function splitDelimitedLine(line, sep) {
  const result = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        // Escaped quote
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === sep && !inQuotes) {
      result.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  result.push(current);
  return result;
}

async function parseIdxFile(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  let defaultSep = ext === ".csv" ? "," : "|";

  const rows = [];
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let headers = null;
  let sep = defaultSep;

  for await (const lineRaw of rl) {
    const line = lineRaw.replace(/\r?\n$/, "");
    const trimmed = line.trim();
    if (!trimmed) continue;

    if (!headers) {
      // Auto-detect delimiter from header
      if (trimmed.includes("|")) sep = "|";
      else if (trimmed.includes(",")) sep = ",";

      headers = splitDelimitedLine(trimmed, sep).map((h) => h.trim());
      continue;
    }

    const cols = splitDelimitedLine(line, sep);
    const row = {};
    for (let i = 0; i < headers.length; i++) {
      const key = headers[i];
      row[key] = cols[i] !== undefined ? cols[i] : "";
    }
    rows.push(row);
  }

  return rows;
}

// -------------------------------------
// Main ingestion
// -------------------------------------
export async function ingestListings() {
  log("➡️  ingestListings()");
  log("🚀 IDX ingestion started...");
  log(`   RAW_ROOT        = ${RAW_ROOT}`);
  log(`   NORMALIZED_DIR  = ${NORMALIZED_DIR}`);

  await fsp.mkdir(NORMALIZED_DIR, { recursive: true });

  const listingsPath = path.join(NORMALIZED_DIR, "listings.ndjson");
  const propertiesPath = path.join(NORMALIZED_DIR, "properties.ndjson");

  const listingsStream = fs.createWriteStream(listingsPath, {
    flags: "w",
    encoding: "utf8",
  });

  const propertiesStream = fs.createWriteStream(propertiesPath, {
    flags: "w",
    encoding: "utf8",
  });

  let totalListings = 0;
  let totalFiles = 0;

  // key -> propertyDoc
  const propertyMap = new Map();

  for (const { folder } of PROPERTY_FOLDERS) {
    const propRoot = path.join(RAW_ROOT, folder);
    if (!fs.existsSync(propRoot)) continue;

    const dirEntries = await fsp.readdir(propRoot, { withFileTypes: true });
    const statusFolders = dirEntries.filter((d) => d.isDirectory()).map((d) => d.name);

    for (const statusFolder of statusFolders) {
      const statusDir = path.join(propRoot, statusFolder);
      const files = (await fsp.readdir(statusDir)).filter((f) => {
        const lower = f.toLowerCase();
        return lower.endsWith(".txt") || lower.endsWith(".csv");
      });

      if (!files.length) continue;

      log(``);
      log(`📂 Folder: ${folder.toUpperCase()} / ${statusFolder.toUpperCase()}`);
      log(`   Found ${files.length} file(s).`);

      for (const file of files) {
        const filePath = path.join(statusDir, file);
        totalFiles++;
        log(`   → Processing file: ${file}`);

        const rows = await parseIdxFile(filePath);

        for (const rawRow of rows) {
          const listing = normalizeListing(rawRow, {
            source: path.relative(MLS_ROOT, filePath),
          });
          if (!listing) continue;

          listingsStream.write(JSON.stringify(listing) + "\n");
          totalListings++;

          const propDoc = listingToPropertyDoc(listing);
          if (propDoc && !propertyMap.has(propDoc.propertyKey)) {
            propertyMap.set(propDoc.propertyKey, propDoc);
          }
        }
      }
    }
  }

  // Flush properties
  for (const prop of propertyMap.values()) {
    propertiesStream.write(JSON.stringify(prop) + "\n");
  }

  listingsStream.end();
  propertiesStream.end();

  log("");
  log("✅ IDX ingestion complete.");
  log(`   Total files processed:   ${totalFiles}`);
  log(`   Total listings ingested: ${totalListings}`);
  log(`   listings → ${listingsPath}`);
  log(`   properties → ${propertiesPath}`);
}

// Allow direct execution: node mls/scripts/ingestIDX.js
if (import.meta.url === `file://${process.argv[1]}`) {
  ingestListings().catch((err) => {
    console.error("❌ IDX ingestion failed:", err);
    process.exit(1);
  });
}
