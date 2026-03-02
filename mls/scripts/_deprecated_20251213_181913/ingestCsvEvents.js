// mls/scripts/ingestCsvEvents.js
// ESM — ingest MLS CSV sold / event files into normalized NDJSON

import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ROOT = path.resolve(__dirname, "..", "..");
const RAW_ROOT = path.join(ROOT, "mls", "raw");
const NORMALIZED_DIR = path.join(ROOT, "mls", "normalized");
const OUTPUT_FILE = path.join(NORMALIZED_DIR, "listings.ndjson");

function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

// Very simple CSV parser assuming lines like:
// "A","B","C" — commas only separate fields, all double-quoted
function parseCsvLine(line) {
  const trimmed = line.trim();
  if (!trimmed) return [];

  // Remove leading/trailing quotes if present
  let inner = trimmed;
  if (inner.startsWith('"') && inner.endsWith('"')) {
    inner = inner.slice(1, -1);
  }

  // Split on "," boundaries
  return inner.split(/","/g);
}

function cleanNumber(str) {
  if (!str) return null;
  const cleaned = String(str).replace(/[^0-9.-]/g, "");
  if (cleaned === "" || cleaned === "-" || cleaned === ".") return null;
  const num = Number(cleaned);
  return Number.isNaN(num) ? null : num;
}

function detectFromFilename(fullPath, propFolder) {
  const base = path.basename(fullPath).toLowerCase();

  const typeCode =
    propFolder === "single_family"
      ? "SF"
      : propFolder === "multi_family"
      ? "MF"
      : propFolder === "condo"
      ? "CONDO"
      : propFolder === "land"
      ? "LAND"
      : "OTHER";

  let statusCode = null;
  let statusGroup = null;

  if (base.includes("sold")) {
    statusCode = "SOLD";
    statusGroup = "sold";
  } else if (base.includes("active")) {
    statusCode = "ACTIVE";
    statusGroup = "active";
  } else if (base.includes("ctg") || base.includes("under")) {
    statusCode = "UNDER_AGREEMENT";
    statusGroup = "under_contract";
  } else if (base.includes("can")) {
    statusCode = "CANCELED";
    statusGroup = "canceled";
  } else if (base.includes("exp")) {
    statusCode = "EXPIRED";
    statusGroup = "expired";
  } else if (base.includes("wnd") || base.includes("withdraw")) {
    statusCode = "WITHDRAWN";
    statusGroup = "withdrawn";
  } else if (base.includes("price")) {
    statusCode = "PRICE_CHANGE";
    statusGroup = "price_change";
  } else {
    statusCode = "OTHER";
    statusGroup = "other";
  }

  return { propertyType: typeCode, statusCode, statusGroup };
}

async function ingestCsvFile(fullPath, propFolder, outputStream) {
  console.log(`   → Ingesting ${path.relative(ROOT, fullPath)} ...`);

  const fileInfo = detectFromFilename(fullPath, propFolder);

  const rl = readline.createInterface({
    input: fs.createReadStream(fullPath),
    crlfDelay: Infinity,
  });

  let header = null;
  let count = 0;

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    if (!header) {
      header = parseCsvLine(trimmed);
      continue;
    }

    const cols = parseCsvLine(trimmed);
    if (!cols.length) continue;

    const row = {};
    header.forEach((name, idx) => {
      row[name] = cols[idx] ?? "";
    });

    const address = (row["ADDRESS"] || "").trim();
    const town = (row["TOWN_DESC"] || "").trim();
    const zip = (row["ZIP_CODE"] || "").trim();
    const zip4 = (row["ZIP_CODE_4"] || "").trim();

    const record = {
      // Meta
      sourceFile: path.basename(fullPath),
      propertyType: fileInfo.propertyType, // SF / MF / CONDO / LAND
      statusCode: fileInfo.statusCode,
      statusGroup: fileInfo.statusGroup,

      // Address / key
      addressFull: address,
      town,
      zip,
      zip4: zip4 || null,
      propertyKey: address && zip ? `${address}|${zip}` : null,

      // Basic characteristics
      bedrooms: cleanNumber(row["NO_BEDROOMS"]),
      bathsDescription: (row["BTH_DESC"] || "").trim(),
      salePrice: cleanNumber(row["SALE_PRICE"]),
      settledDate: (row["SETTLED_DATE"] || "").trim() || null,
      sqft: cleanNumber(row["SQUARE_FEET"]) ?? cleanNumber(row["TOTAL_BLDG_SF_CI"]),
      buildingSqftCI: cleanNumber(row["TOTAL_BLDG_SF_CI"]),
      lotSize: cleanNumber(row["LOT_SIZE"]),
      yearBuilt: cleanNumber(row["YEAR_BUILT"]),
      propTypeRaw: (row["PROP_TYPE"] || "").trim(),
      style: (row["STYLE_SF"] || "").trim(),
      garageSpaces: cleanNumber(row["GARAGE_SPACES_SF"]),
      parkingSpaces: cleanNumber(row["PARKING_SPACES_SF"]),
      marketTime: cleanNumber(row["MARKET_TIME"]),
      listPrice: cleanNumber(row["LIST_PRICE"]),
      pricePerSqft: cleanNumber(row["PRICE_PER_SQFT"]),
      heating: (row["HEATING_SF"] || "").trim(),
      siteCondition: (row["SITE_CONDITION_CI"] || "").trim(),
      remarks: (row["REMARKS"] || "").trim(),
      firmRemarks: (row["FIRM_RMK1"] || "").trim(),
      units: cleanNumber(row["RSU_UNITS_CI"]),

      // Raw row in case we need to debug later
      raw: row,
    };

    outputStream.write(JSON.stringify(record) + "\n");
    count++;
  }

  console.log(`     ✔ Ingested ${count} rows from ${path.basename(fullPath)}`);
}

async function main() {
  console.log("====================================================");
  console.log("     INGEST MLS CSV EVENT / SOLD FILES");
  console.log("====================================================");
  console.log(`RAW root:       ${RAW_ROOT}`);
  console.log(`Normalized dir: ${NORMALIZED_DIR}`);
  console.log(`Output:         ${OUTPUT_FILE}`);
  console.log("====================================================");

  ensureDir(NORMALIZED_DIR);
  const outputStream = fs.createWriteStream(OUTPUT_FILE, { flags: "w" });

  const typeFolders = [
    "single_family",
    "multi_family",
    "condo",
    "land",
  ];

  let totalFiles = 0;

  for (const typeFolder of typeFolders) {
    const eventsDir = path.join(RAW_ROOT, typeFolder, "events");
    if (!fs.existsSync(eventsDir)) continue;

    const files = fs
  .readdirSync(eventsDir, { withFileTypes: true })
  .filter((d) => d.isFile())
  .map((d) => d.name)
  .filter((f) => f.toLowerCase().endsWith(".csv"));

    if (!files.length) continue;

    console.log(`📂 ${typeFolder}/events — ${files.length} file(s)`);

    for (const file of files) {
      const fullPath = path.join(eventsDir, file);
      await ingestCsvFile(fullPath, typeFolder, outputStream);
      totalFiles++;
    }
  }

  outputStream.end();

  console.log("----------------------------------------------------");
  console.log(`✅ CSV event/sold ingestion complete.`);
  console.log(`   Files processed: ${totalFiles}`);
  console.log(`   Output:          ${OUTPUT_FILE}`);
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ CSV ingestion failed:", err);
  process.exit(1);
});
