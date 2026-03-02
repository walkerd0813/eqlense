// mls/scripts/routeCsvEvents.js
// ESM — routes MLS CSV status/sold files out of manual_inbox into raw/*/events

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Root = backend/
const ROOT = path.resolve(__dirname, "..", "..");
const MANUAL_INBOX = path.join(ROOT, "mls", "manual_inbox");
const RAW_ROOT = path.join(ROOT, "mls", "raw");

function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function detectTypeAndStatus(filename) {
  // Examples:
  //  SF_bom_120725.csv
  //  SF_CAN_120725.csv
  //  condo_WND_120725.csv
  //  land_EXP_120725.csv
  const match = filename.match(/^(sf|mf|condo|land)_([a-z]+)_/i);
  if (!match) return null;

  const [_, typeCodeRaw, statusCodeRaw] = match;
  const typeCode = typeCodeRaw.toLowerCase();
  const statusCode = statusCodeRaw.toLowerCase();

  let propFolder;
  switch (typeCode) {
    case "sf":
      propFolder = "single_family";
      break;
    case "mf":
      propFolder = "multi_family";
      break;
    case "condo":
      propFolder = "condo";
      break;
    case "land":
      propFolder = "land";
      break;
    default:
      propFolder = "other";
  }

  // You can use this later if you want
  const statusMap = {
    bom: "BACK_ON_MARKET",
    can: "CANCELED",
    cso: "COMING_SOON",
    ctg: "CONTINGENT",
    exp: "EXPIRED",
    pcg: "PRICE_CHANGE",
    ua: "UNDER_AGREEMENT",
    wnd: "WITHDRAWN",
    price_change: "PRICE_CHANGE",
  };

  const normalizedStatus =
    statusMap[statusCode] || statusCode.toUpperCase();

  return {
    propFolder,
    statusCode: normalizedStatus,
  };
}

function main() {
  console.log("===============================================");
  console.log("   ROUTING MLS CSV EVENT / SOLD FILES");
  console.log("===============================================");
  console.log(`Manual inbox: ${MANUAL_INBOX}`);
  console.log(`Raw root:     ${RAW_ROOT}`);
  console.log("===============================================");

  if (!fs.existsSync(MANUAL_INBOX)) {
    console.error("❌ manual_inbox does not exist. Nothing to route.");
    process.exit(1);
  }

  const entries = fs.readdirSync(MANUAL_INBOX, { withFileTypes: true });
  let routed = 0;
  let skipped = 0;

  for (const entry of entries) {
    if (!entry.isFile()) continue;

    const ext = path.extname(entry.name).toLowerCase();
    if (ext !== ".csv") {
      continue; // let your existing TXT logic handle the rest
    }

    const info = detectTypeAndStatus(entry.name);
    if (!info) {
      console.log(`⚠️ CSV not recognized, leaving in inbox: ${entry.name}`);
      skipped++;
      continue;
    }

    const { propFolder, statusCode } = info;

    const destDir = path.join(RAW_ROOT, propFolder, "events");
    ensureDir(destDir);

    const src = path.join(MANUAL_INBOX, entry.name);
    const dest = path.join(destDir, entry.name);

    fs.renameSync(src, dest);

    console.log(
      `→ Routed ${entry.name} → ${path.relative(
        ROOT,
        dest
      )} [${propFolder}, ${statusCode}]`
    );
    routed++;
  }

  console.log("-----------------------------------------------");
  console.log(`✅ Routed CSV files:   ${routed}`);
  console.log(`⚠️ Left in inbox:      ${skipped}`);
  console.log("===============================================");
  console.log("   ROUTING COMPLETE");
  console.log("===============================================");
}

main();
