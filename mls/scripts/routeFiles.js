import fs from "fs";
import path from "path";

const MANUAL_INBOX = path.resolve("mls/manual_inbox");
const RAW_ROOT = path.resolve("mls/raw");

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function classifyFile(filename) {
  const name = filename.toUpperCase();

  let property = null;
  if (name.startsWith("CONDO_")) property = "condo";
  else if (name.startsWith("SF(1)_")) property = "single_family";
  else if (name.startsWith("MF_")) property = "multi_family";
  else if (name.startsWith("LAND_")) property = "land";

  if (!property) return null;

  let status = "events";
  if (name.includes("_ACTIVE_")) status = "active";
  else if (name.includes("_SOLD_")) status = "sold";
  else if (name.includes("_OTHER_") || name.includes("_OTHERS_")) status = "events";

  return { property, status };
}

console.log("====================================================");
console.log(" ROUTING FILES FROM manual_inbox");
console.log("====================================================");

const files = fs.readdirSync(MANUAL_INBOX);

for (const file of files) {
  if (!file.toLowerCase().endsWith(".csv")) continue;

  const classification = classifyFile(file);
  if (!classification) {
    console.warn(`⚠️ Skipping unknown file: ${file}`);
    continue;
  }

  const { property, status } = classification;

  const targetDir = path.join(RAW_ROOT, property, status);
  ensureDir(targetDir);

  const src = path.join(MANUAL_INBOX, file);
  const dest = path.join(targetDir, file);

  fs.renameSync(src, dest);
  console.log(`→ Routed ${file} → raw/${property}/${status}`);
}

console.log("----------------------------------------------------");
console.log(" ROUTING COMPLETE");
