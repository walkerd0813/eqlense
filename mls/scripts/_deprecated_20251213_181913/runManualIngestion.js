// mls/scripts/manualIngest.js
// ----------------------------------------------------
// MASTER MANUAL INGESTION PIPELINE
// Drops all IDX files into mls/manual_inbox/
// Run:  node mls/scripts/manualIngest.js
// ----------------------------------------------------

import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Resolve directory
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ROOT = path.join(__dirname, "..");
const INBOX = path.join(ROOT, "manual_inbox");

// Import actual ingest functions
import { ingestListings } from "./ingestIDX.js";
import { ingestReferenceTables } from "./ingestReferenceTables.js";
import { ingestAgents } from "./agentsIngest.js";
import { ingestOffices } from "./officesIngest.js";
import { verifyListings } from "./verifyListings.js";

// ----------------------------------------------------
// STEP 0 — show inbox contents
// ----------------------------------------------------
async function showInbox() {
  const files = await fs.readdir(INBOX).catch(() => []);
  console.log("\n📂 Files in manual_inbox/:");
  files.forEach((f) => console.log(" •", f));
  console.log("----------------------------------------------------");
  return files;
}

// ----------------------------------------------------
// STEP 1 — Copy files to correct raw folders
// ----------------------------------------------------
async function routeFiles(files) {
  console.log("📦 Routing files into raw folders...");

  for (const file of files) {
    const lower = file.toLowerCase();
    const src = path.join(INBOX, file);

    let destDir;

    if (lower.startsWith("idx_sf")) destDir = "single_family/active";
    else if (lower.startsWith("idx_mf")) destDir = "multi_family/active";
    else if (lower.startsWith("idx_cc") || lower.startsWith("idx_cnd"))
      destDir = "condo/active";
    else if (lower.startsWith("idx_ld")) destDir = "land/active";
    else if (lower.includes("agent")) destDir = "agents";
    else if (lower.includes("office")) destDir = "offices";
    else if (
      lower.includes("area") ||
      lower.includes("town") ||
      lower.includes("ref")
    )
      destDir = "reference";
    else {
      console.log("⚠️ Unknown file type, skipping:", file);
      continue;
    }

    const fullDestDir = path.join(ROOT, "raw", destDir);
    await fs.mkdir(fullDestDir, { recursive: true });

    const dest = path.join(fullDestDir, file);
    await fs.copyFile(src, dest);

    console.log(` → Routed ${file} → raw/${destDir}`);
  }
}

// ----------------------------------------------------
// STEP 2 — Run ingestion & normalization
// ----------------------------------------------------
async function runPipeline() {
  console.log("\n🚀 Running IDX ingestion pipeline...\n");

  console.log("➡️  ingestListings()");
  await ingestListings();

  console.log("➡️  ingestReferenceTables()");
  await ingestReferenceTables();

  console.log("➡️  ingestAgents()");
  await ingestAgents();

  console.log("➡️  ingestOffices()");
  await ingestOffices();

  console.log("➡️  verifyListings()");
  await verifyListings();

  console.log("\n🎉 MANUAL PIPELINE COMPLETE!\n");
}

// ----------------------------------------------------
// MAIN
// ----------------------------------------------------
(async () => {
  console.log("====================================================");
  console.log("        MANUAL IDX INGESTION PIPELINE");
  console.log("====================================================");

  const files = await showInbox();
  if (files.length === 0) {
    console.log("⚠️ No files in manual_inbox/. Nothing to ingest.");
    return;
  }

  await routeFiles(files);
  await runPipeline();

  console.log("All done.\n");
})();
