// mls/scripts/runFullIngestionPipeline.js
// One-shot MLS ingestion pipeline

import dotenvx from "@dotenvx/dotenvx";
dotenvx.config();

import { fileURLToPath, pathToFileURL } from "node:url";
import path from "node:path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Shared dynamic caller (fixed for Windows + ESM)
async function safeCallModule(moduleRelPath, possibleFnNames) {
  const modulePath = path.join(__dirname, moduleRelPath);

  try {
    // IMPORTANT FIX FOR WINDOWS ESM MODULE IMPORTS:
    const mod = await import(pathToFileURL(modulePath).href);

    for (const name of possibleFnNames) {
      const fn = name === "default" ? mod.default : mod[name];
      if (typeof fn === "function") {
        console.log(`⚙️  Running ${path.basename(moduleRelPath)} → ${name}()`);
        await fn();
        return true;
      }
    }

    console.warn(
      `⚠️ No callable export found in ${moduleRelPath}. Tried: ${possibleFnNames.join(", ")}`
    );
    return false;

  } catch (err) {
    console.error(`❌ Error importing/calling ${moduleRelPath}:`, err);
    return false;
  }
}

export async function runFullIngestionPipeline() {
  console.log("==============================================");
  console.log(" 🚀 Starting FULL MLS ingestion pipeline…");
  console.log(" listings → reference → agents → offices → photos → verify");
  console.log("==============================================\n");

  // 1) Listings
  console.log("🔹 Step 1: Listings ingestion (ingestIDX.js)...");
  await safeCallModule("./ingestIDX.js", ["ingestAll"]);
  console.log("✅ Listings ingestion complete.\n");

  // 2) Reference tables
  console.log("🔹 Step 2: Reference tables (ingestReferenceTables.js)...");
  await safeCallModule("./ingestReferenceTables.js", ["ingestReferenceTables"]);
  console.log("✅ Reference ingestion complete.\n");

  // 3) Agents
  console.log("🔹 Step 3: Agents (agentsIngest.js)...");
  await safeCallModule("./agentsIngest.js", ["ingestAgents"]);
  console.log("✅ Agents ingestion complete.\n");

  // 4) Offices
  console.log("🔹 Step 4: Offices (officesIngest.js)...");
  await safeCallModule("./officesIngest.js", ["ingestOffices"]);
  console.log("✅ Offices ingestion complete.\n");

  // 5) Photos
  console.log("🔹 Step 5: Listing photos (ingestListingPhotos.js)...");
  await safeCallModule("./ingestListingPhotos.js", ["ingestListingPhotos"]);
  console.log("✅ Photos ingestion complete.\n");

  // 6) Verification
  console.log("🔹 Step 6: Verification (verifyListings.js)...");
  await safeCallModule("./verifyListings.js", ["verifyListings"]);
  console.log("✅ Verification complete.\n");

  console.log("==============================================");
  console.log(" 🎉 FULL MLS pipeline finished successfully.");
  console.log("==============================================");
}

// Auto-run
runFullIngestionPipeline().catch((err) => {
  console.error("❌ Pipeline failed:", err);
  process.exit(1);
});
