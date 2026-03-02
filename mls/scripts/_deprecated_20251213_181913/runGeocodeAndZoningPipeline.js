// backend/mls/scripts/runGeocodeAndZoningPipeline.js
// ESM-only, orchestrates the full geocode + zoning pipeline for ALL cities.

import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Run a sibling Node script as a child process.
 * This avoids depending on internal exports and stays compatible with
 * your existing single-file CLI-style scripts.
 */
function runStep(label, relativeScriptPath, extraArgs = []) {
  const scriptPath = path.join(__dirname, relativeScriptPath);

  console.log("\n====================================================");
  console.log(` STEP: ${label}`);
  console.log(` Running: node ${relativeScriptPath}`);
  console.log("====================================================");

  const result = spawnSync(
    process.execPath,            // same Node binary
    [scriptPath, ...extraArgs],  // script + args
    {
      stdio: "inherit",          // show output live (like when you run them manually)
      env: process.env,
    }
  );

  if (result.status !== 0) {
    const baseMessage = `Step "${label}" failed with exit code ${result.status}.`;
    const errorMessage = result.error
      ? `${baseMessage}\nUnderlying error: ${result.error.message}`
      : baseMessage;

    throw new Error(errorMessage);
  }

  console.log(`✅ Completed: ${label}`);
}

async function main() {
  console.log("====================================================");
  console.log("  MULTI-CITY GEOCODE + ZONING PIPELINE (V1)");
  console.log("====================================================");
  console.log("Assumptions:");
  console.log(" • Normalized listings: mls/normalized/listings.ndjson");
  console.log(" • Coordinate outputs:  listingsWithCoords_*.ndjson");
  console.log(" • Final coords file:   listingsWithCoords_FINAL.ndjson");
  console.log(" • Zoning outputs:      listingsWithZoning.ndjson");
  console.log("");

  // 1) Build / refresh the MassGIS parcel centroid index.
  //    Safe to re-run; it only reads from your parcel shapefiles and
  //    writes parcelCentroidIndex.json.
  runStep(
    "Build MassGIS parcel centroid index",
    "./buildParcelIndex.js"
  );

  // 2) FAST coordinate attach (Tier 1–3: parcels + address points + basic fuzzy).
  //    Writes listingsWithCoords_FAST.ndjson + unmatched_FAST.ndjson.
  runStep(
    "Attach coordinates to listings (FAST / Tier 1–3)",
    "./attachCoordinatesToListings.js"
  );

  // 3) PASS2 / deeper address-point + fuzzy backup.
  //    Writes listingsWithCoords_PASS2.ndjson + externalUnmatched/externalGeocoded if applicable.
  runStep(
    "Attach coordinates from address points / PASS2 + external",
    "./attachCoordinatesFromAddressPoints.js"
  );

  // 4) Merge all coordinate sources into the immutable FINAL coordinates file.
  //    This is where geocode_source + geocode_confidence should be locked.
  runStep(
    "Merge coordinate sources into listingsWithCoords_FINAL.ndjson",
    "./mergeCoords.js"
  );

  // 5) Attach zoning + civic + overlays from polygons.
  //    Reads listingsWithCoords_FINAL.ndjson and writes listingsWithZoning.ndjson
  //    + listingsWithZoning_unmatched.ndjson.
  runStep(
    "Attach zoning + civic overlays to geocoded listings",
    "./attachZoningToNormalizedListings.js"
  );

  // 6) Summarize coverage so you can see how many got zoning/civic/overlays.
  runStep(
    "Summarize zoning + civic coverage",
    "./summarizeZoningCoverage.js"
  );

  console.log("\n====================================================");
  console.log("  ✅ PIPELINE COMPLETE");
  console.log("     • FINAL COORDS:  mls/normalized/listingsWithCoords_FINAL.ndjson");
  console.log("     • ZONING OUTPUT: mls/normalized/listingsWithZoning.ndjson");
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ Pipeline failed:", err);
  process.exit(1);
});
