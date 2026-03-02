// -------------------------------------------------------
// Convert MassGIS Statewide Parcel SHPs (EAST + WEST) → GeoJSON
// -------------------------------------------------------

import fs from "node:fs";
import path from "node:path";
import shp from "shpjs";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const SHP_DIR = path.join(
  __dirname,
  "../../publicData/parcels/statewide/extracted"
);

const OUTPUT_PATH = path.join(
  __dirname,
  "../../publicData/parcels/parcelPolygons.geojson"
);

async function loadShp(filename) {
  const shpPath = path.join(SHP_DIR, filename);
  console.log(`[PARCEL] Reading ${filename}`);
  const geo = await shp(shpPath);
  console.log(
    `[PARCEL] Loaded ${geo.features.length.toLocaleString()} features`
  );
  return geo.features;
}

async function main() {
  console.log("====================================================");
  console.log(" CONVERT STATEWIDE PARCEL SHP (EAST + WEST)");
  console.log("====================================================");

  if (!fs.existsSync(SHP_DIR)) {
    console.error("❌ Extracted SHP folder not found:", SHP_DIR);
    process.exit(1);
  }

  const shpFiles = fs
    .readdirSync(SHP_DIR)
    .filter((f) => f.toLowerCase().endsWith(".shp"));

  if (!shpFiles.length) {
    console.error("❌ No .shp files found in extracted/");
    process.exit(1);
  }

  console.log(`[PARCEL] Found ${shpFiles.length} SHP files`);

  let allFeatures = [];

  for (const file of shpFiles) {
    const feats = await loadShp(file);
    allFeatures = allFeatures.concat(feats);
  }

  console.log(
    `[PARCEL] Total parcels loaded: ${allFeatures.length.toLocaleString()}`
  );

  const out = {
    type: "FeatureCollection",
    features: allFeatures,
  };

  fs.writeFileSync(OUTPUT_PATH, JSON.stringify(out));

  console.log("====================================================");
  console.log(" PARCEL POLYGON GEOJSON CREATED");
  console.log(" Output:", OUTPUT_PATH);
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ Parcel conversion failed:", err);
  process.exit(1);
});
