// -------------------------------------------------------
// Build unified parcel polygon GeoJSON (EPSG:4326)
// -------------------------------------------------------

import fs from "node:fs";
import path from "node:path";
import fsp from "node:fs/promises";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const STATEWIDE_DIR = path.join(
  __dirname,
  "../../publicData/parcels/statewide"
);

const OUTPUT_PATH = path.join(
  __dirname,
  "../../publicData/parcels/parcelPolygons.geojson"
);

async function main() {
  console.log("====================================================");
  console.log(" BUILD PARCEL POLYGONS (STATEWIDE)");
  console.log("====================================================");

  if (!fs.existsSync(STATEWIDE_DIR)) {
    console.error("❌ Missing statewide parcel directory");
    process.exit(1);
  }

  const files = fs
    .readdirSync(STATEWIDE_DIR)
    .filter((f) => f.endsWith(".geojson"));

  if (!files.length) {
    console.error("❌ No parcel GeoJSON files found in statewide/");
    process.exit(1);
  }

  console.log(`[PARCEL] Found ${files.length} parcel files`);

  const features = [];

  for (const file of files) {
    const fullPath = path.join(STATEWIDE_DIR, file);
    const raw = await fsp.readFile(fullPath, "utf8");
    const geo = JSON.parse(raw);

    if (!geo.features) continue;

    for (const feat of geo.features) {
      if (!feat.geometry) continue;
      if (
        feat.geometry.type !== "Polygon" &&
        feat.geometry.type !== "MultiPolygon"
      )
        continue;

      features.push({
        type: "Feature",
        geometry: feat.geometry,
        properties: {
          parcel_id:
            feat.properties?.MAP_PAR_ID ||
            feat.properties?.PARCEL_ID ||
            null,
          source: "MassGIS",
        },
      });
    }
  }

  console.log(
    `[PARCEL] Writing ${features.length.toLocaleString()} parcel polygons`
  );

  const out = {
    type: "FeatureCollection",
    features,
  };

  await fsp.writeFile(OUTPUT_PATH, JSON.stringify(out));

  console.log("====================================================");
  console.log(" PARCEL POLYGONS BUILT");
  console.log(" Output:", OUTPUT_PATH);
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ Parcel polygon build failed:", err);
  process.exit(1);
});
