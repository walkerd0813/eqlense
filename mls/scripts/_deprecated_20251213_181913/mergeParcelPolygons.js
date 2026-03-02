// -------------------------------------------------------
// STREAM MERGE EAST + WEST PARCEL POLYGONS (LARGE FILE SAFE)
// -------------------------------------------------------

import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const EAST_PATH = path.join(
  __dirname,
  "../../publicData/parcels/parcelPolygons_EAST.geojson"
);

const WEST_PATH = path.join(
  __dirname,
  "../../publicData/parcels/parcelPolygons_WEST.geojson"
);

const OUTPUT_PATH = path.join(
  __dirname,
  "../../publicData/parcels/parcelPolygons.geojson"
);

function ensure(pathToFile) {
  if (!fs.existsSync(pathToFile)) {
    console.error("❌ Missing file:", pathToFile);
    process.exit(1);
  }
}

async function streamFeatures(inputPath, writeStream, isFirst) {
  const rl = readline.createInterface({
    input: fs.createReadStream(inputPath),
    crlfDelay: Infinity
  });

  let started = false;

  for await (const line of rl) {
    const trimmed = line.trim();

    if (!started) {
      if (trimmed.startsWith('"features"')) {
        started = true;
      }
      continue;
    }

    if (trimmed === "]" || trimmed === "]}") break;

    if (trimmed.startsWith("{")) {
      if (!isFirst) writeStream.write(",\n");
      writeStream.write(trimmed);
      isFirst = false;
    }
  }

  return isFirst;
}

async function main() {
  console.log("====================================================");
  console.log(" STREAM MERGING PARCEL POLYGONS (EAST + WEST)");
  console.log("====================================================");

  ensure(EAST_PATH);
  ensure(WEST_PATH);

  const out = fs.createWriteStream(OUTPUT_PATH);
  out.write('{"type":"FeatureCollection","features":[\n');

  let isFirst = true;
  isFirst = await streamFeatures(EAST_PATH, out, isFirst);
  isFirst = await streamFeatures(WEST_PATH, out, isFirst);

  out.write("\n]}\n");
  out.end();

  console.log("✅ Parcel polygon merge complete");
  console.log("Output:", OUTPUT_PATH);
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ Merge failed:", err);
  process.exit(1);
});
