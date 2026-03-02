// mls/scripts/attachZoningToListings.js
// -------------------------------------------------------
// Attach zoning (district + subdistrict + properties)
// using Zoning Engine v4 (publicData/zoning/zoningLookup.js)
// -------------------------------------------------------

import fs from "node:fs";
import fsp from "node:fs/promises";
import readline from "node:readline";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { zoningLookup } from "../../publicData/zoning/zoningLookup.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// INPUT: final coordinates file
const INPUT = path.resolve(
  __dirname,
  "../../mls/normalized/listingsWithCoords_FINAL.ndjson"
);

// OUTPUTS
const OUTPUT_OK = path.resolve(
  __dirname,
  "../../mls/normalized/listingsWithZoning.ndjson"
);
const OUTPUT_BAD = path.resolve(
  __dirname,
  "../../mls/normalized/listingsWithZoning_unmatched.ndjson"
);

async function main() {
  console.log("=======================================================");
  console.log("                 IRONCLAD ZONING ATTACH");
  console.log("=======================================================");
  console.log("Input:", INPUT);
  console.log("Output OK:", OUTPUT_OK);
  console.log("Output BAD:", OUTPUT_BAD);
  console.log("-------------------------------------------------------");

  // Load zoning engine v4
  const engine = await zoningLookup();

  const inStream = fs.createReadStream(INPUT);
  const rl = readline.createInterface({ input: inStream });

  const outGood = fs.createWriteStream(OUTPUT_OK);
  const outBad = fs.createWriteStream(OUTPUT_BAD);

  let total = 0;
  let matched = 0;
  let missingCoords = 0;
  let noZoning = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;

    total++;
    const listing = JSON.parse(line);

    const lat = listing.latitude;
    const lng = listing.longitude;

    if (lat == null || lng == null) {
      missingCoords++;
      outBad.write(line + "\n");
      continue;
    }

    const hit = engine.lookup(lat, lng);

    if (!hit) {
      noZoning++;
      outBad.write(line + "\n");
      continue;
    }

    matched++;

    const enriched = {
      ...listing,
      zoning: {
        zoningCode: hit.zoningCode,
        town: hit.town,
        source: hit.source,
        properties: hit.properties,
      },
    };

    outGood.write(JSON.stringify(enriched) + "\n");

    if (total % 5000 === 0) {
      console.log(`Processed ${total.toLocaleString()}…`);
    }
  }

  console.log("-------------------------------------------------------");
  console.log("                 ZONING ATTACH COMPLETE");
  console.log("-------------------------------------------------------");
  console.log("Total:           ", total);
  console.log("Matched zoning:  ", matched);
  console.log("Missing coords:  ", missingCoords);
  console.log("No zoning match: ", noZoning);
  console.log("-------------------------------------------------------");
  console.log("OK file: ", OUTPUT_OK);
  console.log("BAD file:", OUTPUT_BAD);
  console.log("=======================================================");
}

main().catch((err) => {
  console.error("❌ Fatal:", err);
  process.exit(1);
});
