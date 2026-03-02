import fs from "fs";
import readline from "readline";
import { polygonLookup } from "../../publicData/lookups/polygonLookup.js";
import { mbtaProximity } from "../../publicData/lookups/mbtaProximity.js";
import { openSpaceLookup } from "../../publicData/lookups/openSpaceLookup.js";
import { civicLookup } from "../../publicData/lookups/civicLookup.js";
import { overlayLookup } from "../../publicData/lookups/overlayLookup.js";
import { easementLookup } from "../../publicData/lookups/easementLookup.js";

const INPUT = "C:/seller-app/backend/mls/normalized/listingsWithZoning.ndjson";
const OUTPUT = "C:/seller-app/backend/mls/normalized/listingsWithCivic.ndjson";

console.log("====================================================");
console.log("          CIVIC + MBTA + OVERLAY ENRICHMENT");
console.log("====================================================");
console.log("Input:", INPUT);
console.log("Output:", OUTPUT);
console.log("----------------------------------------------------");

// Load polygon datasets ONCE into memory
await polygonLookup.init();
await mbtaProximity.init();
await openSpaceLookup.init();
await civicLookup.init();
await overlayLookup.init();
await easementLookup.init();

// Stream input → enrich → write out
const rl = readline.createInterface({
  input: fs.createReadStream(INPUT),
  crlfDelay: Infinity
});

const out = fs.createWriteStream(OUTPUT);

let count = 0;

for await (const line of rl) {
  if (!line.trim()) continue;
  let listing = JSON.parse(line);

  const lat = listing.latitude;
  const lon = listing.longitude;

  if (lat && lon) {
    listing.civic = {
      ...civicLookup.get(lat, lon),
      mbta: mbtaProximity.get(lat, lon),
      openSpace: openSpaceLookup.get(lat, lon),
      overlays: overlayLookup.get(lat, lon),
      easements: easementLookup.get(lat, lon),
      boundaries: polygonLookup.get(lat, lon),
    };
  } else {
    listing.civic = null;
  }

  out.write(JSON.stringify(listing) + "\n");
  count++;

  if (count % 5000 === 0) {
    console.log(`   Processed ${count} listings…`);
  }
}

out.end();
console.log("----------------------------------------------------");
console.log("  CIVIC ENRICHMENT COMPLETE");
console.log("  Listings processed:", count);
console.log("----------------------------------------------------");
