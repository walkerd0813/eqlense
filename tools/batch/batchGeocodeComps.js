/**
 * batchGeocodeComps.js
 * Geocodes cleaned comp datasets and saves to comps_geocoded/
 */

const fs = require("fs");
const path = require("path");
const { geocodeAddress } = require("../../publicData/geocoding/geocodeAddress");

const INPUT_DIR = path.join(__dirname, "../../comps_cleaned");
const OUTPUT_DIR = path.join(__dirname, "../../comps_geocoded");

if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR);

const FILES = ["singleFamily.json", "multiFamily.json", "condos.json"];

async function processFile(file) {
  console.log(`📘 Geocoding comps in ${file}…`);

  const inPath = path.join(INPUT_DIR, file);
  const outPath = path.join(OUTPUT_DIR, file);
  const raw = fs.readFileSync(inPath, "utf8");
  const comps = JSON.parse(raw);

  const updated = [];

  for (const c of comps) {
    const addr = c.normalizedAddress || `${c.address}, ${c.town} ${c.zip}`;
    const geo = await geocodeAddress(addr);

    updated.push({
      ...c,
      lat: geo?.lat || null,
      lng: geo?.lng || null,
    });

    await new Promise((r) => setTimeout(r, 1100)); // respect rate limits
  }

  fs.writeFileSync(outPath, JSON.stringify(updated, null, 2));

  console.log(`✓ Saved geocoded comps → ${outPath}`);
}

(async () => {
  console.log("🚀 Starting comp geocoding…");
  for (const file of FILES) {
    await processFile(file);
  }
  console.log("✅ Geocoding complete.");
})();

