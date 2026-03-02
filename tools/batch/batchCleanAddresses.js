/**
 * batchCleanAddresses.js
 * Normalizes all comp addresses so geocoding becomes much more accurate.
 */

const fs = require("fs");
const path = require("path");
const { sanitizeAddress } = require("../../publicData/geocoding/sanitizeAddress");

const INPUT_DIR = path.join(__dirname, "../../comps");
const OUTPUT_DIR = path.join(__dirname, "../../comps_cleaned");

if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR);

const FILES = ["singleFamily.json", "multiFamily.json", "condos.json"];

async function processFile(file) {
  console.log(`📘 Cleaning addresses in ${file}…`);

  const inPath = path.join(INPUT_DIR, file);
  const outPath = path.join(OUTPUT_DIR, file);

  const raw = fs.readFileSync(inPath, "utf8");
  const comps = JSON.parse(raw);

  const cleaned = comps.map((c) => {
    try {
      const normalized = sanitizeAddress(`${c.address}, ${c.town} ${c.zip}`);
      return { ...c, normalizedAddress: normalized };
    } catch (err) {
      console.log("❌ Failed normalizing:", c.address);
      return { ...c, normalizedAddress: c.address };
    }
  });

  fs.writeFileSync(outPath, JSON.stringify(cleaned, null, 2));

  console.log(`✓ Saved cleaned file → ${outPath}`);
}

(async () => {
  console.log("🚀 Starting address cleaning…");
  for (const file of FILES) {
    await processFile(file);
  }
  console.log("✅ Address cleaning complete.");
})();

