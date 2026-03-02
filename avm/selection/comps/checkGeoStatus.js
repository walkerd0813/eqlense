/**
 * checkGeoStatus.js
 * Scans all geocoded comp datasets and reports:
 * - How many have lat/lng
 * - How many are missing lat/lng
 * - Exports clean lists if needed
 */

const fs = require("fs");
const path = require("path");

// -------------------------------
// INPUT — directory containing geocoded files
// -------------------------------
const GEO_DIR = path.join(__dirname, "..", "comps_geocoded");

// -------------------------------
// OUTPUT — summary + optional cleaned files
// -------------------------------
const REPORT_FILE = path.join(__dirname, "geo_report.json");
const CLEAN_DIR = path.join(__dirname, "comps_geo_clean");

if (!fs.existsSync(CLEAN_DIR)) {
  fs.mkdirSync(CLEAN_DIR);
}

function hasLatLng(obj) {
  return (
    obj &&
    typeof obj.lat === "number" &&
    typeof obj.lng === "number" &&
    !isNaN(obj.lat) &&
    !isNaN(obj.lng)
  );
}

(async () => {
  console.log("\n🔍 Checking geocoded comps...\n");

  const summary = {};

  const files = fs.readdirSync(GEO_DIR).filter((f) => f.endsWith(".json"));

  for (const file of files) {
    const fullPath = path.join(GEO_DIR, file);
    const raw = fs.readFileSync(fullPath, "utf8");
    const comps = JSON.parse(raw);

    let ok = 0;
    let bad = 0;

    const cleanList = [];

    for (const comp of comps) {
      if (hasLatLng(comp)) {
        ok++;
        cleanList.push(comp);
      } else {
        bad++;
      }
    }

    summary[file] = {
      total: comps.length,
      withLatLng: ok,
      missingLatLng: bad,
      percentGood: ((ok / comps.length) * 100).toFixed(2) + "%",
    };

    // Save cleaned version (only comps with lat/lng)
    const outPath = path.join(CLEAN_DIR, file);
    fs.writeFileSync(outPath, JSON.stringify(cleanList, null, 2));

    console.log(
      `📄 ${file}: ${ok} valid, ${bad} missing → Clean saved → ${outPath}`
    );
  }

  // Save master summary
  fs.writeFileSync(REPORT_FILE, JSON.stringify(summary, null, 2));

  console.log("\n✅ Completed scan. Report saved at:");
  console.log(REPORT_FILE);
})();
