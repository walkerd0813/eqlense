import fs from "fs";
import readline from "readline";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const inputFile = path.resolve(__dirname, "../normalized/listingsWithZoning.ndjson");

console.log("====================================================");
console.log("         ZONING + CIVIC COVERAGE SUMMARY");
console.log("====================================================");
console.log("Input:", inputFile);
console.log("");

async function summarize() {
  const rl = readline.createInterface({
    input: fs.createReadStream(inputFile),
    crlfDelay: Infinity,
  });

  let total = 0;

  let withZoningDistrict = 0;
  let withSubdistrict = 0;

  let inGCOD = 0;
  let inFlood = 0;
  let inMainStreet = 0;

  let withNeighborhood = 0;
  let withWard = 0;
  let withPolice = 0;
  let withFire = 0;
  let withTrash = 0;
  let withSnowParking = 0;
  let inSnowRouteCorridor = 0;
  let onWater = 0;
  let withZip = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;

    const row = JSON.parse(line);
    total++;

    // Correct structure:
    const zoningRoot = row.zoning;
    if (!zoningRoot) continue;

    const zoning = zoningRoot.zoning;    // district, subdistrict, overlays
    const civic = zoningRoot.civic;      // neighborhood, ward, police, etc.

    // ---- ZONING ----
    if (zoning?.district?.raw) withZoningDistrict++;
    if (zoning?.subdistrict?.raw) withSubdistrict++;

    // overlays
    const overlays = zoning?.overlays || {};
    if (overlays.gcod?.present) inGCOD++;
    if (overlays.flood?.present) inFlood++;
    if (overlays.mainStreet?.present) inMainStreet++;

    // ---- CIVIC ----
    if (civic?.neighborhood?.raw) withNeighborhood++;
    if (civic?.ward?.raw) withWard++;
    if (civic?.policeDistrict?.raw) withPolice++;
    if (civic?.fireDistrict?.raw) withFire++;
    if (civic?.trash?.raw) withTrash++;

    if (civic?.snow?.inParkingZone) withSnowParking++;
    if (civic?.snow?.inSnowRouteCorridor) inSnowRouteCorridor++;
    if (civic?.onWater) onWater++;

    if (civic?.zipcode?.raw) withZip++;

    if (total % 10000 === 0) {
      console.log(`Processed ${total}...`);
    }
  }

  console.log("\n================= RESULTS =================");
  console.log("Total listings:", total);
  console.log("");
  console.log("ZONING:");
  console.log("  With zoning district:   ", withZoningDistrict);
  console.log("  With subdistrict:       ", withSubdistrict);
  console.log("");
  console.log("OVERLAYS:");
  console.log("  In GCOD:                ", inGCOD);
  console.log("  In Coastal Flood:       ", inFlood);
  console.log("  In Main Street District:", inMainStreet);
  console.log("");
  console.log("CIVIC BOUNDARIES:");
  console.log("  With neighborhood:      ", withNeighborhood);
  console.log("  With ward:              ", withWard);
  console.log("  With police district:   ", withPolice);
  console.log("  With fire district:     ", withFire);
  console.log("  With trash day:         ", withTrash);
  console.log("  In snow parking zone:   ", withSnowParking);
  console.log("  In snow route corridor: ", inSnowRouteCorridor);
  console.log("  On water:               ", onWater);
  console.log("  With ZIP:               ", withZip);
  console.log("===========================================");
}

summarize().catch((err) => {
  console.error("ERROR in summarizeZoningCoverage:", err);
});
