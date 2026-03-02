import fs from "node:fs";
import readline from "node:readline";

const INPUTS = [
  "publicData/parcels/parcelCentroids_EAST_xy.csv",
  "publicData/parcels/parcelCentroids_WEST_xy.csv"
];

const OUTPUT = "publicData/parcels/parcelGridIndex.json";

// ~1km grid
const GRID = 0.01;

const grid = {};

async function processCSV(file) {
  if (!fs.existsSync(file)) {
    console.error("❌ Missing:", file);
    process.exit(1);
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(file),
    crlfDelay: Infinity
  });

  let isHeader = true;

  for await (const line of rl) {
    if (isHeader) {
      isHeader = false; // skip header row: X,Y
      continue;
    }

    const [x, y] = line.split(",");
    if (!x || !y) continue;

    const lng = parseFloat(x);
    const lat = parseFloat(y);

    if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;

    const key = `${Math.floor(lat / GRID)},${Math.floor(lng / GRID)}`;
    grid[key] = (grid[key] || 0) + 1;
  }
}

(async () => {
  for (const file of INPUTS) {
    await processCSV(file);
  }

  fs.writeFileSync(OUTPUT, JSON.stringify(grid));
  console.log(`Grid index created with ${Object.keys(grid).length} cells`);
})();
