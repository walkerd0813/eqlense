// backend/scripts/findEarliestDates.js
// Run with: node scripts/findEarliestDates.js

import fs from "fs";
import readline from "readline";

const FILE_PATH = "mls/normalized/listingsWithCoords_FINAL.ndjson";

let earliestList = null;
let latestList = null;

let earliestSold = null;
let latestSold = null;

let count = 0;

function parseDate(value) {
  if (!value) return null;
  const d = new Date(value);
  return isNaN(d.getTime()) ? null : d;
}

async function scanFile() {
  console.log("📘 Scanning NDJSON for earliest & latest dates:", FILE_PATH);

  const stream = fs.createReadStream(FILE_PATH, { encoding: "utf8" });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
    count++;
    if (!line.trim()) continue;

    let obj;
    try {
      obj = JSON.parse(line);
    } catch (e) {
      console.warn("⚠️ Bad JSON, skipping:", line.slice(0, 100));
      continue;
    }

    const listDate = parseDate(obj.listDate);
    const soldDate = parseDate(obj.saleDate || obj.soldDate);

    // EARLIEST LIST
    if (listDate && (!earliestList || listDate < earliestList)) {
      earliestList = listDate;
    }
    // LATEST LIST
    if (listDate && (!latestList || listDate > latestList)) {
      latestList = listDate;
    }

    // EARLIEST SOLD
    if (soldDate && (!earliestSold || soldDate < earliestSold)) {
      earliestSold = soldDate;
    }
    // LATEST SOLD
    if (soldDate && (!latestSold || soldDate > latestSold)) {
      latestSold = soldDate;
    }
  }

  console.log("\n===== RESULTS =====");
  console.log(`Total listings scanned: ${count.toLocaleString()}`);

  console.log(
    `\n📅 Earliest LIST date: ${
      earliestList ? earliestList.toISOString().split("T")[0] : "None"
    }`
  );
  console.log(
    `📅 Latest LIST date: ${
      latestList ? latestList.toISOString().split("T")[0] : "None"
    }`
  );

  console.log(
    `\n📅 Earliest SOLD date: ${
      earliestSold ? earliestSold.toISOString().split("T")[0] : "None"
    }`
  );
  console.log(
    `📅 Latest SOLD date: ${
      latestSold ? latestSold.toISOString().split("T")[0] : "None"
    }`
  );

  console.log("===================\n");
}

scanFile();