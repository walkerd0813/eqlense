// backend/mls/scripts/runPass2.js
// -------------------------------------------------------
// PASS 2 — external geocoder on FAST-unmatched listings
//
// Input:
//   mls/normalized/unmatched_FAST.ndjson
//
// Output:
//   mls/normalized/listingsWithCoords_PASS2.ndjson  (matched by external geocode)
//   mls/normalized/unmatched_PASS2.ndjson          (still no coords)
//
// This uses your existing externalGeocode.js script.
// -------------------------------------------------------

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { execSync } from "child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..", "..");

// Paths
const NORMALIZED_DIR = path.join(ROOT, "mls", "normalized");

const INPUT_UNMATCHED_FAST = path.join(
  NORMALIZED_DIR,
  "unmatched_FAST.ndjson"
);

const OUTPUT_PASS2_MATCHED = path.join(
  NORMALIZED_DIR,
  "listingsWithCoords_PASS2.ndjson"
);

const OUTPUT_PASS2_UNMATCHED = path.join(
  NORMALIZED_DIR,
  "unmatched_PASS2.ndjson"
);

// External geocoder engine (your script)
const EXTERNAL_ENGINE = path.join(
  ROOT,
  "mls",
  "scripts",
  "externalGeocode.js"
);

// Small helper to count NDJSON rows (for logging only)
async function countLines(filePath) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(filePath)) {
      return resolve(0);
    }
    let count = 0;
    const stream = fs.createReadStream(filePath, { encoding: "utf8" });
    let leftover = "";

    stream.on("data", (chunk) => {
      const lines = (leftover + chunk).split("\n");
      leftover = lines.pop() || "";
      for (const line of lines) {
        if (line.trim().length > 0) count++;
      }
    });

    stream.on("end", () => {
      if (leftover.trim().length > 0) count++;
      resolve(count);
    });

    stream.on("error", (err) => reject(err));
  });
}

async function main() {
  console.log("====================================================");
  console.log("               PASS 2 — FULL ENGINE");
  console.log("====================================================");
  console.log(" Input (FAST unmatched):", INPUT_UNMATCHED_FAST);
  console.log(" Output (PASS2 matched):", OUTPUT_PASS2_MATCHED);
  console.log(" Output (PASS2 unmatched):", OUTPUT_PASS2_UNMATCHED);
  console.log(" Engine (external):      ", EXTERNAL_ENGINE);
  console.log("----------------------------------------------------");

  if (!fs.existsSync(INPUT_UNMATCHED_FAST)) {
    console.log(
      "⚠️  No unmatched_FAST.ndjson found. Nothing to do for PASS 2."
    );
    console.log("====================================================");
    return;
  }

  // Ensure output directory exists
  if (!fs.existsSync(NORMALIZED_DIR)) {
    fs.mkdirSync(NORMALIZED_DIR, { recursive: true });
  }

  // Build command: node externalGeocode.js INPUT MATCHED OUT UNMATCHED OUT
  const cmd = [
    "node",
    EXTERNAL_ENGINE,
    INPUT_UNMATCHED_FAST,
    OUTPUT_PASS2_MATCHED,
    OUTPUT_PASS2_UNMATCHED,
  ].join(" ");

  console.log(`Running external geocoder:\n  ${cmd}`);
  console.log("----------------------------------------------------");

  try {
    execSync(cmd, { stdio: "inherit" });
  } catch (err) {
    console.error("❌ PASS 2 FAILED — external geocoder error");
    console.error(err);
    console.log("====================================================");
    process.exit(1);
  }

  // Log simple stats
  const [matchedCount, unmatchedCount] = await Promise.all([
    countLines(OUTPUT_PASS2_MATCHED),
    countLines(OUTPUT_PASS2_UNMATCHED),
  ]);

  console.log("PASS 2 SUMMARY");
  console.log("----------------------------------------------------");
  console.log(
    `PASS2 matched (external geocode):  ${matchedCount.toLocaleString()}`
  );
  console.log(
    `PASS2 unmatched (even after ext): ${unmatchedCount.toLocaleString()}`
  );
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ Fatal error in PASS 2:", err);
  process.exit(1);
});
