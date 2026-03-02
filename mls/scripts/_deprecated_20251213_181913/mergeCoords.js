// backend/mls/scripts/mergeCoords.js
// -------------------------------------------------------
// Merge all coord-attached listings into a single FINAL file.
//
// Inputs:
//   mls/normalized/listingsWithCoords_FAST.ndjson
//   mls/normalized/listingsWithCoords_PASS2.ndjson
//
// Output:
//   mls/normalized/listingsWithCoords_FINAL.ndjson
//
// Behaviour:
//   - Simply concatenates the NDJSON streams
//   - Assumes each file has unique MLS numbers (no dedupe here)
// -------------------------------------------------------

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..", "..");

const NORMALIZED_DIR = path.join(ROOT, "mls", "normalized");

const FAST_PATH = path.join(NORMALIZED_DIR, "listingsWithCoords_FAST.ndjson");
const PASS2_PATH = path.join(
  NORMALIZED_DIR,
  "listingsWithCoords_PASS2.ndjson"
);
const FINAL_PATH = path.join(
  NORMALIZED_DIR,
  "listingsWithCoords_FINAL.ndjson"
);

async function appendFile(srcPath, writer, label) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(srcPath)) {
      console.log(`⚠️  ${label}: file not found, skipping → ${srcPath}`);
      return resolve({ label, count: 0 });
    }

    let count = 0;
    const stream = fs.createReadStream(srcPath, { encoding: "utf8" });
    let leftover = "";

    stream.on("data", (chunk) => {
      const parts = (leftover + chunk).split("\n");
      leftover = parts.pop() || "";
      for (const line of parts) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        writer.write(trimmed + "\n");
        count++;
      }
    });

    stream.on("end", () => {
      if (leftover.trim()) {
        writer.write(leftover.trim() + "\n");
        count++;
      }
      console.log(
        `  ${label}: appended ${count.toLocaleString()} records from ${srcPath}`
      );
      resolve({ label, count });
    });

    stream.on("error", (err) => reject(err));
  });
}

async function main() {
  console.log("====================================================");
  console.log("          MERGING COORDINATE RESULTS");
  console.log("====================================================");
  console.log(" FAST  path:", FAST_PATH);
  console.log(" PASS2 path:", PASS2_PATH);
  console.log(" FINAL path:", FINAL_PATH);
  console.log("----------------------------------------------------");

  if (!fs.existsSync(NORMALIZED_DIR)) {
    console.log("⚠️  Normalized directory not found. Nothing to merge.");
    console.log("====================================================");
    return;
  }

  // Ensure directory for FINAL exists
  fs.mkdirSync(path.dirname(FINAL_PATH), { recursive: true });

  // Overwrite any previous FINAL
  if (fs.existsSync(FINAL_PATH)) {
    fs.unlinkSync(FINAL_PATH);
  }

  const writer = fs.createWriteStream(FINAL_PATH, { encoding: "utf8" });

  const results = [];
  results.push(await appendFile(FAST_PATH, writer, "FAST"));
  results.push(await appendFile(PASS2_PATH, writer, "PASS2"));

  writer.end();

  const total = results.reduce((sum, r) => sum + r.count, 0);

  console.log("----------------------------------------------------");
  console.log("MERGE SUMMARY");
  console.log(
    `  FAST records:   ${results[0].count.toLocaleString()} (Tier1–3)`
  );
  console.log(
    `  PASS2 records:  ${results[1].count.toLocaleString()} (external)`
  );
  console.log(`  TOTAL merged:   ${total.toLocaleString()}`);
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ Fatal error in mergeCoords:", err);
  process.exit(1);
});
