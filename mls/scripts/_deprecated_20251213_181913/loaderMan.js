#!/usr/bin/env node
import { execSync } from "child_process";
import path from "path";
import fs from "fs";

const ROOT = path.resolve("C:/seller-app/backend");
const MLS = path.join(ROOT, "mls");
const SCRIPTS = path.join(MLS, "scripts");

function run(command) {
    console.log(`\n====================================================`);
    console.log(`>>> RUNNING STEP: ${command}`);
    console.log(`----------------------------------------------------`);
    try {
        execSync(command, { stdio: "inherit" });
    } catch (err) {
        console.error("LoaderMan failed:", err);
        process.exit(1);
    }
}

console.log("====================================================");
console.log("                 LOADERMAN PIPELINE");
console.log("====================================================");

// 1) Route IDX + CSV files into RAW folders
run(`node ${SCRIPTS}/routeFiles.js`);
run(`node ${SCRIPTS}/routeCsvEvents.js`);

// 2) Ingest the IDX TXT files
run(`node ${SCRIPTS}/ingestIDX.js`);

// 3) Ingest CSV events (BOM/CAN/PCG/EXP/UA/WND)
run(`node ${SCRIPTS}/ingestCsvEvents.js`);

// 4) Standardize & produce normalized outputs
run(`node ${SCRIPTS}/standardizeListings.js`);

// 5) Verify listings
run(`node ${SCRIPTS}/verifyListings.js`);

// 6) FAST coordinate attach (parcel direct + prefix + address point)
run(`node ${SCRIPTS}/attachCoordinatesFAST.js`);

// 7) Fuzzy Pass 1 (currently NO-OP but produces PASS1 files)
run(`node ${SCRIPTS}/FuzzyPass1.js`);

// 8) Fuzzy Pass 2 (Tier 4 + external geocode)
run(`node ${SCRIPTS}/runPass2.js`);

// 9) Merge FAST + PASS2 into mergedCoords.ndjson
run(`node ${SCRIPTS}/mergeCoords.js`);

// 10) Attach zoning (polygon lookup using zoningLookup.js)
run(`node ${SCRIPTS}/attachZoningToNormalizedListings.js`);

// 11) Attach civic overlays (MBTA, Fire, Trash, Snow, Flood, OpenSpace, Boundaries)
run(`node ${SCRIPTS}/attachCivicLayers.js`);

console.log("====================================================");
console.log("               LOADERMAN COMPLETE!");
console.log("====================================================");
