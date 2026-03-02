
// backend/zoningTest.js

const { lookupZoning } = require("./publicData/zoning/zoningLookup");

console.log("\n--- Running zoning test ---");

const lat = 42.3601;
const lon = -71.0589;

const result = lookupZoning(lat, lon);

console.log("\nRESULT:\n", result);
console.log("\n--- End test ---\n");
"@ | Set-Content zoningTest.js -Encoding UTF8 @"
