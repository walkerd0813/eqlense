// backend/zoningDebugger.js

const { lookupZoning } = require("./publicData/zoning/zoningLookup");

async function run() {
  const args = process.argv.slice(2);

  if (args.length < 2) {
    console.log("Usage: node zoningDebugger.js <lat> <lon>");
    process.exit(1);
  }

  const lat = parseFloat(args[0]);
  const lon = parseFloat(args[1]);

  console.log("\n🔍 Running zoning debugger for:", { lat, lon });
  const result = lookupZoning(lat, lon, { debug: true });

  console.log("\n===== FINAL ZONING RESULT =====");
  console.log(result);
  console.log("===============================\n");
}

run();
