// Run: node avm/selection/debugComps.js

const fs = require("fs");
const path = require("path");

const files = [
  "singleFamily.json",
  "multiFamily.json",
  "condos.json",
];

for (const f of files) {
  const p = path.join(__dirname, "comps", f);

  console.log("\n===============================");
  console.log("Checking:", p);

  if (!fs.existsSync(p)) {
    console.log("❌ File does NOT exist");
    continue;
  }

  try {
    const raw = fs.readFileSync(p, "utf8");
    const json = JSON.parse(raw);

    console.log("Type:", Array.isArray(json) ? "Array ✔" : typeof json);
    console.log("Length:", Array.isArray(json) ? json.length : "N/A");

    if (!Array.isArray(json)) {
      console.log("❌ Not an array — this will break CompSelector");
    }
  } catch (err) {
    console.log("❌ JSON FAILED TO PARSE");
    console.log(err.message);
  }
}
