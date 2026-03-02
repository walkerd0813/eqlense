const path = require("path");

function load(file) {
  const full = path.join(__dirname, "comps", file);
  console.log("\n🔍 Loading:", full);
  const data = require(full);
  console.log("➡ TYPE:", Array.isArray(data) ? "ARRAY" : typeof data);
  console.log("➡ LENGTH:", Array.isArray(data) ? data.length : "N/A");
  console.log("------------------------------");
}

load("singleFamily.json");
load("multiFamily.json");
load("condos.json");
