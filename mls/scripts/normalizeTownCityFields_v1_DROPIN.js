// normalizeTownCityFields_v1_DROPIN.js (ESM)
// Copies `town` into `city` + `city_town` if those keys are missing/empty.

import fs from "fs";
import readline from "readline";

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : null;
}

const IN = arg("--in");
const OUT = arg("--out");
const META = arg("--meta");

if (!IN || !OUT) {
  console.log("Usage: node normalizeTownCityFields_v1_DROPIN.js --in <in.ndjson> --out <out.ndjson> --meta <meta.json>");
  process.exit(1);
}

const rl = readline.createInterface({ input: fs.createReadStream(IN, "utf8"), crlfDelay: Infinity });
const out = fs.createWriteStream(OUT, { encoding: "utf8" });

let total = 0;
let setCity = 0;
let setCityTown = 0;
let townMissing = 0;

for await (const line of rl) {
  if (!line.trim()) continue;
  total++;

  const o = JSON.parse(line);
  const town = (o.town ?? "").toString().trim();

  if (!town) townMissing++;

  const city = (o.city ?? "").toString().trim();
  const cityTown = (o.city_town ?? "").toString().trim();

  if (!city && town) {
    o.city = town;
    setCity++;
  }
  if (!cityTown && town) {
    o.city_town = town;
    setCityTown++;
  }

  out.write(JSON.stringify(o) + "\n");
}

out.end();

const meta = {
  created_at: new Date().toISOString(),
  in: IN,
  out: OUT,
  counts: { total, setCity, setCityTown, townMissing },
};
if (META) fs.writeFileSync(META, JSON.stringify(meta, null, 2), "utf8");
console.log("[done]", meta);
