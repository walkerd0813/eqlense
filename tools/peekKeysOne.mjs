import fs from "node:fs";
import readline from "node:readline";

const file = process.argv[2];
let i = 0;

const rl = readline.createInterface({
  input: fs.createReadStream(file, { encoding: "utf8" }),
  crlfDelay: Infinity
});

for await (const line of rl) {
  if (!line.trim()) continue;
  const o = JSON.parse(line);
  console.log("=== RECORD", (i+1), "===");
  console.log("property_id:", o.property_id);
  console.log("city:", o.city ?? o.source_city ?? o.town ?? null);
  console.log("zip:", o.zip ?? null);
  console.log("Top-level keys:", Object.keys(o).sort().join(", "));
  console.log("parcel_id_norm:", o.parcel_id_norm ?? null);
  console.log("parcel_id_raw:", o.parcel_id_raw ?? null);
  process.exit(0);
}
