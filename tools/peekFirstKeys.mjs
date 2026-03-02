import fs from "node:fs";
import readline from "node:readline";

const file = process.argv[2];
let shown = 0;

if (!file) {
  console.error("usage: node tools/peekFirstKeys.mjs <file.ndjson>");
  process.exit(2);
}

const rl = readline.createInterface({
  input: fs.createReadStream(file, { encoding: "utf8" }),
  crlfDelay: Infinity
});

for await (const line of rl) {
  if (!line.trim()) continue;
  const o = JSON.parse(line);
  console.log(JSON.stringify({
    property_id: o.property_id ?? null,
    parcel_id_norm: o.parcel_id_norm ?? null,
    parcel_id_raw: o.parcel_id_raw ?? null,
    city: o.city ?? o.source_city ?? o.town ?? null,
    zip: o.zip ?? null
  }, null, 2));
  shown++;
  if (shown >= 5) process.exit(0);
}
