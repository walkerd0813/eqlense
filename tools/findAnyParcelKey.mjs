import fs from "node:fs";
import readline from "node:readline";

const file = process.argv[2];
let n=0;

const rl = readline.createInterface({
  input: fs.createReadStream(file, { encoding: "utf8" }),
  crlfDelay: Infinity
});

for await (const line of rl) {
  if (!line.trim()) continue;
  n++;
  const o = JSON.parse(line);

  // try a few likely places too
  const p =
    o.parcel_id_norm ??
    o.parcel_id ??
    o.parcel_id_norm_v2 ??
    (o.ids && (o.ids.parcel_id_norm || o.ids.parcel_id)) ??
    (o.identity && (o.identity.parcel_id_norm || o.identity.parcel_id)) ??
    null;

  if (p) {
    console.log("[HIT] line", n);
    console.log(JSON.stringify({
      property_id: o.property_id ?? null,
      candidate_parcel_key: p,
      parcel_id_norm: o.parcel_id_norm ?? null,
      parcel_id_raw: o.parcel_id_raw ?? null,
      city: o.city ?? o.source_city ?? o.town ?? null,
      zip: o.zip ?? null
    }, null, 2));
    process.exit(0);
  }

  if (n % 500000 === 0) console.log("[progress] scanned", n, "no parcel key yet");
}

console.log("[done] scanned", n, "no parcel key found anywhere in tested paths");
process.exit(1);
