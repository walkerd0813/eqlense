import fs from "node:fs";
import readline from "node:readline";

const file = process.argv[2];
const parcel = process.argv[3];

if (!file || !parcel) {
  console.error("usage: node tools/peekParcel.mjs <file.ndjson> <parcel_id_norm>");
  process.exit(2);
}

const rl = readline.createInterface({ input: fs.createReadStream(file, { encoding: "utf8" }), crlfDelay: Infinity });

for await (const line of rl) {
  if (line.includes(`"parcel_id_norm":"${parcel}"`)) {
    console.log(line.slice(0, 4000));
    process.exit(0);
  }
}
console.log("[miss] parcel not found:", parcel);
process.exit(1);
