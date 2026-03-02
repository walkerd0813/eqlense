import fs from "node:fs";
import readline from "node:readline";

const file = process.argv[2];
const needle = process.argv[3];
if (!file || !needle) {
  console.log("usage: node tools/peekPropertyId.mjs <file.ndjson> <property_id>");
  process.exit(2);
}

const rl = readline.createInterface({
  input: fs.createReadStream(file, { encoding: "utf8" }),
  crlfDelay: Infinity
});

let n = 0;
rl.on("line", (l) => {
  n++;
  if (l.includes(`"property_id":"${needle}"`)) {
    console.log(l.slice(0, 8000));
    process.exit(0);
  }
  if (n % 500000 === 0) console.log("[progress] scanned", n);
});

rl.on("close", () => {
  console.log("[miss] not found:", needle);
  process.exit(1);
});
