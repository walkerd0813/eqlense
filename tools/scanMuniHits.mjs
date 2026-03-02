import fs from "node:fs";
import readline from "node:readline";

const file = process.argv[2];
let n=0, hits=0;

const rl = readline.createInterface({ input: fs.createReadStream(file, { encoding: "utf8" }), crlfDelay: Infinity });

for await (const line of rl) {
  n++;
  if (line.includes('"city_assessor_raw":{')) {
    hits++;
    console.log("[HIT] line", n);
    console.log(line.slice(0, 1200));
    process.exit(0);
  }
  if (n % 500000 === 0) console.log("[progress] scanned", n, "hits", hits);
}
console.log("[done] scanned", n, "hits", hits);
