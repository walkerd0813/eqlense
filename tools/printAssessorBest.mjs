import fs from "node:fs";
import readline from "node:readline";

const file = process.argv[2];
const propId = process.argv[3];

if (!file || !propId) {
  console.log("usage: node tools/printAssessorBest.mjs <file.ndjson> <property_id>");
  process.exit(2);
}

const rl = readline.createInterface({
  input: fs.createReadStream(file, { encoding: "utf8" }),
  crlfDelay: Infinity
});

rl.on("line", (l) => {
  if (!l.includes(`"property_id":"${propId}"`)) return;
  const o = JSON.parse(l);
  console.log(JSON.stringify({ property_id: o.property_id, assessor_best: o.assessor_best }, null, 2));
  process.exit(0);
});

rl.on("close", () => {
  console.log("[miss] not found:", propId);
  process.exit(1);
});
