import fs from "fs";
import readline from "readline";

const inPath = process.argv[2];
const outPath = process.argv[3];

if (!inPath || !outPath) {
  console.log("Usage: node normalizeZip5_v1_DROPIN.js <in.ndjson> <out.ndjson>");
  process.exit(1);
}

const normZip = (z) => {
  if (z == null) return null;
  const digits = String(z).replace(/\D/g, "");
  if (digits.length < 5) return null;
  return digits.slice(0, 5).padStart(5, "0");
};

(async () => {
  const rl = readline.createInterface({ input: fs.createReadStream(inPath, "utf8"), crlfDelay: Infinity });
  const ws = fs.createWriteStream(outPath, "utf8");

  let total = 0, fixed = 0;
  for await (const line of rl) {
    const t = line.trim(); if (!t) continue;
    total++;
    let row; try { row = JSON.parse(t); } catch { continue; }

    const before = row.zip ?? row.ZIP ?? row.zip_code ?? row.zipCode ?? null;
    const after = normZip(before);

    if (after && after !== before) {
      row.zip = after;
      fixed++;
    }
    ws.write(JSON.stringify(row) + "\n");
  }

  ws.end();
  console.log("[done]", { total, fixed });
})();
