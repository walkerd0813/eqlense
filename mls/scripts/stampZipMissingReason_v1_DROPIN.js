import fs from "fs";
import readline from "readline";

const inPath = process.argv[2];
const outPath = process.argv[3];

if (!inPath || !outPath) {
  console.log("Usage: node stampZipMissingReason_v1_DROPIN.js <in.ndjson> <out.ndjson>");
  process.exit(1);
}

const zip5 = (v) => {
  if (v == null) return null;
  const d = String(v).replace(/\D/g, "");
  if (d.length < 5) return null;
  return d.slice(0, 5).padStart(5, "0");
};

(async () => {
  const rl = readline.createInterface({
    input: fs.createReadStream(inPath, "utf8"),
    crlfDelay: Infinity,
  });
  const ws = fs.createWriteStream(outPath, "utf8");

  let total = 0, stamped = 0;

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    total++;

    let row;
    try { row = JSON.parse(t); } catch { continue; }

    const z = zip5(row.zip ?? row.ZIP ?? row.zip_code ?? row.zipCode ?? row.POSTCODE ?? row.ZIP5);
    if (!z) {
      row.zip = null;
      row.zip_missing_reason = row.zip_missing_reason ?? "NO_ZIP_POLYGON_MATCH";
      row.zip_attempted_sources = row.zip_attempted_sources ?? [
        "ZIPCODES_NT_POLY.geojson:POSTCODE",
        "ZIP_Codes.geojson:ZIP5"
      ];
      row.zip_missing_stamped_at = new Date().toISOString();
      stamped++;
    }

    ws.write(JSON.stringify(row) + "\n");
  }

  ws.end();
  console.log("[done]", { total, stamped });
})();
