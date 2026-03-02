import dotenvx from "@dotenvx/dotenvx";
dotenvx.config();

import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const RAW_REF = path.join(__dirname, "..", "raw", "reference");
const REF_OUT = path.join(__dirname, "..", "reference");
const NORMALIZED = path.join(__dirname, "..", "normalized", "reference.ndjson");

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

export async function ingestReferenceTables() {
  console.log("📘 Ingesting Reference Tables…");

  await ensureDir(REF_OUT);

  const files = await fs.readdir(RAW_REF);
  const outRecords = [];

  for (const file of files) {
    const filePath = path.join(RAW_REF, file);
    const ext = path.extname(file).toLowerCase();

    if (![".txt", ".csv", ".json", ".jsonl"].includes(ext)) continue;

    const raw = await fs.readFile(filePath, "utf8");

    let records = [];
    if (ext === ".json" || ext === ".jsonl") {
      const lines = raw.split(/\r?\n/).filter(Boolean);
      records = lines.map((l) => JSON.parse(l));
    } else {
      const lines = raw.split(/\r?\n/).filter(Boolean);
      const header = lines[0].split("|").map((h) => h.trim());
      records = lines.slice(1).map((line) => {
        const cols = line.split("|");
        const rec = {};
        header.forEach((h, i) => (rec[h] = cols[i] || ""));
        return rec;
      });
    }

    // Write grouped json for this ref table
    const jsonOutPath = path.join(REF_OUT, file.replace(ext, ".json"));
    await fs.writeFile(jsonOutPath, JSON.stringify(records, null, 2));

    // Add to master ndjson
    for (const r of records) outRecords.push(r);
  }

  // Write to NDJSON
  await fs.writeFile(
    NORMALIZED,
    outRecords.map((r) => JSON.stringify(r)).join("\n") + "\n"
  );

  console.log("✅ Reference tables ingested.");
}

if (import.meta.url === `file://${process.argv[1]}`) {
  ingestReferenceTables();
}
