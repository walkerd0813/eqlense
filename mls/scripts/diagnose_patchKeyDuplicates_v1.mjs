import fs from "fs";
import readline from "readline";

function pickId(o){ return o?.property_id || o?.parcel_id || null; }

const basePath = process.argv[2];
const outPath  = process.argv[3] || "patchKeyDuplicateReport.json";
if(!basePath){ console.error("Usage: node diagnose_patchKeyDuplicates_v1.mjs <base.ndjson> [out.json]"); process.exit(1); }

const counts = new Map();
let total = 0;
let dupRows = 0;

const rl = readline.createInterface({ input: fs.createReadStream(basePath, { encoding:"utf8" }), crlfDelay: Infinity });
for await (const line of rl){
  if(!line) continue;
  total++;
  const o = JSON.parse(line);
  const id = pickId(o);
  if(!id) continue;
  const n = (counts.get(id) || 0) + 1;
  counts.set(id, n);
  if(n === 2) dupRows++;
  if(total % 500000 === 0) console.log(`...scanned ${total.toLocaleString()} rows`);
}

let maxDup = 1;
for(const n of counts.values()) if(n > maxDup) maxDup = n;

const report = { total_rows: total, unique_ids: counts.size, ids_with_duplicates: dupRows, max_dupe_count: maxDup };
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log("DONE:", report);
