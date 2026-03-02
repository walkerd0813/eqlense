import fs from "node:fs";
import readline from "node:readline";
import path from "node:path";

function parseArgs(argv){
  const o = {};
  for(let i=2;i<argv.length;i++){
    const a = argv[i];
    if(a.startsWith("--")){
      const k = a.slice(2);
      const v = (argv[i+1] && !argv[i+1].startsWith("--")) ? argv[++i] : "true";
      o[k]=v;
    }
  }
  return o;
}

function norm(x){ return (x===undefined||x===null) ? "" : String(x).trim(); }

function pick(obj, cands){
  for(const c of cands){
    if(!c) continue;
    const v = obj && Object.prototype.hasOwnProperty.call(obj,c) ? obj[c] : undefined;
    if(v!==undefined && v!==null && v!=="") return v;
  }
  return undefined;
}

function severityFromKeys(keys){
  let hasHigh=false, hasMed=false, hasLow=false;
  for(const k of keys){
    const s = k.toLowerCase();
    if(s.includes("historic_redlining")) { hasLow=true; continue; }
    if(s.includes("preservation_restriction") || s.includes("historic_district") || s.includes("historic_preservation_overlay") || s.includes("protection_areas")){
      hasHigh=true; continue;
    }
    if(s.includes("neighborhood_conservation")) { hasMed=true; continue; }
    if(s.includes("historic_property_status") || s.includes("historic_property_significance")) { hasLow=true; continue; }
    hasLow=true;
  }
  return hasHigh ? "high" : (hasMed ? "medium" : (hasLow ? "low" : "none"));
}

const args = parseArgs(process.argv);
const inPath = args.in;
const attachmentsPath = args.attachments;
const outPath = args.out;
const statsPath = args.stats;
const asOfDate = args.asOfDate || "UNKNOWN";
const inputHash = args.inputHash || "UNKNOWN";

if(!inPath || !attachmentsPath || !outPath){
  console.error("Usage: node build_phase1b_legal_summary_v1.mjs --in <contract_view_phase1a_env.ndjson> --attachments <PHASE1B__attachments.ndjson> --out <out.ndjson> --stats <stats.json> --asOfDate <YYYY-MM-DD> --inputHash <sha256>");
  process.exit(2);
}

fs.mkdirSync(path.dirname(outPath), { recursive: true });

const map = new Map();
let attRead = 0;
const rlAtt = readline.createInterface({ input: fs.createReadStream(attachmentsPath,{encoding:"utf8"}), crlfDelay: Infinity });
for await (const line of rlAtt){
  const t = line.trim(); if(!t) continue;
  attRead++;
  let row; try { row = JSON.parse(t); } catch { continue; }
  const pid = pick(row, ["property_id","propertyId"]);
  const key = pick(row, ["overlay_key","layer_key","layerKey","feature_layer_key","featureLayerKey"]);
  if(!pid || !key) continue;
  let s = map.get(pid);
  if(!s){ s = new Set(); map.set(pid,s); }
  s.add(String(key));
}
console.error(`[info] Phase1B attachments loaded: rows=${attRead} unique_properties=${map.size}`);

const rl = readline.createInterface({ input: fs.createReadStream(inPath,{encoding:"utf8"}), crlfDelay: Infinity });
const ws = fs.createWriteStream(outPath,{encoding:"utf8"});

let read=0, wrote=0, skipped=0;
let any=0;
const sevCounts = { none:0, low:0, medium:0, high:0 };

for await (const line of rl){
  const t = line.trim(); if(!t) continue;
  read++;
  let row; try { row = JSON.parse(t); } catch { skipped++; continue; }

  const pid = pick(row, ["property_id","propertyId","id"]);
  const keysSet = pid ? map.get(pid) : null;
  const keys = keysSet ? Array.from(keysSet).sort() : [];
  const count = keys.length;

  const sev = severityFromKeys(keys);
  if(count>0){ any++; }
  sevCounts[sev] = (sevCounts[sev] || 0) + 1;

  row.has_local_legal_constraint = count > 0;
  row.local_legal_count = count;
  row.local_legal_severity = sev;
  row.local_legal_keys = keys;

  row.phase1b_as_of_date = norm(asOfDate);
  row.phase1b_input_contract_hash = norm(inputHash);

  ws.write(JSON.stringify(row) + "\n");
  wrote++;
  if(read % 200000 === 0) console.error(`[prog] read=${read} wrote=${wrote} skipped=${skipped} any_local_legal=${any}`);
}

ws.end();

const stats = {
  created_at: new Date().toISOString(),
  as_of_date: asOfDate,
  input_contract: inPath,
  input_contract_sha256: inputHash,
  attachments: attachmentsPath,
  attachments_rows: attRead,
  attachments_unique_properties: map.size,
  rows_read: read,
  rows_written: wrote,
  rows_skipped: skipped,
  any_local_legal: any,
  severity_counts: sevCounts
};

if(statsPath){
  fs.writeFileSync(statsPath, JSON.stringify(stats,null,2), "utf8");
}

console.error(`[done] read=${read} wrote=${wrote} skipped=${skipped} any_local_legal=${any}`);
