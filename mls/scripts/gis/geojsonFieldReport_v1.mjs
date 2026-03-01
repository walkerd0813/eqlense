import fs from "node:fs";
import path from "node:path";

function getArg(name, fallback=null){
  const k = `--${name}`;
  const i = process.argv.indexOf(k);
  return (i >= 0 && process.argv[i+1]) ? process.argv[i+1] : fallback;
}

const inFile  = getArg("in");
const outFile = getArg("out");

if(!inFile || !outFile){
  console.error("Usage: node geojsonFieldReport_v1.mjs --in <geojson> --out <report.json> [--sample 5000]");
  process.exit(1);
}

const sampleN = Number(getArg("sample", "0")) || 0;

function normType(v){
  if(v === null || v === undefined) return "null";
  if(Array.isArray(v)) return "array";
  return typeof v; // string, number, boolean, object
}

function addExample(arr, v, max=6){
  if(arr.length >= max) return;
  const s = (v === null || v === undefined) ? "" : String(v);
  if(!arr.includes(s)) arr.push(s);
}

const raw = fs.readFileSync(inFile, "utf8");
const fc = JSON.parse(raw);
const feats = Array.isArray(fc.features) ? fc.features : [];

const geomTypes = {};
const fields = {}; // key -> {count, types:{}, examples:[]}

let seen = 0;
for(const f of feats){
  seen++;
  if(sampleN > 0 && seen > sampleN) break;

  const g = f && f.geometry ? (f.geometry.type || "null") : "null";
  geomTypes[g] = (geomTypes[g] || 0) + 1;

  const props = (f && f.properties && typeof f.properties === "object") ? f.properties : {};
  for(const k of Object.keys(props)){
    const v = props[k];
    const t = normType(v);
    if(!fields[k]) fields[k] = { count: 0, types: {}, examples: [] };
    fields[k].count++;
    fields[k].types[t] = (fields[k].types[t] || 0) + 1;
    addExample(fields[k].examples, v);
  }
}

const fieldRows = Object.entries(fields)
  .map(([k, r]) => ({ field: k, ...r }))
  .sort((a,b) => (b.count - a.count) || a.field.localeCompare(b.field));

const report = {
  created_at: new Date().toISOString(),
  inFile,
  outFile,
  featureCount_total: feats.length,
  featureCount_scanned: (sampleN > 0 ? Math.min(sampleN, feats.length) : feats.length),
  geometryTypes: geomTypes,
  fields_sorted: fieldRows,
  fields_unique: fieldRows.length
};

fs.mkdirSync(path.dirname(outFile), { recursive: true });
fs.writeFileSync(outFile, JSON.stringify(report, null, 2), "utf8");
console.log(`[done] wrote: ${outFile} (fields=${report.fields_unique}, scanned=${report.featureCount_scanned})`);
