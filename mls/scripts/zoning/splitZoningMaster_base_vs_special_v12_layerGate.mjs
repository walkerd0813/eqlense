import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

function getArg(name, d=null){
  const k=`--${name}`; const i=process.argv.indexOf(k);
  return i>=0 ? process.argv[i+1] : d;
}

const inNdjson = getArg("inNdjson");
const outBase = getArg("outBase");
const outSpecial = getArg("outSpecial");
const outReport = getArg("outReport");

if(!inNdjson || !outBase || !outSpecial || !outReport){
  console.error("Usage: node splitZoningMaster_base_vs_special_v12_layerGate.mjs --inNdjson <ndjson> --outBase <ndjson> --outSpecial <ndjson> --outReport <json>");
  process.exit(1);
}

// BASE = zoning districts layers ONLY (institutional)
// SPECIAL = subdistricts + overlays + historic + everything else
function isBaseLayer(layer){
  const s = String(layer || "");
  if (!s) return false;
  if (/subdistrict/i.test(s)) return false;
  if (/overlay/i.test(s)) return false;
  if (/historic/i.test(s)) return false;
  // keep only true zoning district layers
  return /zoning[_\s-]*districts$/i.test(s);
}

fs.mkdirSync(path.dirname(outBase), {recursive:true});
fs.mkdirSync(path.dirname(outSpecial), {recursive:true});

const wb = fs.createWriteStream(outBase, {encoding:"utf8"});
const ws = fs.createWriteStream(outSpecial, {encoding:"utf8"});

const cityCounts = new Map();
const layerCounts = new Map();
const baseLayerCounts = new Map();
const specLayerCounts = new Map();
let total=0, baseN=0, specN=0;

const rl = readline.createInterface({ input: fs.createReadStream(inNdjson,"utf8"), crlfDelay: Infinity });

for await (const line of rl){
  if(!line) continue;
  const f = JSON.parse(line);
  total++;

  const props = (f && f.properties && typeof f.properties==="object") ? f.properties : {};
  const city = String(props.__city || "UNKNOWN").toUpperCase();
  const layer = String(props.__layer || "UNKNOWN");

  cityCounts.set(city, (cityCounts.get(city)||0)+1);
  layerCounts.set(layer, (layerCounts.get(layer)||0)+1);

  if(isBaseLayer(layer)){
    wb.write(JSON.stringify(f) + "\n");
    baseN++;
    baseLayerCounts.set(layer, (baseLayerCounts.get(layer)||0)+1);
  } else {
    ws.write(JSON.stringify(f) + "\n");
    specN++;
    specLayerCounts.set(layer, (specLayerCounts.get(layer)||0)+1);
  }
}

wb.end(); ws.end();

function topMap(map, n=20){
  return [...map.entries()].sort((a,b)=>b[1]-a[1]).slice(0,n).map(([k,v])=>({key:k, count:v}));
}

const report = {
  generatedAt: new Date().toISOString(),
  inNdjson,
  total,
  baseN,
  specialN: specN,
  topLayersAll: topMap(layerCounts, 30),
  topBaseLayers: topMap(baseLayerCounts, 30),
  topSpecialLayers: topMap(specLayerCounts, 30),
};

fs.writeFileSync(outReport, JSON.stringify(report, null, 2));

console.log("[done] split(v12 layerGate)", {total, baseN, specN, outBase, outSpecial, outReport});
console.log("Top BASE layers:");
console.table(report.topBaseLayers);
console.log("Top SPECIAL layers:");
console.table(report.topSpecialLayers);
