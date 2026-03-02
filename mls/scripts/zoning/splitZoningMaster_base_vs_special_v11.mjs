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
  console.error("Usage: node splitZoningMaster_base_vs_special_v11.mjs --inNdjson <ndjson> --outBase <ndjson> --outSpecial <ndjson> --outReport <json>");
  process.exit(1);
}

const BASE_CODE_KEYS = ["Zoning","ZONECODE","ZCODE","ZONE","ZONING","DIST_CODE","DISTRICT","ZONEDIST","ZONINGDIST","ZONING_DIS"];
const STRONG_SPECIAL_KEYS = [
  "Unique_Cod","Urban_Name","Urban_Cust","Article","ARTICLE","Overlay","OVERLAY",
  "Zoning_Sub","Subdistr_1","Subdistrict","SGOD","40R","AHO","Historic","Waterfront"
];

function hasAnyKey(props, keys){
  if(!props || typeof props !== "object") return false;
  return keys.some(k => Object.prototype.hasOwnProperty.call(props, k));
}

function pickBaseCode(props){
  if(!props || typeof props !== "object") return null;
  for(const k of BASE_CODE_KEYS){
    const v = props[k];
    if(v == null) continue;
    const s = String(v).trim();
    if(s) return {key:k, val:s};
  }
  return null;
}

function looksLikeBaseCode(s){
  const t = String(s).trim();
  if(!t) return false;
  // strict-but-safe: short and no spaces
  if(t.length <= 12 && !t.includes(" ")) return true;
  return false;
}

const cityCounts = new Map();
const layerCounts = new Map();
const cityBaseCounts = new Map();
const citySpecCounts = new Map();

let total=0, baseN=0, specN=0;

fs.mkdirSync(path.dirname(outBase), {recursive:true});
fs.mkdirSync(path.dirname(outSpecial), {recursive:true});

const wb = fs.createWriteStream(outBase, {encoding:"utf8"});
const ws = fs.createWriteStream(outSpecial, {encoding:"utf8"});

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

  const bc = pickBaseCode(props);
  const baseCandidate = !!bc && looksLikeBaseCode(bc.val) && !hasAnyKey(props, STRONG_SPECIAL_KEYS);

  if(baseCandidate){
    wb.write(JSON.stringify(f) + "\n");
    baseN++;
    cityBaseCounts.set(city, (cityBaseCounts.get(city)||0)+1);
  }else{
    ws.write(JSON.stringify(f) + "\n");
    specN++;
    citySpecCounts.set(city, (citySpecCounts.get(city)||0)+1);
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
  topCities: topMap(cityCounts, 30),
  topLayers: topMap(layerCounts, 30),
  boston: {
    total: cityCounts.get("BOSTON")||0,
    base: cityBaseCounts.get("BOSTON")||0,
    special: citySpecCounts.get("BOSTON")||0
  }
};

fs.writeFileSync(outReport, JSON.stringify(report, null, 2));
console.log("[done] split", {total, baseN, specN, outBase, outSpecial, outReport});
console.log("Boston split:", report.boston);
console.log("Top cities:");
console.table(report.topCities);
console.log("Top layers:");
console.table(report.topLayers);
