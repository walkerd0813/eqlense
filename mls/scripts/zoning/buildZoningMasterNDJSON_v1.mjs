import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

function getArg(name, d=null){
  const k=`--${name}`; const i=process.argv.indexOf(k);
  return i>=0 ? process.argv[i+1] : d;
}

const inventoryFile = getArg("inventory");
const outNdjson = getArg("outNdjson");
const outRejects = getArg("outRejects");
const outReport = getArg("outReport");
const includeNonWgs84 = getArg("includeNonWgs84","0") === "1";

if(!inventoryFile || !outNdjson || !outRejects || !outReport){
  console.error("Usage: node buildZoningMasterNDJSON_v1.mjs --inventory <json> --outNdjson <ndjson> --outRejects <ndjson> --outReport <json> [--includeNonWgs84 0|1]");
  process.exit(1);
}

const inv = JSON.parse(fs.readFileSync(inventoryFile,"utf8"));
const files = inv.files || [];

fs.mkdirSync(path.dirname(outNdjson), {recursive:true});
const out = fs.createWriteStream(outNdjson, {encoding:"utf8"});
const rej = fs.createWriteStream(outRejects, {encoding:"utf8"});

let keptFiles=0, skippedFiles=0, keptFeatures=0, rejectedFeatures=0;

function guessCity(rel){
  const parts = rel.split(/[\\\/]/).map(s=>s.trim()).filter(Boolean);
  // Look for a "cities/<CITY>/" convention
  const ci = parts.findIndex(p=>p.toLowerCase()==="cities");
  if(ci>=0 && parts[ci+1]) return parts[ci+1].toUpperCase();
  // Otherwise use parent folder name if it looks like a place
  if(parts.length>=2) return parts[parts.length-2].toUpperCase();
  return null;
}

for(const f of files){
  const okFile = !f.parseError && (includeNonWgs84 ? true : !!f.wgs84MA);
  if(!okFile){ skippedFiles++; continue; }

  let j;
  try{
    j = JSON.parse(fs.readFileSync(f.abs,"utf8"));
  }catch(e){
    skippedFiles++; continue;
  }

  const feats = Array.isArray(j.features) ? j.features : [];
  const cityGuess = guessCity(f.rel);
  const layer = path.basename(f.rel).replace(/\.geojson$/i,"");

  keptFiles++;

  for(const feat of feats){
    if(!feat || !feat.type || feat.type !== "Feature" || !feat.geometry){
      rejectedFeatures++;
      rej.write(JSON.stringify({reason:"bad_feature", src:f.rel})+"\n");
      continue;
    }

    const props = (feat.properties && typeof feat.properties==="object") ? feat.properties : {};
    // Add provenance (does not overwrite existing fields)
    props.__src_rel = f.rel;
    props.__src_sha256 = f.sha256;
    props.__layer = layer;
    if(props.__city == null && cityGuess) props.__city = cityGuess;

    feat.properties = props;

    out.write(JSON.stringify(feat) + "\n");
    keptFeatures++;
  }
}

out.end(); rej.end();

const report = {
  generatedAt: new Date().toISOString(),
  inventory: inventoryFile,
  includeNonWgs84,
  keptFiles, skippedFiles,
  keptFeatures, rejectedFeatures,
  outNdjson, outRejects
};

fs.writeFileSync(outReport, JSON.stringify(report, null, 2));
console.log("[done]", report);
