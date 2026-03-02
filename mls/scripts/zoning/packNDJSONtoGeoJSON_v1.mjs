import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

function getArg(name, d=null){
  const k=`--${name}`; const i=process.argv.indexOf(k);
  return i>=0 ? process.argv[i+1] : d;
}

const inNdjson = getArg("inNdjson");
const outGeojson = getArg("outGeojson");

if(!inNdjson || !outGeojson){
  console.error("Usage: node packNDJSONtoGeoJSON_v1.mjs --inNdjson <ndjson> --outGeojson <geojson>");
  process.exit(1);
}

fs.mkdirSync(path.dirname(outGeojson), {recursive:true});
const w = fs.createWriteStream(outGeojson, {encoding:"utf8"});
w.write('{"type":"FeatureCollection","features":[\n');

let first=true, count=0;
const rl = readline.createInterface({input: fs.createReadStream(inNdjson,"utf8"), crlfDelay: Infinity});

for await (const line of rl){
  if(!line) continue;
  if(!first) w.write(",\n");
  first=false;
  w.write(line);
  count++;
}

w.write("\n]}\n");
w.end();

console.log("[done] wrote", outGeojson, "features:", count);
