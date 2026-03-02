import fs from "node:fs";
import path from "node:path";

function getArg(name, d=null){
  const k=`--${name}`; const i=process.argv.indexOf(k);
  return i>=0 ? process.argv[i+1] : d;
}

const inFile = getArg("in");
const outFile = getArg("out");
const metaFile = getArg("meta");
const city = getArg("city","UNKNOWN");
const layer = getArg("layer","UNKNOWN");
const category = getArg("category","zoning_overlay");
const codeField = getArg("codeField","zonetype");
const nameField = getArg("nameField","longname");

if(!inFile || !outFile || !metaFile){
  console.error("Usage: node normalizeOverlayGeoJSON_v1.mjs --in <geojson> --out <geojson> --meta <json> --city <CITY> --layer <LAYER> [--category zoning_overlay] [--codeField zonetype] [--nameField longname]");
  process.exit(1);
}

const fc = JSON.parse(fs.readFileSync(inFile,"utf8"));
const feats = Array.isArray(fc.features) ? fc.features : [];

let missingCode=0, missingName=0;
const outFeats = feats.map((f) => {
  const props = (f && f.properties && typeof f.properties==="object") ? f.properties : {};
  const code = (props[codeField] ?? null);
  const name = (props[nameField] ?? null);

  if(code==null || String(code).trim()==="") missingCode++;
  if(name==null || String(name).trim()==="") missingName++;

  const norm = {
    overlay_code: code ? String(code).trim() : null,
    overlay_name: name ? String(name).trim() : null,
    overlay_category: category,
    source_city: String(city).toUpperCase(),
    source_layer: String(layer),
    source_objectid: (props.objectid ?? props.OBJECTID ?? null),
    source_globalid: (props.globalid ?? props.GlobalID ?? props.GLOBALID ?? null),
    source_shape_area: (props.shape_Area ?? props.Shape__Area ?? props.SHAPE_Area ?? null),
    source_shape_length: (props.shape_Length ?? props.Shape__Length ?? props.SHAPE_Length ?? null)
  };

  return {
    type: "Feature",
    geometry: f.geometry,
    properties: norm
  };
});

fs.mkdirSync(path.dirname(outFile), {recursive:true});
fs.mkdirSync(path.dirname(metaFile), {recursive:true});

fs.writeFileSync(outFile, JSON.stringify({ type:"FeatureCollection", features: outFeats }));
fs.writeFileSync(metaFile, JSON.stringify({
  generatedAt: new Date().toISOString(),
  inFile,
  outFile,
  city: String(city).toUpperCase(),
  layer: String(layer),
  category,
  featuresIn: feats.length,
  featuresOut: outFeats.length,
  missingCode,
  missingName,
  codeField,
  nameField
}, null, 2));

console.log("[done] normalized overlay geojson", { features: outFeats.length, missingCode, missingName, outFile, metaFile });
