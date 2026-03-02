import fs from "node:fs";
import path from "node:path";

function getArg(name, d=null){
  const k=`--${name}`; const i=process.argv.indexOf(k);
  return i>=0 ? process.argv[i+1] : d;
}

const inFile   = getArg("in");
const outFile  = getArg("out");
const metaFile = getArg("meta");
const city     = getArg("city","UNKNOWN");
const layer    = getArg("layer","UNKNOWN");

if(!inFile || !outFile || !metaFile){
  console.error("Usage: node normalizeBaseDistrictsGeoJSON_v1.mjs --in <geojson> --out <geojson> --meta <json> --city <CITY> --layer <LAYER>");
  process.exit(1);
}

const fc = JSON.parse(fs.readFileSync(inFile,"utf8"));
const feats = Array.isArray(fc.features) ? fc.features : [];

const CODE_KEYS = [
  "zone_name","orig_zone","ZONE","ZONING","ZONECODE","ZONETYPE","DISTRICT","DIST_CODE","ZONEDIST","ZONINGDIST","NAME"
];

const NAME_KEYS = [
  "longname","LongName","zone_desc","ZONE_DESC","district_name","DIST_NAME","NAME"
];

function pick(props, keys){
  if(!props || typeof props!=="object") return null;
  for(const k of keys){
    const v = props[k];
    if(v==null) continue;
    const s = String(v).trim();
    if(s) return { key:k, val:s };
  }
  return null;
}

let missingCode=0, missingName=0;

const outFeats = feats.map((f) => {
  const props = (f && f.properties && typeof f.properties==="object") ? f.properties : {};

  const codePick = pick(props, CODE_KEYS);
  const namePick = pick(props, NAME_KEYS);

  const base_code = codePick ? codePick.val : null;
  const base_name = namePick ? namePick.val : (base_code || null);

  if(!base_code) missingCode++;
  if(!base_name) missingName++;

  const normProps = {
    base_code,
    base_name,
    base_category: "zoning_base",
    source_city: String(city).toUpperCase(),
    source_layer: String(layer),
    source_objectid: (props.objectid ?? props.OBJECTID ?? null),
    source_globalid: (props.globalid ?? props.GlobalID ?? props.GLOBALID ?? null),
    source_last_edited_user: (props.last_edited_user ?? props.lastEditedUser ?? null),
    source_last_edited_date: (props.last_edited_date ?? props.lastEditedDate ?? null),
    source_shape_area: (props.shape_Area ?? props.Shape__Area ?? props.SHAPE_Area ?? null),
    source_shape_length: (props.shape_Length ?? props.Shape__Length ?? props.SHAPE_Length ?? null),
    source_code_field: codePick ? codePick.key : null,
    source_name_field: namePick ? namePick.key : null
  };

  return { type:"Feature", geometry: f.geometry, properties: normProps };
});

fs.mkdirSync(path.dirname(outFile), {recursive:true});
fs.mkdirSync(path.dirname(metaFile), {recursive:true});

fs.writeFileSync(outFile, JSON.stringify({ type:"FeatureCollection", features: outFeats }));
fs.writeFileSync(metaFile, JSON.stringify({
  generatedAt: new Date().toISOString(),
  inFile, outFile,
  city: String(city).toUpperCase(),
  layer: String(layer),
  featuresIn: feats.length,
  featuresOut: outFeats.length,
  missingCode,
  missingName
}, null, 2));

console.log("[done] normalized BASE districts", {features: outFeats.length, missingCode, missingName, outFile, metaFile});
