import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

function getArg(name){
  const k = \--\\;
  const i = process.argv.indexOf(k);
  return i >= 0 ? process.argv[i+1] : null;
}

const layersNdjson = getArg("layersNdjson");
const outDir = getArg("outDir");

if(!layersNdjson || !outDir){
  console.error("Usage: node arcgisSuggestLayerMappings_v1.mjs --layersNdjson <layers_flat.ndjson> --outDir <dir>");
  process.exit(1);
}

fs.mkdirSync(outDir, { recursive:true });

const CATS = {
  zoning_base:        ["zoning district", "zoning_district", "zoning district", "base zoning", "zoning"],
  zoning_overlay:     ["overlay", "overlay district", "special district", "district overlay"],
  smart_growth_40r:   ["40r", "smart growth", "sgod"],
  historic_district:  ["historic", "landmark", "landmarks", "blc"],
  wetlands:           ["wetland", "wetlands", "resource area", "buffer zone"],
  utilities_water_sewer: ["sewer", "water", "drain", "storm", "hydrant", "manhole", "catch basin"],
  mbta_transit:       ["mbta", "subway", "station", "bus", "route", "transit"],
  flood_fema:         ["fema", "flood", "sfha", "flood zone"],
  evacuation:         ["evac", "evacuation"],
  neighborhoods:      ["neighborhood", "neighbourhood", "planning district", "wards", "precinct"],
  trash_recycling:    ["trash", "recycling", "solid waste", "pickup", "collection"],
  snow_emergency:     ["snow", "winter", "emergency parking", "snow emergency", "parking restriction"]
};

function score(name, keys){
  const s = String(name || "").toLowerCase();
  let sc = 0;
  for(const k of keys){
    const kk = k.toLowerCase();
    if(s === kk) sc += 50;
    if(s.includes(kk)) sc += 10;
  }
  // slight boost if it looks like the layer is explicitly "Zoning_*"
  if(/^zoning[\\s_\\-]/i.test(name || "")) sc += 5;
  return sc;
}

const byCity = new Map();

const rl = readline.createInterface({ input: fs.createReadStream(layersNdjson, "utf8"), crlfDelay: Infinity });
for await (const line of rl){
  if(!line.trim()) continue;
  const r = JSON.parse(line);
  const city = String(r.city || "UNKNOWN");
  if(!byCity.has(city)) byCity.set(city, []);
  byCity.get(city).push(r);
}

for(const [city, rows] of byCity.entries()){
  const outCity = { city, categories: {}, totalLayers: rows.length };

  for(const [cat, keys] of Object.entries(CATS)){
    const ranked = rows
      .map(r => ({ ...r, _score: score(r.layerName, keys) }))
      .filter(r => r._score > 0)
      .sort((a,b)=> b._score - a._score)
      .slice(0, 25)
      .map(r => ({
        score: r._score,
        layerName: r.layerName,
        layerId: r.layerId,
        serviceUrl: r.serviceUrl,
        folder: r.folder
      }));

    outCity.categories[cat] = ranked;
  }

  fs.writeFileSync(path.join(outDir, \\_suggestions.json\), JSON.stringify(outCity, null, 2));
}

console.log("✅ Suggestions written to:", outDir);
