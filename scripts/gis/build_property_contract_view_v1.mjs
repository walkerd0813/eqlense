// build_property_contract_view_v1.mjs
// Usage: node build_property_contract_view_v1.mjs --in <properties.ndjson> --out <out.ndjson> --datasetHash <sha256> --asOfDate <YYYY-MM-DD>
import fs from "node:fs";
import readline from "node:readline";
import path from "node:path";

function parseArgs(argv){ const o={}; for(let i=2;i<argv.length;i++){ const a=argv[i];
  if(a.startsWith("--")){ const k=a.slice(2); const v=argv[i+1] && !argv[i+1].startsWith("--") ? argv[++i] : "true"; o[k]=v; }
} return o; }

function getPath(obj,p){ if(!obj||!p) return undefined; const parts=String(p).split("."); let cur=obj;
  for(const part of parts){ if(cur && Object.prototype.hasOwnProperty.call(cur,part)) cur=cur[part]; else return undefined; }
  return cur;
}
function pick(obj,cands){ for(const c of cands){ if(!c) continue;
  const v = c.includes(".") ? getPath(obj,c) : (obj ? obj[c] : undefined);
  if(v!==undefined && v!==null && v!=="") return v;
} return undefined; }
function norm(x){ return (x===undefined||x===null) ? "" : String(x).trim(); }
function deriveCoordGrade(coordSource){
  const s = norm(coordSource).toLowerCase();
  if(s.includes("parcel")) return "A";
  if(s.includes("address")) return "B";
  if(s.includes("external")) return "C";
  return "C";
}

const args=parseArgs(process.argv);
const inPath=args.in, outPath=args.out, datasetHash=args.datasetHash||"UNKNOWN", asOfDate=args.asOfDate||"UNKNOWN";
if(!inPath||!outPath){
  console.error("Usage: node build_property_contract_view_v1.mjs --in <properties.ndjson> --out <out.ndjson> --datasetHash <sha256> --asOfDate <YYYY-MM-DD>");
  process.exit(2);
}
fs.mkdirSync(path.dirname(outPath), { recursive:true });

const rl = readline.createInterface({ input: fs.createReadStream(inPath,{encoding:"utf8"}), crlfDelay:Infinity });
const ws = fs.createWriteStream(outPath,{encoding:"utf8"});

let read=0,wrote=0,skipped=0;

for await (const line of rl){
  const t=line.trim(); if(!t) continue;
  read++;
  let row; try{ row=JSON.parse(t); } catch { skipped++; continue; }

  const property_id = pick(row,["property_id","propertyId","id"]);
  const parcel_id_raw = pick(row,["parcel_id_raw","parcel_id","parcelId","parcel_id_raw_or_equiv"]);
  const town = pick(row,["source_city","town","address_city","city"]);
  const state = pick(row,["source_state","state","address_state"]) || "MA";
  if(!property_id || !parcel_id_raw || !town){ skipped++; continue; }

  const lat = pick(row,["latitude","lat"]);
  const lon = pick(row,["longitude","lon","lng"]);
  const coord_source = pick(row,["coord_source","coordSource"]) || "";
  const coord_confidence_grade = pick(row,["coord_confidence_grade","coordConfidenceGrade"]) || deriveCoordGrade(coord_source);

  const parcel_centroid_lat = (pick(row,["parcel_centroid_lat","centroid_lat"]) ?? lat ?? null);
  const parcel_centroid_lon = (pick(row,["parcel_centroid_lon","centroid_lon"]) ?? lon ?? null);

  const base_zoning_code_raw = pick(row,["base_zoning_code_raw","baseZoningCodeRaw","zoning_code_raw","zoning","zone"]) || "";
  const base_zoning_code_norm = pick(row,["base_zoning_code_norm","baseZoningCodeNorm","zoning_code_norm","zone_norm"]) || base_zoning_code_raw;
  const base_zoning_status = pick(row,["base_zoning_status","baseZoningStatus","zoning_status"]) || (base_zoning_code_raw ? "ATTACHED" : "UNKNOWN");

  const out = {
    property_id,
    parcel_id_raw: norm(parcel_id_raw),
    parcel_id_norm: norm(pick(row,["parcel_id_norm","parcelIdNorm"]) || parcel_id_raw),
    source_city: norm(town),
    source_state: norm(state),

    dataset_hash: norm(datasetHash),
    as_of_date: norm(asOfDate),

    address_city: norm(pick(row,["address_city","city","town"]) || town),
    address_state: norm(state),
    address_zip: norm(pick(row,["address_zip","zip","zipcode"]) || ""),

    latitude: (lat===undefined||lat===null||lat==="") ? null : Number(lat),
    longitude: (lon===undefined||lon===null||lon==="") ? null : Number(lon),
    coord_source: norm(coord_source),
    coord_confidence_grade: norm(coord_confidence_grade),

    parcel_centroid_lat: (parcel_centroid_lat===undefined||parcel_centroid_lat===null||parcel_centroid_lat==="") ? null : Number(parcel_centroid_lat),
    parcel_centroid_lon: (parcel_centroid_lon===undefined||parcel_centroid_lon===null||parcel_centroid_lon==="") ? null : Number(parcel_centroid_lon),

    crs: "EPSG:4326",

    base_zoning_status: norm(base_zoning_status),
    base_zoning_code_raw: norm(base_zoning_code_raw),
    base_zoning_code_norm: norm(base_zoning_code_norm),

    zoning_attach_method: "polygon_centroid",
    zoning_attach_confidence: "B",
    zoning_source_city: norm(town),
    zoning_dataset_hash: "UNKNOWN",
    zoning_as_of_date: norm(asOfDate)
  };

  ws.write(JSON.stringify(out) + "\n");
  wrote++;
  if(read % 200000 === 0) console.error(`[prog] read=${read} wrote=${wrote} skipped=${skipped}`);
}
ws.end();
console.error(`[done] read=${read} wrote=${wrote} skipped=${skipped}`);
