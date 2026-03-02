
import fs from "node:fs";
import fsp from "node:fs/promises";
import readline from "node:readline";
import path from "node:path";
import crypto from "node:crypto";

import booleanPointInPolygon from "@turf/boolean-point-in-polygon";
import bbox from "@turf/bbox";
import area from "@turf/area";
import { point } from "@turf/helpers";

function parseArgs(argv){ const o={}; for(let i=2;i<argv.length;i++){ const a=argv[i]; if(!a.startsWith("--")) continue; const k=a.slice(2); const v=argv[i+1] && !argv[i+1].startsWith("--") ? argv[++i] : true; o[k]=v; } return o; }
async function sha256File(fp){ return new Promise((res,rej)=>{ const h=crypto.createHash("sha256"); const s=fs.createReadStream(fp); s.on("data",d=>h.update(d)); s.on("error",rej); s.on("end",()=>res(h.digest("hex"))); }); }
function pickLatLon(r){ const lat=r.lat ?? r.latitude ?? r?.location?.lat; const lon=r.lon ?? r.lng ?? r.longitude ?? r?.location?.lng ?? r?.location?.lon; const la=lat!=null?Number(lat):null; const lo=lon!=null?Number(lon):null; if(!Number.isFinite(la)||!Number.isFinite(lo)) return null; if(la<41||la>43.6) return null; if(lo<-73.8||lo>-69.3) return null; return {lat:la,lon:lo}; }
function pickLabel(p){ if(!p) return ""; const pref=["ZONING","ZONE","ZONEDIST","DISTRICT","DIST_NAME","NAME","ZONE_CODE","ZONING_CODE"]; for(const k of pref){ if(p[k]!=null) return String(p[k]).trim(); const f=Object.keys(p).find(x=>x.toLowerCase()===k.toLowerCase()); if(f && p[f]!=null) return String(p[f]).trim(); } return ""; }
function bboxContains(bb,lon,lat){ return lon>=bb[0] && lon<=bb[2] && lat>=bb[1] && lat<=bb[3]; }
function gridKey(lon,lat,cell){ return `${Math.floor(lon/cell)}:${Math.floor(lat/cell)}`; }
function cellsForBbox(bb,cell){ const minX=Math.floor(bb[0]/cell), maxX=Math.floor(bb[2]/cell); const minY=Math.floor(bb[1]/cell), maxY=Math.floor(bb[3]/cell); const ks=[]; for(let x=minX;x<=maxX;x++) for(let y=minY;y<=maxY;y++) ks.push(`${x}:${y}`); return ks; }
function chooseWinner(hits){ let best=hits[0]; for(let i=1;i<hits.length;i++){ const h=hits[i]; if(h.area<best.area) best=h; } return best; }
function coordSourceAllowed(row){
  const s=String(row.coord_source||"");
  const allow=/addressIndex|addressPoint|parcel_direct|parcelIndex|parcelCentroid/i.test(s);
  const block=/fuzzy|external|nominatim|google|mapbox|bing/i.test(s);
  return allow && !block;
}

const args=parseArgs(process.argv);
const IN=args.in;
const OUT=args.out;
const META=args.meta;
const ZONING_FILE=args.zoningFile;
const ALLOWLIST_FILE=args.allowlistFile;
const TIER_FIELD=args.tierField||"address_tier";
const TIER_VALUE=args.tierValue||"B";
const CELL=Number(args.cellDeg||0.02);

if(!IN||!OUT||!META||!ZONING_FILE||!ALLOWLIST_FILE){
  console.error("Usage: node ... --in <ndjson> --out <ndjson> --meta <json> --zoningFile <geojson> --allowlistFile <json> [--tierField address_tier] [--tierValue B]");
  process.exit(1);
}

const allowJson=JSON.parse(await fsp.readFile(ALLOWLIST_FILE,"utf8"));
const allowTown=new Set((allowJson.allowlist||[]).map(t=>String(t).toUpperCase()));

const zoningHash=await sha256File(ZONING_FILE);
const zoningName=path.basename(ZONING_FILE);
const zoningGj=JSON.parse(await fsp.readFile(ZONING_FILE,"utf8"));

const grid=new Map();
for(const f of (zoningGj.features||[])){
  if(!f?.geometry) continue;
  const t=f.geometry.type;
  if(t!=="Polygon" && t!=="MultiPolygon") continue;
  const bb=bbox(f);
  const rec={bbox:bb, geometry:f.geometry, label:pickLabel(f.properties||{}), area:area(f)};
  for(const k of cellsForBbox(bb,CELL)){ if(!grid.has(k)) grid.set(k,[]); grid.get(k).push(rec); }
}

const startedAt=new Date().toISOString();

let total=0, tierB=0, alreadyAttached=0, nonB=0, townNotCovered=0, badCoord=0, noCoords=0;
let attached=0, noMatch=0, multiHit=0;

const rl=readline.createInterface({input:fs.createReadStream(IN,{encoding:"utf8"}), crlfDelay:Infinity});
const ws=fs.createWriteStream(OUT,{encoding:"utf8"});

for await (const line of rl){
  const s=line.trim(); if(!s) continue;
  let row; try{ row=JSON.parse(s);}catch{continue;}
  total++;

  const outRow={...row};
  outRow.zoning=outRow.zoning||{};
  outRow.zoning.attach=outRow.zoning.attach||{};

  if(outRow?.zoning?.attach?.status==="attached" && outRow?.zoning?.district){
    alreadyAttached++;
    ws.write(JSON.stringify(outRow)+"\n");
    continue;
  }

  const tv=String(outRow?.[TIER_FIELD]??"").trim();
  if(tv!==String(TIER_VALUE).trim()){
    nonB++;
    ws.write(JSON.stringify(outRow)+"\n");
    continue;
  }

  tierB++;

  const town=String(outRow.town||"UNKNOWN").toUpperCase();
  if(!allowTown.has(town)){
    townNotCovered++;
    outRow.zoning.district = outRow.zoning.district ?? null;
    outRow.zoning.attach = { ...outRow.zoning.attach, status:"gated_out_town_not_covered", tierApplied:"B", asOf:startedAt, zoningSha256:zoningHash, zoningFile:zoningName, flags:["tierB_lower_trust","town_not_covered"] };
    ws.write(JSON.stringify(outRow)+"\n");
    continue;
  }

  const coords=pickLatLon(outRow);
  if(!coords){
    noCoords++;
    outRow.zoning.district = outRow.zoning.district ?? null;
    outRow.zoning.attach = { ...outRow.zoning.attach, status:"no_coords", tierApplied:"B", asOf:startedAt, zoningSha256:zoningHash, zoningFile:zoningName, flags:["tierB_lower_trust"] };
    ws.write(JSON.stringify(outRow)+"\n");
    continue;
  }

  if(!coordSourceAllowed(outRow)){
    badCoord++;
    outRow.zoning.district = outRow.zoning.district ?? null;
    outRow.zoning.attach = { ...outRow.zoning.attach, status:"gated_out_coord_source", tierApplied:"B", asOf:startedAt, zoningSha256:zoningHash, zoningFile:zoningName, flags:["tierB_lower_trust","coord_source_not_allowed"] };
    ws.write(JSON.stringify(outRow)+"\n");
    continue;
  }

  const {lon,lat}=coords;
  const pt=point([lon,lat]);
  const key=gridKey(lon,lat,CELL);
  const cands=grid.get(key)||[];

  const hits=[];
  for(const z of cands){
    if(!bboxContains(z.bbox,lon,lat)) continue;
    try{
      const poly={type:"Feature", geometry:z.geometry, properties:{}};
      if(booleanPointInPolygon(pt,poly)) hits.push(z);
    }catch{}
  }

  if(!hits.length){
    noMatch++;
    outRow.zoning.district=null;
    outRow.zoning.attach={ ...outRow.zoning.attach, status:"no_match", tierApplied:"B", asOf:startedAt, zoningSha256:zoningHash, zoningFile:zoningName, flags:["tierB_lower_trust"], point:{lat,lon} };
    ws.write(JSON.stringify(outRow)+"\n");
    continue;
  }

  const mh=hits.length>1;
  if(mh) multiHit++;
  const winner=mh?chooseWinner(hits):hits[0];

  outRow.zoning.district=(winner.label||"").trim()||null;
  outRow.zoning.attach={ ...outRow.zoning.attach, status:"attached", tierApplied:"B", asOf:startedAt, zoningSha256:zoningHash, zoningFile:zoningName, multiHit:mh, confidence: mh?0.30:0.70, flags: mh?["tierB_lower_trust","multi_hit_possible_overlap"]:["tierB_lower_trust"], point:{lat,lon} };
  attached++;
  ws.write(JSON.stringify(outRow)+"\n");
}

ws.end();

const meta={
  created_at:new Date().toISOString(),
  pass:"tierB_only_with_town_allowlist_gate",
  inputs:{in:IN, zoningFile:ZONING_FILE, allowlistFile:ALLOWLIST_FILE},
  counts:{ total_rows:total, tierB_rows:tierB, alreadyAttached, nonB, townNotCovered, badCoordSource:badCoord, no_coords:noCoords, attached, no_match:noMatch, multi_hit:multiHit }
};

await fsp.writeFile(META, JSON.stringify(meta,null,2), "utf8");
console.log("[done]", meta.counts);
