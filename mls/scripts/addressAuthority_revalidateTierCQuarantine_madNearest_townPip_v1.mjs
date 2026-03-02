import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

process.on("unhandledRejection", (e)=>{ console.error(e); process.exit(1); });
process.on("uncaughtException", (e)=>{ console.error(e); process.exit(1); });

function argsToMap(argv){
  const m={};
  for(let i=2;i<argv.length;i++){
    if(!argv[i].startsWith("--")) continue;
    const k=argv[i].slice(2);
    const v=(argv[i+1] && !argv[i+1].startsWith("--")) ? argv[++i] : "1";
    m[k]=v;
  }
  return m;
}
const a=argsToMap(process.argv);

const basePath=a.base;
const quarantinePath=a.quarantine;
const tilesDir=a.tilesDir;
const townsGeoPath=a.townsGeo;
const outPath=a.out;
const reportPath=a.report;

const maxDistM=Number(a.maxDistM ?? 60);
const tileSize=Number(a.tileSize ?? 0.01);
const tileCacheN=Number(a.tileCacheN ?? 250);

if(!basePath||!quarantinePath||!tilesDir||!townsGeoPath||!outPath||!reportPath){
  console.error("Usage: --base --quarantine --tilesDir --townsGeo --out --report [--maxDistM 60] [--tileSize 0.01] [--tileCacheN 250]");
  process.exit(1);
}
for(const p of [basePath, quarantinePath, townsGeoPath, tilesDir]){
  if(!fs.existsSync(p)){ console.error("Missing:", p); process.exit(1); }
}
fs.mkdirSync(path.dirname(outPath), {recursive:true});
fs.mkdirSync(path.dirname(reportPath), {recursive:true});

const NOW=new Date().toISOString();

function normTown(s){
  let t=String(s??"").toUpperCase().trim();
  t=t.replace(/\bTOWN OF\b/g,"").replace(/\bCITY OF\b/g,"").trim();
  t=t.replace(/[^\w\s]/g," ").replace(/\s+/g," ").trim();
  t=t.replace(/^ST\s+/,"SAINT ").replace(/\s+ST$/," SAINT");
  return t;
}
function zip5(z){
  const s=String(z??"").trim();
  const m=s.match(/^(\d{5})/);
  return m?m[1]:"";
}

// ---------------- Town PIP (EPSG:26986) ----------------
function bboxFromCoords(coords){
  let minX=Infinity,minY=Infinity,maxX=-Infinity,maxY=-Infinity;
  const walk=(c)=>{
    if(typeof c?.[0]==="number" && typeof c?.[1]==="number"){
      const x=c[0], y=c[1];
      if(x<minX)minX=x; if(y<minY)minY=y; if(x>maxX)maxX=x; if(y>maxY)maxY=y;
      return;
    }
    for(const child of c) walk(child);
  };
  walk(coords);
  return {minX,minY,maxX,maxY};
}
function pointInRing(x,y,ring){
  let inside=false;
  for(let i=0,j=ring.length-1;i<ring.length;j=i++){
    const xi=ring[i][0], yi=ring[i][1];
    const xj=ring[j][0], yj=ring[j][1];
    const denom=(yj-yi)||1e-20;
    const intersect=((yi>y)!==(yj>y)) && (x < (xj-xi)*(y-yi)/denom + xi);
    if(intersect) inside=!inside;
  }
  return inside;
}
function pointInPolygon(x,y,poly){
  // poly = [outerRing, holeRing1, ...]
  if(!poly?.length) return false;
  if(!pointInRing(x,y,poly[0])) return false;
  for(let h=1; h<poly.length; h++){
    if(pointInRing(x,y,poly[h])) return false;
  }
  return true;
}
function pointInGeometry(x,y,geom){
  if(!geom) return false;
  if(geom.type==="Polygon"){
    return pointInPolygon(x,y,geom.coordinates);
  }
  if(geom.type==="MultiPolygon"){
    for(const poly of geom.coordinates){
      if(pointInPolygon(x,y,poly)) return true;
    }
    return false;
  }
  return false;
}

console.log("Loading towns geojson (EPSG:26986):", townsGeoPath);
const townsGeo=JSON.parse(fs.readFileSync(townsGeoPath,"utf8"));
const towns=[];
const geomCounts={};
for(const f of (townsGeo.features||[])){
  const g=f.geometry; if(!g) continue;
  geomCounts[g.type]=(geomCounts[g.type]||0)+1;
  const name=normTown(f.properties?.TOWN ?? "");
  if(!name) continue;
  const bb=bboxFromCoords(g.coordinates);
  towns.push({name, bb, geom:g});
}
console.log("Towns loaded:", towns.length, "geomCounts:", geomCounts);

function townForPointXY(x,y){
  for(const t of towns){
    const b=t.bb;
    if(x<b.minX||x>b.maxX||y<b.minY||y>b.maxY) continue;
    if(pointInGeometry(x,y,t.geom)) return t.name;
  }
  return "";
}

// ---------------- Tiles (WGS84) ----------------
function snapKey(v){
  // snap to tile grid with stable formatting
  const snapped = Math.floor(v / tileSize) * tileSize;
  return snapped.toFixed(2);
}
function tileKey(lon,lat){
  return `${snapKey(lon)}|${snapKey(lat)}`;
}

function parseLonLatFromFilename(fn){
  // expects something like tile_-71.23_42.28.ndjson
  const nums = fn.match(/-?\d+(?:\.\d+)?/g);
  if(!nums || nums.length < 2) return null;
  const lon = Number(nums[0]);
  const lat = Number(nums[1]);
  if(!Number.isFinite(lon) || !Number.isFinite(lat)) return null;
  return {lon, lat};
}

console.log("Building tile map from:", tilesDir);
const files = fs.readdirSync(tilesDir).filter(f=>f.toLowerCase().endsWith(".ndjson"));
const tileMap = new Map();
let collisions=0;
for(const fn of files){
  const parsed = parseLonLatFromFilename(fn);
  if(!parsed) continue;
  const k = `${parsed.lon.toFixed(2)}|${parsed.lat.toFixed(2)}`;
  if(tileMap.has(k)) collisions++;
  tileMap.set(k, path.join(tilesDir, fn));
}
console.log("Tile map:", { files: files.length, mapped: tileMap.size, collisions });

const tileCache=new Map();
const tileOrder=[];

async function loadTilePoints(key){
  if(tileCache.has(key)) return tileCache.get(key);
  const p = tileMap.get(key);
  if(!p) return [];
  const pts=[];
  const rl = readline.createInterface({ input: fs.createReadStream(p,{encoding:"utf8"}), crlfDelay: Infinity });
  for await (const line of rl){
    const t=line.trim(); if(!t) continue;
    try{ pts.push(JSON.parse(t)); } catch {}
  }
  tileCache.set(key, pts);
  tileOrder.push(key);
  if(tileOrder.length > tileCacheN){
    const drop = tileOrder.shift();
    tileCache.delete(drop);
  }
  return pts;
}

function haversineM(lat1,lon1,lat2,lon2){
  const R=6371000;
  const toRad=(d)=>d*Math.PI/180;
  const dLat=toRad(lat2-lat1);
  const dLon=toRad(lon2-lon1);
  const a=Math.sin(dLat/2)**2 + Math.cos(toRad(lat1))*Math.cos(toRad(lat2))*Math.sin(dLon/2)**2;
  return 2*R*Math.asin(Math.min(1,Math.sqrt(a)));
}

function pick(obj, keys){
  for(const k of keys){
    const v = obj?.[k];
    if(v!==undefined && v!==null && String(v).trim()!=="") return v;
  }
  return "";
}

function extractMadFields(m){
  const lon = Number(pick(m, ["lon","lng","x","X","LONGITUDE","Longitude"]));
  const lat = Number(pick(m, ["lat","y","Y","LATITUDE","Latitude"]));
  const town = pick(m, ["town","TOWN","municipality","MUNICIPALITY","city","CITY"]);
  const zip = zip5(pick(m, ["zip","ZIP","postcode","POSTCODE","post_code","POST_CODE"]));
  let streetNo = String(pick(m, ["street_no","STREET_NO","house_no","HOUSE_NO","addr_num","ADDR_NUM","number","NUMBER"])).trim();
  let streetName = String(pick(m, ["street_name","STREET_NAME","street","STREET","st_name","ST_NAME"])).trim();

  const full = String(pick(m, ["full_address","FULL_ADDRESS","address","ADDRESS","site_address","SITE_ADDRESS"])).trim();
  if((!streetNo || !streetName) && full){
    const mm = full.match(/^\s*(\d+[A-Z]?)\s+(.*)$/i);
    if(mm){
      if(!streetNo) streetNo = mm[1].trim();
      if(!streetName) streetName = mm[2].trim();
    }
  }

  return { lon, lat, town, zip, streetNo, streetName, full };
}

// ---------------- Targets: use QUARANTINE IDs ----------------
const targets = new Map();
let qTotal=0, qParseErr=0;

const rlQ = readline.createInterface({ input: fs.createReadStream(quarantinePath,{encoding:"utf8"}), crlfDelay: Infinity });
for await (const line of rlQ){
  const t=line.trim(); if(!t) continue;
  let row;
  try{ row=JSON.parse(t); } catch { qParseErr++; continue; }
  qTotal++;
  const id = row.property_id || row.parcel_id;
  if(!id) continue;
  const lon = Number(row.lng ?? row.lon);
  const lat = Number(row.lat);
  const x = Number(row.x_sp);
  const y = Number(row.y_sp);
  targets.set(id, { lon, lat, x, y, baseTown: normTown(row.town), baseZip: zip5(row.zip), baseStreet: String(row.street_name??"").toUpperCase().trim() });
}
console.log("Targets loaded from quarantine:", { qTotal, qParseErr, targetIds: targets.size });

// ---------------- Compute patches by re-running nearest ----------------
let noXY=0, noLatLon=0, noPipTown=0, noTile=0, noCandidate=0, tooFar=0, townMismatch=0, autoAccept=0;
const patchMap = new Map();

let i=0;
for(const [id, t] of targets.entries()){
  i++;
  if(i % 5000 === 0) console.log(`...target nearest ${i.toLocaleString()} / ${targets.size.toLocaleString()}`);

  if(!Number.isFinite(t.lon) || !Number.isFinite(t.lat)){ noLatLon++; continue; }
  if(!Number.isFinite(t.x) || !Number.isFinite(t.y)){ noXY++; continue; }

  const pipTown = townForPointXY(t.x, t.y);
  if(!pipTown){ noPipTown++; continue; }

  // search 3x3 tiles around the snapped tile
  const baseLonKey = Number(snapKey(t.lon));
  const baseLatKey = Number(snapKey(t.lat));

  let best=null;

  for(let dx=-1; dx<=1; dx++){
    for(let dy=-1; dy<=1; dy++){
      const k = `${(baseLonKey + dx*tileSize).toFixed(2)}|${(baseLatKey + dy*tileSize).toFixed(2)}`;
      if(!tileMap.has(k)){ continue; }
      const pts = await loadTilePoints(k);
      for(const p of pts){
        const m = extractMadFields(p);
        if(!Number.isFinite(m.lon) || !Number.isFinite(m.lat)) continue;
        const d = haversineM(t.lat, t.lon, m.lat, m.lon);
        if(!best || d < best.distM){
          best = { distM: d, mad: m };
        }
      }
    }
  }

  if(!best){ noTile++; continue; }
  if(best.distM > maxDistM){ tooFar++; continue; }

  const candTown = normTown(best.mad.town);
  if(!candTown){ noCandidate++; continue; }

  if(candTown !== pipTown){
    townMismatch++;
    continue;
  }

  // accept only if we actually have street name
  if(!best.mad.streetName){
    noCandidate++;
    continue;
  }

  patchMap.set(id, {
    street_no: best.mad.streetNo || "",
    street_name: best.mad.streetName,
    town: pipTown,
    zip: best.mad.zip || "",
    distM: best.distM
  });
  autoAccept++;
}

console.log("PatchMap built:", { patchKeys: patchMap.size, autoAccept, noLatLon, noXY, noPipTown, noTile, tooFar, townMismatch, noCandidate });

// ---------------- Stream base, apply patches ----------------
const out = fs.createWriteStream(outPath, {encoding:"utf8"});
let total=0, applied=0, baseParseErr=0;

const rlB = readline.createInterface({ input: fs.createReadStream(basePath,{encoding:"utf8"}), crlfDelay: Infinity });
for await (const line of rlB){
  const t=line.trim(); if(!t) continue;
  let row;
  try{ row=JSON.parse(t); } catch { baseParseErr++; continue; }
  total++;

  const id = row.property_id || row.parcel_id;
  const patch = id ? patchMap.get(id) : null;

  if(patch){
    if(patch.street_no) row.street_no = patch.street_no;
    if(patch.street_name) row.street_name = patch.street_name;
    row.town = patch.town || row.town;
    if(patch.zip) row.zip = patch.zip;

    row.address_verified = row.address_verified || {};
    row.address_verified.town_pip_stateplane = { verifiedTown: patch.town, distM: patch.distM, at: NOW };

    applied++;
  }

  out.write(JSON.stringify(row) + "\n");
  if(total % 500000 === 0) console.log(`...processed base ${total.toLocaleString()} rows`);
}
out.end();

const report = {
  created_at: NOW,
  in: { basePath, quarantinePath, tilesDir, townsGeoPath },
  params: { maxDistM, tileSize, tileCacheN },
  counts: {
    targets: targets.size,
    patchKeys: patchMap.size,
    applied_to_base: applied,
    base_total: total,
    base_parseErr: baseParseErr,
    quarantine_total: qTotal,
    quarantine_parseErr: qParseErr,
    noLatLon, noXY, noPipTown, noTile, tooFar, townMismatch, noCandidate
  }
};
fs.writeFileSync(reportPath, JSON.stringify(report,null,2), "utf8");
console.log("DONE.");
console.log(JSON.stringify(report,null,2));

// hard-exit safeguard
setTimeout(()=>process.exit(0), 250).unref();
