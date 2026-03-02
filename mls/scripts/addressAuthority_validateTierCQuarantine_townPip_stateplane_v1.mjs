import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

process.on("unhandledRejection", (e) => { console.error(e); process.exit(1); });
process.on("uncaughtException", (e) => { console.error(e); process.exit(1); });

function argMap(argv){
  const m={};
  for(let i=2;i<argv.length;i++){
    if(!argv[i].startsWith("--")) continue;
    const k=argv[i].slice(2);
    const v=(argv[i+1] && !argv[i+1].startsWith("--")) ? argv[++i] : "1";
    m[k]=v;
  }
  return m;
}
const a = argMap(process.argv);

const basePath = a.base;
const quarantinePath = a.quarantine;
const townsGeoPath = a.townsGeo;
const outPath = a.out;
const reportPath = a.report;
const outPromotedPath = a.outPromoted || "";
const outStillQPath = a.outStillQuarantine || "";

if(!basePath||!quarantinePath||!townsGeoPath||!outPath||!reportPath){
  console.error("Usage: --base --quarantine --townsGeo --out --report [--outPromoted] [--outStillQuarantine]");
  process.exit(1);
}
for (const p of [basePath, quarantinePath, townsGeoPath]) {
  if(!fs.existsSync(p)){ console.error("Missing file:", p); process.exit(1); }
}
fs.mkdirSync(path.dirname(outPath), { recursive:true });
fs.mkdirSync(path.dirname(reportPath), { recursive:true });
if(outPromotedPath) fs.mkdirSync(path.dirname(outPromotedPath), { recursive:true });
if(outStillQPath) fs.mkdirSync(path.dirname(outStillQPath), { recursive:true });

const NOW = new Date().toISOString();

function normTown(s){
  let t = String(s??"").toUpperCase().trim();
  t = t.replace(/\bTOWN OF\b/g,"").replace(/\bCITY OF\b/g,"").trim();
  t = t.replace(/[^\w\s]/g," ").replace(/\s+/g," ").trim();
  t = t.replace(/^ST\s+/,"SAINT ").replace(/\s+ST$/," SAINT");
  return t;
}
function zip5(z){
  const s = String(z??"").trim();
  const m = s.match(/^(\d{5})/);
  return m ? m[1] : "";
}

// ---- PIP for Polygon in SAME CRS (EPSG:26986) ----
function bboxFromCoords(coords){
  let minX=Infinity,minY=Infinity,maxX=-Infinity,maxY=-Infinity;
  const walk = (c) => {
    if(typeof c[0]==="number" && typeof c[1]==="number"){
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
    const denom = (yj-yi) || 1e-20;
    const intersect = ((yi>y)!==(yj>y)) && (x < (xj-xi)*(y-yi)/denom + xi);
    if(intersect) inside=!inside;
  }
  return inside;
}
function pointInPoly(x,y,poly){
  if(!poly || poly.length===0) return false;
  if(!pointInRing(x,y,poly[0])) return false;
  for(let h=1; h<poly.length; h++){
    if(pointInRing(x,y,poly[h])) return false;
  }
  return true;
}

console.log("Loading towns geojson (EPSG:26986):", townsGeoPath);
const townsGeo = JSON.parse(fs.readFileSync(townsGeoPath, "utf8"));
const towns = [];
for(const f of (townsGeo.features||[])){
  const g = f.geometry;
  if(!g) continue;
  const name = normTown(f.properties?.TOWN ?? "");
  if(!name) continue;
  const bb = bboxFromCoords(g.coordinates);
  towns.push({ name, bb, coords: g.coordinates });
}
console.log("Towns loaded:", towns.length);

function townForPointXY(x,y){
  for(const t of towns){
    const b=t.bb;
    if(x<b.minX || x>b.maxX || y<b.minY || y>b.maxY) continue;
    // towns are Polygon only per your metadata
    if(pointInPoly(x,y,t.coords)) return t.name;
  }
  return "";
}

function findCandidate(row){
  // 1) common nested objects
  const keys = [
    "mad_nearest","madNearest","mad","nearestMad","nearest","addressAuthority",
    "proposed","suggested","candidate","best"
  ];
  for(const k of keys){
    const v = row[k];
    if(v && typeof v==="object"){
      const tn = v.town ?? v.city;
      const sn = v.street_name ?? v.streetName ?? v.street;
      const no = v.street_no ?? v.streetNo ?? v.number ?? v.street_number;
      const z  = v.zip ?? v.postcode ?? v.post_code;
      if(tn || sn || no || z) return v;
    }
  }
  // 2) top-level fallback fields
  const top = {
    town: row.mad_town ?? row.nearest_town ?? row.cand_town,
    street_no: row.mad_street_no ?? row.nearest_street_no ?? row.cand_street_no,
    street_name: row.mad_street_name ?? row.nearest_street_name ?? row.cand_street_name,
    zip: row.mad_zip ?? row.nearest_zip ?? row.cand_zip
  };
  if(top.town || top.street_no || top.street_name || top.zip) return top;
  return null;
}

// ---- Build patch map from QUARANTINE rows ----
const patchMap = new Map();
let qTotal=0, qParseErr=0, qNoXY=0, qNoTown=0, qNoCand=0, qPromote=0, qKeep=0;

const rlQ = readline.createInterface({ input: fs.createReadStream(quarantinePath, {encoding:"utf8"}), crlfDelay: Infinity });
for await (const line of rlQ){
  const t=line.trim(); if(!t) continue;
  let row;
  try{ row=JSON.parse(t); }catch{ qParseErr++; continue; }
  qTotal++;

  const id = row.property_id || row.parcel_id;
  if(!id){ qKeep++; continue; }

  const x = Number(row.x_sp);
  const y = Number(row.y_sp);
  if(!Number.isFinite(x) || !Number.isFinite(y)){ qNoXY++; qKeep++; continue; }

  const pipTown = townForPointXY(x,y);
  if(!pipTown){ qNoTown++; qKeep++; continue; }

  const cand = findCandidate(row);
  if(!cand){ qNoCand++; qKeep++; continue; }

  const candTownRaw = cand.town ?? cand.city ?? "";
  const candTown = normTown(candTownRaw);

  if(candTown && candTown === pipTown){
    const candStreetNo = String(cand.street_no ?? cand.streetNo ?? cand.number ?? cand.street_number ?? "").trim();
    const candStreetName = String(cand.street_name ?? cand.streetName ?? cand.street ?? "").trim();
    const candZip = zip5(cand.zip ?? cand.postcode ?? cand.post_code ?? "");

    // Require at least street_name to be meaningful
    if(candStreetName){
      patchMap.set(id, {
        street_no: candStreetNo,
        street_name: candStreetName,
        town: pipTown,
        zip: candZip,
        candTownRaw,
        pipTown
      });
      qPromote++;
      continue;
    }
  }
  qKeep++;
}

console.log("Quarantine scan:", { qTotal, qParseErr, qNoXY, qNoTown, qNoCand, qPromote, qKeep, patchKeys: patchMap.size });

// ---- Stream base, apply patches ----
const out = fs.createWriteStream(outPath, {encoding:"utf8"});
const outProm = outPromotedPath ? fs.createWriteStream(outPromotedPath, {encoding:"utf8"}) : null;
const outStill = outStillQPath ? fs.createWriteStream(outStillQPath, {encoding:"utf8"}) : null;

let total=0, applied=0, baseParseErr=0;

const rlB = readline.createInterface({ input: fs.createReadStream(basePath, {encoding:"utf8"}), crlfDelay: Infinity });
for await (const line of rlB){
  const t=line.trim(); if(!t) continue;
  let row;
  try{ row=JSON.parse(t); }catch{ baseParseErr++; continue; }
  total++;

  const id = row.property_id || row.parcel_id;
  const patch = id ? patchMap.get(id) : null;

  if(patch){
    if(patch.street_no) row.street_no = patch.street_no;
    if(patch.street_name) row.street_name = patch.street_name;
    row.town = patch.town || row.town;
    if(patch.zip) row.zip = patch.zip;

    row.address_verified = row.address_verified || {};
    row.address_verified.town_pip_stateplane = {
      verifiedTown: patch.pipTown,
      candidateTownRaw: patch.candTownRaw,
      at: NOW
    };

    if(row.street_no && row.street_name && row.town && row.zip){
      row.address_label = `${row.street_no} ${row.street_name}, ${row.town}, MA ${zip5(row.zip)}`;
    }

    applied++;
    if(outProm) outProm.write(JSON.stringify(row) + "\n");
  } else {
    if(outStill && row.address_tier==="C") outStill.write(JSON.stringify(row) + "\n");
  }

  out.write(JSON.stringify(row) + "\n");

  if(total % 500000 === 0) console.log(`...processed ${total.toLocaleString()} rows`);
}

out.end();
if(outProm) outProm.end();
if(outStill) outStill.end();

const report = {
  created_at: NOW,
  base: basePath,
  quarantine: quarantinePath,
  townsGeo: townsGeoPath,
  out: outPath,
  counts: {
    base_total: total,
    base_parseErr: baseParseErr,
    quarantine_total: qTotal,
    quarantine_parseErr: qParseErr,
    quarantine_promoted_candidates: qPromote,
    quarantine_kept: qKeep,
    quarantine_missing_xy: qNoXY,
    quarantine_no_pip_town: qNoTown,
    quarantine_no_candidate_fields: qNoCand,
    applied_to_base: applied
  }
};

fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
console.log("DONE.");
console.log(JSON.stringify(report, null, 2));

// hard-exit safeguard
setTimeout(() => process.exit(0), 250).unref();
