import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import readline from "node:readline";

function argMap(argv){const m={};for(let i=2;i<argv.length;i++){const a=argv[i];if(a.startsWith("--")){const k=a.slice(2);const v=argv[i+1]&&!argv[i+1].startsWith("--")?argv[++i]:"true";m[k]=v;}}return m;}
function readJSON(p){let s=fs.readFileSync(p,"utf8"); if(s.charCodeAt(0)===0xFEFF) s=s.slice(1); return JSON.parse(s);}
function writeJSON(p,obj){fs.mkdirSync(path.dirname(p),{recursive:true});fs.writeFileSync(p,JSON.stringify(obj,null,2),"utf8");}
function nowTag(){return new Date().toISOString().replace(/[:.]/g,"-");}
function sha256File(p){
  const h=crypto.createHash("sha256");
  const fd=fs.openSync(p,"r");
  const buf=Buffer.alloc(1024*1024);
  let b=0;
  while((b=fs.readSync(fd,buf,0,buf.length,null))>0){ h.update(buf.subarray(0,b)); }
  fs.closeSync(fd);
  return h.digest("hex");
}
function safeLower(v){ return (v??"").toString().trim().toLowerCase(); }
function isSaneNum(n){ return typeof n==="number" && Number.isFinite(n) && n>=0; }

function chooseField(muni, mass, field){
  const mv = muni ? muni[field] : undefined;
  const sv = mass ? mass[field] : undefined;
  const sane = (v)=>{
    if(v===null||v===undefined) return false;
    if(typeof v==="number") return isSaneNum(v) && v!==0;
    if(typeof v==="string") return v.trim().length>0;
    return true;
  };
  if(sane(mv)) return { value: mv, source:"city_assessor", flags:[] };
  if(sane(sv)) return { value: sv, source:"massgis_statewide", flags:["FALLBACK_USED"] };
  return { value: null, source:"unknown", flags:["MISSING"] };
}

async function loadNdjsonToMap(filePath, keyFn, label){
  const map = new Map();
  const rs = fs.createReadStream(filePath, {encoding:"utf8"});
  const rl = readline.createInterface({input: rs, crlfDelay: Infinity});
  let n=0;
  for await (const line of rl){
    if(!line.trim()) continue;
    const obj = JSON.parse(line);
    const k = keyFn(obj);
    if(k) map.set(k, obj);
    n++;
    if(n % 500000 === 0) console.log(`[progress] loaded ${label} ${n}`);
  }
  return { map, rows:n };
}

const args=argMap(process.argv);
const root=args.root?path.resolve(args.root):process.cwd();
const cfgPath=args.config?path.resolve(args.config):path.join(root,"phase4_global_master_merge_attach_config_v4.json");
const cfg=readJSON(cfgPath);

const propertiesIn = path.join(root, cfg.properties_in);
const cityPtr = path.join(root, cfg.city_master_ptr);
const massPtr = path.join(root, cfg.massgis_master_ptr);
const outDir = path.join(root, cfg.out_dir);
const globalOutDir = path.join(root, cfg.global_master_out_dir);
fs.mkdirSync(outDir, {recursive:true});
fs.mkdirSync(globalOutDir, {recursive:true});
fs.mkdirSync(path.join(root,"publicData","_audit","phase4_assessor"), {recursive:true});

console.log(`[info] properties_in: ${propertiesIn}`);
console.log(`[info] city_master_ptr: ${cityPtr}`);
console.log(`[info] massgis_master_ptr: ${massPtr}`);

const cityMeta = readJSON(cityPtr);
const massMeta = readJSON(massPtr);

const municipalList = cityMeta?.cities || [];
const massMasterPath = massMeta?.master_ndjson;
if(!massMasterPath) throw new Error(`[err] massgis CURRENT pointer missing master_ndjson: ${massPtr}`);

console.log(`[info] loading MassGIS master: ${massMasterPath}`);
const mass = await loadNdjsonToMap(massMasterPath, (r)=>r?.parcel_id_norm, "massgis");
console.log(`[done] massgis loaded rows=${mass.rows} keys=${mass.map.size}`);

console.log(`[info] loading municipal masters...`);
const muniMaps = new Map(); // city -> map
let muniTotalRows=0, muniTotalKeys=0;
for(const c of municipalList){
  const city = c.city;
  const p = c.master_ndjson;
  console.log(`[info] municipal: ${city} -> ${p}`);
  const loaded = await loadNdjsonToMap(p, (r)=>r?.parcel_id_norm, `muni:${city}`);
  muniMaps.set(city, loaded.map);
  muniTotalRows += loaded.rows;
  muniTotalKeys += loaded.map.size;
}
console.log(`[done] municipal loaded rows=${muniTotalRows} keys=${muniTotalKeys}`);

const tag = nowTag();
const globalMasterPath = path.join(globalOutDir, `assessor_master__GLOBAL__${tag}__V4.ndjson`);
const globalWS = fs.createWriteStream(globalMasterPath, {encoding:"utf8"});

// Build in-memory globalIndex WHILE writing global master (avoids reopening)
const globalIndex = new Map();
let written=0, massOnly=0, muniPlus=0, muniOnly=0;

for(const [pid, rec] of mass.map.entries()){
  const outRec = { ...rec, evidence:{...(rec.evidence||{}), source_rank:"massgis"} };
  globalIndex.set(pid, outRec);
  globalWS.write(JSON.stringify(outRec)+"\n");
  written++; massOnly++;
  if(written % 500000 === 0) console.log(`[progress] global master wrote ${written}`);
}

for(const [city, m] of muniMaps.entries()){
  for(const [pid, rec] of m.entries()){
    if(globalIndex.has(pid)){
      const merged = { ...globalIndex.get(pid), ...rec, evidence:{...(rec.evidence||{}), source_rank:"municipal_plus_massgis"} };
      globalIndex.set(pid, merged);
      muniPlus++;
    } else {
      globalIndex.set(pid, { ...rec, evidence:{...(rec.evidence||{}), source_rank:"municipal_only"} });
      muniOnly++;
    }
    globalWS.write(JSON.stringify({ ...rec, city, evidence:{...(rec.evidence||{}), source_rank:"municipal"} })+"\n");
  }
}
await new Promise((resolve,reject)=>{ globalWS.on("finish", resolve); globalWS.on("error", reject); globalWS.end(); });

console.log(`[done] global master written_lines=${massOnly + muniTotalKeys} (massOnly=${massOnly}, muniPlus=${muniPlus}, muniOnly=${muniOnly})`);
console.log(`[info] attaching to properties... index_keys=${globalIndex.size}`);

const outPath = path.join(outDir, `properties__with_assessor_global_best__${tag}.ndjson`);
const outWS = fs.createWriteStream(outPath, {encoding:"utf8"});
const rs = fs.createReadStream(propertiesIn, {encoding:"utf8"});
const rl = readline.createInterface({input: rs, crlfDelay: Infinity});

let processed=0, matched=0, unmatched=0, usedMuni=0, usedMass=0;

for await (const line of rl){
  if(!line.trim()) continue;
  const prop = JSON.parse(line);
  const pid = prop?.parcel_id_norm || prop?.parcel_id_raw_norm || prop?.parcel_id || null;

  let muniRec=null, massRec=null;
  if(pid){
    const city = safeLower(prop?.source_city || prop?.city || prop?.address_city || "");
    const cityMap = muniMaps.get(city);
    if(cityMap) muniRec = cityMap.get(pid) || null;
    massRec = mass.map.get(pid) || null;
  }

  const best = {
    valuation: {
      total_value: chooseField(muniRec, massRec, "assessed_total"),
      land_value: chooseField(muniRec, massRec, "assessed_land"),
      building_value: chooseField(muniRec, massRec, "assessed_building"),
      other_value: chooseField(muniRec, massRec, "assessed_other"),
      assessed_year: chooseField(muniRec, massRec, "assessed_year")
    },
    building: {
      year_built: chooseField(muniRec, massRec, "year_built_est"),
      units: chooseField(muniRec, massRec, "units_est"),
      bedrooms: chooseField(muniRec, massRec, "bedrooms"),
      bathrooms: chooseField(muniRec, massRec, "bathrooms"),
      living_area_sqft: chooseField(muniRec, massRec, "living_area_sqft"),
      building_area_sqft: chooseField(muniRec, massRec, "building_area_sqft")
    },
    transaction: {
      last_sale_date: chooseField(muniRec, massRec, "last_sale_date"),
      last_sale_price: chooseField(muniRec, massRec, "last_sale_price"),
      deed_book: chooseField(muniRec, massRec, "deed_book"),
      deed_page: chooseField(muniRec, massRec, "deed_page"),
      registry_id: chooseField(muniRec, massRec, "registry_id")
    },
    ownership: {
      owner_name_1: chooseField(muniRec, massRec, "owner_name_1"),
      owner_company: chooseField(muniRec, massRec, "owner_company"),
      owner_type: chooseField(muniRec, massRec, "owner_type_norm"),
      mailing_address_full: chooseField(muniRec, massRec, "mailing_address_full"),
      mailing_city: chooseField(muniRec, massRec, "mailing_city"),
      mailing_state: chooseField(muniRec, massRec, "mailing_state"),
      mailing_zip: chooseField(muniRec, massRec, "mailing_zip")
    }
  };

  const source_map = {};
  const fallback_fields = [];
  const walk = (obj, prefix="")=>{
    for(const [k,v] of Object.entries(obj)){
      const p = prefix ? `${prefix}.${k}` : k;
      if(v && typeof v === "object" && ("source" in v) && ("value" in v)){
        source_map[p] = v.source;
        if((v.flags||[]).includes("FALLBACK_USED")) fallback_fields.push(p);
      } else if(v && typeof v === "object" && !Array.isArray(v)){
        walk(v, p);
      }
    }
  };
  walk(best);

  const attached = {
    ...prop,
    assessor_by_source: {
      city_assessor_raw: muniRec ? muniRec : null,
      massgis_statewide_raw: massRec ? massRec : null
    },
    assessor_best: best,
    assessor_source_map: source_map,
    assessor_fallback_fields: fallback_fields
  };

  const hasAny = !!(muniRec || massRec);
  if(hasAny){
    matched++;
    if(muniRec){ usedMuni++; }
    else { usedMass++; }
  } else {
    unmatched++;
  }

  outWS.write(JSON.stringify(attached)+"\n");
  processed++;
  if(processed % 200000 === 0){
    console.log(`[progress] processed ${processed} (matched=${matched}, unmatched=${unmatched}, muni=${usedMuni}, massOnly=${usedMass})`);
  }
}
await new Promise((resolve,reject)=>{ outWS.on("finish", resolve); outWS.on("error", reject); outWS.end(); });

const auditPath = path.join(root, "publicData/_audit/phase4_assessor", `phase4_global_master_merge_attach__${tag}__V4.json`);
const currentPtr = path.join(outDir, "CURRENT_PROPERTIES_WITH_ASSESSOR_GLOBAL_BEST.json");

const audit = {
  created_at: new Date().toISOString(),
  config: cfgPath,
  outputs: { globalMasterPath, propertiesOut: outPath, currentPtr, auditPath },
  stats: { processed, matched, unmatched, usedMuni, usedMass, globalIndexKeys: globalIndex.size },
  hashes: { global_master_sha256: sha256File(globalMasterPath), properties_out_sha256: sha256File(outPath) },
  notes: cfg.notes || []
};

writeJSON(auditPath, audit);
writeJSON(currentPtr, { updated_at: new Date().toISOString(), note: "AUTO: Phase4 GLOBAL merge+attach v4", properties_ndjson: outPath, audit: auditPath });

console.log(`[done] wrote audit: ${auditPath}`);
console.log(`[ok] wrote CURRENT pointer: ${currentPtr}`);
console.log(`[done] Phase 4 GLOBAL merge+attach v4 complete.`);
