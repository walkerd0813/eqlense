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

// Stable hash (djb2) for sharding
function shardFor(key, shards){
  let h=5381;
  for(let i=0;i<key.length;i++){ h=((h<<5)+h) + key.charCodeAt(i); h|=0; }
  const u = h >>> 0;
  return u % shards;
}
function shardName(i){ return String(i).padStart(3,"0"); }

async function streamNdjson(inPath, onObj){
  const rs = fs.createReadStream(inPath, {encoding:"utf8"});
  const rl = readline.createInterface({input: rs, crlfDelay: Infinity});
  for await (const line of rl){
    if(!line.trim()) continue;
    onObj(JSON.parse(line));
  }
}

async function loadNdjsonToMap(filePath, keyFn){
  const map = new Map();
  if(!fs.existsSync(filePath)) return map;
  const rs = fs.createReadStream(filePath, {encoding:"utf8"});
  const rl = readline.createInterface({input: rs, crlfDelay: Infinity});
  for await (const line of rl){
    if(!line.trim()) continue;
    const obj = JSON.parse(line);
    const k = keyFn(obj);
    if(k) map.set(k, obj);
  }
  return map;
}

const args=argMap(process.argv);
const root=args.root?path.resolve(args.root):process.cwd();
const cfgPath=args.config?path.resolve(args.config):path.join(root,"phase4_global_master_merge_attach_config_v5.json");
const cfg=readJSON(cfgPath);

const propertiesIn = path.join(root, cfg.properties_in);
const cityPtr = path.join(root, cfg.city_master_ptr);
const massPtr = path.join(root, cfg.massgis_master_ptr);
const outDir = path.join(root, cfg.out_dir);
const workDir = path.join(root, cfg.work_dir);
const shards = Number(cfg.shards || 128);

fs.mkdirSync(outDir, {recursive:true});
fs.mkdirSync(workDir, {recursive:true});
fs.mkdirSync(path.join(workDir,"massgis_shards"), {recursive:true});
fs.mkdirSync(path.join(workDir,"properties_shards"), {recursive:true});
fs.mkdirSync(path.join(root,"publicData","_audit","phase4_assessor"), {recursive:true});

console.log(`[info] properties_in: ${propertiesIn}`);
console.log(`[info] city_master_ptr: ${cityPtr}`);
console.log(`[info] massgis_master_ptr: ${massPtr}`);
console.log(`[info] work_dir: ${workDir}`);
console.log(`[info] shards: ${shards}`);

const cityMeta = readJSON(cityPtr);
const massMeta = readJSON(massPtr);

const municipalList = cityMeta?.cities || [];
const massMasterPath = massMeta?.master_ndjson;
if(!massMasterPath) throw new Error(`[err] massgis CURRENT pointer missing master_ndjson: ${massPtr}`);

console.log(`[info] loading municipal masters into memory...`);
const muniMaps = new Map(); // city -> Map(parcel_id_norm -> record)
let muniKeys=0;
for(const c of municipalList){
  const city = c.city;
  const p = c.master_ndjson;
  console.log(`[info] municipal: ${city} -> ${p}`);
  const map = await loadNdjsonToMap(p, (r)=>r?.parcel_id_norm);
  muniMaps.set(city, map);
  muniKeys += map.size;
}
console.log(`[done] municipal keys=${muniKeys} cities=${muniMaps.size}`);

const tag = nowTag();

function openShardWriters(dir, prefix){
  const writers = new Array(shards);
  for(let i=0;i<shards;i++){
    const p = path.join(dir, `${prefix}_${shardName(i)}.ndjson`);
    writers[i] = fs.createWriteStream(p, {encoding:"utf8"});
  }
  return writers;
}
async function closeShardWriters(writers){
  await Promise.all(writers.map(ws=>new Promise((res,rej)=>{ ws.on("finish",res); ws.on("error",rej); ws.end(); })));
}

// 1) Shard MassGIS master
console.log(`[start] sharding MassGIS master -> ${path.join(workDir,"massgis_shards")}`);
const massWriters = openShardWriters(path.join(workDir,"massgis_shards"), "massgis");
let massRows=0;
await streamNdjson(massMasterPath, (rec)=>{
  const pid = rec?.parcel_id_norm;
  if(!pid) return;
  const s = shardFor(pid, shards);
  massWriters[s].write(JSON.stringify(rec)+"\n");
  massRows++;
  if(massRows % 500000 === 0) console.log(`[progress] massgis rows sharded ${massRows}`);
});
await closeShardWriters(massWriters);
console.log(`[done] massgis sharded rows=${massRows}`);

// 2) Shard properties input
console.log(`[start] sharding properties -> ${path.join(workDir,"properties_shards")}`);
const propWriters = openShardWriters(path.join(workDir,"properties_shards"), "props");
let propRows=0;
await streamNdjson(propertiesIn, (prop)=>{
  const pid = prop?.parcel_id_norm || prop?.parcel_id_raw_norm || prop?.parcel_id || null;
  const s = pid ? shardFor(pid, shards) : 0;
  propWriters[s].write(JSON.stringify(prop)+"\n");
  propRows++;
  if(propRows % 500000 === 0) console.log(`[progress] properties rows sharded ${propRows}`);
});
await closeShardWriters(propWriters);
console.log(`[done] properties sharded rows=${propRows}`);

// 3) Attach shard-by-shard
const outPath = path.join(outDir, `properties__with_assessor_global_best__${tag}__V5.ndjson`);
const outWS = fs.createWriteStream(outPath, {encoding:"utf8"});

let processed=0, matched=0, unmatched=0, usedMuni=0, usedMass=0;

for(let i=0;i<shards;i++){
  const massShardPath = path.join(workDir,"massgis_shards",`massgis_${shardName(i)}.ndjson`);
  const propsShardPath = path.join(workDir,"properties_shards",`props_${shardName(i)}.ndjson`);

  const massMap = await loadNdjsonToMap(massShardPath, (r)=>r?.parcel_id_norm);
  if(!fs.existsSync(propsShardPath)){
    console.log(`[info] shard ${shardName(i)} massgis_keys=${massMap.size} props=missing`);
    continue;
  }
  console.log(`[info] shard ${shardName(i)} massgis_keys=${massMap.size} props=present`);

  const rs = fs.createReadStream(propsShardPath, {encoding:"utf8"});
  const rl = readline.createInterface({input: rs, crlfDelay: Infinity});

  for await (const line of rl){
    if(!line.trim()) continue;
    const prop = JSON.parse(line);
    const pid = prop?.parcel_id_norm || prop?.parcel_id_raw_norm || prop?.parcel_id || null;

    let muniRec=null, massRec=null;
    if(pid){
      const city = safeLower(prop?.source_city || prop?.city || prop?.address_city || "");
      const cityMap = muniMaps.get(city);
      if(cityMap) muniRec = cityMap.get(pid) || null;
      massRec = massMap.get(pid) || null;
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
      if(muniRec) usedMuni++;
      else usedMass++;
    } else {
      unmatched++;
    }

    outWS.write(JSON.stringify(attached)+"\n");
    processed++;
    if(processed % 200000 === 0){
      console.log(`[progress] processed ${processed} (matched=${matched}, unmatched=${unmatched}, muni=${usedMuni}, massOnly=${usedMass})`);
    }
  }
}

await new Promise((resolve,reject)=>{ outWS.on("finish", resolve); outWS.on("error", reject); outWS.end(); });

const auditPath = path.join(root, "publicData/_audit/phase4_assessor", `phase4_global_master_merge_attach__${tag}__V5.json`);
const currentPtr = path.join(outDir, "CURRENT_PROPERTIES_WITH_ASSESSOR_GLOBAL_BEST.json");

const audit = {
  created_at: new Date().toISOString(),
  config: cfgPath,
  outputs: { propertiesOut: outPath, currentPtr, auditPath, workDir, shards },
  stats: { processed, matched, unmatched, usedMuni, usedMass, muniCities: muniMaps.size, muniKeys, massgisShardedRows: massRows },
  hashes: { properties_out_sha256: sha256File(outPath) },
  notes: cfg.notes || []
};

writeJSON(auditPath, audit);
writeJSON(currentPtr, { updated_at: new Date().toISOString(), note: "AUTO: Phase4 GLOBAL merge+attach v5 (sharded)", properties_ndjson: outPath, audit: auditPath });

console.log(`[done] wrote audit: ${auditPath}`);
console.log(`[ok] wrote CURRENT pointer: ${currentPtr}`);
console.log(`[done] Phase 4 GLOBAL merge+attach v5 complete.`);
