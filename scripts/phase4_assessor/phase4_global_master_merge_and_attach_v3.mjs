import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import readline from "node:readline";

function argMap(argv){const m={};for(let i=2;i<argv.length;i++){const a=argv[i];if(a.startsWith("--")){const k=a.slice(2);const v=argv[i+1]&&!argv[i+1].startsWith("--")?argv[++i]:"true";m[k]=v;}}return m;}
function readJSON(p){let s=fs.readFileSync(p,"utf8"); if(s.charCodeAt(0)===0xFEFF) s=s.slice(1); return JSON.parse(s);}
function writeJSON(p,obj){fs.mkdirSync(path.dirname(p),{recursive:true});fs.writeFileSync(p,JSON.stringify(obj,null,2),"utf8");}
function nowTag(){return new Date().toISOString().replace(/[:.]/g,"-");}
function sha256File(p){const h=crypto.createHash("sha256");const fd=fs.openSync(p,"r");const buf=Buffer.alloc(1024*1024);let b=0;while((b=fs.readSync(fd,buf,0,buf.length,null))>0){h.update(buf.subarray(0,b));}fs.closeSync(fd);return h.digest("hex");}
function sha256Text(s){return crypto.createHash("sha256").update(s).digest("hex");}

function normStr(v){if(v===null||v===undefined) return null; const s=String(v).trim(); return s? s : null;}
function toNumber(v){if(v===null||v===undefined) return null; const s=String(v).replace(/[$,]/g,"").trim(); if(!s) return null; const n=Number(s); return Number.isFinite(n)?n:null;}
function toInt(v){const n=toNumber(v); return n===null?null:Math.trunc(n);}
function parseDate(v){const s=normStr(v); if(!s) return null; if(/^\d{4}-\d{2}-\d{2}$/.test(s)) return s; const t=Date.parse(s); return Number.isFinite(t)?new Date(t).toISOString().slice(0,10):null;}

function parcelKeyVariants(v){
  const s0=normStr(v); if(!s0) return [];
  const s=s0.replace(/\s+/g,"");
  const alnum=s.replace(/[^0-9A-Za-z]/g,"");
  const noLeadZeros=alnum.replace(/^0+/,"");
  const variants=new Set([s,alnum,noLeadZeros].filter(Boolean));
  return [...variants];
}

function sane(dotted, v){
  if(v===null||v===undefined) return false;
  if(typeof v==="string" && !v.trim()) return false;
  if(dotted.includes("assessed_") || dotted.includes("sale_price")) {
    const n=toNumber(v); if(n===null||n<=0) return false;
    if(dotted.includes("sale_price") && n<1000) return false;
    return true;
  }
  if(dotted.includes("units")) { const n=toInt(v); return n!==null && n>0 && n<=500; }
  if(dotted.includes("year_built")) { const n=toInt(v); return n!==null && n>=1600 && n<=2035; }
  if(dotted.includes("date")) return !!parseDate(v);
  return true;
}

function setDeep(obj,dotted,val){
  const parts=dotted.split(".");
  let cur=obj;
  for(let i=0;i<parts.length-1;i++){
    const p=parts[i];
    if(!cur[p]||typeof cur[p]!=="object") cur[p]={};
    cur=cur[p];
  }
  cur[parts[parts.length-1]]=val;
}

function cityHints(rec){
  const cands=[rec.source_city,rec.city,rec.city_norm,rec.town,rec.town_name,rec.town_norm,rec.address_city,rec.addr_city]
    .map(x=>(x??"").toString().trim().toLowerCase()).filter(Boolean);
  const uniq=[]; for(const c of cands) if(!uniq.includes(c)) uniq.push(c);
  return uniq;
}

function findLatestPropertiesNdjson(root){
  const dir=path.join(root,"publicData","properties");
  const files=fs.readdirSync(dir).filter(f=>f.toLowerCase().endsWith(".ndjson")).map(f=>path.join(dir,f));
  if(!files.length) throw new Error(`[err] no ndjson in ${dir}`);
  files.sort((a,b)=>fs.statSync(b).mtimeMs-fs.statSync(a).mtimeMs);
  return files[0];
}

async function loadNdjsonToGlobalMap(ndjsonPath, sourceLabel){
  const map=new Map();
  const rl=readline.createInterface({input:fs.createReadStream(ndjsonPath,{encoding:"utf8"}), crlfDelay:Infinity});
  for await (const line of rl){
    if(!line.trim()) continue;
    let rec; try{rec=JSON.parse(line);}catch{continue;}
    const pid=rec.parcel_id_norm||rec.parcel_id||rec.parcel_id_raw||null;
    for(const k of parcelKeyVariants(pid)) if(!map.has(k)) map.set(k, { rec, sourceLabel });
  }
  return map;
}

function mergeRecords(muniRec, massRec, dottedFields){
  // Both are assessor master records shape. Merge into a global master record with per-field source_map.
  const out = {};
  // carry ids
  out.city = (muniRec?.city ?? massRec?.city ?? "").toString().trim().toLowerCase();
  out.parcel_id_norm = muniRec?.parcel_id_norm ?? massRec?.parcel_id_norm ?? null;

  // flat fields we store in master (keep consistent with your v2 master)
  const flatMap = {
    "owner.owner_name_1":"owner_name_1",
    "owner.owner_name_2":"owner_name_2",
    "owner.owner_company":"owner_company",
    "owner.owner_type_norm":"owner_type_norm",
    "owner.mailing_address_full":"mailing_address_full",
    "owner.mailing_city":"mailing_city",
    "owner.mailing_state":"mailing_state",
    "owner.mailing_zip":"mailing_zip",
    "owner.owner_occupied_flag":"owner_occupied_flag",

    "valuation.assessed_land":"assessed_land",
    "valuation.assessed_building":"assessed_building",
    "valuation.assessed_other":"assessed_other",
    "valuation.assessed_total":"assessed_total",
    "valuation.assessed_exemptions":"assessed_exemptions",
    "valuation.assessed_year":"assessed_year",

    "txn.last_sale_date":"last_sale_date",
    "txn.last_sale_price":"last_sale_price",
    "txn.deed_book":"deed_book",
    "txn.deed_page":"deed_page",
    "txn.registry_id":"registry_id",

    "building.year_built_est":"year_built_est",
    "building.units_est":"units_est",

    "lot.lot_size_raw":"lot_size_raw",
    "lot.lot_units_raw":"lot_units_raw",

    "zoning.zoning_raw":"zoning_raw"
  };

  const source_map = {};
  for(const dotted of dottedFields){
    const flat = flatMap[dotted];
    const mv = muniRec ? muniRec[flat] : null;
    const sv = massRec ? massRec[flat] : null;

    let chosen = null;
    let src = "unknown";
    if(sane(dotted, mv)) { chosen = mv; src="city_assessor"; }
    else if(sane(dotted, sv)) { chosen = sv; src="statewide_parcels"; }
    else { chosen = null; src="unknown"; }

    out[flat] = chosen;
    if(chosen !== null) source_map[dotted] = src;
  }
  out.source_map = source_map;

  // evidence
  const asOf = new Date().toISOString();
  out.evidence = {
    as_of: asOf,
    municipal_present: !!muniRec,
    massgis_present: !!massRec,
    municipal_source_id: muniRec?.evidence?.municipal_source_id ?? null,
    municipal_source_path: muniRec?.evidence?.municipal_source_path ?? null,
    massgis_layer: massRec?.evidence?.massgis_layer ?? massRec?.evidence?.layer ?? null,
    massgis_where: massRec?.evidence?.massgis_where ?? null,
    dataset_hash: null
  };
  out.qa_flags = [];
  return out;
}

function confidenceFor(src){ if(src==="city_assessor") return "A"; if(src==="statewide_parcels") return "B"; return "C"; }

function buildBest(masterRec, dottedFields){
  const best={}; const source_map={}; const fallback_fields=[];
  const asOf=masterRec?.evidence?.as_of||null;
  const datasetHash=masterRec?.evidence?.dataset_hash||null;

  const flatMap = {
    "owner.owner_name_1":"owner_name_1",
    "owner.owner_name_2":"owner_name_2",
    "owner.owner_company":"owner_company",
    "owner.owner_type_norm":"owner_type_norm",
    "owner.mailing_address_full":"mailing_address_full",
    "owner.mailing_city":"mailing_city",
    "owner.mailing_state":"mailing_state",
    "owner.mailing_zip":"mailing_zip",
    "owner.owner_occupied_flag":"owner_occupied_flag",

    "valuation.assessed_land":"assessed_land",
    "valuation.assessed_building":"assessed_building",
    "valuation.assessed_other":"assessed_other",
    "valuation.assessed_total":"assessed_total",
    "valuation.assessed_exemptions":"assessed_exemptions",
    "valuation.assessed_year":"assessed_year",

    "txn.last_sale_date":"last_sale_date",
    "txn.last_sale_price":"last_sale_price",
    "txn.deed_book":"deed_book",
    "txn.deed_page":"deed_page",
    "txn.registry_id":"registry_id",

    "building.year_built_est":"year_built_est",
    "building.units_est":"units_est",

    "lot.lot_size_raw":"lot_size_raw",
    "lot.lot_units_raw":"lot_units_raw",

    "zoning.zoning_raw":"zoning_raw"
  };

  for(const dotted of dottedFields){
    const flat=flatMap[dotted];
    const v0 = masterRec ? masterRec[flat] : null;
    const src0 = masterRec?.source_map?.[dotted] ?? "unknown";
    const src = src0==="city_assessor" ? "city_assessor" : (src0==="statewide_parcels" ? "statewide_parcels" : "unknown");

    const obj={
      value: v0 ?? null,
      source: src,
      as_of: asOf,
      dataset_hash: datasetHash,
      confidence: confidenceFor(src),
      flags: []
    };
    if(obj.source!=="city_assessor" && obj.value!==null) obj.flags.push("FALLBACK_USED");
    if(obj.source==="statewide_parcels" && obj.value!==null) fallback_fields.push(dotted);
    source_map[dotted]=obj.source;

    if(!sane(dotted,obj.value)){ obj.value=null; obj.flags.push("MISSING_OR_UNSANE"); }
    setDeep(best,dotted,obj);
  }
  return { best, source_map, fallback_fields };
}

const args=argMap(process.argv);
const root=args.root?path.resolve(args.root):process.cwd();
const cfgPath=args.config?path.resolve(args.config):path.join(root,"phase4_global_master_merge_attach_config_v3.json");
const cfg=readJSON(cfgPath);

const propsIn = (cfg.inputs.properties_ndjson||"").startsWith("AUTO_LATEST") ? findLatestPropertiesNdjson(root) : path.resolve(root,cfg.inputs.properties_ndjson);

const cityPtr = readJSON(path.join(root,cfg.inputs.city_master_ptr));
const massPtrPath = path.join(root,cfg.inputs.massgis_master_ptr);
const massPtr = readJSON(massPtrPath);

const dottedFields = cfg.field_contract?.dotted_fields || [];

console.log(`[info] properties_in: ${propsIn}`);
console.log(`[info] city_master_ptr: ${path.join(root,cfg.inputs.city_master_ptr)}`);
console.log(`[info] massgis_master_ptr: ${massPtrPath}`);

const massNd = massPtr.master_ndjson;
console.log(`[info] loading MassGIS master: ${massNd}`);
const massMap = await loadNdjsonToGlobalMap(massNd, "statewide_parcels");

console.log(`[info] loading municipal masters...`);
const muniMaps = [];
for(const c of (cityPtr.cities||[])){
  console.log(`[info] municipal: ${c.city} -> ${c.master_ndjson}`);
  muniMaps.push({ city:c.city, map: await loadNdjsonToGlobalMap(c.master_ndjson, "city_assessor") });
}

// Build global merged master
const outDir = path.join(root, cfg.outputs.global_master_dir || "publicData/assessors/_global_master");
fs.mkdirSync(outDir,{recursive:true});
const tag=nowTag();
const outMasterNd = path.join(outDir, `assessor_master__GLOBAL__${tag}__V3.ndjson`);
const outAudit = path.join(root,"publicData","_audit","phase4_assessor",`phase4_global_master_merge_attach__${tag}.json`);
fs.mkdirSync(path.dirname(outAudit),{recursive:true});

console.log(`[info] writing GLOBAL master: ${outMasterNd}`);

const wsM = fs.createWriteStream(outMasterNd,{encoding:"utf8"});

let merged=0, massOnly=0, muniOnly=0, muniPlus=0;

// We iterate MassGIS map keys and merge with any municipal record found by same key.
// NOTE: keys map to same record; we need unique parcel ids, so we'll iterate records not keys.
const seenParcel = new Set();

// helper to pick a municipal rec for a parcel (first match among cities)
function findMuniForKeys(keys){
  for(const {map} of muniMaps){
    for(const k of keys) if(map.has(k)) return map.get(k).rec;
  }
  return null;
}

// write merged for MassGIS records
for(const {rec:massRec} of massMap.values()){
  const pid = massRec.parcel_id_norm;
  if(!pid) continue;
  if(seenParcel.has(pid)) continue;
  seenParcel.add(pid);

  const keys = parcelKeyVariants(pid);
  const muniRec = findMuniForKeys(keys);

  const outRec = mergeRecords(muniRec, massRec, dottedFields);
  outRec.evidence.dataset_hash = sha256Text(JSON.stringify(outRec.source_map));
  wsM.write(JSON.stringify(outRec)+"\n");
  merged++;
  if(muniRec) muniPlus++; else massOnly++;
  if(merged % 500000 === 0) console.log(`[progress] global master wrote ${merged}`);
}

// Add municipal-only parcels not present in MassGIS (rare)
for(const {map} of muniMaps){
  for(const {rec:muniRec} of map.values()){
    const pid = muniRec.parcel_id_norm;
    if(!pid) continue;
    if(seenParcel.has(pid)) continue;
    seenParcel.add(pid);
    const outRec = mergeRecords(muniRec, null, dottedFields);
    outRec.evidence.dataset_hash = sha256Text(JSON.stringify(outRec.source_map));
    wsM.write(JSON.stringify(outRec)+"\n");
    merged++; muniOnly++;
  }
}
wsM.end();

console.log(`[done] global master rows=${merged} (massOnly=${massOnly}, muniPlus=${muniPlus}, muniOnly=${muniOnly})`);

// Write CURRENT pointer for global master
const currentGlobalPtr = path.join(outDir,"CURRENT_ASSESSOR_MASTER_GLOBAL.json");
writeJSON(currentGlobalPtr,{updated_at:new Date().toISOString(),note:"AUTO: GLOBAL assessor master v3 (city wins, MassGIS fallback)", master_ndjson: outMasterNd, audit: outAudit});

// Now attach to Property Spine using global master
const outPropsDir = path.join(root, cfg.outputs.attached_out_dir || "publicData/properties/_attached/phase4_assessor_best");
fs.mkdirSync(outPropsDir,{recursive:true});
const outPropsNd = path.join(outPropsDir, `properties__with_assessor_best__${tag}__V3.ndjson`);

console.log(`[info] building global index for attach...`);
const globalMap = new Map();
{
  const rl=readline.createInterface({input:fs.createReadStream(outMasterNd,{encoding:"utf8"}), crlfDelay:Infinity});
  for await (const line of rl){
    if(!line.trim()) continue;
    let rec; try{rec=JSON.parse(line);}catch{continue;}
    const pid = rec.parcel_id_norm;
    for(const k of parcelKeyVariants(pid)) if(!globalMap.has(k)) globalMap.set(k, rec);
  }
}
console.log(`[info] global index keys=${globalMap.size}`);

console.log(`[info] attaching to properties -> ${outPropsNd}`);
const wsP = fs.createWriteStream(outPropsNd,{encoding:"utf8"});
const rlP = readline.createInterface({input:fs.createReadStream(propsIn,{encoding:"utf8"}), crlfDelay:Infinity});

let n=0, matched=0, unmatched=0, fallbackAny=0;
for await (const line of rlP){
  if(!line.trim()) continue;
  let rec; try{rec=JSON.parse(line);}catch{continue;}
  n++;

  const pid = rec.parcel_id_norm || rec.parcel_id || rec.parcel_id_raw || null;
  const keys = parcelKeyVariants(pid);

  let master=null;
  for(const k of keys){ if(globalMap.has(k)){ master=globalMap.get(k); break; } }

  if(!rec.raw_by_source || typeof rec.raw_by_source!=="object") rec.raw_by_source={};

  if(master){
    matched++;
    rec.raw_by_source.assessor_city_raw = master.evidence?.municipal_present
      ? { source_id: master.evidence?.municipal_source_id, source_path: master.evidence?.municipal_source_path, as_of: master.evidence?.as_of, dataset_hash: master.evidence?.dataset_hash }
      : null;
    rec.raw_by_source.parcels_statewide_assess_raw = master.evidence?.massgis_present
      ? { layer: master.evidence?.massgis_layer, where: master.evidence?.massgis_where, as_of: master.evidence?.as_of, dataset_hash: master.evidence?.dataset_hash }
      : null;

    if(!rec.best_current_attributes || typeof rec.best_current_attributes!=="object") rec.best_current_attributes={};
    const { best, source_map, fallback_fields } = buildBest(master, dottedFields);
    rec.best_current_attributes.assessor = best;
    rec.best_current_attributes.assessor_source_map = source_map;
    rec.best_current_attributes.assessor_fallback_fields = fallback_fields;
    rec.best_current_attributes.assessor_match_mode = "global_master_v3";
    if(fallback_fields.length) fallbackAny++;
  } else {
    unmatched++;
  }

  wsP.write(JSON.stringify(rec)+"\n");
  if(n%200000===0) console.log(`[progress] processed ${n} (matched=${matched}, unmatched=${unmatched})`);
}
wsP.end();

const audit = {
  created_at:new Date().toISOString(),
  config: cfgPath,
  inputs:{
    properties_ndjson: propsIn,
    massgis_master_ndjson: massNd,
    municipal_master_ptr: path.join(root,cfg.inputs.city_master_ptr),
    massgis_master_ptr: massPtrPath
  },
  outputs:{
    global_master_ndjson: outMasterNd,
    current_global_master_ptr: currentGlobalPtr,
    properties_out_ndjson: outPropsNd
  },
  stats:{
    global_master_rows: merged,
    global_master_mass_only: massOnly,
    global_master_muni_plus_mass: muniPlus,
    global_master_muni_only: muniOnly,
    properties_rows: n,
    properties_matched: matched,
    properties_unmatched: unmatched,
    properties_any_fallback_fields: fallbackAny
  },
  dataset_hashes:{
    properties_in_sha256: sha256File(propsIn),
    global_master_sha256: sha256File(outMasterNd)
  },
  rules:{
    precedence:"city_assessor > statewide_parcels(MassGIS) > null",
    fallback_flag:"FALLBACK_USED when winner != city_assessor",
    match:"parcel_id_norm variants (strip spaces/non-alnum/leading zeros)"
  }
};
writeJSON(outAudit,audit);
console.log(`[done] wrote audit: ${outAudit}`);

const currentPropsPtr = path.join(outPropsDir,"CURRENT_PROPERTIES_WITH_ASSESSOR_BEST.json");
writeJSON(currentPropsPtr,{updated_at:new Date().toISOString(),note:"AUTO: properties with assessor best v3 (global master merge)",out_ndjson: outPropsNd,audit: outAudit});
console.log(`[ok] wrote CURRENT pointer: ${currentPropsPtr}`);
