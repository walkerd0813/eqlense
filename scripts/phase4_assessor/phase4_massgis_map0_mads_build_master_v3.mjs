import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

function argMap(argv){const m={};for(let i=2;i<argv.length;i++){const a=argv[i];if(a.startsWith("--")){const k=a.slice(2);const v=argv[i+1]&&!argv[i+1].startsWith("--")?argv[++i]:"true";m[k]=v;}}return m;}
function readJSON(p){let s=fs.readFileSync(p,"utf8"); if(s.charCodeAt(0)===0xFEFF) s=s.slice(1); return JSON.parse(s);}
function writeJSON(p,obj){fs.mkdirSync(path.dirname(p),{recursive:true});fs.writeFileSync(p,JSON.stringify(obj,null,2),"utf8");}
function nowTag(){return new Date().toISOString().replace(/[:.]/g,"-");}
function sha256Text(s){return crypto.createHash("sha256").update(s).digest("hex");}
function sha256File(p){
  const h=crypto.createHash("sha256");
  const fd=fs.openSync(p,"r");
  const buf=Buffer.alloc(1024*1024);
  let b=0;
  while((b=fs.readSync(fd,buf,0,buf.length,null))>0){ h.update(buf.subarray(0,b)); }
  fs.closeSync(fd);
  return h.digest("hex");
}

function normStr(v){ if(v===null||v===undefined) return null; const s=String(v).trim(); return s? s : null; }
function toNumber(v){ if(v===null||v===undefined) return null; const s=String(v).replace(/[$,]/g,"").trim(); if(!s) return null; const n=Number(s); return Number.isFinite(n)?n:null; }
function toInt(v){ const n=toNumber(v); return n===null?null:Math.trunc(n); }
function parseDate(v){
  const s=normStr(v);
  if(!s) return null;
  if(/^\d{8}$/.test(s)){
    const yyyy=s.slice(0,4), mm=s.slice(4,6), dd=s.slice(6,8);
    return `${yyyy}-${mm}-${dd}`;
  }
  if(/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  const t=Date.parse(s);
  return Number.isFinite(t)?new Date(t).toISOString().slice(0,10):null;
}
function pickParcelId(attrs, candidates){
  for(const k of candidates){
    const s = normStr(attrs?.[k]);
    if(s) return s;
  }
  return null;
}
function normalizeOwnerType(owner1, ownCo){
  const s = (owner1||ownCo||"").toString().toLowerCase();
  if(!s) return "unknown";
  if(s.includes(" llc")||s.endsWith("llc")||s.includes("l.l.c")) return "llc";
  if(s.includes(" trust")||s.includes("trs")) return "trust";
  if(s.includes(" inc")||s.endsWith("inc")||s.includes("corp")||s.includes(" corporation")||s.includes(" co.")) return "corp";
  return "individual";
}
async function fetchJson(url){
  const res = await fetch(url);
  if(!res.ok){
    const txt = await res.text().catch(()=>"(no body)");
    throw new Error(`[http ${res.status}] ${url} :: ${txt.slice(0,300)}`);
  }
  return await res.json();
}
function buildQueryUrl(layerUrl, params){
  const usp = new URLSearchParams();
  for(const [k,v] of Object.entries(params)){
    if(v===undefined||v===null) continue;
    usp.set(k, String(v));
  }
  return `${layerUrl.replace(/\/$/,"")}/query?${usp.toString()}`;
}

const args=argMap(process.argv);
const root=args.root?path.resolve(args.root):process.cwd();
const cfgPath=args.config?path.resolve(args.config):path.join(root,"phase4_massgis_map0_mads_config_v2.json");
const cfg=readJSON(cfgPath);

let layerUrl = cfg.layer_url;
const where = cfg.where || "1=1";
const pageSize = cfg.page_size || 2000;
const outFields = cfg.out_fields || "*";
const returnGeometry = !!cfg.return_geometry;

const outBase = path.join(root, cfg.out_dir || "publicData/assessors/massgis_statewide");
const outMasterDir = path.join(outBase, cfg.output_master_subdir || "_master");
const outAuditDir = path.join(root, cfg.audit_dir || "publicData/_audit/phase4_assessor");
fs.mkdirSync(outMasterDir, {recursive:true});
fs.mkdirSync(outAuditDir, {recursive:true});


// Resolve MapServer vs FeatureServer automatically.
// Some services do not support attribute queries on MapServer, but do on FeatureServer.
async function resolveQueryableLayerUrl(url){
  const clean = url.replace(/\/$/,"");
  // Try as-is
  try {
    const meta = await fetchJson(`${clean}?f=pjsonon`);
    if(meta && !meta.error) return clean;
  } catch(e) {}

  // Try FeatureServer equivalent
  const feature = clean.replace(/\/MapServer\//i, "/FeatureServer/");
  if(feature !== clean){
    try {
      const meta2 = await fetchJson(`${feature}?f=pjsonon`);
      if(meta2 && !meta2.error) return feature;
    } catch(e) {}
  }
  return clean;
}

// Count helper with retries + server-error detection.
async function getCount(url, whereClause){
  const tries = [
    buildQueryUrl(url, { f:"pjson", where: whereClause, returnCountOnly:true }),
    buildQueryUrl(url, { f:"json",  where: whereClause, returnCountOnly:true }),
    buildQueryUrl(url, { f:"pjson", where: whereClause, returnCountOnly:true, outFields:"OBJECTID" }),
    buildQueryUrl(url, { f:"json",  where: whereClause, returnCountOnly:true, outFields:"OBJECTID" })
  ];
  let lastErr = null;
  for(const u of tries){
    const j = await fetchJson(u);
    if(j?.error){ lastErr = j.error; continue; }
    if(typeof j?.count === "number") return j.count;
  }
  throw new Error(`[count] failed for ${url} :: ${JSON.stringify(lastErr || {}).slice(0,300)}`);
}

console.log(`[info] layer: ${layerUrl}`);
layerUrl = await resolveQueryableLayerUrl(layerUrl);

console.log(`[info] where: ${where}`);
console.log(`[info] page_size: ${pageSize}`);
console.log(`[info] returnGeometry: ${returnGeometry}`);

const meta = await fetchJson(`${layerUrl.replace(/\/$/,"")}?f=pjsonon`);
const maxRecordCount = meta?.maxRecordCount || null;
console.log(`[info] layerName: ${meta?.name || "(unknown)"} maxRecordCount=${maxRecordCount}`);

const total = await getCount(layerUrl, where);
console.log(`[info] count=${total}`);

const tag = nowTag();
const outMaster = path.join(outMasterDir, `assessor_master__massgis_statewide__${tag}__MAP0__V3.ndjson`);
fs.writeFileSync(outMaster, "", "utf8");
const ws = fs.createWriteStream(outMaster, {encoding:"utf8", flags:"a"});

const pidCandidates = ["PROP_ID","LOC_ID","MAP_PAR_ID"];

let fetched = 0;
let written = 0;
let nullPid = 0;

for(let offset=0; offset<total; offset += pageSize){
  const qUrl = buildQueryUrl(layerUrl, {
    f:"json",
    where,
    outFields,
    returnGeometry,
    resultOffset: offset,
    resultRecordCount: pageSize,
    orderByFields: "OBJECTID",
    outSR: 4326
  });
  const data = await fetchJson(qUrl);
  const feats = data?.features || [];
  fetched += feats.length;

  for(const ft of feats){
    const attrs = ft?.attributes || {};
    const parcelIdNorm = pickParcelId(attrs, pidCandidates);
    if(!parcelIdNorm){ nullPid++; continue; }

    const owner1 = normStr(attrs["OWNER1"]);
    const ownCo = normStr(attrs["OWN_CO"]);
    const ownerType = normalizeOwnerType(owner1, ownCo);

    const rec = {
      city: (normStr(attrs["CITY"]) || "").toLowerCase() || null,
      parcel_id_norm: parcelIdNorm,

      owner_name_1: owner1,
      owner_name_2: null,
      owner_company: ownCo,
      owner_type_norm: ownerType,

      mailing_address_full: normStr(attrs["OWN_ADDR"]),
      mailing_city: normStr(attrs["OWN_CITY"]),
      mailing_state: normStr(attrs["OWN_STATE"]),
      mailing_zip: normStr(attrs["OWN_ZIP"]),
      owner_occupied_flag: null,
      is_absentee_owner: null,

      assessed_total: toInt(attrs["TOTAL_VAL"]),
      assessed_land: toInt(attrs["LAND_VAL"]),
      assessed_building: toInt(attrs["BLDG_VAL"]),
      assessed_other: toInt(attrs["OTHER_VAL"]),
      assessed_exemptions: null,
      assessed_year: toInt(attrs["FY"]),

      last_sale_date: parseDate(attrs["LS_DATE"]),
      last_sale_price: toInt(attrs["LS_PRICE"]),
      deed_book: normStr(attrs["LS_BOOK"]),
      deed_page: normStr(attrs["LS_PAGE"]),
      registry_id: normStr(attrs["REG_ID"]),

      use_code_norm: normStr(attrs["USE_CODE"]),
      property_class: "unknown",
      zoning_raw: normStr(attrs["ZONING"]),

      lot_size_raw: attrs["LOT_SIZE"] ?? null,
      lot_units_raw: normStr(attrs["LOT_UNITS"]),
      lot_sqft_est: null,

      year_built_est: toInt(attrs["YEAR_BUILT"]),
      units_est: toInt(attrs["UNITS"]),
      rooms_total: toInt(attrs["NUM_ROOMS"]),
      stories: normStr(attrs["STORIES"]),
      style_raw: normStr(attrs["STYLE"]),
      building_area_sqft: toInt(attrs["BLD_AREA"]),
      res_area_sqft: toInt(attrs["RES_AREA"]),

      // Lat/Lon intentionally not fetched here (geometry disabled). Property Spine already has coords.
      lat: null,
      lon: null,

      evidence: {
        as_of: new Date().toISOString(),
        municipal_present: false,
        massgis_present: true,
        massgis_layer: layerUrl,
        massgis_where: where,
        dataset_hash: null
      },
      qa_flags: []
    };

    rec.evidence.dataset_hash = sha256Text(JSON.stringify({
      layer: rec.evidence.massgis_layer,
      where: rec.evidence.massgis_where,
      schema: "assessor_master_massgis_map0_v2"
    }));

    ws.write(JSON.stringify(rec) + "\n");
    written++;
  }

  console.log(`[progress] fetched ${Math.min(fetched,total)}/${total} (written=${written})`);
}

ws.end();
await new Promise((res, rej)=>{ ws.on('finish', res); ws.on('error', rej); });

const currentPtr = path.join(outBase, "CURRENT_MASSGIS_STATEWIDE_ASSESSOR_MASTER.json");
const auditPath = path.join(outAuditDir, `phase4_massgis_map0_mads_master__${tag}.json`);

const audit = {
  created_at: new Date().toISOString(),
  config: cfgPath,
  source: { layerUrl, where, outFields, returnGeometry, pageSize, maxRecordCount, layerName: meta?.name || null },
  outputs: { master_ndjson: outMaster, current_ptr: currentPtr, audit: auditPath },
  stats: { count_total: total, fetched, written, null_parcel_id_norm: nullPid },
  dataset_hashes: { master_sha256: sha256File(outMaster) },
  notes: cfg.notes || []
};

writeJSON(auditPath, audit);
writeJSON(currentPtr, {
  updated_at: new Date().toISOString(),
  note: "AUTO: MassGIS MAP0 MADS master v2",
  master_ndjson: outMaster,
  audit: auditPath,
  layer: layerUrl
});

console.log(`[ok] wrote master: ${outMaster}`);
console.log(`[ok] wrote CURRENT pointer: ${currentPtr}`);
console.log(`[ok] wrote audit: ${auditPath}`);
