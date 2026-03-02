import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";

function argMap(argv) {
  const m = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : "true";
      m[k] = v;
    }
  }
  return m;
}

function readJSON(p) {
  let s = fs.readFileSync(p, "utf8");
  if (s.charCodeAt(0) === 0xFEFF) s = s.slice(1);
  return JSON.parse(s);
}

function writeJSON(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}

function sha256File(p) {
  const buf = fs.readFileSync(p);
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function normKey(s) {
  return (s || "").toString().trim().toLowerCase().replace(/[^a-z0-9]+/g, "_");
}

const FIELD_CANDIDATES = {
  parcel_id: ["map_par_id","map_parid","par_id","parcel_id","parcelid","loc_id","gis_id","pid","parid","mapparid","par_num","parcel_num","prop_id"],
  land_value: ["land_val","landvalue","av_land","assessed_land","land_assess","lnd_val","lndvalue","land"],
  building_value: ["bldg_val","bldgvalue","building_val","buildingvalue","av_bldg","assessed_bldg","impr_val","improvement_value","bld_val"],
  total_value: ["total_val","totalvalue","assessed_total","av_total","tot_val","totvalue","full_val","assess_total"],
  last_sale_date: ["sale_date","last_sale_date","lastsaledate","deed_date","date_sale","sale_dt","last_transfer_date"],
  last_sale_price: ["sale_price","last_sale_price","lastsaleprice","price_sale","sale_amt","sale_amount","last_transfer_price","deed_price"],
  site_addr: ["site_addr","site_address","address","location","loc_addr","street_addr","street_address"],
  owner: ["owner","owner_name","own_name","owner1","owner_1","prop_owner"]
};

function pickField(propsKeys, logicalName) {
  const keys = propsKeys.map(k => normKey(k));
  const cand = FIELD_CANDIDATES[logicalName] || [];
  for (const c of cand) {
    const idx = keys.indexOf(c);
    if (idx >= 0) return propsKeys[idx];
  }
  return null;
}

function safeReadGeoJSON(p) {
  let s = fs.readFileSync(p, "utf8");
  if (s.charCodeAt(0) === 0xFEFF) s = s.slice(1);
  return JSON.parse(s);
}

function countNonNull(features, field) {
  if (!field) return 0;
  let n = 0;
  for (const f of features) {
    const v = f?.properties?.[field];
    if (v !== null && v !== undefined && `${v}`.trim() !== "") n++;
  }
  return n;
}

async function fetchJSON(url, params) {
  const u = new URL(url);
  Object.entries(params || {}).forEach(([k,v]) => u.searchParams.set(k, String(v)));
  const res = await fetch(u.toString(), { headers: { "User-Agent": "EquityLens/phase4_assessor_v3" } });
  if (!res.ok) {
    const txt = await res.text().catch(()=>"");
    throw new Error(`[http ${res.status}] ${u} :: ${txt.slice(0,200)}`);
  }
  return res.json();
}

async function downloadArcGISLayerToGeoJSON(layerUrl, outPathAbs, where = "1=1") {
  const queryUrl = layerUrl.replace(/\/+$/,"") + "/query";

  const countResp = await fetchJSON(queryUrl, { f:"json", where, returnCountOnly:"true" });
  const count = countResp.count ?? null;
  if (count === null) throw new Error(`[err] could not read count from ${queryUrl}`);
  console.log(`[info] ${layerUrl} where="${where}" count=${count}`);

  const features = [];
  const pageSize = 2000;
  let offset = 0;
  while (offset < count) {
    const page = await fetchJSON(queryUrl, {
      f: "json",
      where,
      outFields: "*",
      returnGeometry: "true",
      resultRecordCount: pageSize,
      resultOffset: offset
    });
    if (page.error) throw new Error(`[err] arcgis error: ${JSON.stringify(page.error)}`);
    const fsIn = page.features || [];
    for (const f of fsIn) features.push({ type:"Feature", properties: f.attributes || {}, geometry: f.geometry || null });
    offset += fsIn.length || pageSize;
    console.log(`[progress] fetched ${Math.min(offset, count)}/${count}`);
    if (fsIn.length === 0) break;
    await sleep(50);
  }

  const geojson = { type:"FeatureCollection", features };
  fs.mkdirSync(path.dirname(outPathAbs), { recursive: true });
  fs.writeFileSync(outPathAbs, JSON.stringify(geojson), "utf8");
  console.log(`[done] wrote ${outPathAbs} features=${features.length}`);
  return { featureCount: features.length };
}

function auditGeoJSON(city, sourceId, absPath) {
  const gj = safeReadGeoJSON(absPath);
  const features = gj.features || [];
  const n = features.length;
  const keys = n ? Object.keys(features[0].properties || {}) : [];

  const map = {};
  for (const logical of ["parcel_id","land_value","building_value","total_value","last_sale_date","last_sale_price","site_addr","owner"]) {
    map[logical] = pickField(keys, logical);
  }

  const coverage = {};
  for (const [logical, field] of Object.entries(map)) {
    const nonNull = countNonNull(features, field);
    coverage[logical] = { field, non_null: nonNull, pct: n ? Math.round((nonNull / n) * 1000)/10 : 0 };
  }

  return { city, sourceId, path: absPath, features: n, keys_sample: keys.slice(0, 120), field_map: map, coverage };
}

function normalizeToNDJSON(city, sourceId, absGeoPath, outNdPath) {
  const gj = safeReadGeoJSON(absGeoPath);
  const features = gj.features || [];
  const n = features.length;
  const keys = n ? Object.keys(features[0].properties || {}) : [];

  const map = {};
  for (const logical of ["parcel_id","land_value","building_value","total_value","last_sale_date","last_sale_price","site_addr","owner"]) {
    map[logical] = pickField(keys, logical);
  }

  fs.mkdirSync(path.dirname(outNdPath), { recursive: true });
  const out = fs.createWriteStream(outNdPath, { encoding: "utf8" });

  let written = 0;
  for (const f of features) {
    const p = f.properties || {};
    const rec = {
      city,
      source_id: sourceId,
      evidence: { source_path: absGeoPath, as_of: new Date().toISOString() },
      parcel_id_raw: map.parcel_id ? p[map.parcel_id] : null,
      land_value_raw: map.land_value ? p[map.land_value] : null,
      building_value_raw: map.building_value ? p[map.building_value] : null,
      total_value_raw: map.total_value ? p[map.total_value] : null,
      last_sale_date_raw: map.last_sale_date ? p[map.last_sale_date] : null,
      last_sale_price_raw: map.last_sale_price ? p[map.last_sale_price] : null,
      site_addr_raw: map.site_addr ? p[map.site_addr] : null,
      owner_raw: map.owner ? p[map.owner] : null,
      raw: p
    };
    out.write(JSON.stringify(rec) + "\n");
    written++;
  }
  out.end();
  return { written, field_map: map };
}

const args = argMap(process.argv);
const root = args.root ? path.resolve(args.root) : process.cwd();
const configPath = args.config ? path.resolve(args.config) : path.join(root, "phase4_assessor_sources_v1.json");

if (!fs.existsSync(configPath)) throw new Error(`[err] missing config: ${configPath}`);
const cfg = readJSON(configPath);

const auditDir = path.join(root, "publicData", "_audit", "phase4_assessor");
fs.mkdirSync(auditDir, { recursive: true });

const runTag = new Date().toISOString().replace(/[:.]/g, "-");
const run = {
  created_at: new Date().toISOString(),
  root,
  config: configPath,
  sources_processed: [],
  sources_missing: [],
  outputs: {}
};

for (const src of (cfg.sources || [])) {
  if (src.type === "missing_source") {
    run.sources_missing.push(src);
    continue;
  }

  if (src.type === "local_geojson") {
    const abs = path.isAbsolute(src.path) ? src.path : path.join(root, src.path);
    if (!fs.existsSync(abs)) {
      run.sources_missing.push({ ...src, reason: "file_not_found", abs_path: abs });
      console.log(`[warn] missing local file: ${abs}`);
      continue;
    }
    const audit = auditGeoJSON(src.city, src.id, abs);
    const ndOut = path.join(root, "publicData", "assessors", src.city, "_normalized", `assessor_normalized__${src.city}__${runTag}.ndjson`);
    const norm = normalizeToNDJSON(src.city, src.id, abs, ndOut);

    run.sources_processed.push({ ...src, abs_path: abs, sha256: sha256File(abs), audit, normalized_ndjson: ndOut, normalized_rows: norm.written });
    console.log(`[ok] audited local ${src.city} features=${audit.features} -> ${ndOut}`);
    continue;
  }

  if (src.type === "arcgis_layer") {
    const outRel = src.out_path;
    const outAbs = path.isAbsolute(outRel) ? outRel : path.join(root, outRel);
    await downloadArcGISLayerToGeoJSON(src.layer_url, outAbs, src.where || "1=1");

    const audit = auditGeoJSON(src.city, src.id, outAbs);
    const ndOut = path.join(root, "publicData", "assessors", src.city, "_normalized", `assessor_normalized__${src.city}__${runTag}.ndjson`);
    const norm = normalizeToNDJSON(src.city, src.id, outAbs, ndOut);

    run.sources_processed.push({ ...src, abs_path: outAbs, sha256: sha256File(outAbs), audit, normalized_ndjson: ndOut, normalized_rows: norm.written });
    console.log(`[ok] downloaded+audited arcgis ${src.city} features=${audit.features} -> ${ndOut}`);
    continue;
  }

  run.sources_missing.push({ ...src, reason: "unknown_or_template_type" });
}

const outAudit = path.join(auditDir, `phase4_assessor_inventory_harvest__${runTag}.json`);
writeJSON(outAudit, run);
run.outputs.audit = outAudit;

console.log(`[done] wrote audit: ${outAudit}`);
