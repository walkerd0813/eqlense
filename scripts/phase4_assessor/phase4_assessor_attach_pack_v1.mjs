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

function sha256Text(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}

function sha256File(p) {
  const buf = fs.readFileSync(p);
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function nowTag() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function normParcelId(v) {
  if (v === null || v === undefined) return null;
  let s = String(v).trim();
  if (!s) return null;
  // Normalize common MassGIS MAP_PAR_ID patterns: keep alnum and underscore/hyphen
  s = s.replace(/\s+/g, "");
  return s;
}

function toNumber(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).replace(/[$,]/g, "").trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function parseLSDate(ls) {
  if (ls === null || ls === undefined) return null;
  const s = String(ls).trim();
  if (!s) return null;
  // Many assessor LS_DATE are YYYYMMDD (8 chars)
  if (/^\d{8}$/.test(s)) {
    const y = s.slice(0,4), m = s.slice(4,6), d = s.slice(6,8);
    return `${y}-${m}-${d}`;
  }
  // Try ISO-ish
  const t = Date.parse(s);
  if (Number.isFinite(t)) return new Date(t).toISOString().slice(0,10);
  return null;
}

async function fetchJSON(url, params) {
  const u = new URL(url);
  Object.entries(params || {}).forEach(([k,v]) => u.searchParams.set(k, String(v)));
  const res = await fetch(u.toString(), { headers: { "User-Agent": "EquityLens/phase4_assessor_attach_v1" } });
  if (!res.ok) {
    const txt = await res.text().catch(()=> "");
    throw new Error(`[http ${res.status}] ${u} :: ${txt.slice(0,200)}`);
  }
  return res.json();
}

async function arcgisCount(layerUrl, where) {
  const q = layerUrl.replace(/\/+$/,"") + "/query";
  const j = await fetchJSON(q, { f:"json", where, returnCountOnly:"true" });
  if (j.error) throw new Error(JSON.stringify(j.error));
  return j.count ?? 0;
}

async function arcgisPage(layerUrl, where, offset, pageSize, outFields) {
  const q = layerUrl.replace(/\/+$/,"") + "/query";
  const j = await fetchJSON(q, {
    f:"json",
    where,
    outFields: outFields.join(","),
    returnGeometry: "false",
    resultRecordCount: pageSize,
    resultOffset: offset
  });
  if (j.error) throw new Error(JSON.stringify(j.error));
  return j.features || [];
}

async function downloadMassGISSubsetToMap(layerUrl, where, outFields) {
  const count = await arcgisCount(layerUrl, where);
  console.log(`[info] MassGIS where="${where}" count=${count}`);
  const pageSize = 2000;
  let offset = 0;
  const m = new Map(); // parcel_id_norm -> record
  while (offset < count) {
    const feats = await arcgisPage(layerUrl, where, offset, pageSize, outFields);
    if (feats.length === 0) break;
    for (const f of feats) {
      const a = f.attributes || {};
      const pid = normParcelId(a.MAP_PAR_ID ?? a.MAP_PARID ?? a.PARCEL_ID ?? a.PARCELID);
      if (!pid) continue;
      // Keep the first; if duplicates, prefer one with TOTAL_VAL present
      if (!m.has(pid)) {
        m.set(pid, a);
      } else {
        const cur = m.get(pid);
        const curTot = toNumber(cur.TOTAL_VAL ?? cur.TOTALVALUE);
        const nxtTot = toNumber(a.TOTAL_VAL ?? a.TOTALVALUE);
        if (curTot === null && nxtTot !== null) m.set(pid, a);
      }
    }
    offset += feats.length;
    console.log(`[progress] MassGIS fetched ${Math.min(offset, count)}/${count}`);
    await sleep(20);
  }
  return { count, map: m };
}

function findLatestNormalizedNdjson(root, city) {
  const dir = path.join(root, "publicData", "assessors", city, "_normalized");
  if (!fs.existsSync(dir)) return null;
  const files = fs.readdirSync(dir).filter(f => f.toLowerCase().endsWith(".ndjson"));
  if (files.length === 0) return null;
  const full = files.map(f => path.join(dir, f));
  full.sort((a,b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
  return full[0];
}

function readNdjsonToMap(ndPath) {
  const m = new Map(); // parcel_id_norm -> record
  const s = fs.readFileSync(ndPath, "utf8").split(/\r?\n/);
  for (const line of s) {
    if (!line.trim()) continue;
    let obj;
    try { obj = JSON.parse(line); } catch { continue; }
    const pid = normParcelId(obj.parcel_id_norm ?? obj.parcel_id_raw);
    if (!pid) continue;
    m.set(pid, obj);
  }
  return m;
}

function buildCombinedRecord(city, municipalRec, massgisAttrs, datasetMeta) {
  // municipalRec is from inventory normalized output (raw fields + raw snapshot)
  // massgisAttrs is MassGIS attributes row
  const out = {
    city,
    parcel_id_norm: null,
    source_precedence: "municipal_then_massgis_fallback",
    evidence: {
      as_of: new Date().toISOString(),
      municipal_source_id: municipalRec?.source_id ?? null,
      municipal_source_path: municipalRec?.evidence?.source_path ?? null,
      municipal_row_present: !!municipalRec,
      massgis_layer: datasetMeta.massgis_layer_url,
      massgis_where: datasetMeta.massgis_where,
      massgis_row_present: !!massgisAttrs,
      dataset_hash: datasetMeta.dataset_hash
    },
    // normalized outputs
    assessed_land: null,
    assessed_building: null,
    assessed_total: null,
    assessed_year: null,
    last_sale_date: null,
    last_sale_price: null,
    use_code_norm: null,
    units_est: null,
    year_built_est: null,
    sqft_est: null,
    lot_sqft_est: null,
    // raw retainers (small)
    raw: {
      municipal: municipalRec?.raw ?? null,
      massgis: massgisAttrs ?? null
    },
    qa_flags: []
  };

  // Parcel ID
  out.parcel_id_norm = normParcelId(
    municipalRec?.parcel_id_norm ?? municipalRec?.parcel_id_raw ??
    massgisAttrs?.MAP_PAR_ID ?? massgisAttrs?.PARCEL_ID
  );

  // Helpers to pick "best" value
  const pick = (munVal, mgVal, conv) => {
    const a = conv ? conv(munVal) : munVal;
    if (a !== null && a !== undefined && `${a}`.trim?.() !== "") return a;
    const b = conv ? conv(mgVal) : mgVal;
    if (b !== null && b !== undefined && `${b}`.trim?.() !== "") return b;
    return null;
  };

  // assessed values
  out.assessed_land = pick(municipalRec?.land_value_raw, massgisAttrs?.LAND_VAL, toNumber);
  out.assessed_building = pick(municipalRec?.building_value_raw, massgisAttrs?.BLDG_VAL, toNumber);
  out.assessed_total = pick(municipalRec?.total_value_raw, massgisAttrs?.TOTAL_VAL, toNumber);
  out.assessed_year = pick(municipalRec?.raw?.FY ?? municipalRec?.assessed_year_raw, massgisAttrs?.FY, (v)=> {
    const n = toNumber(v);
    return n === null ? null : Math.trunc(n);
  });

  // last sale
  out.last_sale_date = pick(municipalRec?.last_sale_date_raw, massgisAttrs?.LS_DATE, parseLSDate);
  out.last_sale_price = pick(municipalRec?.last_sale_price_raw, massgisAttrs?.LS_PRICE, toNumber);

  // structure
  out.use_code_norm = pick(municipalRec?.raw?.USE_CODE ?? municipalRec?.use_code_raw, massgisAttrs?.USE_CODE, (v)=> v ? String(v).trim() : null);
  out.units_est = pick(municipalRec?.raw?.UNITS ?? municipalRec?.units_raw, massgisAttrs?.UNITS, (v)=> {
    const n = toNumber(v);
    return n === null ? null : Math.trunc(n);
  });
  out.year_built_est = pick(municipalRec?.raw?.YEAR_BUILT ?? municipalRec?.year_built_raw, massgisAttrs?.YEAR_BUILT, (v)=> {
    const n = toNumber(v);
    return n === null ? null : Math.trunc(n);
  });
  out.sqft_est = pick(municipalRec?.raw?.BLD_AREA ?? municipalRec?.living_area_sqft_raw, massgisAttrs?.BLD_AREA, (v)=> {
    const n = toNumber(v);
    return n === null ? null : Math.trunc(n);
  });
  // LOT_SIZE in acres or sqft? LOT_UNITS indicates unit; we keep raw numeric without unit conversion for now
  out.lot_sqft_est = pick(municipalRec?.raw?.LOT_SIZE ?? municipalRec?.lot_area_sqft_raw, massgisAttrs?.LOT_SIZE, (v)=> {
    const n = toNumber(v);
    return n;
  });

  // QA flags
  if (!out.parcel_id_norm) out.qa_flags.push("MISSING_PARCEL_ID");
  if (out.assessed_total !== null && out.assessed_total < 0) out.qa_flags.push("NEGATIVE_TOTAL_VAL");
  if (out.assessed_land !== null && out.assessed_building !== null && out.assessed_total !== null) {
    if (out.assessed_total + 1e-6 < (out.assessed_land + out.assessed_building)) out.qa_flags.push("TOTAL_LT_LAND_PLUS_BUILDING");
  }
  return out;
}

function coverageStats(records) {
  const n = records.length;
  const fields = ["assessed_total","assessed_land","assessed_building","last_sale_date","last_sale_price","use_code_norm","units_est","year_built_est","sqft_est","lot_sqft_est"];
  const c = {};
  for (const f of fields) c[f] = 0;
  for (const r of records) {
    for (const f of fields) {
      const v = r[f];
      if (v !== null && v !== undefined && `${v}`.trim?.() !== "") c[f] += 1;
    }
  }
  const pct = {};
  for (const f of fields) pct[f] = n ? Math.round((c[f]/n)*1000)/10 : 0;
  return { n, counts: c, pct };
}

const args = argMap(process.argv);
const root = args.root ? path.resolve(args.root) : process.cwd();
const configPath = args.config ? path.resolve(args.config) : path.join(root, "phase4_assessor_attach_config_v1.json");

const cfg = readJSON(configPath);
const massgisLayer = cfg.massgis_layer_url;

const outAuditDir = path.join(root, "publicData", "_audit", "phase4_assessor");
fs.mkdirSync(outAuditDir, { recursive: true });

const tag = nowTag();
const run = {
  created_at: new Date().toISOString(),
  config: configPath,
  massgis_layer_url: massgisLayer,
  per_city: [],
  outputs: {}
};

for (const c of (cfg.cities || [])) {
  const city = c.city;
  const where = c.massgis_where;
  console.log(`\n[city] ${city}`);

  const municipalLatest = findLatestNormalizedNdjson(root, city);
  const municipalMap = municipalLatest ? readNdjsonToMap(municipalLatest) : new Map();
  console.log(`[info] municipal_latest: ${municipalLatest ?? "(none)"}`);
  console.log(`[info] municipal_rows: ${municipalMap.size}`);

  // Pull MassGIS subset (attributes only)
  const outFields = ["MAP_PAR_ID","CITY","ZIP","TOWN_ID","BLDG_VAL","LAND_VAL","TOTAL_VAL","FY","LS_DATE","LS_PRICE","USE_CODE","UNITS","YEAR_BUILT","BLD_AREA","LOT_SIZE","LOT_UNITS"];
  const mg = await downloadMassGISSubsetToMap(massgisLayer, where, outFields);
  console.log(`[info] massgis_rows: ${mg.map.size}`);

  // Combine union of parcel IDs
  const ids = new Set([...municipalMap.keys(), ...mg.map.keys()]);
  const combined = [];
  for (const pid of ids) {
    const mun = municipalMap.get(pid) || null;
    const mgRow = mg.map.get(pid) || null;
    const datasetMeta = {
      dataset_hash: sha256Text(`${massgisLayer}||${where}||${tag}`),
      massgis_layer_url: massgisLayer,
      massgis_where: where
    };
    const rec = buildCombinedRecord(city, mun, mgRow, datasetMeta);
    combined.push(rec);
  }

  // Coverage stats
  const before = municipalLatest ? (()=>{
    const arr = Array.from(municipalMap.values()).map(r => ({
      assessed_total: r.total_value_raw,
      assessed_land: r.land_value_raw,
      assessed_building: r.building_value_raw,
      last_sale_date: r.last_sale_date_raw,
      last_sale_price: r.last_sale_price_raw
    }));
    // not perfect; quick pct on raw presence
    const n = arr.length;
    const pct = {
      assessed_total: n ? Math.round((arr.filter(x=>x.assessed_total!=null && `${x.assessed_total}`.trim()!=="").length/n)*1000)/10 : 0,
      assessed_land: n ? Math.round((arr.filter(x=>x.assessed_land!=null && `${x.assessed_land}`.trim()!=="").length/n)*1000)/10 : 0,
      assessed_building: n ? Math.round((arr.filter(x=>x.assessed_building!=null && `${x.assessed_building}`.trim()!=="").length/n)*1000)/10 : 0,
      last_sale_date: n ? Math.round((arr.filter(x=>x.last_sale_date!=null && `${x.last_sale_date}`.trim()!=="").length/n)*1000)/10 : 0,
      last_sale_price: n ? Math.round((arr.filter(x=>x.last_sale_price!=null && `${x.last_sale_price}`.trim()!=="").length/n)*1000)/10 : 0
    };
    return { n, pct };
  })() : { n: 0, pct: {} };

  const after = coverageStats(combined);

  // Write combined ndjson
  const outDir = path.join(root, "publicData", "assessors", city, "_master");
  fs.mkdirSync(outDir, { recursive: true });
  const outNd = path.join(outDir, `assessor_master__${city}__${tag}.ndjson`);
  const ws = fs.createWriteStream(outNd, { encoding: "utf8" });
  for (const r of combined) ws.write(JSON.stringify(r) + "\n");
  ws.end();

  console.log(`[ok] wrote master ndjson: ${outNd}`);
  run.per_city.push({
    city,
    municipal_latest: municipalLatest,
    municipal_rows: municipalMap.size,
    massgis_where: where,
    massgis_count: mg.count,
    massgis_rows: mg.map.size,
    union_rows: combined.length,
    coverage_before: before,
    coverage_after: after,
    outputs: { master_ndjson: outNd }
  });
}

// Write audit + CURRENT pointer
const auditPath = path.join(outAuditDir, `phase4_assessor_attach_pack_v1__${tag}.json`);
writeJSON(auditPath, run);
console.log(`\n[done] wrote audit: ${auditPath}`);

const currentPtr = path.join(root, "publicData", "assessors", "_frozen", "CURRENT_PHASE4_ASSESSOR_MASTER.json");
writeJSON(currentPtr, { updated_at: new Date().toISOString(), note: "AUTO: Phase4 Assessor Attach Pack v1", audit: auditPath, cities: run.per_city.map(x=>({city:x.city, master_ndjson:x.outputs.master_ndjson})) });
console.log(`[ok] wrote CURRENT pointer: ${currentPtr}`);
