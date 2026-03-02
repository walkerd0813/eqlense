import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import readline from "node:readline";

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
  const h = crypto.createHash("sha256");
  const fd = fs.openSync(p, "r");
  const buf = Buffer.alloc(1024 * 1024);
  let bytes = 0;
  while ((bytes = fs.readSync(fd, buf, 0, buf.length, null)) > 0) {
    h.update(buf.subarray(0, bytes));
  }
  fs.closeSync(fd);
  return h.digest("hex");
}
function sha256Text(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}
function nowTag() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}
function normStr(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s ? s : null;
}
function toNumber(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).replace(/[$,]/g, "").trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}
function toInt(v) {
  const n = toNumber(v);
  return n === null ? null : Math.trunc(n);
}
function parseDate(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  if (!s) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  const t = Date.parse(s);
  if (Number.isFinite(t)) return new Date(t).toISOString().slice(0,10);
  return null;
}
function parcelKeyVariants(v) {
  const s0 = normStr(v);
  if (!s0) return [];
  const s = s0.replace(/\s+/g,"");
  const alnum = s.replace(/[^0-9A-Za-z]/g,"");
  const noLeadZeros = alnum.replace(/^0+/, "");
  const variants = new Set([s, alnum, noLeadZeros].filter(Boolean));
  return [...variants];
}
function saneField(pathKey, value) {
  if (value === null || value === undefined) return false;
  if (typeof value === "string" && !value.trim()) return false;
  if (pathKey.includes("assessed_") || pathKey.includes("sale_price") || pathKey.includes("value")) {
    const n = toNumber(value);
    if (n === null) return false;
    if (n <= 0) return false;
    if (pathKey.includes("sale_price") && n < 1000) return false;
    return true;
  }
  if (pathKey.includes("units")) {
    const n = toInt(value);
    if (n === null) return false;
    if (n <= 0 || n > 500) return false;
    return true;
  }
  if (pathKey.includes("year_built")) {
    const n = toInt(value);
    if (n === null) return false;
    if (n < 1600 || n > 2035) return false;
    return true;
  }
  if (pathKey.includes("date")) return !!parseDate(value);
  return true;
}
function confidenceFor(source) {
  if (source === "city_assessor") return "A";
  if (source === "statewide_parcels") return "B";
  return "C";
}
function setDeep(obj, dotted, v) {
  const parts = dotted.split(".");
  let cur = obj;
  for (let i=0;i<parts.length-1;i++){
    const p = parts[i];
    if (!cur[p] || typeof cur[p] !== "object") cur[p] = {};
    cur = cur[p];
  }
  cur[parts[parts.length-1]] = v;
}
function findLatestPropertiesNdjson(root) {
  const dir = path.join(root, "publicData", "properties");
  if (!fs.existsSync(dir)) throw new Error(`[err] missing directory: ${dir}`);
  const files = fs.readdirSync(dir)
    .filter(f => f.toLowerCase().endsWith(".ndjson"))
    .map(f => path.join(dir, f));
  if (files.length === 0) throw new Error(`[err] no .ndjson files in ${dir}`);
  files.sort((a,b)=> fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
  return files[0];
}

async function loadAssessorMasters(root, currentPtrRel) {
  const ptrPath = path.join(root, currentPtrRel);
  const ptr = readJSON(ptrPath);

  const mapCity = new Map();     // city -> Map(parcelKeyVariant -> masterRec)
  const mapGlobal = new Map();   // parcelKeyVariant -> masterRec (first seen)

  for (const c of (ptr.cities || [])) {
    const city = c.city;
    const nd = c.master_ndjson;
    const m = new Map();
    console.log(`[info] loading assessor master: ${city} -> ${nd}`);
    const rl = readline.createInterface({ input: fs.createReadStream(nd, { encoding: "utf8" }), crlfDelay: Infinity });
    for await (const line of rl) {
      if (!line.trim()) continue;
      let rec;
      try { rec = JSON.parse(line); } catch { continue; }
      const pid = rec.parcel_id_norm;
      for (const k of parcelKeyVariants(pid)) {
        m.set(k, rec);
        if (!mapGlobal.has(k)) mapGlobal.set(k, rec);
      }
    }
    mapCity.set(city, m);
  }
  return { ptrPath, ptr, mapCity, mapGlobal };
}

function buildBestFromAssessor(masterRec, bestTemplateFields) {
  const best = {};
  const source_map = {};
  const fallback_fields = [];

  const asOf = masterRec?.evidence?.as_of || null;
  const datasetHash = masterRec?.evidence?.dataset_hash || null;

  const fieldMap = {
    "valuation.assessed_total": "assessed_total",
    "valuation.assessed_land": "assessed_land",
    "valuation.assessed_building": "assessed_building",
    "valuation.assessed_other": "assessed_other",
    "valuation.assessed_exemptions": "assessed_exemptions",
    "valuation.assessed_year": "assessed_year",
    "building.units_est": "units_est",
    "building.year_built_est": "year_built_est",
    "txn.last_sale_date": "last_sale_date",
    "txn.last_sale_price": "last_sale_price",
    "owner.owner_name_1": "owner_name_1",
    "owner.owner_name_2": "owner_name_2",
    "owner.owner_company": "owner_company",
    "owner.owner_type_norm": "owner_type_norm",
    "owner.mailing_address_full": "mailing_address_full",
    "owner.owner_occupied_flag": "owner_occupied_flag"
  };

  for (const dotted of bestTemplateFields) {
    const src0 = (masterRec?.source_map && masterRec.source_map[dotted]) ? masterRec.source_map[dotted] : "unknown";
    const src = src0 === "statewide_parcels" ? "statewide_parcels" : (src0 === "city_assessor" ? "city_assessor" : "unknown");
    const v0 = masterRec ? masterRec[fieldMap[dotted]] : null;

    const obj = {
      value: v0 ?? null,
      source: src,
      as_of: asOf,
      dataset_hash: datasetHash,
      confidence: confidenceFor(src),
      flags: []
    };

    if (obj.source !== "city_assessor" && obj.value !== null) obj.flags.push("FALLBACK_USED");
    if (obj.source === "statewide_parcels" && obj.value !== null) fallback_fields.push(dotted);
    source_map[dotted] = obj.source;

    if (!saneField(dotted, obj.value)) {
      obj.value = null;
      obj.flags.push("MISSING_OR_UNSANE");
    }
    setDeep(best, dotted, obj);
  }

  return { best, source_map, fallback_fields };
}

function cityHints(rec) {
  const cands = [
    rec.source_city, rec.city, rec.city_norm, rec.city_name,
    rec.address_city, rec.addr_city, rec.town, rec.town_name, rec.town_norm
  ].map(x => (x ?? "").toString().trim().toLowerCase()).filter(Boolean);
  const uniq = [];
  for (const c of cands) if (!uniq.includes(c)) uniq.push(c);
  return uniq;
}

const args = argMap(process.argv);
const root = args.root ? path.resolve(args.root) : process.cwd();
const cfgPath = args.config ? path.resolve(args.config) : path.join(root, "phase4_property_assessor_best_config_v2.json");
const cfg = readJSON(cfgPath);

const inProperties = (cfg.inputs?.properties_ndjson || "").startsWith("AUTO_LATEST")
  ? findLatestPropertiesNdjson(root)
  : path.resolve(root, cfg.inputs.properties_ndjson);

const assessorPtrRel = cfg.inputs.assessor_master_current_ptr || "publicData/assessors/_frozen/CURRENT_PHASE4_ASSESSOR_MASTER.json";
const { ptrPath, ptr, mapCity, mapGlobal } = await loadAssessorMasters(root, assessorPtrRel);

const outDir = path.join(root, cfg.outputs?.out_dir || "publicData/properties/_attached/phase4_assessor_best");
fs.mkdirSync(outDir, { recursive: true });
const tag = nowTag();
const outNd = path.join(outDir, `properties__with_assessor_best__${tag}.ndjson`);
const outAudit = path.join(root, "publicData", "_audit", "phase4_assessor", `phase4_property_assessor_best_attach__${tag}.json`);
fs.mkdirSync(path.dirname(outAudit), { recursive: true });

console.log(`[info] properties_in: ${inProperties}`);
console.log(`[info] assessor_ptr: ${ptrPath}`);
console.log(`[info] global_assessor_keys: ${mapGlobal.size}`);
console.log(`[info] out: ${outNd}`);

const bestFields = cfg.field_contract?.provenance_object_fields || [];

let n=0, matched=0, unmatched=0, matched_global=0, matched_city=0, fallbackAny=0, city_mismatch=0;
const ws = fs.createWriteStream(outNd, { encoding: "utf8" });
const rl = readline.createInterface({ input: fs.createReadStream(inProperties, { encoding: "utf8" }), crlfDelay: Infinity });

for await (const line of rl) {
  if (!line.trim()) continue;
  let rec;
  try { rec = JSON.parse(line); } catch { continue; }
  n += 1;

  const pid = rec.parcel_id_norm || rec.parcel_id || rec.parcel_id_raw || rec.parcel_id_source || null;
  const keyList = parcelKeyVariants(pid);

  let master = null;
  let matchMode = "none";

  // 1) try city hints
  const hints = cityHints(rec);
  for (const h of hints) {
    const cm = mapCity.get(h);
    if (!cm) continue;
    for (const k of keyList) {
      if (cm.has(k)) { master = cm.get(k); matchMode = "city"; break; }
    }
    if (master) break;
  }

  // 2) global fallback
  if (!master) {
    for (const k of keyList) {
      if (mapGlobal.has(k)) { master = mapGlobal.get(k); matchMode = "global"; break; }
    }
  }

  const out = rec;

  if (!out.raw_by_source || typeof out.raw_by_source !== "object") out.raw_by_source = {};
  if (!out.raw_by_source.parcels_statewide_raw && out.parcels_statewide_raw) out.raw_by_source.parcels_statewide_raw = out.parcels_statewide_raw;

  if (master) {
    matched += 1;
    if (matchMode === "city") matched_city += 1;
    if (matchMode === "global") matched_global += 1;

    // city mismatch flag if we matched globally but the record has a city and it differs from assessor master city
    if (matchMode === "global" && hints.length) {
      const masterCity = (master.city || "").toString().trim().toLowerCase();
      if (masterCity && !hints.includes(masterCity)) city_mismatch += 1;
    }

    out.raw_by_source.assessor_city_raw = master.evidence?.municipal_present
      ? { source_id: master.evidence?.municipal_source_id, source_path: master.evidence?.municipal_source_path, as_of: master.evidence?.as_of, dataset_hash: master.evidence?.dataset_hash }
      : null;
    out.raw_by_source.parcels_statewide_assess_raw = master.evidence?.massgis_present
      ? { layer: master.evidence?.massgis_layer, where: master.evidence?.massgis_where, as_of: master.evidence?.as_of, dataset_hash: master.evidence?.dataset_hash }
      : null;

    if (!out.best_current_attributes || typeof out.best_current_attributes !== "object") out.best_current_attributes = {};
    const { best, source_map, fallback_fields } = buildBestFromAssessor(master, bestFields);

    out.best_current_attributes.assessor = best;
    out.best_current_attributes.assessor_source_map = source_map;
    out.best_current_attributes.assessor_fallback_fields = fallback_fields;
    out.best_current_attributes.assessor_match_mode = matchMode;

    if (fallback_fields.length) fallbackAny += 1;
  } else {
    unmatched += 1;
  }

  ws.write(JSON.stringify(out) + "\n");
  if (n % 200000 === 0) console.log(`[progress] processed ${n} rows (matched=${matched}, unmatched=${unmatched}, city=${matched_city}, global=${matched_global})`);
}
ws.end();

const audit = {
  created_at: new Date().toISOString(),
  config: cfgPath,
  inputs: { properties_ndjson: inProperties, assessor_current_ptr: ptrPath, assessor_city_count: (ptr.cities||[]).length },
  outputs: { out_ndjson: outNd },
  stats: { properties_rows: n, matched, unmatched, matched_city, matched_global, city_mismatch_global: city_mismatch, fallback_any: fallbackAny },
  dataset_hashes: { properties_in_sha256: sha256File(inProperties), assessor_ptr_sha256: sha256Text(JSON.stringify(ptr)) },
  rules: {
    precedence: "city_assessor > statewide_parcels > null",
    fallback_flag: "FALLBACK_USED when winner != city_assessor",
    match_strategy: "try city hints, then global parcel_id index",
    sane_gates: ["assessed_* > 0", "units 1..500", "year_built 1600..2035", "sale_price >= 1000", "dates parse"]
  }
};
writeJSON(outAudit, audit);
console.log(`[done] wrote audit: ${outAudit}`);

const currentPtr = path.join(outDir, "CURRENT_PROPERTIES_WITH_ASSESSOR_BEST.json");
writeJSON(currentPtr, { updated_at: new Date().toISOString(), note: "AUTO: Phase4 PropertySpine assessor-best attach v2 (global index fix)", out_ndjson: outNd, audit: outAudit });
console.log(`[ok] wrote CURRENT pointer: ${currentPtr}`);
