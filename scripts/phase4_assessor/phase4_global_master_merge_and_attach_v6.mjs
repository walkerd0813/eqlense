/**
 * Phase 4 — GLOBAL assessor master merge + PropertySpine attach (v6)
 * Fixes:
 *  - Property spine uses `parcel_id` (NOT parcel_id_norm). We key attaches on:
 *      key = normalizeParcelKey(property.parcel_id) || normalizeParcelKey(stripPrefix(property.property_id))
 *  - Municipal masters may not share same key; build multi-key index from whatever IDs exist.
 *  - Writes raw-by-source blocks explicitly:
 *      assessor_by_source: { city_assessor_raw?, massgis_statewide_raw? }
 *
 * Memory safe:
 *  - Shards MassGIS master and property file, loads one shard at a time.
 */

import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import crypto from "node:crypto";

function readJSON(p) {
  return JSON.parse(fs.readFileSync(p, "utf8").replace(/^\uFEFF/, ""));
}
function ensureDir(p) { fs.mkdirSync(p, { recursive: true }); }

function sha256File(filePath) {
  const h = crypto.createHash("sha256");
  const fd = fs.openSync(filePath, "r");
  const buf = Buffer.allocUnsafe(1024 * 1024);
  try {
    while (true) {
      const n = fs.readSync(fd, buf, 0, buf.length, null);
      if (!n) break;
      h.update(buf.subarray(0, n));
    }
  } finally { fs.closeSync(fd); }
  return h.digest("hex");
}

function stripPropertyPrefix(propertyId) {
  if (!propertyId || typeof propertyId !== "string") return null;
  const m = propertyId.match(/ma:parcel:(.*)$/i);
  return m ? m[1].trim() : propertyId.trim();
}

function normalizeParcelKey(v) {
  if (v == null) return null;
  const s = String(v).trim();
  if (!s) return null;
  return s.replace(/^"+|"+$/g, "").replace(/\s+/g, " ").toUpperCase();
}

function parcelKeyVariants(v) {
  const k = normalizeParcelKey(v);
  if (!k) return [];
  const out = new Set([k]);
  out.add(k.replace(/\s/g, ""));
  out.add(k.replace(/[-]/g, ""));
  out.add(k.replace(/[-\s]/g, ""));
  if (/^\d+$/.test(k)) out.add(String(parseInt(k, 10)));
  return [...out].filter(Boolean);
}

function getPropertyParcelKey(o) {
  const k1 = normalizeParcelKey(o.parcel_id);
  if (k1) return k1;
  const k2 = normalizeParcelKey(stripPropertyPrefix(o.property_id));
  if (k2) return k2;
  return null;
}

function parseArgs(argv) {
  const args = { config: null };
  for (let i = 2; i < argv.length; i++) if (argv[i] === "--config") args.config = argv[++i];
  if (!args.config) throw new Error("missing --config <path>");
  return args;
}

async function* ndjsonLines(filePath) {
  const rl = readline.createInterface({ input: fs.createReadStream(filePath, { encoding: "utf8" }), crlfDelay: Infinity });
  for await (const line of rl) { const t = line.trim(); if (t) yield t; }
}

function shardIndexForKey(key, shards) {
  const h = crypto.createHash("sha256").update(key).digest();
  return h.readUInt32BE(0) % shards;
}

function writeNDJSONLine(fd, obj) { fs.writeSync(fd, JSON.stringify(obj) + "\n"); }

function loadMunicipalMasters(cityPtrPath) {
  const ptr = readJSON(cityPtrPath);
  const cities = ptr.cities || [];
  return cities.map(c => ({ city: c.city, path: c.master_ndjson }));
}

async function buildMunicipalIndex(muniMasters) {
  const idx = new Map();
  let seen = 0;
  for (const m of muniMasters) {
    let loaded = 0;
    for await (const line of ndjsonLines(m.path)) {
      const o = JSON.parse(line);
      const candidates = [];
      for (const f of ["parcel_key_norm","parcel_id_norm","parcel_id","MAP_PAR_ID","LOC_ID","PROP_ID","map_par_id","loc_id","prop_id"]) {
        if (o[f] != null) candidates.push(o[f]);
      }
      for (const c of candidates) for (const v of parcelKeyVariants(c)) if (!idx.has(v)) idx.set(v, o);
      loaded++; seen++;
      if (loaded % 250000 === 0) console.log(`[progress] muni ${m.city} loaded ${loaded}`);
    }
    console.log(`[info] muni index loaded city=${m.city} (done)`);
  }
  console.log(`[info] municipal keys indexed=${idx.size} records_seen=${seen}`);
  return { idx, seen, keys: idx.size };
}

async function shardMassGISMaster(massPtrPath, workDir, shards) {
  const ptr = readJSON(massPtrPath);
  const masterPath = ptr.master_ndjson || ptr.path || ptr.master || ptr.ndjson;
  if (!masterPath) throw new Error(`MassGIS pointer missing master_ndjson/path fields: ${massPtrPath}`);

  ensureDir(workDir);
  const shardDir = path.join(workDir, "massgis_shards");
  ensureDir(shardDir);

  const fds = Array.from({ length: shards }, (_, i) => {
    const p = path.join(shardDir, `massgis_shard_${String(i).padStart(3, "0")}.ndjson`);
    return { p, fd: fs.openSync(p, "w") };
  });

  let written = 0;
  for await (const line of ndjsonLines(masterPath)) {
    const o = JSON.parse(line);
    const keyRaw = o.map_par_id ?? o.MAP_PAR_ID ?? o.loc_id ?? o.LOC_ID ?? o.parcel_id_norm ?? o.parcel_id ?? null;
    const key = normalizeParcelKey(keyRaw);
    if (!key) continue;
    writeNDJSONLine(fds[shardIndexForKey(key, shards)].fd, o);
    written++;
    if (written % 500000 === 0) console.log(`[progress] massgis sharding wrote ${written}`);
  }
  for (const f of fds) fs.closeSync(f.fd);
  console.log(`[done] massgis sharded rows=${written} -> ${shardDir}`);
  return { shardDir, written, masterPath };
}

async function shardProperties(propertiesIn, workDir, shards) {
  ensureDir(workDir);
  const shardDir = path.join(workDir, "prop_shards");
  ensureDir(shardDir);

  const fds = Array.from({ length: shards }, (_, i) => {
    const p = path.join(shardDir, `props_shard_${String(i).padStart(3, "0")}.ndjson`);
    return { p, fd: fs.openSync(p, "w") };
  });

  let processed = 0;
  for await (const line of ndjsonLines(propertiesIn)) {
    const o = JSON.parse(line);
    const key = getPropertyParcelKey(o);
    const si = key ? shardIndexForKey(key, shards) : 0;
    writeNDJSONLine(fds[si].fd, o);
    processed++;
    if (processed % 500000 === 0) console.log(`[progress] properties sharding processed ${processed}`);
  }
  for (const f of fds) fs.closeSync(f.fd);
  console.log(`[done] properties sharded rows=${processed} -> ${shardDir}`);
  return { shardDir, processed };
}

async function loadMassGISShardToMap(shardPath) {
  const map = new Map();
  let rows = 0;
  for await (const line of ndjsonLines(shardPath)) {
    const o = JSON.parse(line);
    const keyRaw = o.map_par_id ?? o.MAP_PAR_ID ?? o.loc_id ?? o.LOC_ID ?? o.parcel_id_norm ?? o.parcel_id ?? null;
    for (const v of parcelKeyVariants(keyRaw)) if (!map.has(v)) map.set(v, o);
    rows++;
  }
  return { map, rows, keys: map.size };
}

function buildAssessorBest({ muniRec, massRec }) {
  const bySource = {};
  if (muniRec) bySource.city_assessor_raw = muniRec;
  if (massRec) bySource.massgis_statewide_raw = massRec;

  const best = {};
  const source_map = {};
  const fallback_fields = [];

  function setBest(pathKey, valueObj, source) {
    const parts = pathKey.split(".");
    let cur = best;
    for (let i = 0; i < parts.length - 1; i++) { cur[parts[i]] = cur[parts[i]] || {}; cur = cur[parts[i]]; }
    cur[parts.at(-1)] = valueObj;
    source_map[pathKey] = source;
    if (source !== "city_assessor") fallback_fields.push(pathKey);
  }

  function pick(fieldMuni, fieldMass) {
    if (muniRec && muniRec[fieldMuni] != null) return { v: muniRec[fieldMuni], s: "city_assessor" };
    if (massRec && massRec[fieldMass] != null) return { v: massRec[fieldMass], s: "massgis_statewide" };
    return { v: null, s: "unknown" };
  }

  const tot = pick("assessed_total", "total_val"); if (tot.v != null) setBest("valuation.total_value", { value: tot.v }, tot.s);
  const land = pick("assessed_land", "land_val"); if (land.v != null) setBest("valuation.land_value", { value: land.v }, land.s);
  const bld = pick("assessed_building", "bldg_val"); if (bld.v != null) setBest("valuation.building_value", { value: bld.v }, bld.s);
  const oth = pick("assessed_other", "other_val"); if (oth.v != null) setBest("valuation.other_value", { value: oth.v }, oth.s);
  const fy = pick("assessed_year", "fy"); if (fy.v != null) setBest("valuation.assessment_year", { value: fy.v }, fy.s);

  const sd = pick("last_sale_date", "ls_date"); if (sd.v != null) setBest("transaction.last_sale_date", { value: sd.v }, sd.s);
  const sp = pick("last_sale_price", "ls_price"); if (sp.v != null) setBest("transaction.last_sale_price", { value: sp.v }, sp.s);

  const uc = pick("use_code_norm", "use_code"); if (uc.v != null) setBest("structure.use_code", { value: uc.v }, uc.s);
  const units = pick("units_est", "units"); if (units.v != null) setBest("structure.units", { value: units.v }, units.s);
  const yb = pick("year_built_est", "year_built"); if (yb.v != null) setBest("structure.year_built", { value: yb.v }, yb.s);

  const lot = pick("lot_sqft_est", "lot_size"); if (lot.v != null) setBest("site.lot_size", { value: lot.v }, lot.s);
  const barea = pick("building_area_sqft", "bld_area"); if (barea.v != null) setBest("structure.building_area_sqft", { value: barea.v }, barea.s);

  return {
    assessor_by_source: bySource,
    assessor_best: best,
    assessor_source_map: source_map,
    assessor_fallback_fields: [...new Set(fallback_fields.filter(f => source_map[f] !== "city_assessor"))]
  };
}

async function attachShard(propsShardPath, massgisShardPath, muniIndex, outFd) {
  const { map: massMap } = await loadMassGISShardToMap(massgisShardPath);
  let processed = 0, matched = 0, usedMuni = 0, usedMass = 0, unmatched = 0;

  for await (const line of ndjsonLines(propsShardPath)) {
    const o = JSON.parse(line);
    const key = getPropertyParcelKey(o);
    let muniRec = null, massRec = null;

    if (key) {
      for (const v of parcelKeyVariants(key)) {
        if (!muniRec && muniIndex.has(v)) muniRec = muniIndex.get(v);
        if (!massRec && massMap.has(v)) massRec = massMap.get(v);
        if (muniRec && massRec) break;
      }
    }

    if (muniRec) usedMuni++;
    if (massRec) usedMass++;

    if (muniRec || massRec) {
      matched++;
      const built = buildAssessorBest({ muniRec, massRec });
      o.assessor_by_source = built.assessor_by_source;
      o.assessor_best = built.assessor_best;
      o.assessor_source_map = built.assessor_source_map;
      o.assessor_fallback_fields = built.assessor_fallback_fields;
      if (!o.parcel_id_norm) o.parcel_id_norm = key;
    } else {
      unmatched++;
    }

    writeNDJSONLine(outFd, o);
    processed++;
  }

  return { processed, matched, unmatched, usedMuni, usedMass };
}

async function main() {
  const args = parseArgs(process.argv);
  const cfg = readJSON(args.config);

  const propertiesIn = cfg.properties_in;
  const cityPtr = cfg.city_master_ptr;
  const massPtr = cfg.massgis_master_ptr;
  const outDir = cfg.out_dir;
  const workDir = cfg.work_dir;
  const shards = cfg.shards ?? 128;

  console.log("[start] Phase 4 — GLOBAL assessor master merge + PropertySpine attach (v6)");
  console.log("[info] root:", process.cwd());
  console.log("[info] config:", args.config);
  console.log("[info] properties_in:", propertiesIn);
  console.log("[info] city_master_ptr:", cityPtr);
  console.log("[info] massgis_master_ptr:", massPtr);
  console.log("[info] shards:", shards);

  ensureDir(outDir);
  ensureDir(workDir);

  const muniMasters = loadMunicipalMasters(cityPtr);
  console.log("[info] building municipal multi-key index...");
  const muni = await buildMunicipalIndex(muniMasters);
  const muniIndex = muni.idx;

  console.log("[info] sharding MassGIS master...");
  const massShard = await shardMassGISMaster(massPtr, workDir, shards);

  console.log("[info] sharding properties...");
  const propShard = await shardProperties(propertiesIn, workDir, shards);

  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const outPath = path.join(outDir, `${cfg.output_prefix}__${ts}__V6.ndjson`);
  const outFd = fs.openSync(outPath, "w");

  let total = { processed: 0, matched: 0, unmatched: 0, usedMuni: 0, usedMass: 0 };

  for (let i = 0; i < shards; i++) {
    const ms = path.join(massShard.shardDir, `massgis_shard_${String(i).padStart(3, "0")}.ndjson`);
    const ps = path.join(propShard.shardDir, `props_shard_${String(i).padStart(3, "0")}.ndjson`);
    if (!fs.existsSync(ps)) continue;
    if (!fs.existsSync(ms)) fs.writeFileSync(ms, "", "utf8");
    const r = await attachShard(ps, ms, muniIndex, outFd);
    total.processed += r.processed;
    total.matched += r.matched;
    total.unmatched += r.unmatched;
    total.usedMuni += r.usedMuni;
    total.usedMass += r.usedMass;
    if (i % 16 === 0) console.log(`[progress] shard ${i} processed=${total.processed} matched=${total.matched} muni=${total.usedMuni} mass=${total.usedMass}`);
  }
  fs.closeSync(outFd);

  const auditDir = path.join(process.cwd(), "publicData", "_audit", "phase4_assessor");
  ensureDir(auditDir);
  const auditPath = path.join(auditDir, `phase4_global_master_merge_attach__${ts}__V6.json`);
  const currentPtrPath = path.join(outDir, cfg.current_ptr_name || "CURRENT_PROPERTIES_WITH_ASSESSOR_GLOBAL_BEST.json");

  const audit = {
    created_at: new Date().toISOString(),
    config: args.config,
    outputs: { propertiesOut: outPath, currentPtr: currentPtrPath, auditPath, workDir, shards },
    stats: {
      processed: total.processed,
      matched: total.matched,
      unmatched: total.unmatched,
      usedMuni: total.usedMuni,
      usedMass: total.usedMass,
      muniCities: muniMasters.length,
      muniKeysIndexed: muni.keys,
      muniRecordsSeen: muni.seen,
      massgisShardedRows: massShard.written
    },
    hashes: { properties_out_sha256: sha256File(outPath) },
    notes: [
      "v6: keys on property.parcel_id (fallback to property_id-derived).",
      "v6: municipal index uses multi-key candidates and safe variants.",
      "v6: writes assessor_by_source.city_assessor_raw and assessor_by_source.massgis_statewide_raw explicitly.",
      "v6: backfills parcel_id_norm when missing (non-breaking)."
    ]
  };

  fs.writeFileSync(auditPath, JSON.stringify(audit, null, 2), "utf8");
  fs.writeFileSync(currentPtrPath, JSON.stringify({
    updated_at: new Date().toISOString(),
    note: "AUTO: Phase4 GLOBAL merge+attach v6 (parcel_id key + muni variants)",
    properties_ndjson: outPath,
    audit: auditPath
  }, null, 2), "utf8");

  console.log("[done] wrote audit:", auditPath);
  console.log("[ok] wrote CURRENT pointer:", currentPtrPath);
  console.log("[done] Phase 4 GLOBAL merge+attach v6 complete.");
}

main().catch(err => { console.error(err); process.exit(1); });
