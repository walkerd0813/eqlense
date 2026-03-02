import fs from "node:fs";

function pickNdjsonFromPointer(ptrObj, label){
  if (!ptrObj || typeof ptrObj !== "object") return null;

  // common keys weÃ¢â‚¬â„¢ve used across packs
  const candidates = [
    "ndjson",
    "master_ndjson",
    "masterNdjson",
    "assessor_master_ndjson",
    "assessorMasterNdjson",
    "city_master_ndjson",
    "cityMasterNdjson",
    "properties_ndjson",
    "file",
    "path"
  ];

  for (const k of candidates) {
    const v = ptrObj[k];
    if (typeof v === "string" && v.toLowerCase().endsWith(".ndjson")) return v;
  }

  // sometimes nested: { outputs: { masterNdjson: "..." } }
  const nests = ["outputs","data","current","pointers","files"];
  for (const nk of nests) {
    const n = ptrObj[nk];
    if (n && typeof n === "object") {
      for (const k of candidates) {
        const v = n[k];
        if (typeof v === "string" && v.toLowerCase().endsWith(".ndjson")) return v;
      }
    }
  }

  return null;
}import path from "node:path";
import readline from "node:readline";
import crypto from "node:crypto";

function readJSON(p){
  const txt = fs.readFileSync(p,"utf8").replace(/^\uFEFF/,"");
  return JSON.parse(txt);
}
function ensureDir(p){ fs.mkdirSync(p,{recursive:true}); }
function sha256File(p){
  const h=crypto.createHash("sha256");
  const s=fs.createReadStream(p);
  return new Promise((res,rej)=>{ s.on("data",(d)=>h.update(d)); s.on("end",()=>res(h.digest("hex"))); s.on("error",rej); });
}
function nowTag(){
  return new Date().toISOString().replace(/[:.]/g,"-");
}

function makeVariants(k){
  if (!k || typeof k!=="string") return [];
  const raw = k.trim();
  if (!raw) return [];
  const noSpace = raw.replace(/\s+/g,"");
  const noDash  = raw.replace(/-/g,"");
  const digits  = raw.replace(/[^0-9]/g,"");
  // preserve order, dedupe
  const out = [];
  for (const v of [raw, noSpace, noDash, digits]){
    if (v && !out.includes(v)) out.push(v);
  }
  return out;
}

// Map key->record with ambiguity protection
function putMap(map, amb, key, rec){
  if (!key) return;
  if (amb.has(key)) return;
  if (!map.has(key)) { map.set(key, rec); return; }
  // collision -> ambiguous
  map.delete(key);
  amb.add(key);
}

async function shardNdjson(inPath, outDir, shards, keyFn){
  ensureDir(outDir);
  const out = Array.from({length:shards},(_,i)=>fs.createWriteStream(path.join(outDir, `shard_${String(i).padStart(3,"0")}.ndjson`), {encoding:"utf8"}));
  const rl = readline.createInterface({ input: fs.createReadStream(inPath,{encoding:"utf8"}), crlfDelay: Infinity });
  let n=0;
  for await (const line of rl){
    if (!line) continue;
    let o; try { o=JSON.parse(line); } catch { continue; }
    const k = keyFn(o) ?? "";
    const h = crypto.createHash("md5").update(String(k)).digest("hex");
    const idx = parseInt(h.slice(0,8),16) % shards;
    out[idx].write(line+"\n");
    n++;
    if (n % 500000 === 0) console.log("[progress] sharding processed", n);
  }
  for (const w of out) w.end();
  return n;
}

function pickBestKeyFromProperty(o){
  // property.parcel_id is present for all, but we also honor parcel_id_norm if exists
  const p = (o.parcel_id ?? "").toString().trim();
  const pn = (o.parcel_id_norm ?? "").toString().trim();
  if (p) return p;
  if (pn) return pn;
  // fallback: derive from property_id like "ma:parcel:XXXXX"
  const pid = (o.property_id ?? "").toString();
  const m = pid.match(/^ma:parcel:(.+)$/i);
  return m ? m[1].trim() : "";
}

function pickKeyFromMassgis(o){
  // Use parcel_id_norm if present; else parcel_id; else whatever exists
  const a = (o.parcel_id_norm ?? "").toString().trim();
  const b = (o.parcel_id ?? "").toString().trim();
  return a || b || "";
}

function deepClone(x){ return x ? JSON.parse(JSON.stringify(x)) : x; }

function buildAssessorBestWithProvenance(srcRaw, srcName){
  // Minimal: preserve existing best fields if present; otherwise create best blocks.
  // This phase expects assessor_by_source already written in v6 output.
  // We do NOT overwrite; attach happens elsewhere. Here just pass-through.
  return;
}

async function main(){
  const args = process.argv.slice(2);
  const cfgIdx = args.indexOf("--config");
  if (cfgIdx === -1 || !args[cfgIdx+1]) {
    console.log("usage: node scripts/...v7.mjs --config <config.json>");
    process.exit(2);
  }
  const configPath = args[cfgIdx+1];
  const cfg = readJSON(configPath);

  const ROOT = cfg.root ?? ".";
  const propertiesIn = cfg.propertiesIn ?? cfg.properties_in ?? cfg.properties ?? cfg.propertiesPath ?? cfg.properties_in_path;
  const cityMasterPtr = cfg.cityMasterPtr ?? cfg.city_master_ptr;
  const massgisMasterPtr = cfg.massgisMasterPtr ?? cfg.massgis_master_ptr;
  const workDir = cfg.workDir ?? path.join(ROOT,"publicData","assessors","_global_master","_work_v7");
  const shards = cfg.shards ?? 128;

  if (!propertiesIn) throw new Error("config missing propertiesIn/properties_in");
  if (!cityMasterPtr) throw new Error("config missing cityMasterPtr/city_master_ptr");
  if (!massgisMasterPtr) throw new Error("config missing massgisMasterPtr/massgis_master_ptr");

  console.log("[start] Phase4 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â GLOBAL assessor master merge + PropertySpine attach (v7)");
  console.log("[info] config:", configPath);
  console.log("[info] properties_in:", propertiesIn);
  console.log("[info] city_master_ptr:", cityMasterPtr);
  console.log("[info] massgis_master_ptr:", massgisMasterPtr);
  console.log("[info] shards:", shards);

  ensureDir(workDir);

  const cityPtr = readJSON(cityMasterPtr);
  const massPtr = readJSON(massgisMasterPtr);

  const cityMasterPath = cityPtr.master_ndjson ?? cityPtr.path ?? cityPtr.ndjson ?? cityPtr.file;
  const massMasterPath = massPtr.master_ndjson ?? massPtr.path ?? massPtr.ndjson ?? massPtr.file;

  if (!cityMasterPath || !fs.existsSync(cityMasterPath)) {
  const ptrObj = readJSON(cfg.city_master_ptr);
  const picked = pickNdjsonFromPointer(ptrObj);
  if (!picked) throw new Error("city master ndjson missing (could not resolve from pointer): " + cfg.city_master_ptr);
  cityMasterNdjson = picked;
}
  if (!massMasterPath || !fs.existsSync(massMasterPath)) throw new Error("massgis master ndjson missing: "+massMasterPath);

  // ---------- Shard MassGIS + Properties ----------
  const massShardDir = path.join(workDir,"massgis_shards");
  const propShardDir = path.join(workDir,"prop_shards");

  console.log("[info] sharding MassGIS master...");
  const massRows = await shardNdjson(massMasterPath, massShardDir, shards, pickKeyFromMassgis);
  console.log("[done] massgis sharded rows=", massRows, "->", massShardDir);

  console.log("[info] sharding properties...");
  const propRows = await shardNdjson(propertiesIn, propShardDir, shards, pickBestKeyFromProperty);
  console.log("[done] properties sharded rows=", propRows, "->", propShardDir);

  // ---------- Load municipal index (already smaller) ----------
  // v6 built a multi-key muni index; we keep that logic: read CURRENT_PHASE4_ASSESSOR_MASTER.json points to per-city ndjson lists.
  // To stay drop-in, we assume city master is already a merged NDJSON with "city" + "parcel_id_norm" and other fields.
  console.log("[info] building municipal multi-key index...");
  const muniMap = new Map();      // variant -> record
  const muniAmb = new Set();      // variant ambiguous
  let muniSeen = 0, muniIndexed = 0;

  {
    const rl = readline.createInterface({ input: fs.createReadStream(cityMasterPath,{encoding:"utf8"}), crlfDelay: Infinity });
    for await (const line of rl){
      if (!line) continue;
      let o; try { o=JSON.parse(line); } catch { continue; }
      muniSeen++;
      const keyBase = (o.parcel_id_norm ?? o.parcel_id ?? "").toString().trim();
      for (const v of makeVariants(keyBase)) putMap(muniMap, muniAmb, v, o);
      if (muniSeen % 200000 === 0) console.log("[progress] muni index seen", muniSeen);
    }
    muniIndexed = muniMap.size;
  }
  console.log("[done] muni index loaded variants_indexed=", muniIndexed, "records_seen=", muniSeen, "ambiguous=", muniAmb.size);

  // ---------- Process shards: build MassGIS shard map w/ variants and join ----------
  const outDir = path.join(ROOT,"publicData","properties","_attached","phase4_assessor_global_v7");
  ensureDir(outDir);

  const stamp = nowTag();
  const propertiesOut = path.join(outDir, `properties__with_assessor_global_best__${stamp}__V7.ndjson`);
  const outW = fs.createWriteStream(propertiesOut,{encoding:"utf8"});

  const stats = {
    processed: 0,
    matched: 0,
    unmatched: 0,
    usedMuni: 0,
    usedMass: 0,
    matched_by_variant: { raw:0, noSpace:0, noDash:0, digitsOnly:0 },
    ambiguous_massgis_variant_hits: 0,
    ambiguous_muni_variant_hits: 0
  };

  function findByVariants(map, amb, baseKey){
    const vars = makeVariants(baseKey);
    const labels = ["raw","noSpace","noDash","digitsOnly"];
    for (let i=0;i<vars.length;i++){
      const v = vars[i];
      if (amb.has(v)) return { hit:null, variant:labels[i] ?? "raw", ambiguous:true };
      const rec = map.get(v);
      if (rec) return { hit:rec, variant:labels[i] ?? "raw", ambiguous:false };
    }
    return { hit:null, variant:null, ambiguous:false };
  }

  for (let s=0; s<shards; s++){
    const shardName = `shard_${String(s).padStart(3,"0")}.ndjson`;
    const massShardPath = path.join(massShardDir, shardName);
    const propShardPath = path.join(propShardDir, shardName);
    if (!fs.existsSync(propShardPath)) continue;

    // build mass map for this shard
    const massMap = new Map();
    const massAmb = new Set();
    if (fs.existsSync(massShardPath)){
      const rl = readline.createInterface({ input: fs.createReadStream(massShardPath,{encoding:"utf8"}), crlfDelay: Infinity });
      for await (const line of rl){
        if (!line) continue;
        let o; try { o=JSON.parse(line); } catch { continue; }
        const kBase = pickKeyFromMassgis(o);
        for (const v of makeVariants(kBase)) putMap(massMap, massAmb, v, o);
      }
    }

    const rlP = readline.createInterface({ input: fs.createReadStream(propShardPath,{encoding:"utf8"}), crlfDelay: Infinity });
    for await (const line of rlP){
      if (!line) continue;
      let p; try { p=JSON.parse(line); } catch { continue; }

      stats.processed++;

      const pKey = pickBestKeyFromProperty(p);

      // muni first
      const muniRes = findByVariants(muniMap, muniAmb, pKey);
      let used = null;
      let usedName = null;
      let usedVariant = null;

      if (muniRes.ambiguous){
        stats.ambiguous_muni_variant_hits++;
      } else if (muniRes.hit){
        used = muniRes.hit;
        usedName = "city_assessor";
        usedVariant = muniRes.variant;
        stats.usedMuni++;
        stats.matched++;
        if (usedVariant) stats.matched_by_variant[usedVariant] = (stats.matched_by_variant[usedVariant]||0)+1;
      } else {
        // massgis
        const massRes = findByVariants(massMap, massAmb, pKey);
        if (massRes.ambiguous){
          stats.ambiguous_massgis_variant_hits++;
        } else if (massRes.hit){
          used = massRes.hit;
          usedName = "massgis_statewide";
          usedVariant = massRes.variant;
          stats.usedMass++;
          stats.matched++;
          if (usedVariant) stats.matched_by_variant[usedVariant] = (stats.matched_by_variant[usedVariant]||0)+1;
        }
      }

      if (!used) stats.unmatched++;

      // Attach into assessor_by_source (preserve existing if present)
      p.assessor_by_source = p.assessor_by_source ?? {};
      if (usedName === "city_assessor") p.assessor_by_source.city_assessor_raw = used;
      if (usedName === "massgis_statewide") p.assessor_by_source.massgis_statewide_raw = used;

      // Backfill parcel_id_norm safely if missing
      if (!p.parcel_id_norm && pKey) p.parcel_id_norm = pKey;

      // Write out
      outW.write(JSON.stringify(p) + "\n");

      if (stats.processed % 500000 === 0) {
        console.log("[progress] processed", stats.processed, "matched", stats.matched, "muni", stats.usedMuni, "mass", stats.usedMass, "unmatched", stats.unmatched);
      }
    }
  }

  outW.end();

  const auditDir = path.join(ROOT,"publicData","_audit","phase4_assessor");
  ensureDir(auditDir);
  const auditPath = path.join(auditDir, `phase4_global_master_merge_attach__${stamp}__V7.json`);

  const sha = await sha256File(propertiesOut);

  const audit = {
    created_at: new Date().toISOString(),
    config: configPath,
    outputs: { propertiesOut, auditPath, workDir, shards },
    stats,
    hashes: { properties_out_sha256: sha },
    notes: [
      "v7: deterministic key-variant matching (raw/noSpace/noDash/digitsOnly) on BOTH property and MassGIS keys.",
      "v7: ambiguity guard ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â if a variant collides to multiple records, variant is rejected (no attach).",
      "v7: muni preferred over MassGIS when both match."
    ]
  };

  fs.writeFileSync(auditPath, JSON.stringify(audit,null,2), "utf8");
  console.log("[done] wrote audit:", auditPath);
  console.log("[done] output:", propertiesOut);
  console.log("[done] stats:", JSON.stringify(stats,null,2));
}

main().catch((e)=>{ console.error("[fatal]", e); process.exit(1); });
