#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import crypto from "node:crypto";

function readJSON(p){
  const raw = fs.readFileSync(p);
  const s = raw[0]===0xEF && raw[1]===0xBB && raw[2]===0xBF ? raw.slice(3).toString("utf8") : raw.toString("utf8");
  return JSON.parse(s);
}
function parseArgs(argv){
  const out = {};
  for (let i=2;i<argv.length;i++){
    if (argv[i]==="--config") out.config = argv[++i];
  }
  return out;
}
function sha256File(p){
  const h = crypto.createHash("sha256");
  const fd = fs.openSync(p, "r");
  const buf = Buffer.allocUnsafe(1024*1024);
  let n;
  while ((n = fs.readSync(fd, buf, 0, buf.length, null)) > 0){
    h.update(buf.subarray(0,n));
  }
  fs.closeSync(fd);
  return h.digest("hex");
}

function normParcelKeys(v){
  if (v == null) return [];
  const s = String(v).trim();
  if (!s) return [];
  const keys = new Set();
  keys.add(s);
  keys.add(s.replace(/\s+/g,""));
  keys.add(s.replace(/-/g,""));
  keys.add(s.replace(/[-\s]+/g,""));
  const digits = s.replace(/\D/g,"");
  if (digits) keys.add(digits);
  if (digits) keys.add(digits.replace(/^0+/,""));
  return [...keys].filter(Boolean);
}

async function buildIndex(ndjsonPath, keyField){
  const index = new Map();
  const rl = readline.createInterface({ input: fs.createReadStream(ndjsonPath,{encoding:"utf8"}), crlfDelay: Infinity });
  let n=0;
  for await (const line of rl){
    if (!line) continue;
    n++;
    if (n % 500000 === 0) console.log("[progress] index scanned", n, "from", path.basename(ndjsonPath));
    let obj;
    try { obj = JSON.parse(line); } catch { continue; }
    const keys = normParcelKeys(obj[keyField] ?? obj.parcel_id_norm ?? obj.parcel_id ?? obj.parcelId ?? obj.map_par_id ?? obj.MAP_PAR_ID);
    for (const k of keys){
      if (!index.has(k)) index.set(k, line);
    }
  }
  return index;
}

function pickBest(cityRaw, massRaw){
  const src = cityRaw ?? massRaw;
  if (!src) return null;
  const out = { valuation:{}, transaction:{}, structure:{}, site:{} };
  const get = (o, ...names)=> {
    for (const n of names){
      if (o && o[n] != null && o[n] !== "") return o[n];
    }
    return null;
  };
  const assessed_total = get(src, "assessed_total");
  const assessed_land = get(src, "assessed_land");
  const assessed_building = get(src, "assessed_building");
  const assessed_other = get(src, "assessed_other");
  const assessed_year = get(src, "assessed_year");
  const last_sale_date = get(src, "last_sale_date");
  const last_sale_price = get(src, "last_sale_price");
  const use_code = get(src, "use_code_norm");
  const year_built = get(src, "year_built_est");
  const units = get(src, "units_est");
  const bldg_area = get(src, "building_area_sqft");
  const lot = get(src, "lot_size_raw");
  const lot_units = get(src, "lot_units_raw");

  if (assessed_total != null) out.valuation.total_value = { value: Number(assessed_total) };
  if (assessed_land != null) out.valuation.land_value = { value: Number(assessed_land) };
  if (assessed_building != null) out.valuation.building_value = { value: Number(assessed_building) };
  if (assessed_other != null) out.valuation.other_value = { value: Number(assessed_other) };
  if (assessed_year != null) out.valuation.assessment_year = { value: Number(assessed_year) };

  if (last_sale_date != null) out.transaction.last_sale_date = { value: String(last_sale_date) };
  if (last_sale_price != null) out.transaction.last_sale_price = { value: Number(last_sale_price) };

  if (use_code != null) out.structure.use_code = { value: String(use_code) };
  if (units != null) out.structure.units = { value: Number(units) };
  if (year_built != null) out.structure.year_built = { value: Number(year_built) };
  if (bldg_area != null) out.structure.building_area_sqft = { value: Number(bldg_area) };

  if (lot != null) {
    out.site.lot_size = { value: typeof lot === "number" ? lot : Number(lot) };
    if (lot_units != null) out.site.lot_units = { value: String(lot_units) };
  }
  out.meta = { best_source: cityRaw ? "city_assessor" : "massgis_statewide", upgraded_at: new Date().toISOString() };
  return out;
}

async function main(){
  const args = parseArgs(process.argv);
  if (!args.config) throw new Error("missing --config <file>");
  const cfgPath = path.resolve(args.config);
  const cfg = readJSON(cfgPath);

  const propertiesIn = path.resolve(cfg.properties_in);
  const cityPtr = readJSON(path.resolve(cfg.city_master_ptr));
  const massPtr = readJSON(path.resolve(cfg.massgis_master_ptr));
  const cityNd = path.resolve(cityPtr.merged_ndjson ?? cityPtr.master_ndjson ?? cityPtr.ndjson ?? cityPtr.file);
  const massNd = path.resolve(massPtr.master_ndjson ?? massPtr.ndjson ?? massPtr.file ?? massPtr.path);

  if (!fs.existsSync(propertiesIn)) throw new Error("properties_in missing: " + propertiesIn);
  if (!fs.existsSync(cityNd)) throw new Error("city master ndjson missing: " + cityNd);
  if (!fs.existsSync(massNd)) throw new Error("massgis master ndjson missing: " + massNd);

  const outDir = path.resolve(cfg.out_dir);
  fs.mkdirSync(outDir, { recursive: true });
  const ts = new Date().toISOString().replaceAll(":","-").replaceAll(".","-");
  const outNd = path.join(outDir, `properties__with_assessor_global_best__${ts}__${cfg.version_tag ?? "V8"}.ndjson`);
  const auditPath = path.join(outDir, `phase4_global_attach_audit__${ts}__${cfg.version_tag ?? "V8"}.json`);

  console.log("[start] Phase4 GLOBAL attach v8");
  console.log("[info] properties_in:", propertiesIn);
  console.log("[info] cityNd:", cityNd);
  console.log("[info] massNd:", massNd);
  console.log("[info] out:", outNd);

  console.log("[info] indexing city assessor master...");
  const cityIndex = await buildIndex(cityNd, "parcel_id_norm");

  console.log("[info] indexing massgis statewide master...");
  const massIndex = await buildIndex(massNd, "parcel_id_norm");

  const out = fs.createWriteStream(outNd, { encoding:"utf8" });
  const rl = readline.createInterface({ input: fs.createReadStream(propertiesIn,{encoding:"utf8"}), crlfDelay: Infinity });

  let n=0, hitCity=0, hitMass=0, any=0;
  for await (const line of rl){
    if (!line) continue;
    n++;
    if (n % 500000 === 0) console.log("[progress] processed", n, "any", any, "city", hitCity, "mass", hitMass);

    let p;
    try { p = JSON.parse(line); } catch { continue; }

    const keys = normParcelKeys(p.parcel_id ?? p.parcel_id_norm ?? p.parcelId ?? p.parcel);
    let cityRaw = null, massRaw = null;

    for (const k of keys){
      const cl = cityIndex.get(k);
      if (cl){ cityRaw = JSON.parse(cl); break; }
    }
    for (const k of keys){
      const ml = massIndex.get(k);
      if (ml){ massRaw = JSON.parse(ml); break; }
    }

    if (cityRaw || massRaw) any++;
    if (cityRaw) hitCity++;
    if (!cityRaw && massRaw) hitMass++;

    p.assessor_by_source = p.assessor_by_source ?? {};
    if (cityRaw) p.assessor_by_source.city_assessor_raw = cityRaw;
    if (massRaw) p.assessor_by_source.massgis_statewide_raw = massRaw;

    const best = pickBest(cityRaw, massRaw);
    if (best) {
      p.assessor_best = best;
      const sm = {};
      const prefer = (hasCity, hasMass)=> hasCity ? "city_assessor" : (hasMass ? "massgis_statewide" : null);
      sm["valuation.total_value"] = prefer(!!cityRaw && !!best?.valuation?.total_value, !!massRaw && !!best?.valuation?.total_value);
      sm["valuation.land_value"] = prefer(!!cityRaw && !!best?.valuation?.land_value, !!massRaw && !!best?.valuation?.land_value);
      sm["valuation.building_value"] = prefer(!!cityRaw && !!best?.valuation?.building_value, !!massRaw && !!best?.valuation?.building_value);
      sm["valuation.other_value"] = prefer(!!cityRaw && !!best?.valuation?.other_value, !!massRaw && !!best?.valuation?.other_value);
      sm["valuation.assessment_year"] = prefer(!!cityRaw && !!best?.valuation?.assessment_year, !!massRaw && !!best?.valuation?.assessment_year);
      sm["transaction.last_sale_date"] = prefer(!!cityRaw && !!best?.transaction?.last_sale_date, !!massRaw && !!best?.transaction?.last_sale_date);
      sm["transaction.last_sale_price"] = prefer(!!cityRaw && !!best?.transaction?.last_sale_price, !!massRaw && !!best?.transaction?.last_sale_price);
      sm["structure.year_built"] = prefer(!!cityRaw && !!best?.structure?.year_built, !!massRaw && !!best?.structure?.year_built);
      sm["structure.units"] = prefer(!!cityRaw && !!best?.structure?.units, !!massRaw && !!best?.structure?.units);
      sm["structure.building_area_sqft"] = prefer(!!cityRaw && !!best?.structure?.building_area_sqft, !!massRaw && !!best?.structure?.building_area_sqft);
      sm["site.lot_size"] = prefer(!!cityRaw && !!best?.site?.lot_size, !!massRaw && !!best?.site?.lot_size);
      p.assessor_source_map = sm;

      const fb = [];
      for (const [k,v] of Object.entries(sm)){
        if (v && v !== "city_assessor") fb.push(k);
      }
      p.assessor_fallback_fields = fb;
    }

    out.write(JSON.stringify(p) + "\n");
  }
  await new Promise(r=>out.end(r));
  const outHash = sha256File(outNd);
  const audit = {
    created_at: new Date().toISOString(),
    config: cfg,
    rows_in: n,
    attached_any: any,
    attached_city: hitCity,
    attached_massgis_only: hitMass,
    out_ndjson: outNd,
    out_sha256: outHash,
    notes: [
      "Coverage boost baseline: index city merged masters + MassGIS statewide by multiple parcel key variants.",
      "Run your assessor_best provenance + MA tax_fy fill scripts after this (v5/v6 packs)."
    ]
  };
  fs.writeFileSync(auditPath, JSON.stringify(audit, null, 2), "utf8");
  console.log("[ok] wrote audit:", auditPath);
  console.log("[ok] out sha256:", outHash);
}

main().catch(e=>{
  console.error("[fatal]", e);
  process.exit(1);
});
