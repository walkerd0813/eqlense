import fs from "node:fs";
import readline from "node:readline";
import path from "node:path";
import { readJSON, writeJSON, ensureDirSync, nowStamp, sha1File, containsAny, normCity, safeLonLat, haversineMeters } from "./lib/utils.mjs";
import { describeService, listLayers, dumpLayerToGeoJSON } from "./lib/arcgis.mjs";

function arg(name, def=null) {
  const i = process.argv.findIndex(a => a === `--${name}`);
  if (i === -1) return def;
  const v = process.argv[i+1];
  return (v === undefined) ? def : v;
}

const root = arg("root");
const noAttach = String(arg("noAttach", "false")).toLowerCase() === "true";

if (!root) {
  console.error("Missing --root");
  process.exit(1);
}

console.log("====================================================");
console.log("PHASE 3 — UTILITIES FREEZE + (OPTIONAL) ATTACH v1");
console.log("====================================================");
console.log(`[info] root: ${root}`);
console.log(`[info] noAttach: ${noAttach}`);

const stamp = nowStamp();

const cfgPath = path.join(root, "scripts", "packs", "phase3_utilities_pack_v1", "config", "phase3_sources.json");
const cfg = readJSON(cfgPath);

const outFrozenDir = path.join(root, "publicData", "overlays", "_frozen");
const outDictDir   = path.join(root, "publicData", "overlays", "_frozen", "_dict");
const outAttachDir = path.join(root, "publicData", "overlays", "_attachments");
const auditDir     = path.join(root, "publicData", "_audit", "phase3_utilities");
ensureDirSync(outFrozenDir);
ensureDirSync(outDictDir);
ensureDirSync(outAttachDir);
ensureDirSync(auditDir);

// contract pointer (reuse Phase 2 pointer file)
const contractPtr = path.join(root, "publicData", "_contracts", "CURRENT_CONTRACT_VIEW_MA.json");
let contractIn = null;
if (fs.existsSync(contractPtr)) {
  const ptr = readJSON(contractPtr);
  contractIn = ptr.current && fs.existsSync(ptr.current) ? ptr.current : null;
}

const contractBase = contractIn ? readJSON(contractIn) : { schema_version: "v1", phases: {} };

function shouldIncludeLayer(name, include, exclude) {
  const n = String(name || "").toLowerCase();
  if (exclude && exclude.length && exclude.some(k => n.includes(String(k).toLowerCase()))) return false;
  if (include && include.length) return include.some(k => n.includes(String(k).toLowerCase()));
  return true;
}

async function expandSource(src) {
  // returns array of { city, layer_key, service_url, layer_url, layer_name }
  const out = [];
  const include = cfg.layer_pick_rules?.include_keywords || [];
  const exclude = cfg.layer_pick_rules?.exclude_keywords || [];
  const city = src.city;

  if (src.type === "layer") {
    out.push({
      city,
      layer_key: `${normCity(city)}__${String(src.label || "layer").toLowerCase().replace(/[^a-z0-9]+/g,"_")}`,
      layer_url: src.url,
      layer_name: src.label || src.url
    });
    return out;
  }

  if (src.type === "mapserver_root" || src.type === "featureserver_root") {
    const layers = await listLayers(src.url);
    for (const l of layers) {
      if (!shouldIncludeLayer(l.name, include, exclude)) continue;
      out.push({
        city,
        layer_key: `${normCity(city)}__${String(l.name).toLowerCase().replace(/[^a-z0-9]+/g,"_")}`,
        layer_url: `${src.url}/${l.id}`,
        layer_name: l.name
      });
    }
    return out;
  }

  if (src.type === "services_root") {
    // For services roots, just describe and pick services that look like MapServer/FeatureServer roots (shallow)
    // Many servers list folders/services; ArcGIS Online differs. We'll attempt to read pjson and look for "services"/"folders".
    const meta = await describeService(src.url);
    const services = Array.isArray(meta.services) ? meta.services : [];
    for (const s of services) {
      const t = String(s.type || "");
      if (!(t === "MapServer" || t === "FeatureServer")) continue;
      const name = String(s.name || "");
      if (!shouldIncludeLayer(name, include, exclude)) continue;
      const serviceUrl = `${src.url}/${name}/${t}`;
      // list layers under that service
      try {
        const layers = await listLayers(serviceUrl);
        for (const l of layers) {
          if (!shouldIncludeLayer(l.name, include, exclude)) continue;
          out.push({
            city,
            layer_key: `${normCity(city)}__${String(name + "_" + l.name).toLowerCase().replace(/[^a-z0-9]+/g,"_")}`,
            layer_url: `${serviceUrl}/${l.id}`,
            layer_name: `${name} :: ${l.name}`
          });
        }
      } catch (e) {
        // If listing layers fails, skip safely.
      }
    }
    return out;
  }

  return out;
}

const discovered = [];
const warnings = [];

for (const src of cfg.sources) {
  try {
    console.log(`[scan] ${src.city} :: ${src.label} (${src.type})`);
    const layers = await expandSource(src);
    for (const l of layers) discovered.push(l);
    console.log(`[scan] +layers: ${layers.length}`);
  } catch (e) {
    warnings.push({ city: src.city, url: src.url, error: e.message });
    console.warn(`[warn] failed to expand source for ${src.city}: ${e.message}`);
  }
}

if (!discovered.length) {
  console.warn("[warn] no layers discovered. Check config sources.");
}

function classifyLayer(name) {
  const n = String(name || "").toLowerCase();
  if (n.includes("water")) return "water";
  if (n.includes("sewer")) return "sewer";
  if (n.includes("storm") || n.includes("drain")) return "storm";
  if (n.includes("gas")) return "gas";
  if (n.includes("electric")) return "electric";
  if (n.includes("easement") || n.includes("encumbrance") || n.includes("right_of_way") || n.includes("row")) return "easement_row";
  if (n.includes("road") || n.includes("street") || n.includes("bridge")) return "roads";
  return "other";
}

const dict = {
  phase: "phase3_utilities",
  version: "v1",
  created_at: new Date().toISOString(),
  layers: []
};

for (const l of discovered) {
  const layerClass = classifyLayer(l.layer_name);
  const outBase = `utility__${normCity(l.city)}__${layerClass}__${l.layer_key}__phase3__v1__${stamp}`;
  const outGeo = path.join(outFrozenDir, `${outBase}.geojson`);
  try {
    const res = await dumpLayerToGeoJSON({ layerUrl: l.layer_url, outPath: outGeo });
    const hash = sha1File(outGeo);
    dict.layers.push({
      layer_key: outBase.replace(/__\d{8}_\d{6}$/,""), // stable-ish
      city: l.city,
      utility_class: layerClass,
      source_type: "arcgis",
      source_url: l.layer_url,
      display_name: `${l.city} — ${l.layer_name}`,
      frozen_file: outGeo,
      dataset_hash: hash,
      feature_count: res.feature_count,
      attach_mode: "nearest_distance_proxy",
      confidence_grade: "C",
      notes: "Distance-only proxy (nearest vertex/centroid). Does not assert service connection."
    });
    console.log(`[freeze] ${l.city} :: ${l.layer_name} -> features=${res.feature_count}`);
  } catch (e) {
    warnings.push({ layer: l.layer_name, url: l.layer_url, error: e.message });
    console.warn(`[warn] freeze failed for ${l.layer_name}: ${e.message}`);
  }
}

const dictOut = path.join(outDictDir, `phase3_utilities_dictionary__v1__${stamp}.json`);
writeJSON(dictOut, dict);

const dictPtr = path.join(outDictDir, "CURRENT_PHASE3_UTILITIES_DICT.json");
writeJSON(dictPtr, { current: dictOut });

console.log(`[dict] wrote ${dictOut}`);
console.log(`[ptr]  wrote ${dictPtr}`);

if (noAttach) {
  // Write contract phase stub + pointer update, but skip attachments.
  const contractOut = path.join(root, "publicData", "_contracts", `contract_view_ma__phase3_utilities__v1__${stamp}.json`);
  const next = structuredClone(contractBase);
  next.phases = next.phases || {};
  next.phases.phase3_utilities = {
    dictionary: dictOut,
    attachments: null,
    created_at: new Date().toISOString()
  };
  writeJSON(contractOut, next);

  if (fs.existsSync(contractPtr)) {
    const bak = `${contractPtr}.bak_${stamp}`;
    fs.copyFileSync(contractPtr, bak);
  }
  writeJSON(contractPtr, { current: contractOut });

  console.log(`[contract] wrote ${contractOut}`);
  console.log(`[pointer] updated ${contractPtr}`);
  console.log("[done] freeze-only complete.");
  process.exit(0);
}

// ----------------------------
// ATTACH (distance proxy)
// ----------------------------
const propsPathGuess = contractBase?.inputs?.properties_ndjson || null;
let propsPath = propsPathGuess;

// Fallback heuristic to your known file if contract doesn't include it.
if (!propsPath) {
  const p46 = path.join(root, "publicData", "properties", "properties_v46_withBaseZoning__20251220_from_v44__NAMEFIX.ndjson");
  propsPath = fs.existsSync(p46) ? p46 : null;
}
if (!propsPath) {
  console.warn("[warn] Could not locate properties ndjson. Attach skipped.");
}

const attachOut = path.join(outAttachDir, `phase3_utilities__attachments__v1__${stamp}.ndjson`);

function featurePointsForDistance(geom) {
  // distance proxy: points from coordinates (vertices) + centroid for polygons
  if (!geom) return [];
  const t = geom.type;
  const c = geom.coordinates;
  const pts = [];
  const push = (p) => {
    const ok = safeLonLat(p);
    if (ok) pts.push([ok.lon, ok.lat]);
  };

  if (t === "Point") push(c);
  else if (t === "MultiPoint") c.forEach(push);
  else if (t === "LineString") c.forEach(push);
  else if (t === "MultiLineString") c.forEach(ls => ls.forEach(push));
  else if (t === "Polygon") {
    // vertices + rough centroid (average of first ring)
    const ring = (c && c[0]) ? c[0] : [];
    ring.forEach(push);
    let sx=0, sy=0, n=0;
    for (const p of ring) {
      const ok = safeLonLat(p);
      if (!ok) continue;
      sx += ok.lon; sy += ok.lat; n++;
    }
    if (n) pts.push([sx/n, sy/n]);
  } else if (t === "MultiPolygon") {
    for (const poly of c || []) {
      const ring = (poly && poly[0]) ? poly[0] : [];
      ring.forEach(push);
      let sx=0, sy=0, n=0;
      for (const p of ring) {
        const ok = safeLonLat(p);
        if (!ok) continue;
        sx += ok.lon; sy += ok.lat; n++;
      }
      if (n) pts.push([sx/n, sy/n]);
    }
  }
  return pts;
}

function minDistMeters(lon, lat, pts) {
  let best = Infinity;
  for (const p of pts) {
    const d = haversineMeters(lon, lat, p[0], p[1]);
    if (d < best) best = d;
  }
  return Number.isFinite(best) ? best : null;
}

if (propsPath) {
  console.log(`[attach] reading properties: ${propsPath}`);
  const layersForAttach = dict.layers.filter(l => l.feature_count > 0 && fs.existsSync(l.frozen_file));
  console.log(`[attach] layers eligible: ${layersForAttach.length}`);

  // Pre-load layer point sets (memory heavy but manageable for initial run; Phase 3 can be split later)
  const layerPointCache = new Map();
  for (const l of layersForAttach) {
    const fc = JSON.parse(fs.readFileSync(l.frozen_file, "utf8"));
    const feats = Array.isArray(fc.features) ? fc.features : [];
    const pts = [];
    for (const f of feats) {
      const p = featurePointsForDistance(f.geometry);
      for (const q of p) pts.push(q);
    }
    layerPointCache.set(l.layer_key, pts);
    console.log(`[attach] cached pts: ${l.layer_key} -> ${pts.length}`);
  }

  const out = fs.createWriteStream(attachOut, { encoding: "utf8" });

  const rl = readline.createInterface({
    input: fs.createReadStream(propsPath, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  let n = 0;
  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    let row = null;
    try { row = JSON.parse(line); } catch { continue; }
    n++;
    if (n % 200000 === 0) console.log(`[progress] scanned properties: ${n}`);

    const lon = row?.lon ?? row?.longitude ?? row?.lng;
    const lat = row?.lat ?? row?.latitude;
    if (typeof lon !== "number" || typeof lat !== "number") continue;

    const ok = safeLonLat([lon, lat]);
    if (!ok) continue;

    const property_id = row.property_id || row.parcel_id || row.id;
    if (!property_id) continue;

    for (const l of layersForAttach) {
      const pts = layerPointCache.get(l.layer_key) || [];
      const d = minDistMeters(ok.lon, ok.lat, pts);
      if (d === null) continue;

      const rec = {
        phase: "phase3_utilities",
        property_id,
        layer_key: l.layer_key,
        city: l.city,
        utility_class: l.utility_class,
        attach_method: "nearest_distance_proxy",
        distance_m: d,
        attach_confidence: "C",
        attach_as_of_date: new Date().toISOString(),
        evidence: {
          source_frozen_geojson: l.frozen_file,
          source_url: l.source_url,
          dataset_hash: l.dataset_hash
        }
      };
      out.write(JSON.stringify(rec) + "\n");
    }
  }

  out.end();
  console.log(`[attach] wrote ${attachOut}`);
}

// ----------------------------
// Contract update + pointer
// ----------------------------
const contractOut = path.join(root, "publicData", "_contracts", `contract_view_ma__phase3_utilities__v1__${stamp}.json`);
const next = structuredClone(contractBase);
next.phases = next.phases || {};
next.phases.phase3_utilities = {
  dictionary: dictOut,
  attachments: propsPath ? attachOut : null,
  created_at: new Date().toISOString(),
  rules: "Distance-only utilities access proxy; do not infer service connections."
};
writeJSON(contractOut, next);

if (fs.existsSync(contractPtr)) {
  const bak = `${contractPtr}.bak_${stamp}`;
  fs.copyFileSync(contractPtr, bak);
}
writeJSON(contractPtr, { current: contractOut });

const auditOut = path.join(auditDir, `phase3_utilities_run__v1__${stamp}.json`);
writeJSON(auditOut, {
  created_at: new Date().toISOString(),
  root,
  config: cfgPath,
  dict: dictOut,
  attachments: propsPath ? attachOut : null,
  contract_in: contractIn,
  contract_out: contractOut,
  warnings
});

console.log(`[contract] wrote ${contractOut}`);
console.log(`[pointer] updated ${contractPtr}`);
console.log(`[audit] ${auditOut}`);
console.log("[done] Phase 3 complete.");
