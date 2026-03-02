import fs from "node:fs";
import path from "node:path";
import { readJSON, writeJSON, sha256File, isoStamp, layerHay, norm, hasAny } from "./_phase3_utils.mjs";

const ROOT = process.cwd();
const MIN_PER_TYPE = Number(process.env.PHASE3_MIN_PER_TYPE || 4);
const MAX_PER_TYPE = Number(process.env.PHASE3_MAX_PER_TYPE || 4); // canonicalize to 4/type unless fewer

const currentPtr = path.join(ROOT, "publicData", "overlays", "_frozen", "_dict", "CURRENT_PHASE3_UTILITIES_DICT.json");
const contractPtr = path.join(ROOT, "publicData", "_contracts", "CURRENT_CONTRACT_VIEW_MA.json");

if (!fs.existsSync(currentPtr)) {
  console.error("[err] missing CURRENT dict pointer:", currentPtr);
  process.exit(1);
}

const ptr = readJSON(currentPtr);
const dictPath = typeof ptr === "string" ? ptr : (ptr.path || ptr.dict_path || ptr.current || ptr.target);
const absDictPath = path.isAbsolute(dictPath) ? dictPath : path.join(ROOT, dictPath);

if (!fs.existsSync(absDictPath)) {
  console.error("[err] dict file not found:", absDictPath);
  process.exit(1);
}

const dict = readJSON(absDictPath);
const dictHash = sha256File(absDictPath);

const layers = dict.layers || dict.entries || dict.items || [];
if (!Array.isArray(layers) || layers.length === 0) {
  console.error("[err] dict has no layers array. Keys:", Object.keys(dict));
  process.exit(1);
}

const NOISE_PATTERNS = [
  "road", "street", "centerline", "pavement", "sidewalk", "curb", "traffic",
  "address", "parcel", "building footprint", "orth", "ortho", "aerial", "label",
  "basemap", "imagery", "contour", "topo", "hillshade", "boundary", "district",
  "watershed", "overlay", "protection", "zone", "zoning", "wetland", "fema",
  "flood", "conservation", "open space", "park", "trail", "bike", "bus",
  "school", "police", "fire", "precinct", "ward", "neighborhood"
];

const NETWORK_KEYWORDS = [
  "water main", "watermain", "hydrant", "valve", "gate valve", "service", "lateral", "curb stop",
  "sewer", "sanitary", "force main", "forcemain", "manhole", "gravity",
  "storm", "drain", "drainage", "catch basin", "catchbasin", "inlet", "outfall", "culvert"
];

function cityKey(layer) {
  return (layer.city || layer.source_city || layer.municipality || layer.town || "UNKNOWN").toString();
}

function guessType(hay) {
  const h = norm(hay);
  if (h.includes("storm") || h.includes("drain") || h.includes("catch basin") || h.includes("catchbasin") || h.includes("inlet") || h.includes("outfall") || h.includes("culvert")) return "storm";
  if (h.includes("sewer") || h.includes("sanitary") || h.includes("manhole") || h.includes("force main") || h.includes("forcemain")) return "sewer";
  if (h.includes("water") || h.includes("hydrant") || h.includes("valve") || h.includes("gate valve")) return "water";
  return "unknown";
}

function scoreLayer(type, hay) {
  const h = norm(hay);
  const rules = {
    water: [
      { k: "water main", s: 100 },
      { k: "watermain", s: 95 },
      { k: "hydrant", s: 90 },
      { k: "gate valve", s: 85 },
      { k: "valve", s: 80 },
      { k: "service", s: 75 },
      { k: "lateral", s: 72 },
      { k: "curb stop", s: 70 }
    ],
    sewer: [
      { k: "force main", s: 100 },
      { k: "forcemain", s: 98 },
      { k: "gravity", s: 92 },
      { k: "sanitary", s: 90 },
      { k: "sewer", s: 85 },
      { k: "manhole", s: 80 },
      { k: "lateral", s: 75 },
      { k: "service", s: 72 }
    ],
    storm: [
      { k: "storm drain", s: 100 },
      { k: "storm", s: 95 },
      { k: "drainage", s: 90 },
      { k: "catch basin", s: 88 },
      { k: "catchbasin", s: 88 },
      { k: "inlet", s: 84 },
      { k: "outfall", s: 82 },
      { k: "culvert", s: 78 },
      { k: "drain", s: 75 }
    ]
  };
  let best = 0;
  for (const r of (rules[type] || [])) {
    if (h.includes(r.k)) best = Math.max(best, r.s);
  }
  if (h.includes("featureserver") || h.includes("mapserver")) best += 2;
  return best;
}

function isNoise(layer) {
  const hay = layerHay(layer);
  return hasAny(hay, NOISE_PATTERNS);
}

function isNetwork(layer) {
  const hay = layerHay(layer);
  return hasAny(hay, NETWORK_KEYWORDS);
}

const kept = [];
const dropped = [];
const quarantined = []; // feature_count=0

// Step 1: remove hard noise + quarantine zero-count
const prelim = [];
for (const layer of layers) {
  const hay = layerHay(layer);
  const fc = Number(layer.feature_count ?? layer.count ?? layer.features ?? -1);

  if (fc === 0) {
    quarantined.push({ layer, reason: "feature_count=0" });
    continue;
  }
  if (isNoise(layer)) {
    dropped.push({ layer, reason: "noise_pattern" });
    continue;
  }
  // unknown but not network → drop
  const t = guessType(hay);
  if (t === "unknown" && !isNetwork(layer)) {
    dropped.push({ layer, reason: "unknown_non_network" });
    continue;
  }
  prelim.push({ layer, hay, type: t, score: (t === "unknown" ? 0 : scoreLayer(t, hay)) });
}

// Step 2: per-city selection: keep top MAX_PER_TYPE per (water/sewer/storm). Keep unknown only if it is network.
const byCity = new Map();
for (const item of prelim) {
  const city = cityKey(item.layer);
  if (!byCity.has(city)) byCity.set(city, { water: [], sewer: [], storm: [], unknown: [] });
  byCity.get(city)[item.type]?.push(item);
}

function pickTop(arr, n) {
  return arr
    .sort((a,b) => (b.score - a.score))
    .slice(0, n);
}

const selectionReport = {};
for (const [city, buckets] of byCity.entries()) {
  const w = pickTop(buckets.water, MAX_PER_TYPE);
  const s = pickTop(buckets.sewer, MAX_PER_TYPE);
  const st = pickTop(buckets.storm, MAX_PER_TYPE);

  // Keep “unknown” only if network (rare, but safe)
  const unk = buckets.unknown.filter(x => isNetwork(x.layer));

  // Enforce “at least MIN per type if available” (we are already taking up to MAX; if MAX < MIN, that's user override)
  selectionReport[city] = {
    available: { water: buckets.water.length, sewer: buckets.sewer.length, storm: buckets.storm.length, unknown: buckets.unknown.length },
    kept: { water: w.length, sewer: s.length, storm: st.length, unknown: unk.length },
    shortfalls_vs_min: {
      water: Math.max(0, Math.min(MIN_PER_TYPE, buckets.water.length) - w.length),
      sewer: Math.max(0, Math.min(MIN_PER_TYPE, buckets.sewer.length) - s.length),
      storm: Math.max(0, Math.min(MIN_PER_TYPE, buckets.storm.length) - st.length)
    }
  };

  const keepSet = new Set([...w, ...s, ...st, ...unk].map(x => x.layer));
  for (const item of [...buckets.water, ...buckets.sewer, ...buckets.storm, ...buckets.unknown]) {
    if (keepSet.has(item.layer)) {
      kept.push(item.layer);
    } else {
      dropped.push({ layer: item.layer, reason: "not_in_topN_per_type" });
    }
  }
}

// Step 3: write FINAL dict + update CURRENT pointers
const outDir = path.join(ROOT, "publicData", "overlays", "_frozen", "_dict");
fs.mkdirSync(outDir, { recursive: true });

const stamp = isoStamp();
const finalDictPath = path.join(outDir, `phase3_utilities_dictionary__v1__${stamp}__FINAL__TOP${MAX_PER_TYPE}PER_TYPE.json`);

const finalDict = {
  ...dict,
  meta: {
    ...(dict.meta || {}),
    phase: "phase3_utilities",
    refined_from: absDictPath,
    refined_from_sha256: dictHash,
    refined_at: new Date().toISOString(),
    selection: { min_per_type: MIN_PER_TYPE, max_per_type: MAX_PER_TYPE },
  },
  layers: kept
};

writeJSON(finalDictPath, finalDict);
const finalHash = sha256File(finalDictPath);

// Update CURRENT dict pointer (preserve pointer shape if possible)
let newPtr = ptr;
if (typeof ptr === "string") newPtr = finalDictPath;
else if (ptr && typeof ptr === "object") newPtr = { ...ptr, path: finalDictPath };

writeJSON(currentPtr, newPtr);

// Update contract view pointer if present
if (fs.existsSync(contractPtr)) {
  const contract = readJSON(contractPtr);
  contract.phase3_utilities = contract.phase3_utilities || {};
  if (typeof contract.phase3_utilities === "string") contract.phase3_utilities = finalDictPath;
  else contract.phase3_utilities.path = finalDictPath;
  contract.phase3_utilities_sha256 = finalHash;
  contract.phase3_utilities_updated_at = new Date().toISOString();
  writeJSON(contractPtr, contract);
}

const auditOutDir = path.join(ROOT, "publicData", "_audit", "phase3_utilities");
fs.mkdirSync(auditOutDir, { recursive: true });
const outAudit = path.join(auditOutDir, `phase3_utilities_FINAL_prune__${stamp}.json`);

writeJSON(outAudit, {
  created_at: new Date().toISOString(),
  input_dict: absDictPath,
  input_sha256: dictHash,
  output_dict: finalDictPath,
  output_sha256: finalHash,
  selection: { min_per_type: MIN_PER_TYPE, max_per_type: MAX_PER_TYPE },
  per_city_selection: selectionReport,
  counts: { input_layers: layers.length, prelim_layers: prelim.length, kept_layers: kept.length, dropped_layers: dropped.length, quarantined_layers: quarantined.length },
  quarantined: quarantined.slice(0, 2000).map(q => ({ reason: q.reason, display: q.layer.display_name || "", url: q.layer.url || "" })),
  dropped: dropped.slice(0, 2000).map(d => ({ reason: d.reason, display: d.layer.display_name || "", url: d.layer.url || "" }))
});

console.log("[done] wrote FINAL dict:", finalDictPath);
console.log("[info] output_sha256:", finalHash);
console.log("[done] wrote prune audit:", outAudit);
