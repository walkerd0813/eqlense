import fs from "node:fs";
import path from "node:path";
import { readJSON, writeJSON, sha256File, isoStamp, layerHay, norm, hasAny } from "./_phase3_utils.mjs";

// --- Config (tunable defaults) ---
const DEFAULT_MIN_PER_TYPE = 4; // user requirement: target ~4 water, 4 sewer, 4 storm per city when available
const DEFAULT_MAX_PER_TYPE = 4; // we canonicalize to 4/type (12/city) unless fewer available
const ROOT = process.cwd();

const currentPtr = path.join(ROOT, "publicData", "overlays", "_frozen", "_dict", "CURRENT_PHASE3_UTILITIES_DICT.json");
if (!fs.existsSync(currentPtr)) {
  console.error("[err] missing CURRENT dict pointer:", currentPtr);
  process.exit(1);
}

const ptr = readJSON(currentPtr);
const dictPath = typeof ptr === "string" ? ptr : (ptr.path || ptr.dict_path || ptr.current || ptr.target);
if (!dictPath) {
  console.error("[err] CURRENT dict pointer format unexpected. Contents keys:", Object.keys(ptr));
  process.exit(1);
}
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

// --- Classification ---
const NOISE_PATTERNS = [
  "road", "street", "centerline", "pavement", "sidewalk", "curb", "traffic",
  "address", "parcel", "building footprint", "orth", "ortho", "aerial", "label",
  "basemap", "imagery", "contour", "topo", "hillshade", "boundary", "district",
  "watershed", "overlay", "protection", "zone", "zoning", "wetland", "fema",
  "flood", "conservation", "open space", "park", "trail", "bike", "bus",
  "school", "police", "fire", "precinct", "ward", "neighborhood"
];

const NETWORK_KEYWORDS = [
  // water
  "water main", "watermain", "hydrant", "valve", "gate valve", "service", "lateral", "curb stop",
  // sewer
  "sewer", "sanitary", "force main", "forcemain", "manhole", "gravity",
  // storm
  "storm", "drain", "drainage", "catch basin", "catchbasin", "inlet", "outfall", "culvert"
];

function guessType(hay) {
  const h = norm(hay);
  if (h.includes("storm") || h.includes("drain") || h.includes("catch basin") || h.includes("catchbasin") || h.includes("inlet") || h.includes("outfall") || h.includes("culvert")) return "storm";
  if (h.includes("sewer") || h.includes("sanitary") || h.includes("manhole") || h.includes("force main") || h.includes("forcemain")) return "sewer";
  if (h.includes("water") || h.includes("hydrant") || h.includes("valve") || h.includes("gate valve")) return "water";
  return "unknown";
}

function scoreLayer(type, hay) {
  const h = norm(hay);
  // higher = more core network
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
  // small boost if URL/path includes FeatureServer/MapServer (tends to be “real” layers)
  if (h.includes("featureserver") || h.includes("mapserver")) best += 2;
  return best;
}

function cityKey(layer) {
  return (layer.city || layer.source_city || layer.municipality || layer.town || "UNKNOWN").toString();
}

// --- Build audit data ---
const byCity = {};
const suspicious = [];
const zeros = [];
const missingGeo = [];
const allTyped = [];

for (const layer of layers) {
  const city = cityKey(layer);
  const hay = layerHay(layer);
  const type = guessType(hay);
  const featureCount = Number(layer.feature_count ?? layer.count ?? layer.features ?? -1);

  const isNoise = hasAny(hay, NOISE_PATTERNS);
  const isNetwork = hasAny(hay, NETWORK_KEYWORDS);
  const isSuspicious = isNoise || (type === "unknown" && !isNetwork);

  const outPath = layer.out_path || layer.file || layer.geojson_path || null;
  if (outPath) {
    const absOut = path.isAbsolute(outPath) ? outPath : path.join(ROOT, outPath);
    if (!fs.existsSync(absOut)) missingGeo.push({ city, out_path: outPath, display: layer.display_name || "", url: layer.url || "" });
  }

  if (featureCount === 0) zeros.push({ city, display: layer.display_name || "", url: layer.url || "", out_path: outPath || "" });

  if (!byCity[city]) byCity[city] = { total: 0, water: 0, sewer: 0, storm: 0, unknown: 0, suspicious: 0 };
  byCity[city].total += 1;
  byCity[city][type] = (byCity[city][type] || 0) + 1;

  if (isSuspicious) {
    byCity[city].suspicious += 1;
    suspicious.push({ city, type, isNoise, isNetwork, feature_count: featureCount, display: layer.display_name || layer.layer_name || "", url: layer.url || "", out_path: outPath || "" });
  }

  allTyped.push({
    city,
    type,
    score: (type === "unknown" ? 0 : scoreLayer(type, hay)),
    hay,
    feature_count: featureCount,
    display: layer.display_name || layer.layer_name || layer.name || "",
    url: layer.url || "",
    out_path: outPath || "",
  });
}

// --- “Target 12/city” readiness ---
const target = {};
for (const [city, v] of Object.entries(byCity)) {
  target[city] = {
    want_per_type: DEFAULT_MIN_PER_TYPE,
    counts: { water: v.water, sewer: v.sewer, storm: v.storm, unknown: v.unknown, total: v.total },
    shortfalls: {
      water: Math.max(0, DEFAULT_MIN_PER_TYPE - (v.water || 0)),
      sewer: Math.max(0, DEFAULT_MIN_PER_TYPE - (v.sewer || 0)),
      storm: Math.max(0, DEFAULT_MIN_PER_TYPE - (v.storm || 0))
    }
  };
}

const report = {
  created_at: new Date().toISOString(),
  dict_path: absDictPath,
  dict_sha256: dictHash,
  total_layers: layers.length,
  per_city_counts: byCity,
  per_city_target_4x3: target,
  suspicious_count: suspicious.length,
  suspicious_layers: suspicious.slice(0, 1000),
  zero_feature_layers_count: zeros.length,
  zero_feature_layers: zeros.slice(0, 1000),
  missing_geojson_paths_count: missingGeo.length,
  missing_geojson_paths: missingGeo.slice(0, 1000),
  notes: [
    "This report does not modify the dictionary.",
    "Per-city target is ~4 water + 4 sewer + 4 storm when available; some cities may be missing categories."
  ]
};

const outDir = path.join(ROOT, "publicData", "_audit", "phase3_utilities");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, `phase3_utilities_FINAL_audit__${isoStamp()}.json`);
writeJSON(outPath, report);

console.log("[done] wrote audit:", outPath);
console.log("[info] total_layers:", report.total_layers);
console.log("[info] suspicious_count:", report.suspicious_count);
console.log("[info] zero_feature_layers_count:", report.zero_feature_layers_count);
console.log("[info] missing_geojson_paths_count:", report.missing_geojson_paths_count);
