import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

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
  // Strip UTF-8 BOM if present
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

function norm(s) {
  return (s || "").toString().toLowerCase().replace(/\s+/g, " ").trim();
}

function hasAny(hay, needles) {
  const h = norm(hay);
  return needles.some(n => h.includes(n));
}

const NOISE = [
  "road","street","centerline","pavement","sidewalk","curb","traffic",
  "address","parcel","building footprint","orth","ortho","aerial","label",
  "basemap","imagery","contour","topo","hillshade","boundary","district",
  "watershed","overlay","protection","zone","zoning","wetland","fema",
  "flood","conservation","open space","park","trail","bike","bus",
  "school","police","fire","precinct","ward","neighborhood"
];

const ADJ = ["easement","right-of-way","row","pump","station","treatment","plant","tank","vault","meter","pit","box","chamber"];

const KW = [
  "water main","watermain","hydrant","valve","gate valve","service","lateral","curb stop",
  "sewer","sanitary","force main","forcemain","manhole","gravity",
  "storm","drain","drainage","catch basin","catchbasin","inlet","outfall","culvert"
];

function classify(layer) {
  const hay = [
    layer.display_name, layer.layer_name, layer.name, layer.title, layer.url, layer.out_path
  ].filter(Boolean).join(" | ");

  const isNoise = hasAny(hay, NOISE);
  const isNetwork = hasAny(hay, KW);
  const isAdj = hasAny(hay, ADJ);

  const h = norm(hay);
  let type = "unknown";
  if (h.includes("storm") || h.includes("drain") || h.includes("catch basin") || h.includes("inlet") || h.includes("outfall")) type = "storm";
  else if (h.includes("sewer") || h.includes("sanitary") || h.includes("manhole") || h.includes("force main")) type = "sewer";
  else if (h.includes("water") || h.includes("hydrant") || h.includes("valve")) type = "water";

  const score = () => {
    let p = 0;
    if (type === "water") {
      if (h.includes("main") || h.includes("watermain")) p += 4;
      if (h.includes("hydrant")) p += 4;
      if (h.includes("valve") || h.includes("gate")) p += 3;
      if (h.includes("service") || h.includes("lateral") || h.includes("curb stop")) p += 2;
    } else if (type === "sewer") {
      if (h.includes("force") || h.includes("forcemain")) p += 4;
      if (h.includes("gravity") || h.includes("main")) p += 3;
      if (h.includes("manhole")) p += 4;
      if (h.includes("lateral") || h.includes("service") || h.includes("connection")) p += 2;
    } else if (type === "storm") {
      if (h.includes("catch basin") || h.includes("catchbasin") || h.includes("inlet")) p += 4;
      if (h.includes("outfall")) p += 4;
      if (h.includes("drain") || h.includes("drainage") || h.includes("main")) p += 3;
      if (h.includes("culvert")) p += 2;
    }
    if (isNetwork) p += 2;
    if (isNoise) p -= 10;
    if (isAdj && !isNetwork) p -= 3;
    return p;
  };

  const suspicious = isNoise || (isAdj && !isNetwork) || (type === "unknown" && !isNetwork);
  return { hay, type, isNoise, isAdj, isNetwork, suspicious, score: score() };
}

function nowTag() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

const args = argMap(process.argv);
const root = args.root ? path.resolve(args.root) : process.cwd();
const maxPerType = Math.max(1, parseInt(args.maxPerType || "4", 10));

const currentPtr = path.join(root, "publicData", "overlays", "_frozen", "_dict", "CURRENT_PHASE3_UTILITIES_DICT.json");
const contractPtr = path.join(root, "publicData", "_contracts", "CURRENT_CONTRACT_VIEW_MA.json");

if (!fs.existsSync(currentPtr)) throw new Error(`[err] missing CURRENT pointer: ${currentPtr}`);
const ptr = readJSON(currentPtr);
const dictPath = (typeof ptr === "string") ? ptr : (ptr.path || ptr.dict_path || ptr.current || ptr.target);
if (!dictPath) throw new Error(`[err] CURRENT pointer unexpected format: ${JSON.stringify(ptr).slice(0,200)}`);

const absDictPath = path.isAbsolute(dictPath) ? dictPath : path.join(root, dictPath);
if (!fs.existsSync(absDictPath)) throw new Error(`[err] dict file not found: ${absDictPath}`);

const dict = readJSON(absDictPath);
const layers = dict.layers || dict.entries || dict.items || [];
if (!Array.isArray(layers) || layers.length === 0) throw new Error("[err] dict has no layers array at dict.layers (or entries/items)");

const dictHash = sha256File(absDictPath);

const auditDir = path.join(root, "publicData", "_audit", "phase3_utilities");
fs.mkdirSync(auditDir, { recursive: true });

const audit = {
  created_at: new Date().toISOString(),
  root,
  current_pointer: currentPtr,
  contract_pointer: fs.existsSync(contractPtr) ? contractPtr : null,
  source_dict_path: absDictPath,
  source_dict_sha256: dictHash,
  total_layers: layers.length,
  max_per_type: maxPerType,
  by_city: {},
  suspicious_layers: [],
  zero_feature_layers: [],
  missing_geojson_paths: []
};

const byCityBuckets = new Map();

for (const layer of layers) {
  const city = (layer.city || layer.source_city || layer.municipality || layer.town || "UNKNOWN").toString();
  const c = classify(layer);
  const featureCount = Number(layer.feature_count ?? layer.count ?? layer.features ?? -1);

  if (!audit.by_city[city]) audit.by_city[city] = { total: 0, water: 0, sewer: 0, storm: 0, unknown: 0, suspicious: 0, zero: 0 };
  audit.by_city[city].total += 1;
  audit.by_city[city][c.type] = (audit.by_city[city][c.type] || 0) + 1;

  if (c.suspicious) {
    audit.by_city[city].suspicious += 1;
    audit.suspicious_layers.push({
      city,
      type: c.type,
      isNoise: c.isNoise,
      isAdj: c.isAdj,
      isNetwork: c.isNetwork,
      display_name: layer.display_name || layer.layer_name || layer.name || layer.title || "",
      url: layer.url || "",
      out_path: layer.out_path || "",
      feature_count: featureCount
    });
  }

  if (featureCount === 0) {
    audit.by_city[city].zero += 1;
    audit.zero_feature_layers.push({
      city,
      display_name: layer.display_name || layer.layer_name || layer.name || layer.title || "",
      out_path: layer.out_path || "",
      url: layer.url || ""
    });
  }

  const outPath = layer.out_path || layer.file || layer.geojson_path || null;
  if (outPath) {
    const absOut = path.isAbsolute(outPath) ? outPath : path.join(root, outPath);
    if (!fs.existsSync(absOut)) audit.missing_geojson_paths.push({ city, out_path: outPath });
  }

  if (!byCityBuckets.has(city)) byCityBuckets.set(city, { water: [], sewer: [], storm: [], unknown: [] });
  byCityBuckets.get(city)[c.type].push({ layer, score: c.score, hay: c.hay, type: c.type, cls: c });
}

const auditPath = path.join(auditDir, `phase3_utilities_FINAL_audit__${nowTag()}.json`);
writeJSON(auditPath, audit);

// PRUNE
const quarantine = [];
const kept = [];
const dropped = [];

for (const [city, buckets] of byCityBuckets.entries()) {
  for (const type of ["water","sewer","storm"]) {
    const arr = buckets[type]
      .filter(x => {
        const fc = Number(x.layer.feature_count ?? x.layer.count ?? -1);
        if (fc === 0) return false;
        if (x.cls.isNoise) return false;
        if (x.cls.isAdj && !x.cls.isNetwork) return false;
        return x.cls.isNetwork || !x.cls.suspicious;
      })
      .sort((a,b) => (b.score - a.score));

    const sel = arr.slice(0, maxPerType);
    for (const x of sel) kept.push(x.layer);

    const zeros = buckets[type].filter(x => Number(x.layer.feature_count ?? x.layer.count ?? -1) === 0);
    for (const z of zeros) quarantine.push({ city, type, reason: "feature_count=0", display_name: z.layer.display_name || z.layer.layer_name || z.layer.name || z.layer.title || "" });

    const keepSet = new Set(sel.map(x => x.hay));
    for (const x of buckets[type]) {
      const fc = Number(x.layer.feature_count ?? x.layer.count ?? -1);
      const reason = (fc === 0) ? "feature_count=0" :
        x.cls.isNoise ? "noise_pattern" :
        (x.cls.isAdj && !x.cls.isNetwork) ? "utility_adjacent_not_network" :
        (!keepSet.has(x.hay) ? "not_in_topN" : null);

      if (reason && !(keepSet.has(x.hay))) dropped.push({ city, type, reason, display_name: x.layer.display_name || x.layer.layer_name || x.layer.name || x.layer.title || "" });
    }
  }
}

const finalDict = {
  ...dict,
  created_at: new Date().toISOString(),
  phase: "phase3_utilities",
  selection_rule: `Top ${maxPerType} per city per type (water/sewer/storm) when available; exclude noise; exclude zero-feature; exclude utility-adjacent unless network.`,
  source_dict_path: absDictPath,
  source_dict_sha256: dictHash,
  max_per_type: maxPerType,
  layers: kept
};

const frozenDictDir = path.join(root, "publicData", "overlays", "_frozen", "_dict");
fs.mkdirSync(frozenDictDir, { recursive: true });
const finalDictPath = path.join(frozenDictDir, `phase3_utilities_dictionary__v1__${nowTag()}__FINAL__TOP${maxPerType}PER_TYPE.json`);
writeJSON(finalDictPath, finalDict);

// Update CURRENT pointers
writeJSON(currentPtr, { path: finalDictPath, updated_at: new Date().toISOString(), note: "AUTO: Phase 3 FINAL pack v3 (BOM-safe)" });

if (fs.existsSync(contractPtr)) {
  const contract = readJSON(contractPtr);
  contract.phase3_utilities = finalDictPath;
  contract.updated_at = new Date().toISOString();
  writeJSON(contractPtr, contract);
}

const pruneReport = {
  created_at: new Date().toISOString(),
  source_dict_path: absDictPath,
  source_dict_sha256: dictHash,
  final_dict_path: finalDictPath,
  final_dict_sha256: sha256File(finalDictPath),
  max_per_type: maxPerType,
  kept_layers: kept.length,
  quarantined_layers: quarantine.length,
  dropped_layers: dropped.length,
  quarantine: quarantine.slice(0, 2000),
  dropped: dropped.slice(0, 2000)
};
const prunePath = path.join(auditDir, `phase3_utilities_FINAL_prune__${nowTag()}.json`);
writeJSON(prunePath, pruneReport);

const signoff = {
  created_at: new Date().toISOString(),
  phase: "Phase 3 Utilities",
  status: "SIGNOFF_READY",
  pointers: {
    CURRENT_PHASE3_UTILITIES_DICT: currentPtr,
    CURRENT_CONTRACT_VIEW_MA: fs.existsSync(contractPtr) ? contractPtr : null
  },
  dict: {
    final_dict_path: finalDictPath,
    final_dict_sha256: pruneReport.final_dict_sha256,
    rule: finalDict.selection_rule
  },
  audit_artifacts: { audit: auditPath, prune: prunePath },
  notes: [
    "Target structure: up to 4 water + 4 sewer + 4 storm/drain per city when available; cities may be missing categories.",
    "Zero-feature layers are quarantined (excluded from final dict).",
    "Noise patterns removed.",
    "BOM-safe JSON reader enabled."
  ]
};

const signoffPath = path.join(auditDir, `PHASE3_UTILITIES_SIGNOFF__${nowTag()}.json`);
writeJSON(signoffPath, signoff);

console.log("[ok] audit:", auditPath);
console.log("[ok] final dict:", finalDictPath);
console.log("[ok] prune report:", prunePath);
console.log("[ok] signoff:", signoffPath);
console.log("[done] Phase 3 FINAL v3 complete.");
