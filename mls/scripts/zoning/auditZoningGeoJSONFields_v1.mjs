import fs from "node:fs";

function sample(arr, n = 10) {
  const out = [];
  for (let i = 0; i < arr.length && out.length < n; i++) out.push(arr[i]);
  return out;
}

function inc(map, k, by = 1) {
  map.set(k, (map.get(k) || 0) + by);
}

function isObj(x) {
  return x && typeof x === "object" && !Array.isArray(x);
}

const args = process.argv.slice(2);
const getArg = (k, d = null) => {
  const i = args.indexOf(k);
  return i >= 0 ? args[i + 1] : d;
};

const file = getArg("--file");
const out = getArg("--out", null);

if (!file) {
  console.error('Usage: node auditZoningGeoJSONFields_v1.mjs --file "path\\to\\zoning.geojson" [--out "report.json"]');
  process.exit(1);
}

const raw = fs.readFileSync(file, "utf8");
const gj = JSON.parse(raw);

if (!gj || !Array.isArray(gj.features)) {
  console.error("Not a valid GeoJSON FeatureCollection with features[]");
  process.exit(1);
}

const keyFreq = new Map();
const keyDistinct = new Map(); // key -> Set (bounded)
const keySamples = new Map();  // key -> []
const comboFreq = new Map();   // joined keys signature -> count

const MAX_DISTINCT = 2000; // cap to prevent memory blowups
const MAX_SAMPLES = 10;

for (const f of gj.features) {
  const props = (f && isObj(f.properties)) ? f.properties : {};
  const keys = Object.keys(props).sort();
  inc(comboFreq, keys.join("|") || "(no_props)");

  for (const k of keys) {
    inc(keyFreq, k);

    const v = props[k];
    if (!keyDistinct.has(k)) keyDistinct.set(k, new Set());
    const s = keyDistinct.get(k);
    if (s.size < MAX_DISTINCT) {
      const vv = (v === null) ? "null" : (typeof v === "string" || typeof v === "number" || typeof v === "boolean")
        ? String(v)
        : (Array.isArray(v) ? `[array:${v.length}]` : `[${typeof v}]`);
      s.add(vv);
    }

    if (!keySamples.has(k)) keySamples.set(k, []);
    const arr = keySamples.get(k);
    if (arr.length < MAX_SAMPLES) arr.push(v);
  }
}

const topKeys = [...keyFreq.entries()].sort((a,b)=>b[1]-a[1]).slice(0, 60);
const topCombos = [...comboFreq.entries()].sort((a,b)=>b[1]-a[1]).slice(0, 40);

const report = {
  file,
  featureCount: gj.features.length,
  topKeys: topKeys.map(([k,c]) => ({
    key: k,
    freq: c,
    distinctApprox: keyDistinct.get(k)?.size ?? 0,
    samples: sample(keySamples.get(k) ?? [], 10),
  })),
  topPropertyKeyCombos: topCombos.map(([sig,c]) => ({ sig, count: c })),
  heuristics: {
    possibleDistrictFields: topKeys.map(([k]) => k).filter(k => /dist|zone|zoning|district|sub|overlay|olay|ol_/i.test(k)),
  }
};

if (out) {
  fs.writeFileSync(out, JSON.stringify(report, null, 2));
  console.log(`[done] wrote: ${out}`);
} else {
  console.log(JSON.stringify(report, null, 2));
}
