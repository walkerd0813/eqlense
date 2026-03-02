import fs from "node:fs";
import path from "node:path";

function arg(name) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : null;
}

const inFile = arg("in");
const outFile = arg("out");
const city = (arg("city") || "").toLowerCase();
const kind = (arg("kind") || "").toLowerCase();
const sourceUrl = arg("sourceUrl") || "";

if (!inFile || !outFile) {
  console.error("usage: node geojsonStandardize_v1.mjs --in <file> --out <file> --city <city> --kind <kind> --sourceUrl <url>");
  process.exit(1);
}

const st = fs.statSync(inFile);
const MAX_PARSE_BYTES = 150 * 1024 * 1024; // 150MB safety
if (st.size > MAX_PARSE_BYTES) {
  console.error(`[warn] standardize skipped (large file): ${inFile}`);
  process.exit(0);
}

const gj = JSON.parse(fs.readFileSync(inFile, "utf8"));
const feats = Array.isArray(gj?.features) ? gj.features : [];
const ingestedAt = new Date().toISOString();

function pickBestField(propsList, keys) {
  // choose field with highest non-empty count
  let best = null;
  let bestScore = -1;
  for (const k of keys) {
    let score = 0;
    for (const p of propsList) {
      const v = p?.[k];
      if (v !== null && v !== undefined && v !== "") score += 1;
    }
    if (score > bestScore) { bestScore = score; best = k; }
  }
  return best;
}

function findCandidates(allKeys, includes) {
  const out = [];
  const lower = allKeys.map(k => [k, String(k).toLowerCase()]);
  for (const [orig, low] of lower) {
    if (includes.some(t => low.includes(t))) out.push(orig);
  }
  return out;
}

const propsList = feats.map(f => (f?.properties && typeof f.properties === "object") ? f.properties : {});
const allKeys = new Set();
for (const p of propsList) for (const k of Object.keys(p)) allKeys.add(k);
const allKeyList = [...allKeys.values()];

let zoneCodeField = null;
let zoneNameField = null;

if (kind.startsWith("zoning")) {
  const codeCandidates = [
    ...findCandidates(allKeyList, ["zone_code"]),
    ...findCandidates(allKeyList, ["zoning_code"]),
    ...findCandidates(allKeyList, ["zone", "code"]),
    ...findCandidates(allKeyList, ["district", "code"]),
    ...findCandidates(allKeyList, ["zone"])
  ];
  const nameCandidates = [
    ...findCandidates(allKeyList, ["zone_name"]),
    ...findCandidates(allKeyList, ["zoning_name"]),
    ...findCandidates(allKeyList, ["district", "name"]),
    ...findCandidates(allKeyList, ["zone", "name"]),
    ...findCandidates(allKeyList, ["description"]),
    ...findCandidates(allKeyList, ["desc"])
  ];

  // de-dupe while preserving order
  const dedupe = (arr) => [...new Set(arr)];
  zoneCodeField = pickBestField(propsList, dedupe(codeCandidates));
  zoneNameField = pickBestField(propsList, dedupe(nameCandidates));
}

for (const f of feats) {
  if (!f.properties || typeof f.properties !== "object") f.properties = {};

  // universal audit fields (never remove original fields)
  f.properties.city = f.properties.city ?? city;
  f.properties.source_url = f.properties.source_url ?? sourceUrl;
  f.properties.source_layer = f.properties.source_layer ?? "";
  f.properties.ingested_at = f.properties.ingested_at ?? ingestedAt;

  // Equity Lens internal audit namespace (explicit)
  f.properties.el_city = city;
  f.properties.el_kind = kind;
  f.properties.el_source_url = sourceUrl;
  f.properties.el_ingested_at = ingestedAt;

  if (kind.startsWith("zoning")) {
    const zc = zoneCodeField ? f.properties[zoneCodeField] : null;
    const zn = zoneNameField ? f.properties[zoneNameField] : null;

    if (f.properties.zone_code === undefined) f.properties.zone_code = zc ?? null;
    if (f.properties.zone_name === undefined) f.properties.zone_name = zn ?? null;

    if (f.properties.zone_label === undefined) {
      const a = (f.properties.zone_code ?? "").toString().trim();
      const b = (f.properties.zone_name ?? "").toString().trim();
      f.properties.zone_label = (a && b) ? `${a} - ${b}` : (a || b || null);
    }
  }
}

const out = { ...gj, features: feats };
fs.mkdirSync(path.dirname(outFile), { recursive: true });
fs.writeFileSync(outFile, JSON.stringify(out), "utf8");
console.error(`[ok] standardized -> ${outFile}`);
console.error(`[info] zoning field picks: code=${zoneCodeField || "n/a"} name=${zoneNameField || "n/a"}`);
