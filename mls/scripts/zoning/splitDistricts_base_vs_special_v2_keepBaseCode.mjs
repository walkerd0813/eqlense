import fs from "node:fs";
import path from "node:path";

function getArg(name) {
  const key = `--${name}`;
  const i = process.argv.indexOf(key);
  return i >= 0 ? process.argv[i + 1] : null;
}

const inFile = getArg("in");
const outBase = getArg("outBase");
const outSpecial = getArg("outSpecial");

if (!inFile || !outBase || !outSpecial) {
  console.error("Usage: node splitDistricts_base_vs_special_v2_keepBaseCode.mjs --in <DISTRICTS.geojson> --outBase <base.geojson> --outSpecial <special.geojson>");
  process.exit(1);
}

const fc = JSON.parse(fs.readFileSync(inFile, "utf8"));
const feats = Array.isArray(fc.features) ? fc.features : [];

const BASE_CODE_KEYS = [
  "Zoning","ZONECODE","ZONE","ZONING","DISTRICT","DIST_CODE","ZONEDIST","ZONINGDIST","ZONING_DIS","DIST_NAME","NAME"
];

const STRONG_SPECIAL_KEYS = ["Urban_Name","Unique_Cod","Urban_Cust"];

function pickBaseCode(props) {
  if (!props || typeof props !== "object") return null;
  for (const k of BASE_CODE_KEYS) {
    const v = props[k];
    if (v == null) continue;
    const s = String(v).trim();
    if (s) return { key: k, val: s };
  }
  return null;
}

function looksLikeCode(s) {
  const t = String(s).trim();
  if (!t) return false;
  if (t.length <= 12 && !t.includes(" ")) return true;
  if (/[0-9]/.test(t) || /-/.test(t)) return true;
  return false;
}

function hasAnyKey(props, keys) {
  if (!props || typeof props !== "object") return false;
  return keys.some((k) => Object.prototype.hasOwnProperty.call(props, k));
}

const base = [];
const special = [];

for (const f of feats) {
  const props = (f && f.properties) ? f.properties : {};
  const bc = pickBaseCode(props);

  if (bc && looksLikeCode(bc.val)) { base.push(f); continue; }
  if (hasAnyKey(props, STRONG_SPECIAL_KEYS)) { special.push(f); continue; }
  if (bc) { special.push(f); continue; }
  special.push(f);
}

function writeFC(fp, features) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify({ type: "FeatureCollection", features }));
}

writeFC(outBase, base);
writeFC(outSpecial, special);

console.log("[done]", {
  inFile,
  total: feats.length,
  base: base.length,
  special: special.length,
  outBase,
  outSpecial
});
