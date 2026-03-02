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
  console.error("Usage: node splitDistricts_base_vs_special_v3_bostonGate.mjs --in <DISTRICTS.geojson> --outBase <base.geojson> --outSpecial <special.geojson>");
  process.exit(1);
}

const fc = JSON.parse(fs.readFileSync(inFile, "utf8"));
const feats = Array.isArray(fc.features) ? fc.features : [];

// IMPORTANT: remove NAME from base keys (it causes bad classification).
const BASE_CODE_KEYS = [
  "Zoning","ZONECODE","ZCODE","ZONE","ZONING","DISTRICT","DIST_CODE","ZONEDIST","ZONINGDIST","ZONING_DIS","ZONEDESC","ZONECLASS","ZONE_TYPE"
];

// Boston “base” signature in your dataset often includes POLY_TYPE + Zoning, and overlays often include ARTICLE/STAGE/OVERLAP/etc.
const BOSTON_BASE_MUST_HAVE = ["POLY_TYPE","Zoning"];
const BOSTON_SPECIAL_ANY = ["ARTICLE","Article","STAGE","OVERLAP","OrdNum","VOLUME","Type","Label","Amendment_","Latest_Ame","Urban_Name","Unique_Cod","Urban_Cust","Zoning_Sub","Subdistr_1","Subdistric","Zoning_Sub","Zoning_Dis"];

function hasAnyKey(props, keys) {
  if (!props || typeof props !== "object") return false;
  return keys.some((k) => Object.prototype.hasOwnProperty.call(props, k));
}
function hasAllKeys(props, keys) {
  if (!props || typeof props !== "object") return false;
  return keys.every((k) => Object.prototype.hasOwnProperty.call(props, k));
}
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

const base = [];
const special = [];

for (const f of feats) {
  const props = (f && f.properties) ? f.properties : {};
  const city = String(props.__city || "").toLowerCase();

  // Boston override: only accept the “clean base” signature; everything else goes special.
  if (city === "boston") {
    const okBase = hasAllKeys(props, BOSTON_BASE_MUST_HAVE) && !hasAnyKey(props, BOSTON_SPECIAL_ANY);
    (okBase ? base : special).push(f);
    continue;
  }

  // Other cities: base if it has a plausible code-like district field; else special
  const bc = pickBaseCode(props);
  if (bc && looksLikeCode(bc.val)) base.push(f);
  else special.push(f);
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
