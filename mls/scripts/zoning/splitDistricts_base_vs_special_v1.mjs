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
  console.error("Usage: node splitDistricts_base_vs_special_v1.mjs --in <DISTRICTS.geojson> --outBase <base.geojson> --outSpecial <special.geojson>");
  process.exit(1);
}

const fc = JSON.parse(fs.readFileSync(inFile, "utf8"));
const feats = Array.isArray(fc.features) ? fc.features : [];

const SPECIAL_KEYS = [
  "Zoning_Sub", "Subdistric", "Subdistr_1", "Urban_Name", "Urban_Cust", "Unique_Cod", "Article",
  "OVERLAP", "Type", "STAGE", "DISTRICT"
];

function hasAnyKey(props, keys) {
  if (!props || typeof props !== "object") return false;
  return keys.some((k) => Object.prototype.hasOwnProperty.call(props, k));
}

function isSpecial(props) {
  return hasAnyKey(props, SPECIAL_KEYS);
}

// Base “anchor” keys: a feature must have at least one of these to be considered base-ish
const BASEISH_KEYS = ["Zoning", "ZONECODE", "ZONE", "ZONING", "DIST_NAME", "NAME"];

const base = [];
const special = [];

for (const f of feats) {
  const props = (f && f.properties) ? f.properties : {};
  const hasBaseish = hasAnyKey(props, BASEISH_KEYS);

  if (hasBaseish && !isSpecial(props)) base.push(f);
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
