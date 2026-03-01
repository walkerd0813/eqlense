import fs from "node:fs";
import path from "node:path";

function getArg(name) {
  const key = `--${name}`;
  const i = process.argv.indexOf(key);
  return i >= 0 ? process.argv[i + 1] : null;
}

const inFile    = getArg("in");
const outFile   = getArg("out");
const city      = getArg("city") || "UNKNOWN";
const category  = getArg("category") || "unknown";
const layerUrl  = getArg("layerUrl") || null;
const codeField = getArg("codeField");   // optional
const nameField = getArg("nameField");   // optional

if (!inFile || !outFile) {
  console.error("Usage: node normalizeCityLayerGeoJSON_v1.mjs --in <geojson> --out <geojson> --city <CITY> --category <cat> [--layerUrl <url>] [--codeField <field>] [--nameField <field>]");
  process.exit(1);
}

const fc = JSON.parse(fs.readFileSync(inFile, "utf8"));
const feats = Array.isArray(fc.features) ? fc.features : [];

function pick(props, k) {
  if (!props || typeof props !== "object") return null;
  const v = props[k];
  if (v == null) return null;
  const s = String(v).trim();
  return s ? s : null;
}

const out = {
  type: "FeatureCollection",
  features: feats.map((f) => {
    const props = (f && f.properties && typeof f.properties === "object") ? f.properties : {};
    const code = codeField ? pick(props, codeField) : null;
    const name = nameField ? pick(props, nameField) : null;

    return {
      ...f,
      properties: {
        ...props,
        el_city: String(city).toUpperCase(),
        el_category: category,
        el_code: code,
        el_name: name,
        el_layerUrl: layerUrl
      }
    };
  })
};

fs.mkdirSync(path.dirname(outFile), { recursive: true });
fs.writeFileSync(outFile, JSON.stringify(out));

console.log("[done] normalized", {
  inFile,
  outFile,
  features: out.features.length,
  city: String(city).toUpperCase(),
  category,
  codeField: codeField || null,
  nameField: nameField || null
});
