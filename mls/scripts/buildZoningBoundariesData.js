import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ROOT = path.resolve(__dirname, "../../publicData/zoning");
const OUT  = path.resolve(ROOT, "zoningBoundariesData.geojson");

// Only merge **districts** layers (per your strict rule)
function walk(dir, out=[]) {
  if (!fs.existsSync(dir)) return out;
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) walk(p, out);
    else out.push(p);
  }
  return out;
}

function pickZoneProps(props) {
  if (!props || typeof props !== "object") return { zoneCode: null, zoneName: null, taxonomyConfidence: 0.1 };

  const CODE_KEYS = ["DISTRICT","DISTRICT_N","DIST","ZONE","ZONING","ZONING_ID","ZONE_ID","ZONEDIST","ZONINGDIST","ZDIST","CODE"];
  const NAME_KEYS = ["NAME","DISTRICT_N","DISTRICT","ZONE_NAME","ZONING_NAM","ZONINGNAME","LABEL","DESC","DESCRIPTION"];

  let zoneCode = null;
  let zoneName = null;

  for (const k of CODE_KEYS) {
    const v = props[k];
    if (typeof v === "string" && v.trim()) { zoneCode = v.trim(); break; }
    if (typeof v === "number") { zoneCode = String(v); break; }
  }
  for (const k of NAME_KEYS) {
    const v = props[k];
    if (typeof v === "string" && v.trim()) { zoneName = v.trim(); break; }
    if (typeof v === "number") { zoneName = String(v); break; }
  }

  let taxonomyConfidence = 0.15;
  if (zoneCode) taxonomyConfidence = 0.95;
  else if (zoneName) taxonomyConfidence = 0.75;

  return { zoneCode, zoneName, taxonomyConfidence };
}

const all = walk(ROOT);
const districtFiles = all.filter(p =>
  p.toLowerCase().includes(`${path.sep}districts${path.sep}`) &&
  p.toLowerCase().endsWith(".geojson")
);

console.log("====================================================");
console.log(" BUILD zoningBoundariesData.geojson (DISTRICTS ONLY)");
console.log("====================================================");
console.log("Root:", ROOT);
console.log("District files found:", districtFiles.length);

if (districtFiles.length === 0) {
  console.error("❌ No district GeoJSON files found under publicData/zoning/**/districts/");
  process.exit(1);
}

const out = fs.createWriteStream(OUT, { flags: "w", encoding: "utf8" });
out.write('{"type":"FeatureCollection","features":[');

let totalIn = 0;
let totalOut = 0;
let first = true;

for (const filePath of districtFiles) {
  let gj;
  try {
    gj = JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    console.warn("⚠️ Skip unreadable JSON:", filePath);
    continue;
  }

  const feats = Array.isArray(gj?.features) ? gj.features : [];
  totalIn += feats.length;

  const rel = path.relative(ROOT, filePath).split(path.sep);
  const jurisdiction = rel[0] || "unknown";

  for (const f of feats) {
    if (!f || f.type !== "Feature" || !f.geometry) continue;

    const props = f.properties || {};
    const { zoneCode, zoneName, taxonomyConfidence } = pickZoneProps(props);

    const outFeat = {
      type: "Feature",
      geometry: f.geometry,
      properties: {
        ...props,
        __jurisdiction: jurisdiction,
        __layer: "district",
        __sourceFile: path.relative(ROOT, filePath).replaceAll("\\", "/"),
        __zoneCode: zoneCode,
        __zoneName: zoneName,
        __taxonomyConfidence: taxonomyConfidence
      }
    };

    const s = JSON.stringify(outFeat);
    if (!first) out.write(",");
    out.write(s);
    first = false;
    totalOut++;

    if (totalOut % 50000 === 0) console.log(`[merge] wrote=${totalOut}`);
  }
}

out.write("]}");
out.end();

console.log("====================================================");
console.log("✅ Done.");
console.log("Input features scanned:", totalIn);
console.log("Output features written:", totalOut);
console.log("Output:", OUT);
console.log("====================================================");
