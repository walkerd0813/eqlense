import fs from "node:fs";
import path from "node:path";

const ROOT = path.resolve("publicData/zoning");
const OUT  = path.resolve("publicData/zoning/zoningBoundariesData_DISTRICTS_v2.geojson");

function walk(dir){
  const out = [];
  for (const ent of fs.readdirSync(dir, { withFileTypes:true })) {
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) out.push(...walk(p));
    else out.push(p);
  }
  return out;
}

function pickLabel(props){
  const keys = [
    "DISTRICT","District","district",
    "ZONE","Zone","zone",
    "ZONING","Zoning","zoning",
    "ZONE_CODE","zone_code","ZONECODE","ZCODE",
    "NAME","Name","name",
    "LABEL","Label","label",
    "CODE","Code","code"
  ];
  for (const k of keys){
    const v = props?.[k];
    if (typeof v === "string" && v.trim()) return v.trim();
    if (typeof v === "number") return String(v);
  }
  return null;
}

function getCitySlug(filePath){
  // ...\publicData\zoning\<city>\districts\....
  const rel = filePath.split(path.sep).join("/");
  const i = rel.toLowerCase().indexOf("/publicdata/zoning/");
  if (i === -1) return "unknown";
  const rest = rel.slice(i + "/publicData/zoning/".length);
  const parts = rest.split("/");
  return (parts[0] || "unknown").toLowerCase();
}

const all = walk(ROOT).filter(p => p.toLowerCase().endsWith(".geojson") && p.toLowerCase().includes(`${path.sep}districts${path.sep}`));
console.log("====================================================");
console.log(" BUILD zoningBoundariesData_DISTRICTS_v2.geojson");
console.log("====================================================");
console.log("Root:", ROOT);
console.log("District files found:", all.length);

let scanned=0, written=0;
const features = [];

for (const f of all){
  let json;
  try {
    json = JSON.parse(fs.readFileSync(f, "utf8"));
  } catch (e){
    console.log("skip bad json:", f);
    continue;
  }
  const city = getCitySlug(f);
  const feats = json?.features || [];
  for (const feat of feats){
    scanned++;
    if (!feat || !feat.geometry) continue;
    const props = feat.properties || {};
    const label = pickLabel(props);

    feat.properties = {
      ...props,
      __layer: "district",
      __city: city,
      __label: label,
      __sourceFile: path.basename(f),
      __sourcePath: f.split(path.sep).join("/")
    };
    features.push(feat);
    written++;
  }
}

const out = { type:"FeatureCollection", features };
fs.writeFileSync(OUT, JSON.stringify(out));
console.log("====================================================");
console.log("✅ Done.");
console.log("Input features scanned:", scanned);
console.log("Output features written:", written);
console.log("Output:", OUT);
console.log("====================================================");
