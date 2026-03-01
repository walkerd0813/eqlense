// ESM
import fs from "fs";
import crypto from "crypto";
import path from "path";

function arg(name, def = null) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : def;
}

function sha256File(fp) {
  const h = crypto.createHash("sha256");
  h.update(fs.readFileSync(fp));
  return h.digest("hex");
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText} for ${url}`);
  return await res.json();
}

function ensureDir(fp) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
}

const layerUrl = arg("layerUrl");
const out = arg("out");
const metaOut = arg("meta");
const bboxStr = arg("bbox"); // "minLon,minLat,maxLon,maxLat"
const outFields = arg("outFields", "*"); // comma list
const pageSize = Number(arg("pageSize", "2000"));

if (!layerUrl || !out || !bboxStr) {
  console.error("Usage: --layerUrl <.../MapServer/28> --bbox \"minLon,minLat,maxLon,maxLat\" --out <file.geojson> [--meta meta.json] [--outFields a,b,c]");
  process.exit(1);
}

const [minLon, minLat, maxLon, maxLat] = bboxStr.split(",").map(Number);
if (![minLon, minLat, maxLon, maxLat].every(Number.isFinite)) {
  throw new Error(`Bad bbox: ${bboxStr}`);
}

const queryBase = `${layerUrl.replace(/\/+$/, "")}/query`;

let all = [];
let offset = 0;

while (true) {
  const qs = new URLSearchParams({
    where: "1=1",
    outFields,
    returnGeometry: "true",
    f: "geojson",
    resultRecordCount: String(pageSize),
    resultOffset: String(offset),
    geometry: `${minLon},${minLat},${maxLon},${maxLat}`,
    geometryType: "esriGeometryEnvelope",
    inSR: "4326",
    outSR: "4326",
    spatialRel: "esriSpatialRelIntersects",
  });

  const url = `${queryBase}?${qs.toString()}`;
  const json = await fetchJson(url);

  const feats = json?.features || [];
  all.push(...feats);

  if (feats.length < pageSize) break;
  offset += pageSize;
}

const fc = { type: "FeatureCollection", features: all };
ensureDir(out);
fs.writeFileSync(out, JSON.stringify(fc));

const meta = {
  created_at: new Date().toISOString(),
  layerUrl,
  bbox: { minLon, minLat, maxLon, maxLat },
  outFields,
  features: all.length,
  sha256: sha256File(out),
};

if (metaOut) {
  ensureDir(metaOut);
  fs.writeFileSync(metaOut, JSON.stringify(meta, null, 2));
}

console.log("[done]", meta);
