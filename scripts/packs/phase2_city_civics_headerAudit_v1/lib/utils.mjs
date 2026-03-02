import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

export function nowIso() { return new Date().toISOString(); }
export function ensureDirSync(p) { fs.mkdirSync(p, { recursive: true }); }
export function safeReadJsonSync(p) { return JSON.parse(fs.readFileSync(p, "utf-8")); }

export async function readGeoJSON(p) {
  const raw = await fsp.readFile(p, "utf-8");
  const gj = JSON.parse(raw);
  if (!gj || gj.type !== "FeatureCollection" || !Array.isArray(gj.features)) {
    throw new Error(`Invalid GeoJSON FeatureCollection: ${p}`);
  }
  return gj;
}

export function formatDateStamp(d = new Date()) {
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

export function inferFeatureType(geom) {
  if (!geom || !geom.type) return "unknown";
  if (geom.type === "Polygon" || geom.type === "MultiPolygon") return "polygon";
  if (geom.type === "Point" || geom.type === "MultiPoint") return "point";
  if (geom.type === "LineString" || geom.type === "MultiLineString") return "line";
  return "unknown";
}

export function listContracts(root) {
  const dir = path.join(root, "publicData", "_contracts");
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir)
    .filter(f => /^contract_view_ma__phase2_city_civics__v1__.*\.json$/i.test(f))
    .map(f => path.join(dir, f))
    .sort((a,b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
}

export function findPointerCandidates(root) {
  const hits = [];
  const maxDepth = 7;

  function walk(dir, depth) {
    if (depth > maxDepth) return;
    let entries = [];
    try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const e of entries) {
      const p = path.join(dir, e.name);
      if (e.isDirectory()) {
        if (e.name === "node_modules") continue;
        walk(p, depth + 1);
      } else if (e.isFile()) {
        if (/CURRENT_CONTRACT_VIEW_MA/i.test(e.name) && e.name.toLowerCase().endsWith(".json")) {
          hits.push(p);
        }
      }
    }
  }
  walk(root, 0);
  return hits;
}

export async function writePointer(root, contractPath) {
  const pointerDir = path.join(root, "publicData", "_contracts");
  await fsp.mkdir(pointerDir, { recursive: true });
  const pointerPath = path.join(pointerDir, "CURRENT_CONTRACT_VIEW_MA.json");
  const obj = { current: contractPath, updated_at: nowIso() };
  await fsp.writeFile(pointerPath, JSON.stringify(obj, null, 2), "utf-8");
  return pointerPath;
}

