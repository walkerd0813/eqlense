import fs from "node:fs";
import path from "node:path";
import { ensureDirSync } from "./utils.mjs";

async function getJSON(url) {
  const u = url.includes("?") ? `${url}&f=pjson` : `${url}?f=pjson`;
  const res = await fetch(u, { headers: { "User-Agent": "EquityLens/Phase3" } });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${u}`);
  return res.json();
}

export async function describeService(url) {
  return getJSON(url);
}

export async function listLayers(url) {
  const meta = await getJSON(url);
  const layers = Array.isArray(meta.layers) ? meta.layers : [];
  return layers.map(l => ({ id: l.id, name: l.name }));
}

async function queryCount(layerUrl) {
  const u = `${layerUrl}/query?where=1%3D1&returnCountOnly=true&f=json`;
  const res = await fetch(u);
  if (!res.ok) throw new Error(`count HTTP ${res.status}`);
  const j = await res.json();
  return Number(j.count || 0);
}

async function queryPage(layerUrl, offset, num) {
  const params = new URLSearchParams({
    where: "1=1",
    outFields: "*",
    returnGeometry: "true",
    f: "geojson",
    resultOffset: String(offset),
    resultRecordCount: String(num)
  });
  const u = `${layerUrl}/query?${params.toString()}`;
  const res = await fetch(u);
  if (!res.ok) throw new Error(`query HTTP ${res.status} at offset=${offset}`);
  return res.json();
}

export async function dumpLayerToGeoJSON({ layerUrl, outPath }) {
  ensureDirSync(path.dirname(outPath));

  const count = await queryCount(layerUrl);
  if (!count) {
    fs.writeFileSync(outPath, JSON.stringify({ type: "FeatureCollection", features: [] }), "utf8");
    return { feature_count: 0 };
  }

  const pageSize = 1000;
  let offset = 0;
  const all = [];
  while (offset < count) {
    const page = await queryPage(layerUrl, offset, pageSize);
    const feats = Array.isArray(page.features) ? page.features : [];
    for (const f of feats) all.push(f);
    offset += feats.length ? feats.length : pageSize;
    if (!feats.length) break;
  }

  fs.writeFileSync(outPath, JSON.stringify({ type: "FeatureCollection", features: all }), "utf8");
  return { feature_count: all.length };
}
