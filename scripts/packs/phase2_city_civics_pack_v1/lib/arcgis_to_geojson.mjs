import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

async function fetchJson(url) {
  const res = await fetch(url, { redirect: "follow" });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status} for ${url} :: ${text.slice(0,200)}`);
  }
  return await res.json();
}

export async function arcgisLayerToGeoJSON({ layerUrl, outPath, where = "1=1", outFields = "*", chunkSize = 2000 }) {
  // ArcGIS query endpoint
  const queryUrl = layerUrl.endsWith("/query") ? layerUrl : `${layerUrl.replace(/\/+$/,"")}/query`;
  const features = [];
  let resultOffset = 0;
  let exceeded = true;

  // get objectIdFieldName (optional)
  let oidField = null;
  try {
    const meta = await fetchJson(`${layerUrl}?f=pjson`);
    oidField = meta.objectIdField;
  } catch {}

  while (exceeded) {
    const params = new URLSearchParams({
      f: "geojson",
      where,
      outFields,
      resultOffset: String(resultOffset),
      resultRecordCount: String(chunkSize),
      returnGeometry: "true",
      outSR: "4326"
    });
    if (oidField) params.set("orderByFields", `${oidField} ASC`);

    const url = `${queryUrl}?${params.toString()}`;
    const gj = await fetchJson(url);

    if (!gj || !Array.isArray(gj.features)) {
      throw new Error(`Unexpected ArcGIS GeoJSON response from ${url}`);
    }
    features.push(...gj.features);

    // ArcGIS may include exceededTransferLimit in GeoJSON properties (not guaranteed)
    exceeded = Boolean(gj.exceededTransferLimit) || (gj.features.length === chunkSize);
    resultOffset += chunkSize;

    if (gj.features.length === 0) exceeded = false;
  }

  const fc = { type: "FeatureCollection", features };
  await fsp.mkdir(path.dirname(outPath), { recursive: true });
  await fsp.writeFile(outPath, JSON.stringify(fc), "utf-8");
  return { outPath, featureCount: features.length };
}

