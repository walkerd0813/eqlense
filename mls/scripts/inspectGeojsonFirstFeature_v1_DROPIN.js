
/**
 * INSPECT GEOJSON (first feature only) - drop-in
 * ------------------------------------------------------------
 * Safe for huge GeoJSON FeatureCollections (streams file, does NOT JSON.parse whole file).
 *
 * Usage:
 *   node .\mls\scripts\inspectGeojsonFirstFeature_v1_DROPIN.js --file C:\path\to\parcelPolygons.geojson
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function getArg(name, fallback = null) {
  const key = `--${name}`;
  const i = process.argv.findIndex((a) => a === key);
  if (i >= 0 && i + 1 < process.argv.length) return process.argv[i + 1];
  return fallback;
}

function normKey(k) {
  return String(k ?? "").toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function chooseParcelIdKey(props) {
  if (!props || typeof props !== "object") return null;
  const keys = Object.keys(props);
  if (keys.length === 0) return null;

  const preferred = [
    (nk) => nk === "LOCID" || nk.startsWith("LOCID"),
    (nk) => nk === "MAPPARID" || nk.startsWith("MAPPARID") || nk.startsWith("MAPPAR"),
    (nk) => nk === "PARCELID" || nk.startsWith("PARCELID"),
    (nk) => nk === "PARID" || nk.includes("PARID"),
    (nk) => nk === "PID" || nk.endsWith("PID"),
  ];

  for (const test of preferred) {
    for (const k of keys) {
      const nk = normKey(k);
      if (test(nk)) return k;
    }
  }

  for (const k of keys) {
    const nk = normKey(k);
    if (nk.includes("LOC") || nk.includes("PAR") || nk.includes("MAP")) return k;
  }

  return keys[0];
}

function statePlaneGuessFromCoordPair(pair) {
  if (!Array.isArray(pair) || pair.length < 2) return false;
  const x = Number(pair[0]);
  const y = Number(pair[1]);
  return Number.isFinite(x) && Number.isFinite(y) && Math.abs(x) > 10000 && Math.abs(y) > 10000;
}

/**
 * Streaming GeoJSON FeatureCollection parser (no deps)
 * Yields each feature object under top-level "features": [...]
 */
async function* streamGeojsonFeatures(filePath) {
  const rs = fs.createReadStream(filePath, { encoding: "utf8", highWaterMark: 1024 * 1024 });
  let buf = "";
  let state = "seekFeatures";
  let i = 0;

  let inString = false;
  let escape = false;
  let depth = 0;
  let objStart = -1;

  const keepTail = (n = 200) => {
    if (buf.length > n) buf = buf.slice(buf.length - n);
    i = 0;
  };

  for await (const chunk of rs) {
    buf += chunk;

    while (i < buf.length) {
      if (state === "seekFeatures") {
        const idx = buf.indexOf('"features"', i);
        if (idx < 0) {
          keepTail(200);
          break;
        }
        i = idx + 10;
        state = "seekArrayStart";
        continue;
      }
      if (state === "seekArrayStart") {
        const idx = buf.indexOf("[", i);
        if (idx < 0) {
          keepTail(200);
          break;
        }
        i = idx + 1;
        state = "seekObjStart";
        continue;
      }
      if (state === "seekObjStart") {
        while (i < buf.length && (buf[i] === " " || buf[i] === "\n" || buf[i] === "\r" || buf[i] === "\t" || buf[i] === ",")) i++;
        if (i >= buf.length) break;
        if (buf[i] === "]") return;
        if (buf[i] !== "{") {
          i++;
          continue;
        }
        state = "inObj";
        objStart = i;
        inString = false;
        escape = false;
        depth = 0;
        continue;
      }
      if (state === "inObj") {
        const ch = buf[i];
        if (inString) {
          if (escape) escape = false;
          else if (ch === "\\") escape = true;
          else if (ch === '"') inString = false;
        } else {
          if (ch === '"') inString = true;
          else if (ch === "{") depth++;
          else if (ch === "}") depth--;
        }
        i++;
        if (!inString && depth === 0 && objStart >= 0 && i > objStart) {
          const text = buf.slice(objStart, i);
          let obj = null;
          try {
            obj = JSON.parse(text);
          } catch {
            objStart = -1;
            state = "seekObjStart";
            continue;
          }
          yield obj;
          return; // ONLY first feature
        }
      }
    }

    if (state !== "inObj" && buf.length > 5 * 1024 * 1024) keepTail(2000);
  }
}

async function main() {
  const file = getArg("file", null);
  if (!file) {
    console.error("❌ Missing --file <path to geojson>");
    process.exit(1);
  }
  const FILE = path.resolve(__dirname, file);

  if (!fs.existsSync(FILE)) {
    console.error("❌ Not found:", FILE);
    process.exit(1);
  }

  console.log("====================================================");
  console.log(" INSPECT GEOJSON (FIRST FEATURE)");
  console.log("====================================================");
  console.log("FILE:", FILE);
  console.log("----------------------------------------------------");

  let first = null;
  for await (const feat of streamGeojsonFeatures(FILE)) {
    first = feat;
  }

  if (!first) {
    console.error("❌ No features detected under top-level 'features' array.");
    process.exit(1);
  }

  const props = first.properties || {};
  const propsKeys = Object.keys(props);
  const geomType = first?.geometry?.type ?? null;

  // Coordinate sample depends on geometry type
  let coordSample = null;
  const coords = first?.geometry?.coordinates;
  if (Array.isArray(coords)) {
    if (geomType === "Point") coordSample = coords;
    else if (geomType === "Polygon") coordSample = coords?.[0]?.[0] ?? null;        // first vertex of outer ring
    else if (geomType === "MultiPolygon") coordSample = coords?.[0]?.[0]?.[0] ?? null;
    else coordSample = coords?.[0] ?? null;
  }

  const pidKey = chooseParcelIdKey(props);
  const pidVal = pidKey ? props[pidKey] : null;

  const result = {
    firstFeatureKeys: Object.keys(first),
    geometryType: geomType,
    firstPropsKeys: propsKeys.slice(0, 60),
    propsKeyCount: propsKeys.length,
    suggestedParcelIdField: pidKey,
    sampleParcelIdValue: pidVal,
    coordSample,
    coordSampleLooksStatePlane: statePlaneGuessFromCoordPair(coordSample),
    hasFeatureId: first?.id ?? null,
  };

  console.log(JSON.stringify(result, null, 2));
  console.log("----------------------------------------------------");
  console.log("Interpretation:");
  console.log("- propsKeyCount > 0 AND suggestedParcelIdField != null => this file carries IDs.");
  console.log("- coordSampleLooksStatePlane=true => coords likely EPSG:26986 (StatePlane), must reproject to EPSG:4326 before patching.");
  console.log("- geometryType Polygon/MultiPolygon => you must run Centroids (QGIS) to turn it into points before using parcel-id patcher.");
  console.log("====================================================");
}

main().catch((e) => {
  console.error("❌ inspect failed:", e);
  process.exit(1);
});
