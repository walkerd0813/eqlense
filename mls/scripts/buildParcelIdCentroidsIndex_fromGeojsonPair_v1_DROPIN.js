
/**
 * BUILD PARCEL-ID -> (lat,lon) INDEX by MERGING TWO GEOJSON FEATURECOLLECTIONS (v1)
 * ------------------------------------------------------------------------------
 * Use this when you have:
 *   A) idsGeojson:   parcel centroids with parcel id attributes but StatePlane coords (properties populated)
 *   B) coordsGeojson:parcel centroids reprojected to EPSG:4326 but attributes stripped (properties empty)
 *
 * This script streams BOTH files and assumes they are in the SAME feature order.
 * It outputs NDJSON lines: { parcel_id, lat, lon }
 *
 * Usage (PowerShell):
 *   node .\mls\scripts\buildParcelIdCentroidsIndex_fromGeojsonPair_v1_DROPIN.js `
 *     --idsGeojson    C:\seller-app\backend\publicData\parcels\parcelCentroids.geojson `
 *     --coordsGeojson C:\seller-app\backend\publicData\parcels\parcelCentroids_wgs84_WITH_IDS.geojson `
 *     --out           C:\seller-app\backend\publicData\parcels\parcelIdCentroids_wgs84.ndjson `
 *     --meta          C:\seller-app\backend\publicData\parcels\parcelIdCentroids_wgs84_meta.json
 *
 * Then patch:
 *   node .\mls\scripts\patchMissingCoordsFromParcelIdIndex_v3_DROPIN.js --in <v14> --parcelIndex <ndjson> --out <v15> --meta <meta>
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

function inMA(lat, lon) {
  return (
    Number.isFinite(lat) &&
    Number.isFinite(lon) &&
    lat >= 41.0 &&
    lat <= 43.6 &&
    lon >= -73.9 &&
    lon <= -69.0
  );
}

function chooseParcelIdKey(props) {
  if (!props || typeof props !== "object") return null;
  const keys = Object.keys(props);
  if (keys.length === 0) return null;

  const preferred = [
    (nk) => nk === "LOCID" || nk.startsWith("LOCID"),
    (nk) => nk === "MAPPARID" || nk.startsWith("MAPPARID") || nk.startsWith("MAPPAR"),
    (nk) => nk === "PARCELID" || nk.startsWith("PARCELID"),
    (nk) => nk.includes("PARID"),
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

function extractParcelId(feature, pidKeyHint) {
  if (!feature || typeof feature !== "object") return null;
  const p = feature.properties || {};

  if (pidKeyHint && p[pidKeyHint] != null && String(p[pidKeyHint]).trim() !== "") return String(p[pidKeyHint]);

  const fromProps =
    p.parcel_id ??
    p.parcelId ??
    p.PARCEL_ID ??
    p.PARCELID ??
    p.LOC_ID ??
    p.LOCID ??
    p.loc_id ??
    p.locid ??
    p.MAP_PAR_ID ??
    p.map_par_id ??
    p.MAPPARID ??
    p.mapparid ??
    p.pid ??
    p.PID;

  if (fromProps != null && String(fromProps).trim() !== "") return String(fromProps);

  if (feature.id != null && String(feature.id).trim() !== "") return String(feature.id);
  return null;
}

function extractLonLat(feature) {
  const coords = feature?.geometry?.coordinates;
  if (!Array.isArray(coords) || coords.length < 2) return null;
  const lon = Number(coords[0]);
  const lat = Number(coords[1]);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  if (!inMA(lat, lon)) return null;
  return { lat, lon };
}

/**
 * Streaming GeoJSON FeatureCollection parser (no dependencies)
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
          } catch (e) {
            objStart = -1;
            state = "seekObjStart";
            continue;
          }

          yield obj;

          buf = buf.slice(i);
          i = 0;
          objStart = -1;
          state = "seekObjStart";
        }
        continue;
      }
    }

    if (state !== "inObj" && buf.length > 5 * 1024 * 1024) keepTail(2000);
  }
}

async function main() {
  const IDS = path.resolve(__dirname, getArg("idsGeojson", "../../publicData/parcels/parcelCentroids.geojson"));
  const COORDS = path.resolve(__dirname, getArg("coordsGeojson", "../../publicData/parcels/parcelCentroids_wgs84_WITH_IDS.geojson"));
  const OUT = path.resolve(__dirname, getArg("out", "../../publicData/parcels/parcelIdCentroids_wgs84.ndjson"));
  const META = path.resolve(__dirname, getArg("meta", OUT.replace(/\.ndjson$/i, "_meta.json")));

  if (!fs.existsSync(IDS)) {
    console.error("❌ idsGeojson not found:", IDS);
    process.exit(1);
  }
  if (!fs.existsSync(COORDS)) {
    console.error("❌ coordsGeojson not found:", COORDS);
    process.exit(1);
  }

  console.log("====================================================");
  console.log(" BUILD PARCEL-ID CENTROIDS INDEX (MERGE GEOJSON PAIR) ");
  console.log("====================================================");
  console.log("IDS   :", IDS);
  console.log("COORDS:", COORDS);
  console.log("OUT   :", OUT);
  console.log("META  :", META);
  console.log("----------------------------------------------------");

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  const out = fs.createWriteStream(OUT, "utf8");

  const itA = streamGeojsonFeatures(IDS)[Symbol.asyncIterator]();
  const itB = streamGeojsonFeatures(COORDS)[Symbol.asyncIterator]();

  let scannedPairs = 0;
  let wrote = 0;
  let missingPid = 0;
  let badCoords = 0;

  let pidKey = null;

  while (true) {
    const [a, b] = await Promise.all([itA.next(), itB.next()]);
    if (a.done || b.done) break;

    const fa = a.value;
    const fb = b.value;

    if (!pidKey) {
      const k = chooseParcelIdKey(fa?.properties || {});
      if (k) {
        pidKey = k;
        console.log(`[detect] parcel id key candidate: "${pidKey}"`);
      }
    }

    const pid = extractParcelId(fa, pidKey);
    if (!pid || pid.trim() === "") {
      missingPid++;
      scannedPairs++;
      if (scannedPairs % 250000 === 0) console.log(`[progress] scanned ${scannedPairs.toLocaleString()} wrote ${wrote.toLocaleString()} missingPid ${missingPid.toLocaleString()} badCoords ${badCoords.toLocaleString()}`);
      continue;
    }

    const ll = extractLonLat(fb);
    if (!ll) {
      badCoords++;
      scannedPairs++;
      if (scannedPairs % 250000 === 0) console.log(`[progress] scanned ${scannedPairs.toLocaleString()} wrote ${wrote.toLocaleString()} missingPid ${missingPid.toLocaleString()} badCoords ${badCoords.toLocaleString()}`);
      continue;
    }

    out.write(JSON.stringify({ parcel_id: pid, lat: ll.lat, lon: ll.lon }) + "\n");
    wrote++;
    scannedPairs++;

    if (scannedPairs % 250000 === 0) console.log(`[progress] scanned ${scannedPairs.toLocaleString()} wrote ${wrote.toLocaleString()} missingPid ${missingPid.toLocaleString()} badCoords ${badCoords.toLocaleString()}`);
  }

  out.end();

  const meta = {
    ranAt: new Date().toISOString(),
    idsGeojson: IDS,
    coordsGeojson: COORDS,
    out: OUT,
    scannedPairs,
    wrote,
    missingPid,
    badCoords,
    detectedPidKey: pidKey,
    note: "Assumes idsGeojson and coordsGeojson have identical feature ordering.",
  };
  fs.writeFileSync(META, JSON.stringify(meta, null, 2));

  console.log("====================================================");
  console.log("[done]", { scannedPairs, wrote, missingPid, badCoords });
  console.log("OUT :", OUT);
  console.log("META:", META);
  console.log("====================================================");
}

main().catch((e) => {
  console.error("❌ build index failed:", e);
  process.exit(1);
});
