#!/usr/bin/env node
/**
 * PATCH MISSING COORDS FROM PARCELS.GPKG (missing-only) — v3
 * --------------------------------------------------------
 * For properties rows missing lat/lng, compute a fallback centroid from the parcel
 * polygon geometry in parcels.gpkg and patch those rows.
 *
 * v3 upgrades:
 *  - Robust parcel_id matching (case/whitespace/slash/hyphen/leading-zero normalization)
 *  - Auto-detects PID field casing in GDAL output
 *  - Adds a safety MA bounding-box check to prevent bad coords from being written
 *
 * Usage (positional args, same style as v2):
 *   node patchMissingCoordsFromParcelsGPKG_missingOnly_v3_DROPIN.js \
 *     <IN_PROPERTIES.ndjson> <PARCELS.gpkg> <LAYER> <PID_FIELD> <OUT_PROPERTIES.ndjson> [OUT_META.json]
 *
 * Defaults are set to continue your chain:
 *   IN  = publicData/properties/properties_statewide_geo_zip_district_v6_coords.ndjson
 *   OUT = publicData/properties/properties_statewide_geo_zip_district_v7_coords.ndjson
 */

import fs from "fs";
import path from "path";
import readline from "readline";
import { spawn } from "child_process";

const DEFAULT_IN = "publicData/properties/properties_statewide_geo_zip_district_v6_coords.ndjson";
const DEFAULT_GPKG = "publicData/parcels/parcels.gpkg";
const DEFAULT_LAYER = "parcels";
const DEFAULT_PID_FIELD = "LOC_ID";
const DEFAULT_OUT = "publicData/properties/properties_statewide_geo_zip_district_v7_coords.ndjson";

// Rough MA bbox (very forgiving) for sanity-checking centroids.
const MA_BBOX = {
  minLat: 40.0,
  maxLat: 43.8,
  minLng: -73.8,
  maxLng: -69.4,
};

function nowIso() {
  return new Date().toISOString();
}

function isFiniteNum(n) {
  return typeof n === "number" && Number.isFinite(n);
}

function inMaBbox(lat, lng) {
  return (
    isFiniteNum(lat) &&
    isFiniteNum(lng) &&
    lat >= MA_BBOX.minLat &&
    lat <= MA_BBOX.maxLat &&
    lng >= MA_BBOX.minLng &&
    lng <= MA_BBOX.maxLng
  );
}

function collapse(s) {
  return String(s ?? "")
    .toUpperCase()
    .replace(/[\u00A0\t\r\n]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePidKey(s) {
  // Keep separators but normalize spacing around them.
  return collapse(s)
    .replace(/\s*\/\s*/g, "/")
    .replace(/\s*-\s*/g, "-")
    .replace(/\s*\.\s*/g, ".")
    .replace(/\s*_/g, "_");
}

function compactPidKey(s) {
  // Remove spaces entirely (some ids are "43011 0023" vs "430110023").
  return normalizePidKey(s).replace(/\s+/g, "");
}

function trimLeadingZerosDigitsOnly(s) {
  const t = collapse(s);
  if (!/^\d+$/.test(t)) return null;
  const trimmed = t.replace(/^0+/, "");
  return trimmed.length ? trimmed : "0";
}

function normalizeTown(v) {
  return String(v || "").toUpperCase().trim();
}

function resolveTownField(props) {
  if (!props || typeof props !== "object") return null;

  const candidates = [
    "TOWN",
    "TOWN_NAME",
    "TOWNNAME",
    "MUNI",
    "MUNICIPALITY",
    "CITY",
    "MUN",
    "COMMUNITY",
  ];

  for (const c of candidates) {
    if (Object.prototype.hasOwnProperty.call(props, c) && props[c] != null && String(props[c]).trim()) return c;
  }

  // Case-insensitive search
  const keys = Object.keys(props);
  const byLower = new Map(keys.map((k) => [k.toLowerCase(), k]));
  for (const c of candidates) {
    const k = byLower.get(c.toLowerCase());
    if (k && props[k] != null && String(props[k]).trim()) return k;
  }
  return null;
}

function townJoin(town, token) {
  const t = normalizeTown(town);
  const x = token == null ? "" : String(token).trim();
  if (!t || !x) return null;
  return `${t}|${x}`;
}

function addUnique(map, key, value) {
  if (!key) return;
  if (!map.has(key)) {
    map.set(key, value);
    return;
  }
  const cur = map.get(key);
  if (cur === value) return;
  // Mark as ambiguous — we won't use this key for matching.
  map.set(key, null);
}

async function collectMissingParcelIds(inPath) {
  const missingExact = new Set();
  const missingExactTown = new Set();

  // These maps convert "normalized PID" → "raw PID as it appears in properties file"
  // If multiple different raw PIDs collapse to the same normalized key, value becomes null (ambiguous) for safety.
  const normToOrig = new Map();
  const compactToOrig = new Map();
  const zeroTrimToOrig = new Map();

  const normToOrigTown = new Map();
  const compactToOrigTown = new Map();
  const zeroTrimToOrigTown = new Map();

  let total = 0;
  let missing = 0;
  let missingNoPid = 0;

  const rl = readline.createInterface({
    input: fs.createReadStream(inPath),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if (!line) continue;
    total++;
    if (total % 250000 === 0) {
      console.log(`[collect] scanned ${total.toLocaleString()} rows... missing=${missing.toLocaleString()}`);
    }

    let r;
    try {
      r = JSON.parse(line);
    } catch {
      continue;
    }

    const hasLatLng = isFiniteNum(r.lat) && isFiniteNum(r.lng);
    if (hasLatLng) continue;

    missing++;

    const pidRaw = r.parcel_id == null ? null : String(r.parcel_id).trim();
    if (!pidRaw) {
      missingNoPid++;
      continue;
    }

    const town = normalizeTown(r.town);

    // Exact
    missingExact.add(pidRaw);
    const tk = townJoin(town, pidRaw);
    if (tk) missingExactTown.add(tk);

    // Normalized (aggressive)
    const nk = normalizePidKey(pidRaw);
    if (nk) {
      addUnique(normToOrig, nk, pidRaw);
      const tnk = townJoin(town, nk);
      if (tnk) addUnique(normToOrigTown, tnk, pidRaw);
    }

    // Compact (keeps _ and -)
    const ck = compactPidKey(pidRaw);
    if (ck) {
      addUnique(compactToOrig, ck, pidRaw);
      const tck = townJoin(town, ck);
      if (tck) addUnique(compactToOrigTown, tck, pidRaw);
    }

    // Digits-only leading zero trim
    const zt = trimLeadingZerosDigitsOnly(pidRaw);
    if (zt) {
      addUnique(zeroTrimToOrig, zt, pidRaw);
      const tzt = townJoin(town, zt);
      if (tzt) addUnique(zeroTrimToOrigTown, tzt, pidRaw);
    }
  }

  console.log(`[collect] total rows: ${total.toLocaleString()}`);
  console.log(`[collect] missing coords: ${missing.toLocaleString()} (no parcel_id: ${missingNoPid.toLocaleString()})`);
  console.log(`[collect] unique missing parcel_id: ${missingExact.size.toLocaleString()}`);
  console.log(`[collect] normalized map sizes: norm=${normToOrig.size.toLocaleString()} compact=${compactToOrig.size.toLocaleString()} zeroTrim=${zeroTrimToOrig.size.toLocaleString()}`);

  return {
    totalRows: total,
    missingRows: missing,
    missingNoPid,
    missingExact,
    missingExactTown,
    normToOrig,
    compactToOrig,
    zeroTrimToOrig,
    normToOrigTown,
    compactToOrigTown,
    zeroTrimToOrigTown,
  };
}

function centroidFromGeometry(geom) {
  if (!geom || !geom.type || !geom.coordinates) return null;

  // Point
  if (geom.type === "Point") {
    const [x, y] = geom.coordinates;
    if (isFiniteNum(x) && isFiniteNum(y)) return { lng: x, lat: y, method: "point" };
    return null;
  }

  // Polygon (take outer ring)
  if (geom.type === "Polygon") {
    const ring = geom.coordinates?.[0];
    if (!Array.isArray(ring) || ring.length < 3) return null;
    return polygonRingCentroid(ring);
  }

  // MultiPolygon (choose largest by absolute area)
  if (geom.type === "MultiPolygon") {
    let best = null;
    let bestAbs = -1;
    for (const poly of geom.coordinates || []) {
      const ring = poly?.[0];
      if (!Array.isArray(ring) || ring.length < 3) continue;
      const c = polygonRingCentroid(ring);
      if (!c) continue;
      const a = Math.abs(c.area);
      if (a > bestAbs) {
        bestAbs = a;
        best = c;
      }
    }
    if (!best) return null;
    return { lng: best.lng, lat: best.lat, method: "multipolygon_largest_outerring", areaAbs: bestAbs };
  }

  // GeometryCollection fallback: pick first polygon-ish geometry.
  if (geom.type === "GeometryCollection" && Array.isArray(geom.geometries)) {
    for (const g of geom.geometries) {
      const c = centroidFromGeometry(g);
      if (c) return { ...c, method: `collection:${c.method}` };
    }
  }

  return null;
}

// Signed area & centroid for a linear ring (x=lng, y=lat)
function polygonRingCentroid(ring) {
  let a = 0;
  let cx = 0;
  let cy = 0;

  for (let i = 0; i < ring.length - 1; i++) {
    const [x1, y1] = ring[i];
    const [x2, y2] = ring[i + 1];
    if (!isFiniteNum(x1) || !isFiniteNum(y1) || !isFiniteNum(x2) || !isFiniteNum(y2)) continue;
    const f = x1 * y2 - x2 * y1;
    a += f;
    cx += (x1 + x2) * f;
    cy += (y1 + y2) * f;
  }

  if (!isFiniteNum(a) || a === 0) return null;
  a = a / 2;
  cx = cx / (6 * a);
  cy = cy / (6 * a);
  if (!isFiniteNum(cx) || !isFiniteNum(cy)) return null;

  return { lng: cx, lat: cy, area: a, method: "polygon_outerring" };
}

function resolvePidField(props, preferredField) {
  if (!props || typeof props !== "object") return null;
  if (preferredField in props) return preferredField;
  const prefLower = String(preferredField || "").toLowerCase();
  if (prefLower) {
    for (const k of Object.keys(props)) {
      if (k.toLowerCase() === prefLower) return k;
    }
  }
  // Common fallbacks
  const fallbacks = ["LOC_ID", "loc_id", "LOCID", "locid", "PARCEL_ID", "parcel_id", "MAP_PAR_ID", "map_par_id"];
  for (const f of fallbacks) {
    if (f in props) return f;
    const fl = f.toLowerCase();
    for (const k of Object.keys(props)) {
      if (k.toLowerCase() === fl) return k;
    }
  }
  return null;
}

async function buildCentroidMapsFromGpkg({ gpkgPath, layer, pidField, missingExact, normToOrig, compactToOrig, zeroTrimToOrig }) {
  // Maps keyed by the *original* parcel_id string from properties file.
  const centroidByPid = new Map();
  // Map keyed by normalized parcel id (only if unique).
  const centroidByNorm = new Map();

  // NOTE: We output GeoJSONSeq to stdout so we can stream in Node.
  const args = [
    "-f",
    "GeoJSONSeq",
    "/vsistdout/",
    gpkgPath,
    layer,
    "-t_srs",
    "EPSG:4326",
    "-skipfailures",
  ];

  console.log("[gpkg] streaming parcels from ogr2ogr...");
  // On Windows, ogr2ogr should be on PATH if you’ve been running the other scripts.
  const proc = spawn("ogr2ogr", args, { stdio: ["ignore", "pipe", "pipe"] });

  let stderr = "";
  proc.stderr.on("data", (d) => (stderr += d.toString()));

  const rl = readline.createInterface({ input: proc.stdout });

  let seen = 0;
  let kept = 0;
  let badCoords = 0;
  let ambiguousHits = 0;
  let detectedPidField = null;
  let detectedTownField = null;
  let samplePrinted = false;

  for await (const line of rl) {
    if (!line.trim()) continue;
    let feat;
    try {
      feat = JSON.parse(line);
    } catch {
      continue;
    }

    seen++;
    if (seen % 500000 === 0) {
      console.log("[gpkg] progress", { seen, kept, badCoords, ambiguousHits });
    }

    const props = feat?.properties;
    if (!detectedPidField) {
      detectedPidField = resolvePidField(props, pidField);
      if (!samplePrinted && props) {
        samplePrinted = true;
        console.log("[gpkg] detectedPidField:", detectedPidField);
        console.log("[gpkg] sample keys:", Object.keys(props).slice(0, 12));
        if (detectedPidField) console.log("[gpkg] sample pid:", props[detectedPidField]);
      }
    }

    const useField = detectedPidField || pidField;
    const pidVal = props?.[useField];
    if (pidVal == null) continue;

    const pidStrRaw = String(pidVal);

    // Decide which original parcel_id (from properties file) this feature should patch.
    let targetPid = null;

    // 1) Exact
    if (missingExact.has(pidStrRaw)) {
      targetPid = pidStrRaw;
    } else {
      // 2) Normalized
      const nk = normalizePidKey(pidStrRaw);
      const mapped = nk ? normToOrig.get(nk) : null;
      if (mapped && mapped !== null) {
        targetPid = mapped;
      } else {
        // 3) Compact
        const ck = compactPidKey(pidStrRaw);
        const mapped2 = ck ? compactToOrig.get(ck) : null;
        if (mapped2 && mapped2 !== null) {
          targetPid = mapped2;
        } else {
          // 4) Digits-only leading-zero trim
          const zt = trimLeadingZerosDigitsOnly(pidStrRaw);
          const mapped3 = zt ? zeroTrimToOrig.get(zt) : null;
          if (mapped3 && mapped3 !== null) targetPid = mapped3;
        }
      }

      // Ambiguous normalized keys are stored as null; count and skip.
      if (!targetPid && (normToOrig.get(normalizePidKey(pidStrRaw)) === null || compactToOrig.get(compactPidKey(pidStrRaw)) === null)) {
        ambiguousHits++;
      }
    }

    if (!targetPid) continue;

    // If we already computed a centroid for this pid, skip.
    if (centroidByPid.has(targetPid)) continue;

    const c = centroidFromGeometry(feat?.geometry);
    if (!c || !inMaBbox(c.lat, c.lng)) {
      badCoords++;
      continue;
    }

    centroidByPid.set(targetPid, {
      lat: c.lat,
      lng: c.lng,
      method: c.method,
      areaAbs: c.areaAbs ?? null,
    });

    const nkTarget = normalizePidKey(targetPid);
    if (nkTarget && !centroidByNorm.has(nkTarget)) {
      centroidByNorm.set(nkTarget, centroidByPid.get(targetPid));
    }

    kept++;
  }

  const exitCode = await new Promise((resolve) => proc.on("close", resolve));
  if (exitCode !== 0) {
    console.error("❌ ogr2ogr failed with exit code", exitCode);
    if (stderr) console.error(stderr.slice(0, 2000));
    throw new Error(`ogr2ogr failed (exit ${exitCode})`);
  }

  console.log("[gpkg] done", {
    seen,
    kept,
    mapSize: centroidByPid.size,
    badCoords,
    ambiguousHits,
    detectedPidField: detectedPidField || null,
  });

  return { centroidByPid, centroidByNorm, detectedPidField: detectedPidField || null, badCoords, ambiguousHits };
}

async function patchNdjson({ inPath, outPath, centroidByPid, centroidByNorm }) {
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  const out = fs.createWriteStream(outPath);
  const rl = readline.createInterface({ input: fs.createReadStream(inPath) });

  let total = 0;
  let missingBefore = 0;
  let patched = 0;
  let stillMissing = 0;
  let patchedByNorm = 0;
  let badCentroid = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;
    total++;
    if (total % 500000 === 0) {
      console.log("[progress] patch", { total, missingBefore, patched, stillMissing, patchedByNorm, badCentroid });
    }

    const r = JSON.parse(line);
    const has = isFiniteNum(r.lat) && isFiniteNum(r.lng);

    if (has) {
      out.write(JSON.stringify(r) + "\n");
      continue;
    }

    missingBefore++;

    const pid = r.parcel_id != null ? String(r.parcel_id) : null;
    let c = pid ? centroidByPid.get(pid) : null;
    let used = "pid";

    if (!c && pid) {
      const nk = normalizePidKey(pid);
      c = nk ? centroidByNorm.get(nk) : null;
      if (c) used = "norm";
    }

    if (c && inMaBbox(c.lat, c.lng)) {
      r.lat = c.lat;
      r.lng = c.lng;
      r.coord_crs = "EPSG:4326";
      r.coord_source = used === "norm" ? "parcels.gpkg:centroid->wgs84:norm" : "parcels.gpkg:centroid->wgs84";
      r.centroid_source = "parcels.gpkg";
      r.coord_key_used = r.coord_key_used ?? null;
      patched++;
      if (used === "norm") patchedByNorm++;
    } else {
      if (c) badCentroid++;
      stillMissing++;
    }

    out.write(JSON.stringify(r) + "\n");
  }

  out.end();
  await new Promise((resolve) => out.on("finish", resolve));

  return { total, missingBefore, patched, stillMissing, patchedByNorm, badCentroid };
}

async function main() {
  const inPath = process.argv[2] || DEFAULT_IN;
  const gpkgPath = process.argv[3] || DEFAULT_GPKG;
  const layer = process.argv[4] || DEFAULT_LAYER;
  const pidField = process.argv[5] || DEFAULT_PID_FIELD;
  const outPath = process.argv[6] || DEFAULT_OUT;
  const metaPath = process.argv[7] || outPath.replace(/\.ndjson$/i, "_meta.json");

  console.log("====================================================");
  console.log(" PATCH MISSING COORDS FROM PARCELS.GPKG (missing-only) — v3");
  console.log("====================================================");
  console.log("IN_PROPERTIES:", path.resolve(inPath));
  console.log("PARCELS_GPKG: ", path.resolve(gpkgPath));
  console.log("PARCELS_LAYER:", layer);
  console.log("PID_FIELD:    ", pidField);
  console.log("OUT_PROPERTIES:", path.resolve(outPath));
  console.log("OUT_META:      ", path.resolve(metaPath));
  console.log("----------------------------------------------------");

  if (!fs.existsSync(inPath)) throw new Error(`IN_PROPERTIES not found: ${inPath}`);
  if (!fs.existsSync(gpkgPath)) throw new Error(`PARCELS_GPKG not found: ${gpkgPath}`);

  console.log("[scan] collecting missing parcel_ids...");
  const missInfo = await collectMissingParcelIds(inPath);
  console.log("[scan] done", {
    total: missInfo.total,
    missing: missInfo.missing,
    uniqueMissingParcelIds: missInfo.missingExact.size,
    normKeys: missInfo.normToOrig.size,
    compactKeys: missInfo.compactToOrig.size,
    zeroTrimKeys: missInfo.zeroTrimToOrig.size,
  });

  if (missInfo.missingExact.size === 0) {
    console.log("✅ No missing coords. Nothing to patch.");
    fs.writeFileSync(metaPath, JSON.stringify({ ok: true, builtAt: nowIso(), total: missInfo.total, missing: 0, patched: 0 }, null, 2));
    return;
  }

  const { centroidByPid, centroidByNorm, detectedPidField, badCoords, ambiguousHits } =
    await buildCentroidMapsFromGpkg({
      gpkgPath,
      layer,
      pidField,
      missingExact: missInfo.missingExact,
      normToOrig: missInfo.normToOrig,
      compactToOrig: missInfo.compactToOrig,
      zeroTrimToOrig: missInfo.zeroTrimToOrig,
    });

  console.log("[write] patching properties NDJSON...");
  const result = await patchNdjson({ inPath, outPath, centroidByPid, centroidByNorm });

  const meta = {
    ok: true,
    builtAt: nowIso(),
    script: "patchMissingCoordsFromParcelsGPKG_missingOnly_v3_DROPIN",
    inputs: {
      inProperties: path.resolve(inPath),
      parcelsGpkg: path.resolve(gpkgPath),
      parcelsLayer: layer,
      pidFieldRequested: pidField,
      pidFieldDetected: detectedPidField,
    },
    scan: {
      total: missInfo.total,
      missingBefore: missInfo.missing,
      uniqueMissingParcelIds: missInfo.missingExact.size,
      normKeyCount: missInfo.normToOrig.size,
      compactKeyCount: missInfo.compactToOrig.size,
      zeroTrimKeyCount: missInfo.zeroTrimToOrig.size,
    },
    gpkg: {
      centroidFound: centroidByPid.size,
      centroidFoundByNormKey: centroidByNorm.size,
      badCoordsFiltered: badCoords,
      ambiguousKeyHitsSkipped: ambiguousHits,
      coordCrs: "EPSG:4326",
      method: "parcel_polygon_centroid_outerring_largestpoly",
      bboxGuard: MA_BBOX,
    },
    stats: result,
  };

  fs.writeFileSync(metaPath, JSON.stringify(meta, null, 2));
  console.log("====================================================");
  console.log("[done]", result);
  console.log("OUT:", path.resolve(outPath));
  console.log("META:", path.resolve(metaPath));
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ patchMissingCoordsFromParcelsGPKG failed:", err?.stack || err?.message || err);
  process.exit(1);
});
