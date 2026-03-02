#!/usr/bin/env node
/**
 * phaseZO_normalizeAttachFreeze_v2.mjs
 *
 * Changes from v1:
 *  - Supports manifest entries with source_subdir ("overlays" or "districts")
 *    so we can freeze historic layers that live under districts/.
 *  - Skips city folders starting with "_" when scanning zoning root (where applicable).
 *
 * Usage (from backend/):
 *  node .\mls\scripts\gis\phaseZO_normalizeAttachFreeze_v2.mjs
 *
 * Optional:
 *  --properties "...\publicData\properties\YOUR_PROPERTIES.ndjson"
 *  --city "boston"
 */

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import readline from "node:readline";

function info(m){ console.log(`[info] ${m}`); }
function warn(m){ console.log(`[warn] ${m}`); }
function done(m){ console.log(`[done] ${m}`); }

function parseArgs(argv){
  const out = {};
  for (let i=2;i<argv.length;i++){
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = (argv[i+1] && !argv[i+1].startsWith("--")) ? argv[++i] : true;
    out[k] = v;
  }
  return out;
}

async function exists(p){ try { await fsp.access(p); return true; } catch { return false; } }
async function ensureDir(p){ await fsp.mkdir(p, { recursive: true }); }

function utcStamp(){
  const d = new Date();
  const pad = (n)=>String(n).padStart(2,"0");
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth()+1)}${pad(d.getUTCDate())}_${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}Z`;
}

async function sha256File(filePath){
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash("sha256");
    const s = fs.createReadStream(filePath);
    s.on("data", (d)=>hash.update(d));
    s.on("error", reject);
    s.on("end", ()=>resolve(hash.digest("hex")));
  });
}

function sha1Hex(s){ return crypto.createHash("sha1").update(s).digest("hex"); }

function isGeoJSONPath(p){
  const ext = path.extname(p).toLowerCase();
  return ext === ".geojson" || ext === ".json";
}

function coordInMABounds(lon, lat){
  return (lat >= 41.0 && lat <= 43.6 && lon >= -73.7 && lon <= -69.3);
}

function bboxInit(){ return { minLon: Infinity, minLat: Infinity, maxLon: -Infinity, maxLat: -Infinity }; }
function bboxUpdate(b, lon, lat){
  if (lon < b.minLon) b.minLon = lon;
  if (lat < b.minLat) b.minLat = lat;
  if (lon > b.maxLon) b.maxLon = lon;
  if (lat > b.maxLat) b.maxLat = lat;
}
function bboxToArray(b){ if (!isFinite(b.minLon)) return null; return [b.minLon, b.minLat, b.maxLon, b.maxLat]; }
function bboxContains(b, lon, lat){ return lon >= b.minLon && lon <= b.maxLon && lat >= b.minLat && lat <= b.maxLat; }

function geomType(g){ return g?.type || "Unknown"; }

function walkCoords(geom, fn){
  if (!geom) return;
  const t = geom.type;
  const c = geom.coordinates;

  const walk = (arr) => {
    if (!Array.isArray(arr)) return;
    if (arr.length === 2 && typeof arr[0] === "number" && typeof arr[1] === "number"){
      fn(arr[0], arr[1]);
      return;
    }
    for (const x of arr) walk(x);
  };

  if (t === "Point") fn(c[0], c[1]);
  else walk(c);
}

function pointInRing(point, ring){
  const x = point[0], y = point[1];
  let inside = false;
  for (let i=0, j=ring.length-1; i<ring.length; j=i++){
    const xi = ring[i][0], yi = ring[i][1];
    const xj = ring[j][0], yj = ring[j][1];
    const intersect = ((yi > y) !== (yj > y)) &&
      (x < (xj - xi) * (y - yi) / ((yj - yi) || 1e-12) + xi);
    if (intersect) inside = !inside;
  }
  return inside;
}

function pointInPolygon(point, polyCoords){
  if (!Array.isArray(polyCoords) || polyCoords.length === 0) return false;
  if (!pointInRing(point, polyCoords[0])) return false;
  for (let i=1;i<polyCoords.length;i++){
    if (pointInRing(point, polyCoords[i])) return false;
  }
  return true;
}

function pointInGeometry(point, geom){
  if (!geom) return false;
  const t = geom.type;
  const c = geom.coordinates;
  if (t === "Polygon") return pointInPolygon(point, c);
  if (t === "MultiPolygon"){
    for (const poly of c){
      if (pointInPolygon(point, poly)) return true;
    }
    return false;
  }
  return false;
}

function toCsv(rows, headers){
  const esc = (v) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    if (/[",\n]/.test(s)) return `"${s.replaceAll('"','""')}"`;
    return s;
  };
  const out = [];
  out.push(headers.join(","));
  for (const r of rows){
    out.push(headers.map(h => esc(r[h])).join(","));
  }
  return out.join("\n");
}

function pickCityFromProperty(obj){
  return (
    obj?.source_city ||
    obj?.source_city_norm ||
    obj?.address_city ||
    obj?.address_city_norm ||
    obj?.city ||
    obj?.town ||
    ""
  );
}

function pickPointFromProperty(obj){
  const lat = obj?.centroid_lat ?? obj?.lat ?? obj?.latitude ?? obj?.coord_lat ?? obj?.centroid?.lat;
  const lon = obj?.centroid_lon ?? obj?.lon ?? obj?.lng ?? obj?.longitude ?? obj?.coord_lon ?? obj?.centroid?.lon;
  if (typeof lat === "number" && typeof lon === "number") return [lon, lat];
  const lat2 = Number(lat);
  const lon2 = Number(lon);
  if (Number.isFinite(lat2) && Number.isFinite(lon2)) return [lon2, lat2];
  return null;
}

async function main(){
  const args = parseArgs(process.argv);
  const root = args.root ? String(args.root) : process.cwd();
  const limitCity = args.city ? String(args.city).toLowerCase() : null;

  let auditDir = args.auditDir ? String(args.auditDir) : null;
  if (!auditDir){
    const ptr = path.join(root, "publicData", "_audit", "phaseZO_overlay_scan", "CURRENT_RUN.txt");
    if (!(await exists(ptr))){
      throw new Error(`No --auditDir provided and CURRENT_RUN.txt not found: ${ptr}`);
    }
    auditDir = (await fsp.readFile(ptr, "utf8")).trim();
  }

  const manifestPath = args.manifest
    ? String(args.manifest)
    : path.join(auditDir, "PHASE_ZO__approved_layers_AUTO.json");

  if (!(await exists(manifestPath))){
    throw new Error(`Missing manifest: ${manifestPath}`);
  }

  const runTs = utcStamp();
  const auditOutBase = path.join(root, "publicData", "_audit", "phaseZO_normalize_attach_freeze");
  const auditOut = path.join(auditOutBase, runTs);
  await ensureDir(auditOut);
  await ensureDir(auditOutBase);
  await fsp.writeFile(path.join(auditOutBase, "CURRENT_RUN.txt"), auditOut, "utf8");

  console.log("====================================================");
  console.log(" Phase ZO — Normalize + Attach + Freeze (v2)");
  console.log(` Root:       ${root}`);
  console.log(` AuditIn:    ${auditDir}`);
  console.log(` Manifest:   ${manifestPath}`);
  console.log(` AuditOut:   ${auditOut}`);
  if (limitCity) console.log(` CityOnly:   ${limitCity}`);
  console.log("====================================================");

  const manifestRaw = JSON.parse(await fsp.readFile(manifestPath, "utf8"));
  const cityMap = manifestRaw?.cities || manifestRaw;
  const cities = Object.keys(cityMap || {}).filter(c => !String(c).startsWith("_"));
  if (!cities.length) throw new Error("Manifest has no cities.");

  const approvedEntries = [];
  for (const city of cities){
    if (limitCity && city.toLowerCase() !== limitCity) continue;
    const arr = cityMap[city] || [];
    for (const e of arr){
      if (e && e.approved === true){
        approvedEntries.push({
          city: String(e.city || city).toLowerCase(),
          layer_key: String(e.layer_key || ""),
          rel_path: String(e.rel_path || ""),
          source_subdir: String(e.source_subdir || "overlays"), // NEW
          score: e.score ?? null,
          reason: e.reason ?? "",
          bucket: e.bucket ?? ""
        });
      }
    }
  }

  if (approvedEntries.length === 0){
    warn("No approved=true entries found. Nothing to do.");
  }

  const finalManifest = {
    meta: {
      created_utc: new Date().toISOString(),
      source_manifest: manifestPath,
      audit_in: auditDir,
      audit_out: auditOut,
      script: "phaseZO_normalizeAttachFreeze_v2",
      notes: [
        "FINAL manifest contains only approved overlays.",
        "Frozen artifacts written under overlays/_phaseZO_frozen/<layer_key>/ regardless of source_subdir."
      ]
    },
    approved: approvedEntries
  };

  const finalJson = path.join(auditOut, "PHASE_ZO__approved_layers_FINAL.json");
  const finalCsv = path.join(auditOut, "PHASE_ZO__approved_layers_FINAL.csv");
  await fsp.writeFile(finalJson, JSON.stringify(finalManifest, null, 2), "utf8");
  await fsp.writeFile(finalCsv, toCsv(approvedEntries, ["city","layer_key","source_subdir","rel_path","bucket","score","reason"]), "utf8");
  done(`FINAL manifest: ${finalJson}`);

  const freezeReport = [];
  const frozenIndex = [];

  for (const e of approvedEntries){
    const city = e.city;
    const layerKey = e.layer_key;

    const sourceSubdir = (e.source_subdir === "districts") ? "districts" : "overlays";
    const sourceRoot = path.join(root, "publicData", "zoning", city, sourceSubdir);
    const inPath = path.join(sourceRoot, e.rel_path);

    const overlaysRoot = path.join(root, "publicData", "zoning", city, "overlays");

    const rowBase = {
      city,
      layer_key: layerKey,
      source_subdir: sourceSubdir,
      rel_path: e.rel_path,
      input_path: inPath,
      input_exists: false,
      input_ext: path.extname(inPath).toLowerCase(),
      processed: false,
      reason: "",
      feature_count: "",
      coord_sanity: "",
      input_sha256: "",
      frozen_geojson: "",
      frozen_sha256: ""
    };

    if (!(await exists(inPath))){
      freezeReport.push({ ...rowBase, reason: "MISSING_INPUT_FILE" });
      warn(`Missing overlay file: ${inPath}`);
      continue;
    }
    rowBase.input_exists = true;

    if (!isGeoJSONPath(inPath)){
      freezeReport.push({ ...rowBase, reason: "NEEDS_CONVERT_NOT_GEOJSON" });
      warn(`NEEDS_CONVERT (not GeoJSON): ${inPath}`);
      continue;
    }

    let gj;
    try { gj = JSON.parse(await fsp.readFile(inPath, "utf8")); }
    catch {
      freezeReport.push({ ...rowBase, reason: "INVALID_JSON" });
      warn(`INVALID_JSON: ${inPath}`);
      continue;
    }

    if (!gj || gj.type !== "FeatureCollection" || !Array.isArray(gj.features)){
      freezeReport.push({ ...rowBase, reason: "NOT_FEATURECOLLECTION" });
      warn(`NOT_FEATURECOLLECTION: ${inPath}`);
      continue;
    }

    const inputSha = await sha256File(inPath);

    const b = bboxInit();
    let coordOk = 0, coordBad = 0;
    const geomCounts = {};
    const featuresOut = gj.features.map((f, idx) => {
      const g = f?.geometry || null;
      const t = geomType(g);
      geomCounts[t] = (geomCounts[t] || 0) + 1;

      const fid = sha1Hex(`${layerKey}:${idx}:${JSON.stringify(g)}`).slice(0, 16);

      walkCoords(g, (lon, lat) => {
        bboxUpdate(b, lon, lat);
        if (coordInMABounds(lon, lat)) coordOk++; else coordBad++;
      });

      const props = (f?.properties && typeof f.properties === "object") ? f.properties : {};
      return {
        type: "Feature",
        geometry: g,
        properties: {
          ...props,
          __el: { feature_id: fid, layer_key: layerKey, city }
        }
      };
    });

    const bboxArr = bboxToArray(b);
    const coordSanity =
      (coordBad === 0 && coordOk > 0) ? "OK_MA_BOUNDS" :
      (coordOk === 0 && coordBad > 0) ? "CRS_SUSPECT_NOT_WGS84" :
      "MIXED_COORDS_CHECK";

    // Always freeze into overlays/_phaseZO_frozen
    const freezeDir = path.join(overlaysRoot, "_phaseZO_frozen", layerKey);
    await ensureDir(freezeDir);

    const outGeo = path.join(freezeDir, "layer.geojson");
    const outManifest = path.join(freezeDir, "MANIFEST.json");

    await fsp.writeFile(outGeo, JSON.stringify({ type: "FeatureCollection", name: layerKey, features: featuresOut }, null, 2), "utf8");
    const frozenSha = await sha256File(outGeo);

    const layerManifest = {
      created_utc: new Date().toISOString(),
      city,
      layer_key: layerKey,
      source_subdir: sourceSubdir,
      input: { path: inPath, sha256: inputSha },
      output: { path: outGeo, sha256: frozenSha },
      stats: {
        feature_count: featuresOut.length,
        geom_type_counts: geomCounts,
        bbox_wgs84: bboxArr,
        coord_sanity: coordSanity,
        coord_ok_samples: coordOk,
        coord_bad_samples: coordBad
      }
    };

    await fsp.writeFile(outManifest, JSON.stringify(layerManifest, null, 2), "utf8");

    freezeReport.push({
      ...rowBase,
      processed: true,
      reason: "FROZEN_OK",
      feature_count: String(featuresOut.length),
      coord_sanity: coordSanity,
      input_sha256: inputSha,
      frozen_geojson: outGeo,
      frozen_sha256: frozenSha
    });

    frozenIndex.push({
      city,
      layer_key: layerKey,
      bbox_wgs84: bboxArr,
      frozen_geojson: outGeo,
      frozen_sha256: frozenSha,
      coord_sanity: coordSanity
    });

    done(`FROZEN: ${city} :: ${layerKey} (features=${featuresOut.length}, sanity=${coordSanity}, source=${sourceSubdir})`);
  }

  const reportCsv = path.join(auditOut, "PHASE_ZO__FREEZE_REPORT.csv");
  await fsp.writeFile(
    reportCsv,
    toCsv(freezeReport, ["city","layer_key","source_subdir","rel_path","input_path","input_exists","input_ext","processed","reason","feature_count","coord_sanity","input_sha256","frozen_geojson","frozen_sha256"]),
    "utf8"
  );
  done(`Freeze report: ${reportCsv}`);

  // Attach step (optional)
  const propertiesPath = args.properties ? String(args.properties) : null;
  if (propertiesPath){
    if (!(await exists(propertiesPath))) throw new Error(`--properties path not found: ${propertiesPath}`);
    info(`ATTACH enabled. Properties: ${propertiesPath}`);

    const layers = [];
    for (const li of frozenIndex){
      if (li.coord_sanity === "CRS_SUSPECT_NOT_WGS84"){
        warn(`Skipping CRS suspect layer: ${li.city} ${li.layer_key}`);
        continue;
      }
      const gj = JSON.parse(await fsp.readFile(li.frozen_geojson, "utf8"));
      const feats = (gj?.features || []).filter(f => {
        const t = f?.geometry?.type;
        return t === "Polygon" || t === "MultiPolygon";
      });
      const featBoxes = feats.map(f => {
        const bb = bboxInit();
        walkCoords(f.geometry, (lon, lat)=>bboxUpdate(bb, lon, lat));
        return { feature: f, bbox: bb };
      });
      layers.push({ city: li.city, layer_key: li.layer_key, dataset_hash: li.frozen_sha256, features: featBoxes });
    }

    const attachOut = path.join(auditOut, "PHASE_ZO__attachments.ndjson");
    const attachStream = fs.createWriteStream(attachOut, { encoding: "utf8" });

    let seen = 0, matched = 0;

    const rl = readline.createInterface({
      input: fs.createReadStream(propertiesPath, { encoding: "utf8" }),
      crlfDelay: Infinity
    });

    for await (const line of rl){
      const s = line.trim();
      if (!s) continue;
      let obj;
      try { obj = JSON.parse(s); } catch { continue; }

      const propCity = String(pickCityFromProperty(obj) || "").toLowerCase();
      if (limitCity && propCity !== limitCity) continue;

      const pt = pickPointFromProperty(obj);
      if (!pt) continue;

      const [lon, lat] = pt;
      if (!coordInMABounds(lon, lat)) continue;

      const property_id = obj.property_id || obj.propertyId || obj.pid || null;
      if (!property_id) continue;

      seen++;

      for (const layer of layers){
        if (layer.city !== propCity) continue;
        for (const fb of layer.features){
          if (!bboxContains(fb.bbox, lon, lat)) continue;
          if (pointInGeometry([lon, lat], fb.feature.geometry)){
            const el = fb.feature?.properties?.__el || {};
            const feature_id = el.feature_id || null;

            attachStream.write(JSON.stringify({
              property_id,
              feature_id,
              layer_key: layer.layer_key,
              attach_method: "pip",
              distance_m: null,
              attach_confidence: "A",
              attach_as_of_date: new Date().toISOString().slice(0,10),
              dataset_hash: layer.dataset_hash
            }) + "\n");
            matched++;
          }
        }
      }

      if (seen % 50000 === 0){
        info(`ATTACH progress: properties_seen=${seen}, matches=${matched}`);
      }
    }

    attachStream.end();
    done(`Attachments written: ${attachOut}`);
    info(`ATTACH summary: properties_seen=${seen}, matches=${matched}`);
  } else {
    info("ATTACH skipped (no --properties provided).");
  }

  await fsp.writeFile(path.join(auditOut, "MANIFEST.json"), JSON.stringify({
    created_utc: new Date().toISOString(),
    audit_in: auditDir,
    audit_out: auditOut,
    used_manifest: manifestPath,
    final_manifest: finalJson,
    freeze_report: reportCsv
  }, null, 2), "utf8");

  console.log("====================================================");
  done("Phase ZO Normalize+Freeze complete.");
  console.log("Audit output:");
  console.log(`  ${auditOut}`);
  console.log("====================================================");
}

main().catch(e => {
  console.error("[fatal]", e?.stack || e);
  process.exit(1);
});
