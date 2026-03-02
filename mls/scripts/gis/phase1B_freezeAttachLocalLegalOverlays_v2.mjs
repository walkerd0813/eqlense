#!/usr/bin/env node
/**
 * phase1B_freezeAttachLocalLegalOverlays_v2.mjs
 *
 * Phase 1B — Local Legal Patch Overlays (historic/preservation/landmark/etc.)
 *
 * This version writes REQUIRED overlay headers into a feature catalog (NDJSON) and enforces
 * an end-of-run header verification report.
 *
 * Outputs (per frozen layer):
 *   publicData/zoning/<city>/historic/_phase1B_frozen/<layer_key>/
 *     - layer.geojson                  (frozen geometry + original props + __el)
 *     - FEATURE_CATALOG.ndjson         (rows with required headers incl geometry/bbox/centroid)
 *     - MANIFEST.json                  (evidence)
 *
 * Outputs (audit run):
 *   publicData/_audit/phase1B_local_legal_freeze/<timestamp>/
 *     - PHASE1B__FREEZE_REPORT.csv
 *     - PHASE1B__MISSING_OR_DISABLED.csv
 *     - PHASE1B__FEATURE_CATALOG.ndjson
 *     - PHASE1B__VERIFY_HEADERS_REPORT.json
 *     - MANIFEST.json
 *
 * Optional attach:
 *   --properties "...\publicData\properties\PROPERTY_SPINE.ndjson"
 * produces:
 *   - PHASE1B__attachments.ndjson  (with required attach headers)
 *   - PHASE1B__ATTACH_VERIFY_HEADERS_REPORT.json
 *
 * Usage:
 *   node .\mls\scripts\gis\phase1B_freezeAttachLocalLegalOverlays_v2.mjs
 *
 * Options:
 *   --manifest "...\PHASE1B_localLegal_manifest_v2.json"
 *   --root "C:\seller-app\backend"
 *   --city "boston"            (limit city)
 *   --properties "...\properties.ndjson"
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

function sha1Hex(s){ return crypto.createHash("sha1").update(s).digest("hex"); }

async function sha256File(filePath){
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash("sha256");
    const s = fs.createReadStream(filePath);
    s.on("data", (d)=>hash.update(d));
    s.on("error", reject);
    s.on("end", ()=>resolve(hash.digest("hex")));
  });
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

function featureTypeFromGeometryType(t){
  if (t === "Point" || t === "MultiPoint") return "point";
  if (t === "LineString" || t === "MultiLineString") return "line";
  if (t === "Polygon" || t === "MultiPolygon") return "polygon";
  return "unknown";
}

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
  for (const r of rows) out.push(headers.map(h => esc(r[h])).join(","));
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

function findSourceObjectId(props){
  if (!props || typeof props !== "object") return "";
  const keys = [
    "source_object_id","SOURCE_OBJECT_ID",
    "OBJECTID","ObjectID","objectid",
    "OBJECT_ID","object_id",
    "FID","fid",
    "ID","Id","id",
    "GLOBALID","GlobalID","globalid",
    "GUID","guid"
  ];
  for (const k of keys){
    if (props[k] !== undefined && props[k] !== null && props[k] !== "") return String(props[k]);
  }
  return "";
}

function pickName(props, fallback){
  if (!props || typeof props !== "object") return fallback;
  const keys = [
    "name","Name","NAME",
    "district","District","DISTRICT",
    "designation","Designation",
    "hd_name","HD_NAME","historic_district",
    "overlay","Overlay"
  ];
  for (const k of keys){
    if (props[k] !== undefined && props[k] !== null && String(props[k]).trim() !== "") return String(props[k]).trim();
  }
  return fallback;
}

function makeLayerKey(city, relPath, keyHint){
  const baseHint = (keyHint && String(keyHint).trim().length > 0)
    ? String(keyHint).toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "")
    : path.basename(relPath).toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  const h = sha1Hex(`${city}:${relPath}`).slice(0, 10);
  return `${baseHint}__${h}`;
}

function verifyHeaders(rows, required){
  const missingCounts = {};
  for (const k of required) missingCounts[k] = 0;

  const sampleN = Math.min(rows.length, 200); // light scan
  for (let i=0;i<sampleN;i++){
    const r = rows[i];
    for (const k of required){
      if (!(k in r)) missingCounts[k] += 1;
    }
  }
  const missing = Object.entries(missingCounts)
    .filter(([,c]) => c > 0)
    .map(([k,c]) => ({ header: k, missing_in_sample: c, sample_size: sampleN }));

  return {
    ok: missing.length === 0,
    sample_size: sampleN,
    missing
  };
}

async function main(){
  const args = parseArgs(process.argv);
  const root = args.root ? String(args.root) : process.cwd();
  const limitCity = args.city ? String(args.city).toLowerCase() : null;

  const manifestPath = args.manifest
    ? String(args.manifest)
    : path.join(root, "mls", "scripts", "gis", "PHASE1B_localLegal_manifest_v2.json");

  if (!(await exists(manifestPath))){
    throw new Error(`Missing manifest: ${manifestPath}`);
  }

  const runTs = utcStamp();
  const auditBase = path.join(root, "publicData", "_audit", "phase1B_local_legal_freeze");
  const auditOut = path.join(auditBase, runTs);
  await ensureDir(auditOut);
  await ensureDir(auditBase);
  await fsp.writeFile(path.join(auditBase, "CURRENT_RUN.txt"), auditOut, "utf8");

  console.log("====================================================");
  console.log(" Phase 1B — Local Legal Patch Overlays (v2)");
  console.log(` Root:      ${root}`);
  console.log(` Manifest:  ${manifestPath}`);
  console.log(` AuditOut:  ${auditOut}`);
  if (limitCity) console.log(` CityOnly:  ${limitCity}`);
  console.log("====================================================");

  const manifest = JSON.parse(await fsp.readFile(manifestPath, "utf8"));
  const meta = manifest?.meta || {};
  const layers = Array.isArray(manifest.layers) ? manifest.layers : [];

  const reqOverlayHeaders = meta?.schema_contract?.overlay_feature_required_headers || [];
  const reqAttachHeaders = meta?.schema_contract?.attachment_required_headers || [];

  const report = [];
  const missingOrDisabled = [];

  const globalFeatureCatalog = [];
  const globalFeatureCatalogNdjsonPath = path.join(auditOut, "PHASE1B__FEATURE_CATALOG.ndjson");
  const globalCatalogStream = fs.createWriteStream(globalFeatureCatalogNdjsonPath, { encoding: "utf8" });

  const perLayerCatalogIndex = [];

  for (const L of layers){
    const city = String(L.city || "").toLowerCase().trim();
    if (!city) continue;
    if (limitCity && city !== limitCity) continue;

    const enabled = !!L.enabled;
    const relPath = String(L.rel_path || "").trim();

    if (!enabled || !relPath){
      missingOrDisabled.push({
        city,
        label: String(L.source_layer_name || L.layer_key_hint || ""),
        rel_path: relPath,
        status: !enabled ? "DISABLED" : "MISSING_PATH"
      });
      continue;
    }

    const inPath = path.isAbsolute(relPath) ? relPath : path.join(root, relPath);
    const layerKey = makeLayerKey(city, relPath.replaceAll("\\", "/"), L.layer_key_hint);
    const jurisdictionType = String(L.jurisdiction_type || meta?.defaults?.jurisdiction_type || "city");
    const jurisdictionName = String(L.jurisdiction_name || city);

    const asOfDate = String(L.as_of_date || meta?.defaults?.as_of_date || new Date().toISOString().slice(0,10));
    const datasetVersion = runTs; // run version
    const sourceSystem = String(L.source_system || "unknown");
    const sourceUrl = String(L.source_url || "");
    const sourceLayerName = String(L.source_layer_name || "");
    const sourceLayerId = String(L.source_layer_id || "");

    const historicClass = String(L.historic_class || "district");
    const historicAuthority = String(L.historic_authority || "");
    const historicStrength = String(L.historic_strength || "hard");

    const designationType = String(L.designation_type || "");
    const designationStatusDefault = String(L.designation_status || "");
    const designationDateDefault = String(L.designation_date || "");
    const notesRawDefault = String(L.notes_raw || "");

    const rowBase = {
      city,
      layer_key: layerKey,
      rel_path: relPath,
      input_path: inPath,
      input_exists: false,
      processed: false,
      reason: "",
      feature_count: "",
      coord_sanity: "",
      input_sha256: "",
      frozen_geojson: "",
      frozen_sha256: "",
      layer_catalog_ndjson: ""
    };

    if (!(await exists(inPath))){
      report.push({ ...rowBase, reason: "MISSING_INPUT_FILE" });
      warn(`Missing: ${inPath}`);
      continue;
    }
    rowBase.input_exists = true;

    let gj;
    try { gj = JSON.parse(await fsp.readFile(inPath, "utf8")); }
    catch {
      report.push({ ...rowBase, reason: "INVALID_JSON" });
      warn(`INVALID_JSON: ${inPath}`);
      continue;
    }

    if (!gj || gj.type !== "FeatureCollection" || !Array.isArray(gj.features)){
      report.push({ ...rowBase, reason: "NOT_FEATURECOLLECTION" });
      warn(`NOT_FEATURECOLLECTION: ${inPath}`);
      continue;
    }

    const inputSha = await sha256File(inPath);

    // Build frozen features + feature catalog
    const bAll = bboxInit();
    let coordOk = 0, coordBad = 0;

    const frozenFeatures = [];
    const layerCatalog = [];

    for (let idx=0; idx<gj.features.length; idx++){
      const f = gj.features[idx];
      const g = f?.geometry || null;
      const t = geomType(g);
      const ft = featureTypeFromGeometryType(t);

      // feature bbox
      const b = bboxInit();
      walkCoords(g, (lon, lat) => {
        bboxUpdate(b, lon, lat);
        bboxUpdate(bAll, lon, lat);
        if (coordInMABounds(lon, lat)) coordOk++; else coordBad++;
      });
      const bb = bboxToArray(b);

      const props = (f?.properties && typeof f.properties === "object") ? f.properties : {};
      const sourceObjectId = findSourceObjectId(props);
      const fallbackName = `${sourceLayerName || layerKey} #${idx+1}`;
      const nm = pickName(props, fallbackName);

      const fidSeed = `${layerKey}:${sourceObjectId || idx}:${JSON.stringify(g)}`;
      const featureId = sha1Hex(fidSeed).slice(0, 16);

      const bboxCenterLon = bb ? (bb[0] + bb[2]) / 2 : null;
      const bboxCenterLat = bb ? (bb[1] + bb[3]) / 2 : null;

      // We store required headers in catalog rows; frozen GeoJSON keeps originals + __el
      frozenFeatures.push({
        type: "Feature",
        geometry: g,
        properties: {
          ...props,
          __el: {
            feature_id: featureId,
            layer_key: layerKey,
            phase: "1B",
            historic_class: historicClass
          }
        }
      });

      const confidenceGrade =
        (coordBad === 0 && coordOk > 0) ? (meta?.defaults?.confidence_grade_by_sanity?.OK_MA_BOUNDS || "A") :
        (coordOk === 0 && coordBad > 0) ? (meta?.defaults?.confidence_grade_by_sanity?.CRS_SUSPECT_NOT_WGS84 || "C") :
        (meta?.defaults?.confidence_grade_by_sanity?.MIXED_COORDS_CHECK || "B");

      // designation fields (best-effort from props)
      const designationId =
        String(props.designation_id ?? props.DESIGNATION_ID ?? props.hd_id ?? props.HD_ID ?? sourceObjectId ?? "");
      const designationName =
        String(props.designation_name ?? props.DESIGNATION_NAME ?? props.hd_name ?? props.HD_NAME ?? nm ?? "");
      const designationStatus =
        String(props.designation_status ?? props.DESIGNATION_STATUS ?? designationStatusDefault ?? "");
      const designationDate =
        String(props.designation_date ?? props.DESIGNATION_DATE ?? designationDateDefault ?? "");

      const notesRaw =
        String(props.notes_raw ?? props.NOTES ?? props.notes ?? notesRawDefault ?? "");

      const catalogRow = {
        // universal required
        feature_id: featureId,
        layer_key: layerKey,
        feature_type: ft,
        name: nm,
        jurisdiction_type: jurisdictionType,
        jurisdiction_name: jurisdictionName,
        source_system: sourceSystem,
        source_url: sourceUrl,
        source_layer_name: sourceLayerName,
        source_layer_id: sourceLayerId,
        source_object_id: sourceObjectId,
        as_of_date: asOfDate,
        dataset_version: datasetVersion,
        dataset_hash: "", // filled after frozen write
        geometry: g,      // required header (NDJSON OK)
        bbox: bb,
        centroid_lat: bboxCenterLat,
        centroid_lon: bboxCenterLon,
        confidence_grade: confidenceGrade,

        // historic-specific required
        historic_class: historicClass,
        historic_authority: historicAuthority,
        historic_strength: historicStrength,
        designation_id: designationId,
        designation_name: designationName,
        designation_type: designationType,
        designation_status: designationStatus,
        designation_date: designationDate,
        notes_raw: notesRaw
      };

      layerCatalog.push(catalogRow);
    }

    const bboxAllArr = bboxToArray(bAll);
    const coordSanity =
      (coordBad === 0 && coordOk > 0) ? "OK_MA_BOUNDS" :
      (coordOk === 0 && coordBad > 0) ? "CRS_SUSPECT_NOT_WGS84" :
      "MIXED_COORDS_CHECK";

    // Freeze dir
    const freezeDir = path.join(root, "publicData", "zoning", city, "historic", "_phase1B_frozen", layerKey);
    await ensureDir(freezeDir);

    const outGeo = path.join(freezeDir, "layer.geojson");
    const outCatalog = path.join(freezeDir, "FEATURE_CATALOG.ndjson");
    const outMan = path.join(freezeDir, "MANIFEST.json");

    await fsp.writeFile(outGeo, JSON.stringify({ type: "FeatureCollection", name: layerKey, features: frozenFeatures }, null, 2), "utf8");
    const frozenSha = await sha256File(outGeo);

    // fill dataset_hash and write catalogs
    const layerCatalogFilled = layerCatalog.map(r => ({ ...r, dataset_hash: frozenSha }));
    const layerCatalogStream = fs.createWriteStream(outCatalog, { encoding: "utf8" });
    for (const r of layerCatalogFilled){
      layerCatalogStream.write(JSON.stringify(r) + "\n");
      globalCatalogStream.write(JSON.stringify(r) + "\n");
      globalFeatureCatalog.push(r);
    }
    layerCatalogStream.end();

    await fsp.writeFile(outMan, JSON.stringify({
      created_utc: new Date().toISOString(),
      phase: "1B",
      city,
      layer_key: layerKey,
      input: { path: inPath, sha256: inputSha },
      output: { path: outGeo, sha256: frozenSha },
      feature_catalog: { path: outCatalog, row_count: layerCatalogFilled.length },
      stats: {
        feature_count: frozenFeatures.length,
        bbox_wgs84: bboxAllArr,
        coord_sanity: coordSanity,
        coord_ok_samples: coordOk,
        coord_bad_samples: coordBad
      },
      schema: {
        required_overlay_headers: reqOverlayHeaders,
        required_attach_headers: reqAttachHeaders
      },
      layer_meta: {
        jurisdiction_type: jurisdictionType,
        jurisdiction_name: jurisdictionName,
        historic_class: historicClass,
        historic_strength: historicStrength,
        historic_authority: historicAuthority,
        designation_type: designationType,
        source_system: sourceSystem,
        source_url: sourceUrl,
        source_layer_name: sourceLayerName,
        source_layer_id: sourceLayerId,
        as_of_date: asOfDate,
        dataset_version: datasetVersion
      },
      compliance_notes: meta?.schema_contract?.locked_rules || []
    }, null, 2), "utf8");

    report.push({
      ...rowBase,
      processed: true,
      reason: "FROZEN_OK",
      feature_count: String(frozenFeatures.length),
      coord_sanity: coordSanity,
      input_sha256: inputSha,
      frozen_geojson: outGeo,
      frozen_sha256: frozenSha,
      layer_catalog_ndjson: outCatalog
    });

    perLayerCatalogIndex.push({
      city, layer_key: layerKey, frozen_geojson: outGeo, dataset_hash: frozenSha,
      feature_catalog_ndjson: outCatalog, feature_count: frozenFeatures.length, coord_sanity: coordSanity
    });

    done(`FROZEN 1B: ${city} :: ${layerKey} (features=${frozenFeatures.length}, sanity=${coordSanity})`);
  }

  globalCatalogStream.end();

  // Audit reports
  const reportCsv = path.join(auditOut, "PHASE1B__FREEZE_REPORT.csv");
  const missingCsv = path.join(auditOut, "PHASE1B__MISSING_OR_DISABLED.csv");
  const catalogIndexJson = path.join(auditOut, "PHASE1B__LAYER_CATALOG_INDEX.json");

  await fsp.writeFile(reportCsv, toCsv(report, ["city","layer_key","rel_path","input_path","input_exists","processed","reason","feature_count","coord_sanity","input_sha256","frozen_geojson","frozen_sha256","layer_catalog_ndjson"]), "utf8");
  await fsp.writeFile(missingCsv, toCsv(missingOrDisabled, ["city","label","rel_path","status"]), "utf8");
  await fsp.writeFile(catalogIndexJson, JSON.stringify(perLayerCatalogIndex, null, 2), "utf8");

  done(`Freeze report: ${reportCsv}`);
  done(`Feature catalog NDJSON: ${globalFeatureCatalogNdjsonPath}`);

  // Verify headers (feature catalog)
  const overlayVerify = verifyHeaders(globalFeatureCatalog, reqOverlayHeaders);
  const verifyReport = {
    phase: "1B",
    kind: "overlay_feature_catalog",
    ok: overlayVerify.ok,
    ...overlayVerify,
    required_headers: reqOverlayHeaders,
    audit_out: auditOut
  };
  const verifyPath = path.join(auditOut, "PHASE1B__VERIFY_HEADERS_REPORT.json");
  await fsp.writeFile(verifyPath, JSON.stringify(verifyReport, null, 2), "utf8");

  if (overlayVerify.ok) done(`VERIFY overlay headers: OK (sample=${overlayVerify.sample_size})`);
  else warn(`VERIFY overlay headers: FAIL (see ${verifyPath})`);

  // Optional attach
  const propertiesPath = args.properties ? String(args.properties) : null;
  if (propertiesPath){
    if (!(await exists(propertiesPath))){
      throw new Error(`--properties not found: ${propertiesPath}`);
    }
    info(`ATTACH enabled. Properties: ${propertiesPath}`);

    // Load per-city layers into memory (polygonal features only), with bbox filter
    const layersByCity = new Map();
    for (const li of perLayerCatalogIndex){
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
        const el = f?.properties?.__el || {};
        return { feature: f, bbox: bb, feature_id: el.feature_id || "" };
      });

      const arr = layersByCity.get(li.city) || [];
      arr.push({
        city: li.city,
        layer_key: li.layer_key,
        dataset_hash: li.dataset_hash,
        features: featBoxes
      });
      layersByCity.set(li.city, arr);
    }

    const attachOut = path.join(auditOut, "PHASE1B__attachments.ndjson");
    const outStream = fs.createWriteStream(attachOut, { encoding: "utf8" });

    const attachRowsForVerify = [];
    let seen = 0, matches = 0;

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
      if (!propCity) continue;
      if (limitCity && propCity !== limitCity) continue;

      const cityLayers = layersByCity.get(propCity);
      if (!cityLayers || cityLayers.length === 0) continue;

      const pt = pickPointFromProperty(obj);
      if (!pt) continue;
      const [lon, lat] = pt;
      if (!coordInMABounds(lon, lat)) continue;

      const property_id = obj.property_id || obj.propertyId || obj.pid || null;
      if (!property_id) continue;

      seen++;

      for (const layer of cityLayers){
        for (const fb of layer.features){
          if (!bboxContains(fb.bbox, lon, lat)) continue;
          if (pointInGeometry([lon, lat], fb.feature.geometry)){
            const row = {
              property_id,
              feature_id: fb.feature_id || null,
              attach_method: "pip",
              distance_m: null,
              attach_confidence: "A",
              attach_as_of_date: new Date().toISOString().slice(0,10),

              // extra helpful fields (not required, but safe)
              layer_key: layer.layer_key,
              dataset_hash: layer.dataset_hash,
              phase: "1B"
            };
            outStream.write(JSON.stringify(row) + "\n");
            matches++;

            if (attachRowsForVerify.length < 300) attachRowsForVerify.push(row);
          }
        }
      }

      if (seen % 50000 === 0){
        info(`ATTACH progress: properties_seen=${seen}, matches=${matches}`);
      }
    }

    outStream.end();
    done(`Attachments written: ${attachOut}`);
    info(`ATTACH summary: properties_seen=${seen}, matches=${matches}`);

    const attachVerify = verifyHeaders(attachRowsForVerify, reqAttachHeaders);
    const attachVerifyReport = {
      phase: "1B",
      kind: "property_feature_attachments",
      ok: attachVerify.ok,
      ...attachVerify,
      required_headers: reqAttachHeaders,
      attach_out: attachOut
    };
    const attachVerifyPath = path.join(auditOut, "PHASE1B__ATTACH_VERIFY_HEADERS_REPORT.json");
    await fsp.writeFile(attachVerifyPath, JSON.stringify(attachVerifyReport, null, 2), "utf8");
    if (attachVerify.ok) done(`VERIFY attach headers: OK (sample=${attachVerify.sample_size})`);
    else warn(`VERIFY attach headers: FAIL (see ${attachVerifyPath})`);
  } else {
    info("ATTACH skipped (no --properties provided).");
  }

  await fsp.writeFile(path.join(auditOut, "MANIFEST.json"), JSON.stringify({
    created_utc: new Date().toISOString(),
    phase: "1B",
    root,
    manifest: manifestPath,
    audit_out: auditOut,
    outputs: {
      reportCsv,
      missingCsv,
      featureCatalogNdjson: globalFeatureCatalogNdjsonPath,
      verifyReport: "PHASE1B__VERIFY_HEADERS_REPORT.json"
    },
    locked_rules: meta?.schema_contract?.locked_rules || []
  }, null, 2), "utf8");

  console.log("====================================================");
  done("Phase 1B v2 complete.");
  console.log("Audit output:");
  console.log(`  ${auditOut}`);
  console.log("Review:");
  console.log("  PHASE1B__FREEZE_REPORT.csv");
  console.log("  PHASE1B__VERIFY_HEADERS_REPORT.json");
  console.log("====================================================");
}

main().catch(e => {
  console.error("[fatal]", e?.stack || e);
  process.exit(1);
});
