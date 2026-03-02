import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";

import { arcgisLayerToGeoJSON } from "./lib/arcgis_to_geojson.mjs";
import { convertShpToGeoJSON } from "./lib/shp_convert.mjs";
import {
  ensureDirSync,
  fileExists,
  formatDateStamp,
  guessCityField,
  inferPropertiesPathFromContract,
  normalizeCityName,
  nowIso,
  safeReadJsonSync,
  safeWriteJsonSync,
  sha256FileSync,
  sha256String,
  tryExtractLatLon,
  withinMABbox,
  detectContractPointerFiles,
  walkFindFilesSync,
  findLatestPropertiesNdjson,
  backupFileSync
} from "./lib/utils.mjs";

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
      args[k] = v;
    }
  }
  return args;
}

function logHeader() {
  console.log("====================================================");
  console.log("PHASE 2 â€” CITY CIVICS FREEZE + ATTACH (ALL CITIES) v1");
  console.log("====================================================");
}

function resolveRoot(args) {
  const root = args.root ? path.resolve(String(args.root)) : process.cwd();
  if (!fs.existsSync(root)) throw new Error(`Root not found: ${root}`);
  return root;
}

function loadConfig(root) {
  const cfgPath = path.join(root, "scripts", "packs", "phase2_city_civics_pack_v1", "data", "phase2_city_layers_v1.json");
  if (!fs.existsSync(cfgPath)) throw new Error(`Missing config: ${cfgPath}`);
  return safeReadJsonSync(cfgPath);
}

function findCurrentContractFile(root) {
  const roots = detectContractPointerFiles(root);
  const matches = [];
  for (const r of roots) {
    const found = walkFindFilesSync(r, (p) => /CURRENT_CONTRACT_VIEW_MA/i.test(path.basename(p)) && p.toLowerCase().endsWith(".json"), 6);
    matches.push(...found);
  }
  // prefer exact filename
  matches.sort((a,b) => {
    const an = path.basename(a).toLowerCase();
    const bn = path.basename(b).toLowerCase();
    const ae = an === "current_contract_view_ma.json" ? 0 : 1;
    const be = bn === "current_contract_view_ma.json" ? 0 : 1;
    return ae - be || b.length - a.length;
  });
  return matches[0] || null;
}

function chooseOutDirs(root) {
  return {
    frozenDir: path.join(root, "publicData", "overlays", "_frozen"),
    attachDir: path.join(root, "publicData", "overlays", "_attachments"),
    auditDir: path.join(root, "publicData", "_audit", "phase2_city_civics"),
    contractsDir: path.join(root, "publicData", "_contracts")
  };
}

function ensureDependenciesOrExplain() {
  // Turf is expected to exist in repo dependencies; load lazily.
}

async function readGeoJSON(filePath) {
  const raw = await fsp.readFile(filePath, "utf-8");
  const gj = JSON.parse(raw);
  if (!gj || gj.type !== "FeatureCollection" || !Array.isArray(gj.features)) {
    throw new Error(`Invalid GeoJSON FeatureCollection: ${filePath}`);
  }
  return gj;
}

function inferFeatureType(geom) {
  if (!geom || !geom.type) return "unknown";
  const t = geom.type;
  if (t === "Polygon" || t === "MultiPolygon") return "polygon";
  if (t === "Point" || t === "MultiPoint") return "point";
  if (t === "LineString" || t === "MultiLineString") return "line";
  return "unknown";
}

function bboxOfFeature(turf, feat) {
  try { return turf.bbox(feat); } catch { return null; }
}

function bboxContainsPoint(bbox, lon, lat) {
  if (!bbox) return true;
  const [minX, minY, maxX, maxY] = bbox;
  return lon >= minX && lon <= maxX && lat >= minY && lat <= maxY;
}

async function freezeLayer({ root, outDirs, layer, geojsonPath, datasetHash, runDateISO }) {
  const cityNorm = normalizeCityName(layer.city || layer.jurisdiction || "massachusetts");
  const key = layer.layer_key;
  const stamp = formatDateStamp();
  const outBase = `civic_city__${cityNorm}__${key}__${layersVersion()}__${stamp}`;
  const outGeo = path.join(outDirs.frozenDir, `${outBase}.geojson`);
  const outMeta = path.join(outDirs.frozenDir, `${outBase}_meta.json`);

  const gj = await readGeoJSON(geojsonPath);

  // add minimal metadata props (non-destructive)
  const fc = {
    type: "FeatureCollection",
    features: gj.features.map((f, idx) => {
      const ft = inferFeatureType(f.geometry);
      const fid = sha256String(`${cityNorm}|${key}|${idx}|${JSON.stringify(f.geometry).slice(0,2000)}`);
      const props = { ...(f.properties || {}) };
      props.__el_feature_id = fid;
      props.__el_layer_key = key;
      props.__el_feature_type = ft;
      props.__el_jurisdiction_name = layer.city || layer.jurisdiction || "Massachusetts";
      props.__el_source_path = layer.source_type === "arcgis" ? layer.url : layer.path;
      props.__el_as_of_date = runDateISO.slice(0,10);
      props.__el_dataset_hash = datasetHash;
      props.__el_dataset_version = layersVersion();
      return { ...f, properties: props };
    })
  };

  ensureDirSync(outDirs.frozenDir);
  await fsp.writeFile(outGeo, JSON.stringify(fc), "utf-8");

  const meta = {
    created_at: nowIso(),
    layer_key: key,
    city: layer.city || null,
    source_type: layer.source_type,
    source_path: layer.source_type === "arcgis" ? layer.url : layer.path,
    local_source_file: geojsonPath,
    frozen_file: outGeo,
    feature_count: fc.features.length,
    dataset_hash: datasetHash,
    dataset_version: layersVersion(),
    as_of_date: runDateISO.slice(0,10)
  };
  await fsp.writeFile(outMeta, JSON.stringify(meta, null, 2), "utf-8");
  return { frozen_geojson: outGeo, meta: outMeta, feature_count: fc.features.length };
}

let _layersVersion = null;
function layersVersion() {
  if (_layersVersion) return _layersVersion;
  _layersVersion = "v1";
  return _layersVersion;
}

async function ensureLocalGeoJSON({ root, outDirs, layer }) {
  // Determine local path
  if (layer.source_type === "file") {
    const p = path.join(root, layer.path);
    if (!fs.existsSync(p)) throw new Error(`Missing file: ${p}`);
    return { localGeoJSON: p, convertedFromShp: false };
  }

  if (layer.source_type === "arcgis") {
    const outPath = path.join(root, layer.out_path);
    if (fs.existsSync(outPath)) return { localGeoJSON: outPath, downloaded: false };
    console.log(`[dl] ArcGIS â†’ GeoJSON: ${layer.layer_key}`);
    const res = await arcgisLayerToGeoJSON({ layerUrl: layer.url, outPath });
    console.log(`[dl] wrote ${res.outPath} (features=${res.featureCount})`);
    return { localGeoJSON: outPath, downloaded: true };
  }

  if (layer.source_type === "shp") {
    const shpPath = path.join(root, layer.path);
    if (!fs.existsSync(shpPath)) throw new Error(`Missing shapefile: ${shpPath}`);
    const outGeo = path.join(root, layer.shp_hint_out);
    if (fs.existsSync(outGeo)) return { localGeoJSON: outGeo, convertedFromShp: false };
    console.log(`[conv] SHP â†’ GeoJSON: ${layer.layer_key}`);
    try {
      const res = convertShpToGeoJSON({ shpPath, outGeoJSONPath: outGeo });
      console.log(`[conv] ok (${res.tool}) -> ${res.out}`);
      return { localGeoJSON: outGeo, convertedFromShp: true };
    } catch (e) {
      console.warn(`[warn] shapefile conversion failed for ${layer.layer_key}. Layer will be frozen/attached only after conversion.\n${e.message}`);
      // leave as missing for further processing
      throw e;
    }
  }

  throw new Error(`Unknown source_type: ${layer.source_type}`);
}

function classifyAttach(layer, defaults) {
  if (layer.attach === false) return { doAttach: false };
  if (layer.kind === "table") return { doAttach: false };
  const kind = layer.kind === "mixed" ? "mixed" : layer.kind;
  if (kind === "polygon") return { doAttach: true, ...defaults.polygon };
  if (kind === "point") return { doAttach: true, ...defaults.point };
  if (kind === "line") return { doAttach: true, ...defaults.line };
  // mixed: freeze only; attach disabled until validated
  return { doAttach: false };
}

function summarizeLayerGeo(gj) {
  const counts = {};
  for (const f of gj.features) {
    const t = inferFeatureType(f.geometry);
    counts[t] = (counts[t] || 0) + 1;
  }
  return counts;
}

async function attachAll({ root, cfg, contractFile, contractObj, propertiesPath }) {
  const outDirs = chooseOutDirs(root);
  ensureDirSync(outDirs.auditDir);
  ensureDirSync(outDirs.attachDir);
  ensureDirSync(outDirs.frozenDir);
  ensureDirSync(outDirs.contractsDir);

  const runDateISO = nowIso();
  const report = {
    created_at: runDateISO,
    root,
    contract_file: contractFile,
    properties_path: propertiesPath,
    layers_processed: [],
    attachments_written: [],
    warnings: []
  };

  console.log(`[info] properties: ${propertiesPath}`);

  // load Turf lazily
  let turf;
  try {
    turf = await import("@turf/turf");
  } catch (e) {
    throw new Error(
      "Missing dependency: @turf/turf. This repo normally has Turf already.\n" +
      "Fix: from backend root run `npm i @turf/turf` (or ensure node_modules exists), then rerun.\n" +
      `Details: ${e.message}`
    );
  }

  // prepare layers: download/convert, read geojson, compute bboxes
  const layers = cfg.cities;
  const defaults = cfg.attach_defaults;
  const maBbox = cfg.ma_bbox_sanity;

  const layerObjs = [];
  for (const layer of layers) {
    const attachSpec = classifyAttach(layer, defaults);
    let localGeo = null;
    let datasetHash = null;
    let frozen = null;
    let geo = null;

    try {
      if (layer.source_type === "file" || layer.source_type === "arcgis" || layer.source_type === "shp") {
                const effectiveLayer = {
          ...layer,
          path: (SOURCE_OVERRIDES[layer.layer_key] ? path.join(root, SOURCE_OVERRIDES[layer.layer_key]) : layer.path)
        };
        const res = await ensureLocalGeoJSON({ root, outDirs, layer: effectiveLayer });
        localGeo = res.localGeoJSON;
      }
      if (localGeo && localGeo.toLowerCase().endsWith(".geojson")) {
        datasetHash = sha256FileSync(localGeo);
        geo = await readGeoJSON(localGeo);
        frozen = await freezeLayer({ root, outDirs, layer: effectiveLayer, geojsonPath: localGeo, datasetHash, runDateISO });
      }
    } catch (e) {
      report.warnings.push({ layer_key: layer.layer_key, city: layer.city, message: e.message });
      // still continue to next layer
      continue;
    }

    // Guard: some layers are non-GeoJSON (tables/CSV). Skip safely.
    if (!geo) {
      report.warnings.push({
        layer_key: layer.layer_key,
        city: layer.city,
        message: "No GeoJSON loaded (non-GeoJSON source). Skipping freeze/attach for this layer."
      });
      continue;
    }
    // sanity CRS check: sample a few coords
    let crsOk = true;
    let sampleChecked = 0;
    for (const f of geo.features) {
      if (sampleChecked >= 25) break;
      const t = inferFeatureType(f.geometry);
      if (t === "point" && Array.isArray(f.geometry.coordinates)) {
        const [x,y] = f.geometry.coordinates;
        if (!withinMABbox(y, x, maBbox)) { crsOk = false; break; }
        sampleChecked++;
      } else if (t === "polygon" || t === "line") {
        // check one coord from first ring/line
        const coords = f.geometry.coordinates;
        const flat = JSON.stringify(coords).match(/-?\d+\.\d+/g);
        if (flat && flat.length >= 2) {
          const x = Number(flat[0]); const y = Number(flat[1]);
          if (!withinMABbox(y, x, maBbox)) { crsOk = false; break; }
          sampleChecked++;
        }
      }
    }

    const bbox = geo.features.length ? turf.bbox(geo) : null;
    const summary = summarizeLayerGeo(geo);

    layerObjs.push({
      ...layer,
      attachSpec,
      localGeo,
      datasetHash,
      frozen,
      geo,
      bbox,
      crsOk,
      geomSummary: summary
    });

    report.layers_processed.push({
      layer_key: layer.layer_key,
      city: layer.city,
      localGeo,
      datasetHash,
      frozen: frozen?.frozen_geojson,
      feature_count: frozen?.feature_count,
      bbox,
      crsOk,
      geomSummary: summary,
      doAttach: attachSpec.doAttach
    });

    if (!crsOk) {
      report.warnings.push({ layer_key: layer.layer_key, city: layer.city, message: "CRS sanity check failed (coords outside MA). Layer frozen but attachment disabled." });
    }
  }

  // Build mapping city -> layers
  const layersByCity = new Map();
  const regionalLayers = [];
  for (const l of layerObjs) {
    if (l.is_regional) {
      regionalLayers.push(l);
      continue;
    }
    const cn = normalizeCityName(l.city);
    if (!layersByCity.has(cn)) layersByCity.set(cn, []);
    layersByCity.get(cn).push(l);
  }

  // Prepare attachment output streams per layer
  const attachStreams = new Map();
  const attachCounts = new Map();

  function getAttachPath(layerKey, cityName) {
    const stamp = formatDateStamp();
    const cityNorm = normalizeCityName(cityName || "regional");
    const fname = `attach__phase2_civic__${cityNorm}__${layerKey}__${layersVersion()}__${stamp}.ndjson`;
    return path.join(outDirs.attachDir, fname);
  }

  for (const l of layerObjs) {
    if (!l.attachSpec.doAttach) continue;
    if (!l.crsOk) continue;
    const p = getAttachPath(l.layer_key, l.city || (l.is_regional ? "regional" : "na"));
    attachStreams.set(l.layer_key, fs.createWriteStream(p, { encoding: "utf-8" }));
    attachCounts.set(l.layer_key, 0);
    report.attachments_written.push({ layer_key: l.layer_key, city: l.city, path: p });
  }

  // Read properties NDJSON stream
  const rl = readline.createInterface({
    input: fs.createReadStream(propertiesPath, { encoding: "utf-8" }),
    crlfDelay: Infinity
  });

  let i = 0;
  let cityFieldKey = null;
  let cityFieldDetectedOnce = false;

  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    i++;
    let rec;
    try { rec = JSON.parse(line); } catch { continue; }
    const { lat, lon } = tryExtractLatLon(rec);
    if (lat == null || lon == null) continue;
    if (!withinMABbox(lat, lon, maBbox)) continue;

    // detect property city key once
    if (!cityFieldDetectedOnce) {
      const det = guessCityField(rec);
      cityFieldKey = det.key;
      cityFieldDetectedOnce = true;
      console.log(`[info] property city field detected: ${cityFieldKey || "(none)"}`);
    }

    let cityName = null;
    if (cityFieldKey) {
      if (cityFieldKey.includes(".")) {
        const parts = cityFieldKey.split(".");
        let cur = rec;
        for (const p of parts) cur = cur?.[p];
        cityName = cur != null ? String(cur) : null;
      } else {
        cityName = rec[cityFieldKey] != null ? String(rec[cityFieldKey]) : null;
      }
    }
    const cityNorm = normalizeCityName(cityName || "");
    const candidates = [];
    if (layersByCity.has(cityNorm)) candidates.push(...layersByCity.get(cityNorm));
    if (regionalLayers.length) candidates.push(...regionalLayers);

    if (candidates.length === 0) continue;

    const propertyId = rec.property_id || rec.parcel_id || rec.id || null;
    if (!propertyId) continue;

    const pt = turf.point([lon, lat]);

    for (const layer of candidates) {
      if (!layer.attachSpec.doAttach) continue;
      if (!layer.crsOk) continue;
      const ws = attachStreams.get(layer.layer_key);
      if (!ws) continue;
      if (layer.bbox && !bboxContainsPoint(layer.bbox, lon, lat)) continue;

      const kind = layer.kind;

      if (kind === "polygon") {
        // attach all matches (MULTI allowed) but only pip
        const matches = [];
        for (const f of layer.geo.features) {
          const fb = bboxOfFeature(turf, f);
          if (fb && !bboxContainsPoint(fb, lon, lat)) continue;
          let inside = false;
          try { inside = turf.booleanPointInPolygon(pt, f); } catch { inside = false; }
          if (inside) {
            const fid = f.properties?.__el_feature_id || null;
            matches.push(fid);
          }
        }
        if (matches.length) {
          for (const fid of matches) {
            const row = {
              property_id: propertyId,
              feature_id: fid,
              layer_key: layer.layer_key,
              attach_method: "pip",
              distance_m: null,
              attach_confidence: layer.attachSpec.confidence || "B",
              attach_as_of_date: runDateISO.slice(0,10),
              source_dataset_hash: layer.datasetHash,
              source_frozen_geojson: layer.frozen?.frozen_geojson || null,
              flags: matches.length > 1 ? ["MULTI_MATCH"] : []
            };
            ws.write(JSON.stringify(row) + "\n");
            attachCounts.set(layer.layer_key, (attachCounts.get(layer.layer_key) || 0) + 1);
          }
        }
      } else if (kind === "point") {
        // nearest point with max distance gate
        const maxD = layer.attachSpec.max_distance_m ?? 75;
        let best = null;
        let bestDist = Infinity;
        for (const f of layer.geo.features) {
          if (!f.geometry || f.geometry.type !== "Point") continue;
          const [x,y] = f.geometry.coordinates;
          const d = turf.distance(pt, turf.point([x,y]), { units: "meters" });
          if (d < bestDist) { bestDist = d; best = f; }
        }
        if (best && bestDist <= maxD) {
          const fid = best.properties?.__el_feature_id || null;
          const row = {
            property_id: propertyId,
            feature_id: fid,
            layer_key: layer.layer_key,
            attach_method: "nearest_point",
            distance_m: Math.round(bestDist * 10) / 10,
            attach_confidence: layer.attachSpec.confidence || "C",
            attach_as_of_date: runDateISO.slice(0,10),
            source_dataset_hash: layer.datasetHash,
            source_frozen_geojson: layer.frozen?.frozen_geojson || null,
            flags: []
          };
          ws.write(JSON.stringify(row) + "\n");
          attachCounts.set(layer.layer_key, (attachCounts.get(layer.layer_key) || 0) + 1);
        }
      } else if (kind === "line") {
        // nearest line with max distance gate
        const maxD = layer.attachSpec.max_distance_m ?? 25;
        let best = null;
        let bestDist = Infinity;
        for (const f of layer.geo.features) {
          const ft = inferFeatureType(f.geometry);
          if (ft !== "line") continue;
          try {
            const np = turf.nearestPointOnLine(f, pt, { units: "meters" });
            const d = np?.properties?.dist ?? null;
            if (d != null && d < bestDist) { bestDist = d; best = f; }
          } catch {}
        }
        if (best && bestDist <= maxD) {
          const fid = best.properties?.__el_feature_id || null;
          const row = {
            property_id: propertyId,
            feature_id: fid,
            layer_key: layer.layer_key,
            attach_method: "nearest_line",
            distance_m: Math.round(bestDist * 10) / 10,
            attach_confidence: layer.attachSpec.confidence || "C",
            attach_as_of_date: runDateISO.slice(0,10),
            source_dataset_hash: layer.datasetHash,
            source_frozen_geojson: layer.frozen?.frozen_geojson || null,
            flags: []
          };
          ws.write(JSON.stringify(row) + "\n");
          attachCounts.set(layer.layer_key, (attachCounts.get(layer.layer_key) || 0) + 1);
        }
      }
    }

    if (i % 200000 === 0) {
      console.log(`[progress] scanned properties: ${i.toLocaleString()}`);
    }
  }

  // close streams
  for (const [k, s] of attachStreams.entries()) s.end();

  // write report
  const stamp = formatDateStamp();
  const reportPath = path.join(outDirs.auditDir, `phase2_city_civics_report__${layersVersion()}__${stamp}.json`);
  report.attachment_counts = Object.fromEntries([...attachCounts.entries()]);
  await fsp.writeFile(reportPath, JSON.stringify(report, null, 2), "utf-8");
  console.log(`[report] ${reportPath}`);

  // update contract view
  const newContract = JSON.parse(JSON.stringify(contractObj || {}));
  newContract.phase2_city_civics = {
    created_at: runDateISO,
    pack_version: layersVersion(),
    frozen_dir: outDirs.frozenDir,
    attachment_dir: outDirs.attachDir,
    report: reportPath,
    layers: report.layers_processed,
    attachments: report.attachments_written
  };

  const newContractFile = path.join(outDirs.contractsDir, `contract_view_ma__phase2_city_civics__${layersVersion()}__${stamp}.json`);
  await fsp.writeFile(newContractFile, JSON.stringify(newContract, null, 2), "utf-8");
  console.log(`[contract] wrote ${newContractFile}`);

  // update pointer if possible
  if (contractFile) {
    try {
      const bak = backupFileSync(contractFile);
      console.log(`[backup] ${bak}`);
      // If pointer-like, update common keys; else overwrite with merged view.
      let existing = safeReadJsonSync(contractFile);
      if (existing && typeof existing === "object" && ("current" in existing || "current_contract" in existing || "path" in existing)) {
        const key = "current" in existing ? "current" : ("current_contract" in existing ? "current_contract" : "path");
        existing[key] = newContractFile;
        existing.updated_at = runDateISO;
        safeWriteJsonSync(contractFile, existing);
        console.log(`[pointer] updated ${contractFile} (${key} -> ${newContractFile})`);
      } else {
        safeWriteJsonSync(contractFile, newContract);
        console.log(`[pointer] updated ${contractFile} (overwrote with new contract view)`);
      }
    } catch (e) {
      console.warn(`[warn] could not update contract pointer: ${e.message}`);
    }
  } else {
    console.warn("[warn] no CURRENT_CONTRACT_VIEW_MA*.json found; contract was written but pointer not updated.");
  }

  console.log("[done] Attach finished.");
}

async function main() {
  logHeader();
  const args = parseArgs(process.argv);
  const root = resolveRoot(args);
  console.log(`[info] root: ${root}`);

  const cfg = loadConfig(root);
  _layersVersion = cfg.version || "v1";

  const contractFile = findCurrentContractFile(root);
  if (contractFile) console.log(`[info] contract pointer found: ${contractFile}`);
  else console.log("[warn] contract pointer not found (CURRENT_CONTRACT_VIEW_MA*.json). We'll still write a new contract view in publicData/_contracts/.");

  let contractObj = null;
  if (contractFile) {
    try { contractObj = safeReadJsonSync(contractFile); } catch {}
    // if pointer-like, attempt to read actual contract file
    if (contractObj && typeof contractObj === "object") {
      const maybePath = contractObj.current || contractObj.current_contract || contractObj.path || null;
      if (maybePath && typeof maybePath === "string") {
        const abs = path.isAbsolute(maybePath) ? maybePath : path.join(root, maybePath);
        if (fs.existsSync(abs)) {
          try {
            contractObj = safeReadJsonSync(abs);
            console.log(`[info] loaded contract view: ${abs}`);
          } catch {}
        }
      }
    }
  }

  // Determine properties path
  let propertiesPath = inferPropertiesPathFromContract(contractObj, root);
  if (!propertiesPath) propertiesPath = findLatestPropertiesNdjson(root);
  if (!propertiesPath) throw new Error("Could not determine properties NDJSON path. Add it to your contract view OR place it under publicData/properties/.");

  await attachAll({ root, cfg, contractFile, contractObj, propertiesPath });
}

main().catch((e) => {
  console.error("[fatal]", e.message);
  process.exit(1);
});




