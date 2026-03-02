#!/usr/bin/env node
/**
 * Phase 2.2 — Regional Planning Agencies (MA) Freeze + Attach
 * - Reads current contract view NDJSON
 * - Reads RPA polygons (GeoJSON)
 * - Attaches (pip) by parcel centroid (or latitude/longitude fallback)
 * - Writes:
 *   - feature catalog (small)
 *   - attachments NDJSON
 *   - next contract view NDJSON with summary headers
 * - Updates pointers:
 *   - CURRENT_CIVIC_REGIONAL_PLANNING_AGENCIES_MA
 *   - CURRENT_CONTRACT_VIEW_PHASE2_2_RPA_MA
 *   - CURRENT_CONTRACT_VIEW_MA
 *
 * No readFileSync on large files. All hashing is streaming.
 */

import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import crypto from "node:crypto";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
      out[k] = v;
    }
  }
  return out;
}

function nowStampZ() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return (
    d.getUTCFullYear().toString() +
    pad(d.getUTCMonth() + 1) +
    pad(d.getUTCDate()) +
    "_" +
    pad(d.getUTCHours()) +
    pad(d.getUTCMinutes()) +
    pad(d.getUTCSeconds()) +
    "Z"
  );
}

function sha256Stream(filePath) {
  return new Promise((resolve, reject) => {
    const h = crypto.createHash("sha256");
    const s = fs.createReadStream(filePath);
    s.on("data", (buf) => h.update(buf));
    s.on("error", reject);
    s.on("end", () => resolve(h.digest("hex").toUpperCase()));
  });
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function writeText(p, txt) {
  ensureDir(path.dirname(p));
  fs.writeFileSync(p, txt, "utf8");
}

function readJsonFile(p) {
  const raw = fs.readFileSync(p, "utf8");
  return JSON.parse(raw);
}

function isFile(p) {
  try {
    return fs.statSync(p).isFile();
  } catch {
    return false;
  }
}

function isDir(p) {
  try {
    return fs.statSync(p).isDirectory();
  } catch {
    return false;
  }
}

function resolvePathMaybe(backendRoot, p) {
  if (!p) return p;
  if (path.isAbsolute(p)) return p;
  return path.join(backendRoot, p);
}

function slugify(s) {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function pickName(props = {}) {
  const keys = [
    "AGENCY",
    "Agency",
    "agency",
    "NAME",
    "Name",
    "name",
    "RPA_NAME",
    "rpa_name",
    "REGION",
    "Region",
    "region",
    "RPA",
    "rpa",
    "Planning",
    "planning",
  ];
  for (const k of keys) {
    const v = props[k];
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  // fallback: first string prop
  for (const [k, v] of Object.entries(props)) {
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  return "";
}

// geometry utils
function bboxOfCoords(coords, bbox) {
  for (const c of coords) {
    if (Array.isArray(c[0])) bboxOfCoords(c, bbox);
    else {
      const x = c[0], y = c[1];
      if (x < bbox[0]) bbox[0] = x;
      if (y < bbox[1]) bbox[1] = y;
      if (x > bbox[2]) bbox[2] = x;
      if (y > bbox[3]) bbox[3] = y;
    }
  }
}
function geomBbox(geom) {
  const bbox = [Infinity, Infinity, -Infinity, -Infinity];
  if (!geom) return null;
  if (geom.type === "Polygon") bboxOfCoords(geom.coordinates, bbox);
  else if (geom.type === "MultiPolygon") bboxOfCoords(geom.coordinates, bbox);
  else return null;
  return bbox;
}
function inBbox(pt, bbox) {
  return pt[0] >= bbox[0] && pt[0] <= bbox[2] && pt[1] >= bbox[1] && pt[1] <= bbox[3];
}

// point in ring (ray casting). ring is [ [x,y], ... ] closed or not.
function pointInRing(pt, ring) {
  const x = pt[0], y = pt[1];
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0], yi = ring[i][1];
    const xj = ring[j][0], yj = ring[j][1];
    const intersect =
      yi > y !== yj > y &&
      x < ((xj - xi) * (y - yi)) / (yj - yi + 0.0) + xi;
    if (intersect) inside = !inside;
  }
  return inside;
}

function pointInPolygon(pt, polyCoords) {
  // polyCoords: [outerRing, hole1, hole2...]
  if (!polyCoords || !polyCoords.length) return false;
  if (!pointInRing(pt, polyCoords[0])) return false;
  for (let i = 1; i < polyCoords.length; i++) {
    if (pointInRing(pt, polyCoords[i])) return false;
  }
  return true;
}

function pointInGeom(pt, geom) {
  if (!geom) return false;
  if (geom.type === "Polygon") return pointInPolygon(pt, geom.coordinates);
  if (geom.type === "MultiPolygon") {
    for (const poly of geom.coordinates) {
      if (pointInPolygon(pt, poly)) return true;
    }
    return false;
  }
  return false;
}

function getBestPointFromRow(row) {
  // Prefer contract latitude/longitude if present; else parcel centroid.
  const lon = Number(row.longitude ?? row.lon ?? row.lng ?? row.long);
  const lat = Number(row.latitude ?? row.lat);
  if (Number.isFinite(lon) && Number.isFinite(lat)) return [lon, lat];

  const clon = Number(row.parcel_centroid_lon ?? row.parcel_centroid_lng);
  const clat = Number(row.parcel_centroid_lat);
  if (Number.isFinite(clon) && Number.isFinite(clat)) return [clon, clat];

  return null;
}

async function verifyHeadersSample(contractPath, requiredKeys, sampleLines, auditDir) {
  ensureDir(auditDir);
  const rl = readline.createInterface({
    input: fs.createReadStream(contractPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let read = 0;
  let bad = 0;
  let headers = new Set();
  for await (const line of rl) {
    if (!line.trim()) continue;
    read++;
    try {
      const obj = JSON.parse(line);
      Object.keys(obj).forEach((k) => headers.add(k));
    } catch {
      bad++;
    }
    if (read >= sampleLines) break;
  }
  rl.close();

  const missing = requiredKeys.filter((k) => !headers.has(k));
  const report = {
    contract: contractPath,
    sampled_lines: read,
    bad_json: bad,
    header_count: headers.size,
    required_keys: requiredKeys,
    missing_required: missing,
    status: missing.length === 0 ? "PASS" : "FAIL",
  };
  fs.writeFileSync(path.join(auditDir, "verify_phase2_2_rpa.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(
    path.join(auditDir, "verify_phase2_2_rpa.txt"),
    [
      `status: ${report.status}`,
      `sampled_lines: ${read}`,
      `bad_json: ${bad}`,
      `header_count: ${headers.size}`,
      `missing_required: ${missing.length ? missing.join(", ") : "(none)"}`,
      "",
      "required_keys:",
      ...requiredKeys.map((k) => `- ${k}`),
      "",
    ].join("\n"),
    "utf8"
  );

  return report;
}

async function main() {
  const args = parseArgs(process.argv);
  const backendRoot = args.backendRoot ? path.resolve(args.backendRoot) : process.cwd();
  const asOfDate = String(args.asOfDate ?? "");
  const contractIn = args.contractIn ? path.resolve(args.contractIn) : null;
  const rpaGeojson = args.rpaGeojson ? path.resolve(args.rpaGeojson) : null;
  const verifySampleLines = Number(args.verifySampleLines ?? 4000);

  if (!asOfDate) throw new Error("--asOfDate required (YYYY-MM-DD)");
  if (!contractIn || !isFile(contractIn)) throw new Error(`--contractIn must be an existing FILE: ${contractIn}`);
  if (!rpaGeojson || !isFile(rpaGeojson)) throw new Error(`--rpaGeojson must be an existing FILE: ${rpaGeojson}`);

  const ts = nowStampZ();
  const overlaysWorkDir = path.join(backendRoot, "publicData", "overlays", "_work", `phase2_2_rpa_run__${ts}`);
  const attachmentsPath = path.join(overlaysWorkDir, "civic_regional_planning_agencies__attachments.ndjson");
  const featureCatalogPath = path.join(overlaysWorkDir, "civic_regional_planning_agencies__feature_catalog.ndjson");

  const freezeDir = path.join(
    backendRoot,
    "publicData",
    "overlays",
    "_frozen",
    `civic_regional_planning_agencies__ma__v1__FREEZE__${ts}`
  );
  const freezeLayerDir = path.join(freezeDir, "civic_regional_planning_agencies");
  ensureDir(overlaysWorkDir);
  ensureDir(freezeLayerDir);

  const propsFrozenDir = path.join(
    backendRoot,
    "publicData",
    "properties",
    "_frozen",
    `contract_view_phase2_2_rpa__ma__v1__FREEZE__${ts}`
  );
  ensureDir(propsFrozenDir);

  const contractOut = path.join(propsFrozenDir, `contract_view_phase2_2_rpa__${asOfDate.replace(/-/g, "")}.ndjson`);

  console.log(`[info] BackendRoot: ${backendRoot}`);
  console.log(`[info] as_of_date: ${asOfDate}`);
  console.log(`[info] contractIn: ${contractIn}`);
  console.log(`[info] rpaGeojson: ${rpaGeojson}`);
  console.log(`[info] outContract: ${contractOut}`);
  console.log(`[info] freezeDir: ${freezeDir}`);

  // Hash inputs
  const contractInHash = await sha256Stream(contractIn);
  const rpaHash = await sha256Stream(rpaGeojson);

  // Load RPA features
  const rpa = readJsonFile(rpaGeojson);
  if (!rpa || rpa.type !== "FeatureCollection" || !Array.isArray(rpa.features)) {
    throw new Error("RPA geojson must be a FeatureCollection");
  }

  const layerKey = "civic_regional_planning_agencies";
  const features = [];
  for (let i = 0; i < rpa.features.length; i++) {
    const f = rpa.features[i];
    if (!f || f.type !== "Feature") continue;
    const geom = f.geometry;
    if (!geom || (geom.type !== "Polygon" && geom.type !== "MultiPolygon")) continue;
    const name = pickName(f.properties) || `rpa_${i + 1}`;
    const key = slugify(name) || `rpa_${i + 1}`;
    const feature_id = crypto.createHash("sha1").update(`${layerKey}:${key}`).digest("hex");
    const bbox = geomBbox(geom);
    features.push({
      feature_id,
      layer_key: layerKey,
      key,
      name,
      geom,
      bbox,
      source_object_id: f.id ?? null,
      raw_properties: f.properties ?? {},
    });
  }
  if (!features.length) {
    const counts = new Map();
    for (const f of rpa.features) {
      const t = f?.geometry?.type ?? "(null)";
      counts.set(t, (counts.get(t) ?? 0) + 1);
    }
    const summary = [...counts.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([t, n]) => `${t}:${n}`)
      .join(", ");
    throw new Error(
      `No Polygon/MultiPolygon features found in RPA dataset. Geometry types seen: ${summary}. ` +
        `This usually means the shapefile selected is an ARC/LINE layer (e.g., RPAS_ARC). ` +
        `Re-run the pack after extraction; it will now auto-pick a polygon layer if present.`
    );
  }

  // Write feature catalog (small)
  const fcOut = fs.createWriteStream(featureCatalogPath, { encoding: "utf8" });
  for (const f of features) {
    const rec = {
      feature_id: f.feature_id,
      layer_key: layerKey,
      feature_type: "polygon",
      name: f.name,
      key: f.key,
      jurisdiction_type: "regional_planning_agency",
      jurisdiction_name: f.name,
      source_system: "local_zip_shp",
      source_path: rpaGeojson,
      as_of_date: asOfDate,
      dataset_hash: rpaHash,
      bbox: f.bbox,
      geometry: f.geom, // small count, OK
    };
    fcOut.write(JSON.stringify(rec) + "\n");
  }
  fcOut.end();

  // Copy source geojson into freeze layer dir (so freeze is self-contained)
  const frozenGeoPath = path.join(freezeLayerDir, path.basename(rpaGeojson));
  fs.copyFileSync(rpaGeojson, frozenGeoPath);

  // Write LAYER_META.json
  const layerMeta = {
    layer_key: layerKey,
    as_of_date: asOfDate,
    source_path: rpaGeojson,
    frozen_path: frozenGeoPath,
    dataset_hash: rpaHash,
    feature_count_total: features.length,
    feature_count_polygon: features.length,
    created_at: new Date().toISOString(),
  };
  fs.writeFileSync(path.join(freezeLayerDir, "LAYER_META.json"), JSON.stringify(layerMeta, null, 2));

  // Attach + write contract out
  const inStream = fs.createReadStream(contractIn, { encoding: "utf8" });
  const rl = readline.createInterface({ input: inStream, crlfDelay: Infinity });

  const outStream = fs.createWriteStream(contractOut, { encoding: "utf8" });
  const attStream = fs.createWriteStream(attachmentsPath, { encoding: "utf8" });

  let read = 0, wrote = 0, matched = 0, badJson = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;
    read++;
    let row;
    try {
      row = JSON.parse(line);
    } catch {
      badJson++;
      continue;
    }

    const pt = getBestPointFromRow(row);
    let hit = null;

    if (pt) {
      for (const f of features) {
        if (f.bbox && !inBbox(pt, f.bbox)) continue;
        if (pointInGeom(pt, f.geom)) {
          hit = f;
          break;
        }
      }
    }

    if (hit) {
      matched++;
      row.has_civic_rpa = true;
      row.civic_rpa_count = 1;
      row.civic_rpa_keys = [hit.key];
      row.civic_rpa_names = [hit.name];
      row.civic_rpa_dataset_hash = rpaHash;
      row.civic_rpa_as_of_date = asOfDate;
      row.civic_rpa_attach_method = "pip_parcel_centroid";

      // property_id is required for attachment rows; if missing, skip attachment but keep contract summary.
      const property_id = row.property_id ?? null;
      if (property_id) {
        const att = {
          property_id,
          feature_id: hit.feature_id,
          layer_key: layerKey,
          attach_method: "pip_parcel_centroid",
          distance_m: null,
          attach_confidence: "A",
          attach_as_of_date: asOfDate,
          dataset_hash: rpaHash,
        };
        attStream.write(JSON.stringify(att) + "\n");
      }
    } else {
      row.has_civic_rpa = false;
      row.civic_rpa_count = 0;
      row.civic_rpa_keys = [];
      row.civic_rpa_names = [];
      row.civic_rpa_dataset_hash = rpaHash;
      row.civic_rpa_as_of_date = asOfDate;
      row.civic_rpa_attach_method = "pip_parcel_centroid";
    }

    outStream.write(JSON.stringify(row) + "\n");
    wrote++;

    if (read % 200000 === 0) {
      console.log(`[prog] read=${read} wrote=${wrote} matched=${matched} bad_json=${badJson}`);
    }
  }

  rl.close();
  outStream.end();
  attStream.end();

  console.log(`[done] read=${read} wrote=${wrote} matched=${matched} bad_json=${badJson}`);

  // Compute output hash (streaming)
  const contractOutHash = await sha256Stream(contractOut);

  // Write MANIFEST.json in freeze dir
  const manifest = {
    phase: "phase2_2_rpa",
    created_at: new Date().toISOString(),
    as_of_date: asOfDate,
    inputs: {
      contract_in: contractIn,
      contract_in_sha256: contractInHash,
      rpa_geojson: rpaGeojson,
      rpa_dataset_hash: rpaHash,
    },
    outputs: {
      freeze_dir: freezeDir,
      feature_catalog_ndjson: featureCatalogPath,
      attachments_ndjson: attachmentsPath,
      contract_out: contractOut,
      contract_out_sha256: contractOutHash,
    },
    stats: { read, wrote, matched, bad_json: badJson, feature_count: features.length },
  };
  fs.writeFileSync(path.join(freezeDir, "MANIFEST.json"), JSON.stringify(manifest, null, 2));

  // Update pointers
  const overlaysFrozenRoot = path.join(backendRoot, "publicData", "overlays", "_frozen");
  const propsFrozenRoot = path.join(backendRoot, "publicData", "properties", "_frozen");

  writeText(path.join(overlaysFrozenRoot, "CURRENT_CIVIC_REGIONAL_PLANNING_AGENCIES_MA.txt"), freezeDir);

  writeText(path.join(propsFrozenRoot, "CURRENT_CONTRACT_VIEW_PHASE2_2_RPA_MA.txt"), contractOut);
  writeText(path.join(propsFrozenRoot, "CURRENT_CONTRACT_VIEW_MA.txt"), contractOut);

  console.log(`[ok] CURRENT_CIVIC_REGIONAL_PLANNING_AGENCIES_MA -> ${freezeDir}`);
  console.log(`[ok] CURRENT_CONTRACT_VIEW_PHASE2_2_RPA_MA -> ${contractOut}`);
  console.log(`[ok] CURRENT_CONTRACT_VIEW_MA -> ${contractOut}`);

  // Verify (headers)
  const required = [
    "property_id",
    "as_of_date",
    "has_civic_rpa",
    "civic_rpa_count",
    "civic_rpa_keys",
    "civic_rpa_names",
    "civic_rpa_dataset_hash",
    "civic_rpa_as_of_date",
    "civic_rpa_attach_method",
  ];
  const auditDir = path.join(backendRoot, "publicData", "_audit", `phase2_2_rpa_verify__${ts}`);
  const rep = await verifyHeadersSample(contractOut, required, Math.max(1, verifySampleLines), auditDir);
  console.log(`[result] ${rep.status} header_count=${rep.header_count}`);
  if (rep.status !== "PASS") process.exitCode = 2;
}

main().catch((e) => {
  console.error("[fatal]", e?.stack || e);
  process.exit(1);
});
