#!/usr/bin/env node
/**
 * Phase 2.1 — Block Groups (v2)
 * Freeze block group layer + attach to CURRENT contract view (streaming NDJSON).
 *
 * Outputs:
 *  - Freeze dir: <freezeDir>/civic_block_groups/{blockGroupBoundaries.geojson, LAYER_META.json}
 *  - Out contract NDJSON: <outContract>
 *  - Optional attachments NDJSON: <outAttachments> (one row per matched property)
 *  - MANIFEST.json in <freezeDir>
 *
 * IMPORTANT:
 *  - Uses streaming SHA256 hashing (safe for >2GB files).
 *  - Point-in-polygon implementation is local (no turf deps).
 */

import fs from "fs";
import path from "path";
import crypto from "crypto";
import readline from "readline";

function die(msg) {
  console.error("[fatal]", msg);
  process.exit(1);
}

function argMap(argv) {
  const m = new Map();
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : "true";
    m.set(k, v);
  }
  return m;
}

async function sha256FileStream(filePath) {
  return await new Promise((resolve, reject) => {
    const hash = crypto.createHash("sha256");
    const s = fs.createReadStream(filePath);
    s.on("data", (d) => hash.update(d));
    s.on("error", reject);
    s.on("end", () => resolve(hash.digest("hex").toUpperCase()));
  });
}

function ensureFile(p, label) {
  if (!p) die(`${label} is required`);
  if (!fs.existsSync(p)) die(`${label} not found: ${p}`);
  const st = fs.statSync(p);
  if (!st.isFile()) die(`${label} must be a FILE: ${p}`);
}

function ensureDir(p, label) {
  if (!p) die(`${label} is required`);
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
  const st = fs.statSync(p);
  if (!st.isDirectory()) die(`${label} must be a directory: ${p}`);
}

function toNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function bboxOfCoords(coords) {
  // coords: nested arrays for Polygon or MultiPolygon
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  const walk = (c) => {
    if (!Array.isArray(c)) return;
    if (typeof c[0] === "number" && typeof c[1] === "number") {
      const x = c[0], y = c[1];
      if (x < minX) minX = x;
      if (y < minY) minY = y;
      if (x > maxX) maxX = x;
      if (y > maxY) maxY = y;
      return;
    }
    for (const cc of c) walk(cc);
  };
  walk(coords);
  if (!Number.isFinite(minX)) return null;
  return [minX, minY, maxX, maxY];
}

function pointInRing(pt, ring) {
  // Ray casting; ring is array of [x,y]
  const x = pt[0], y = pt[1];
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0], yi = ring[i][1];
    const xj = ring[j][0], yj = ring[j][1];
    const intersect = ((yi > y) !== (yj > y)) && (x < ((xj - xi) * (y - yi)) / ((yj - yi) || 1e-15) + xi);
    if (intersect) inside = !inside;
  }
  return inside;
}

function pointInPolygon(pt, geom) {
  // Supports Polygon and MultiPolygon; respects holes
  if (!geom) return false;
  const t = geom.type;
  const c = geom.coordinates;
  if (t === "Polygon") return pointInPolygonCoords(pt, c);
  if (t === "MultiPolygon") {
    for (const poly of c) {
      if (pointInPolygonCoords(pt, poly)) return true;
    }
    return false;
  }
  return false;
}

function pointInPolygonCoords(pt, polyCoords) {
  // polyCoords: [outerRing, hole1, hole2...]
  if (!Array.isArray(polyCoords) || polyCoords.length === 0) return false;
  const outer = polyCoords[0];
  if (!outer || outer.length < 3) return false;
  if (!pointInRing(pt, outer)) return false;
  for (let i = 1; i < polyCoords.length; i++) {
    const hole = polyCoords[i];
    if (hole && hole.length >= 3 && pointInRing(pt, hole)) return false;
  }
  return true;
}

function getGEOID(props) {
  if (!props) return null;
  const keys = ["GEOID", "GEOID20", "GEOID10", "geoid", "geoid20", "geoid10", "GEOID_20", "GEOID_10"];
  for (const k of keys) {
    const v = props[k];
    if (typeof v === "string" && v.trim()) return v.trim();
    if (typeof v === "number" && Number.isFinite(v)) return String(v);
  }
  // Sometimes: TRACTCE + BLKGRPCE + COUNTYFP + STATEFP
  const statefp = props.STATEFP ?? props.statefp;
  const countyfp = props.COUNTYFP ?? props.countyfp;
  const tractce = props.TRACTCE ?? props.tractce;
  const bg = props.BLKGRPCE ?? props.blkgrpce ?? props.BLOCKGRP ?? props.blockgrp;
  if (statefp && countyfp && tractce && bg) {
    return `${String(statefp).padStart(2,"0")}${String(countyfp).padStart(3,"0")}${String(tractce).padStart(6,"0")}${String(bg).slice(-1)}`;
  }
  return null;
}

function splitGEOID(geoid) {
  // 12 chars: SSCCCTTTTTTB
  if (!geoid) return { statefp: null, countyfp: null, tractce: null, bg: null };
  const g = String(geoid).replace(/\s/g, "");
  if (g.length < 12) return { statefp: null, countyfp: null, tractce: null, bg: null };
  return {
    statefp: g.slice(0, 2),
    countyfp: g.slice(2, 5),
    tractce: g.slice(5, 11),
    bg: g.slice(11, 12),
  };
}

function cellKey(lon, lat, cellSize) {
  const x = Math.floor((lon + 180) / cellSize);
  const y = Math.floor((lat + 90) / cellSize);
  return `${x}:${y}`;
}

function neighborKeys(key) {
  const [xs, ys] = key.split(":");
  const x = Number(xs), y = Number(ys);
  const out = [];
  for (let dx = -1; dx <= 1; dx++) {
    for (let dy = -1; dy <= 1; dy++) {
      out.push(`${x + dx}:${y + dy}`);
    }
  }
  return out;
}

async function main() {
  const args = argMap(process.argv);

  const contractIn = args.get("contractIn");
  const blockGroups = args.get("blockGroups");
  const freezeDir = args.get("freezeDir");
  const outContract = args.get("outContract");
  const outAttachments = args.get("outAttachments") || ""; // optional
  const asOfDate = args.get("asOfDate");

  ensureFile(contractIn, "--contractIn");
  ensureFile(blockGroups, "--blockGroups");
  ensureDir(freezeDir, "--freezeDir");
  if (!outContract) die("--outContract is required");
  if (!asOfDate) die("--asOfDate is required (YYYY-MM-DD)");

  const civicDir = path.join(freezeDir, "civic_block_groups");
  ensureDir(civicDir, "civic_block_groups dir");

  console.log("[info] contractIn:", contractIn);
  console.log("[info] blockGroups:", blockGroups);
  console.log("[info] freezeDir:", freezeDir);
  console.log("[info] outContract:", outContract);
  if (outAttachments) console.log("[info] outAttachments:", outAttachments);
  console.log("[info] as_of_date:", asOfDate);

  const contractHash = await sha256FileStream(contractIn);
  const layerHash = await sha256FileStream(blockGroups);

  // Copy source layer into freeze dir (stable name)
  const frozenLayerPath = path.join(civicDir, "blockGroupBoundaries.geojson");
  fs.copyFileSync(blockGroups, frozenLayerPath);

  // Load layer (MA block groups are typically manageable)
  let gj;
  try {
    const raw = fs.readFileSync(blockGroups, "utf-8");
    gj = JSON.parse(raw);
  } catch (e) {
    die("Failed to parse blockGroups GeoJSON (is it valid JSON?): " + e.message);
  }
  if (!gj || gj.type !== "FeatureCollection" || !Array.isArray(gj.features)) {
    die("blockGroups must be a GeoJSON FeatureCollection with features[]");
  }

  const features = [];
  const cellSize = 0.05; // degrees (~5km). Conservative for MA.
  const grid = new Map();

  let fcTotal = 0;
  let fcPoly = 0;

  for (const f of gj.features) {
    fcTotal++;
    const geom = f?.geometry;
    if (!geom || (geom.type !== "Polygon" && geom.type !== "MultiPolygon")) continue;
    fcPoly++;
    const geoid = getGEOID(f?.properties) || null;
    const parts = splitGEOID(geoid);
    const bbox = bboxOfCoords(geom.coordinates);
    if (!bbox) continue;

    const idx = features.length;
    features.push({
      geoid,
      statefp: parts.statefp,
      countyfp: parts.countyfp,
      tractce: parts.tractce,
      bg: parts.bg,
      bbox,
      geom,
    });

    // Insert into every grid cell overlapped by bbox
    const minLon = bbox[0], minLat = bbox[1], maxLon = bbox[2], maxLat = bbox[3];
    const x0 = Math.floor((minLon + 180) / cellSize);
    const x1 = Math.floor((maxLon + 180) / cellSize);
    const y0 = Math.floor((minLat + 90) / cellSize);
    const y1 = Math.floor((maxLat + 90) / cellSize);
    for (let x = x0; x <= x1; x++) {
      for (let y = y0; y <= y1; y++) {
        const k = `${x}:${y}`;
        const arr = grid.get(k) || [];
        arr.push(idx);
        grid.set(k, arr);
      }
    }
  }

  if (features.length === 0) die("No Polygon/MultiPolygon features found in blockGroups layer");

  // Write layer meta
  const layerMeta = {
    layer_key: "civic_block_groups",
    as_of_date: asOfDate,
    source_path: blockGroups,
    frozen_path: frozenLayerPath,
    dataset_hash: layerHash,
    feature_count_total: fcTotal,
    feature_count_polygon: fcPoly,
    index_cell_size_deg: cellSize,
    created_at: new Date().toISOString(),
  };
  fs.writeFileSync(path.join(civicDir, "LAYER_META.json"), JSON.stringify(layerMeta, null, 2), "utf-8");

  // Prepare output writers
  fs.mkdirSync(path.dirname(outContract), { recursive: true });
  const outC = fs.createWriteStream(outContract, { encoding: "utf-8" });

  let outA = null;
  if (outAttachments) {
    fs.mkdirSync(path.dirname(outAttachments), { recursive: true });
    outA = fs.createWriteStream(outAttachments, { encoding: "utf-8" });
  }

  // Stream contract NDJSON
  const rl = readline.createInterface({
    input: fs.createReadStream(contractIn, { encoding: "utf-8" }),
    crlfDelay: Infinity,
  });

  let read = 0, wrote = 0, bad = 0, matched = 0;

  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    read++;
    let rec;
    try {
      rec = JSON.parse(line);
    } catch {
      bad++;
      continue;
    }

    const lon = toNum(rec.parcel_centroid_lon) ?? toNum(rec.longitude);
    const lat = toNum(rec.parcel_centroid_lat) ?? toNum(rec.latitude);

    let match = null;

    if (lon != null && lat != null) {
      const k0 = cellKey(lon, lat, cellSize);
      const keys = neighborKeys(k0); // small hedge near cell edges
      const candidates = new Set();
      for (const kk of keys) {
        const arr = grid.get(kk);
        if (!arr) continue;
        for (const idx of arr) candidates.add(idx);
      }

      // Candidate scan (bbox then pip)
      for (const idx of candidates) {
        const f = features[idx];
        const b = f.bbox;
        if (lon < b[0] || lon > b[2] || lat < b[1] || lat > b[3]) continue;
        if (pointInPolygon([lon, lat], f.geom)) {
          match = f;
          break;
        }
      }
    }

    // Add headers (always present)
    rec.has_civic_block_group = Boolean(match);
    rec.civic_block_group_geoid = match?.geoid ?? null;
    rec.civic_block_group_statefp = match?.statefp ?? null;
    rec.civic_block_group_countyfp = match?.countyfp ?? null;
    rec.civic_block_group_tractce = match?.tractce ?? null;
    rec.civic_block_group_bg = match?.bg ?? null;
    rec.civic_block_group_attach_method = "pip_parcel_centroid";
    rec.civic_block_group_dataset_hash = layerHash;
    rec.civic_block_group_as_of_date = asOfDate;
    rec.phase2_1_input_contract_hash = contractHash;

    outC.write(JSON.stringify(rec) + "\n");
    wrote++;

    if (match) {
      matched++;
      if (outA) {
        const att = {
          property_id: rec.property_id ?? null,
          layer_key: "civic_block_groups",
          feature_type: "polygon",
          feature_id: match.geoid ? `civic_block_groups:${match.geoid}` : null,
          geoid: match.geoid ?? null,
          attach_method: "pip_parcel_centroid",
          attach_confidence: "A",
          attach_as_of_date: asOfDate,
          dataset_hash: layerHash,
        };
        outA.write(JSON.stringify(att) + "\n");
      }
    }

    if (read % 200000 === 0) {
      console.log(`[prog] read=${read} wrote=${wrote} matched=${matched} bad_json=${bad}`);
    }
  }

  outC.end();
  if (outA) outA.end();

  // Wait for streams to close
  await new Promise((r) => outC.on("close", r));
  if (outA) await new Promise((r) => outA.on("close", r));

  const outContractHash = await sha256FileStream(outContract);

  const manifest = {
    phase: "phase2_1_block_groups",
    version: "v2",
    created_at: new Date().toISOString(),
    as_of_date: asOfDate,
    inputs: {
      contract_in: contractIn,
      contract_in_sha256: contractHash,
      block_groups: blockGroups,
      block_groups_sha256: layerHash,
    },
    outputs: {
      freeze_dir: freezeDir,
      frozen_layer: frozenLayerPath,
      out_contract: outContract,
      out_contract_sha256: outContractHash,
      out_attachments: outAttachments || null,
    },
    stats: {
      contract_lines_read: read,
      contract_lines_written: wrote,
      bad_json: bad,
      matched: matched,
    },
  };
  fs.writeFileSync(path.join(freezeDir, "MANIFEST.json"), JSON.stringify(manifest, null, 2), "utf-8");

  console.log("[done] read=%d wrote=%d matched=%d bad_json=%d", read, wrote, matched, bad);
  console.log("[done] out_contract_sha256:", outContractHash);
  console.log("[ok] wrote layer meta:", path.join(civicDir, "LAYER_META.json"));
  console.log("[ok] wrote manifest:", path.join(freezeDir, "MANIFEST.json"));
}

main().catch((e) => die(e?.stack || String(e)));
