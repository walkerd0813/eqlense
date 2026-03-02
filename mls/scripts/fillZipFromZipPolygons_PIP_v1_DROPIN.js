#!/usr/bin/env node
/**
 * fillZipFromZipPolygons_PIP_v1_DROPIN.js
 * Institutional ZIP fill:
 * - If row.zip is missing, fill via Point-in-Polygon using ZIP/ZCTA polygons (GeoJSON)
 * - Uses a lightweight grid index over polygon bboxes for speed
 * - Logs lineage: zip_source, zip_method_version, zip_patched_at
 *
 * Usage:
 * node mls/scripts/fillZipFromZipPolygons_PIP_v1_DROPIN.js ^
 *   --in <properties.ndjson> ^
 *   --zipPolygons <zip_polygons.geojson> ^
 *   --out <properties_out.ndjson> ^
 *   --meta <meta.json> ^
 *   [--zipField <fieldName>] ^
 *   [--cellSize 0.05]
 */

import fs from "fs";
import path from "path";
import readline from "readline";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1];
      if (!v || v.startsWith("--")) out[k] = true;
      else {
        out[k] = v;
        i++;
      }
    }
  }
  return out;
}

function nowISO() { return new Date().toISOString(); }
function die(msg) { console.error(`\n❌ ${msg}\n`); process.exit(1); }
function exists(fp) { return fs.existsSync(fp); }

function normStr(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s.length ? s : null;
}

function isZipMissing(v) {
  const s = normStr(v);
  if (!s) return true;
  const digits = s.replace(/\D/g, "");
  return digits.length < 5;
}

function getLatLng(row) {
  const lat = row.lat ?? row.latitude;
  const lng = row.lng ?? row.lon ?? row.longitude;
  const latNum = typeof lat === "string" ? Number(lat) : lat;
  const lngNum = typeof lng === "string" ? Number(lng) : lng;
  if (!Number.isFinite(latNum) || !Number.isFinite(lngNum)) return null;
  return { lat: latNum, lng: lngNum };
}

function pointInRing(pt, ring) {
  // Ray casting; ring is [[x,y],...]
  let inside = false;
  const x = pt[0], y = pt[1];
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
  // polyCoords = [outerRing, holeRing1, holeRing2...]
  const outer = polyCoords?.[0];
  if (!outer || outer.length < 3) return false;
  if (!pointInRing(pt, outer)) return false;
  for (let h = 1; h < polyCoords.length; h++) {
    const hole = polyCoords[h];
    if (hole && hole.length >= 3 && pointInRing(pt, hole)) return false;
  }
  return true;
}

function pointInGeom(pt, geom) {
  if (!geom) return false;
  if (geom.type === "Polygon") {
    return pointInPolygon(pt, geom.coordinates);
  }
  if (geom.type === "MultiPolygon") {
    for (const poly of geom.coordinates) {
      if (pointInPolygon(pt, poly)) return true;
    }
    return false;
  }
  return false;
}

function bboxOfGeom(geom) {
  // returns [minX,minY,maxX,maxY]
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;

  const scanRing = (ring) => {
    for (const p of ring) {
      const x = p[0], y = p[1];
      if (x < minX) minX = x;
      if (y < minY) minY = y;
      if (x > maxX) maxX = x;
      if (y > maxY) maxY = y;
    }
  };

  if (geom.type === "Polygon") {
    for (const ring of geom.coordinates) scanRing(ring);
  } else if (geom.type === "MultiPolygon") {
    for (const poly of geom.coordinates) for (const ring of poly) scanRing(ring);
  } else {
    return null;
  }

  if (!Number.isFinite(minX)) return null;
  return [minX, minY, maxX, maxY];
}

function autoDetectZipField(props, forced) {
  if (forced) return forced;
  const candidates = ["ZIP", "zip", "ZIPCODE", "ZipCode", "ZCTA5CE10", "ZCTA5", "GEOID10", "GEOID", "POSTCODE"];
  for (const k of candidates) if (props && props[k] != null) return k;
  return null;
}

function buildGridIndex(polys, cellSize) {
  // Global bbox
  let gminX = Infinity, gminY = Infinity, gmaxX = -Infinity, gmaxY = -Infinity;
  for (const p of polys) {
    const [minX, minY, maxX, maxY] = p.bbox;
    if (minX < gminX) gminX = minX;
    if (minY < gminY) gminY = minY;
    if (maxX > gmaxX) gmaxX = maxX;
    if (maxY > gmaxY) gmaxY = maxY;
  }

  const key = (ix, iy) => `${ix}|${iy}`;
  const grid = new Map();

  const toIx = (x) => Math.floor((x - gminX) / cellSize);
  const toIy = (y) => Math.floor((y - gminY) / cellSize);

  polys.forEach((p, idx) => {
    const [minX, minY, maxX, maxY] = p.bbox;
    const ix0 = toIx(minX), ix1 = toIx(maxX);
    const iy0 = toIy(minY), iy1 = toIy(maxY);
    for (let ix = ix0; ix <= ix1; ix++) {
      for (let iy = iy0; iy <= iy1; iy++) {
        const k = key(ix, iy);
        if (!grid.has(k)) grid.set(k, []);
        grid.get(k).push(idx);
      }
    }
  });

  return { grid, gminX, gminY, cellSize, key, toIx, toIy };
}

async function main() {
  const args = parseArgs(process.argv);
  const inPath = args.in;
  const zipPolygonsPath = args.zipPolygons;
  const outPath = args.out;
  const metaPath = args.meta;
  const forcedZipField = args.zipField ? String(args.zipField) : null;
  const cellSize = args.cellSize ? Number(args.cellSize) : 0.05;

  if (!inPath || !zipPolygonsPath || !outPath || !metaPath) {
    console.log(`Usage:
node mls/scripts/fillZipFromZipPolygons_PIP_v1_DROPIN.js ^
  --in <properties.ndjson> ^
  --zipPolygons <zip_polygons.geojson> ^
  --out <out.ndjson> ^
  --meta <meta.json> ^
  [--zipField <fieldName>] ^
  [--cellSize 0.05]
`);
    process.exit(1);
  }

  if (!exists(inPath)) die(`--in not found: ${inPath}`);
  if (!exists(zipPolygonsPath)) die(`--zipPolygons not found: ${zipPolygonsPath}`);

  const inAbs = path.resolve(inPath);
  const outAbs = path.resolve(outPath);
  if (inAbs === outAbs) die("--out must differ from --in");

  console.log("====================================================");
  console.log(" ZIP BACKFILL VIA ZIP POLYGONS (PIP) — v1 DROPIN");
  console.log("====================================================");
  console.log(`[run] started_at: ${nowISO()}`);
  console.log(`[run] node:       ${process.version}`);
  console.log(`[run] in:         ${inPath}`);
  console.log(`[run] zipPolygons: ${zipPolygonsPath}`);
  console.log(`[run] out:        ${outPath}`);
  console.log(`[run] meta:       ${metaPath}`);
  console.log(`[run] cellSize:   ${cellSize}`);
  console.log(`[run] zipField:   ${forcedZipField ?? "(auto)"}`);
  console.log("----------------------------------------------------");

  // Load ZIP polygons (should be MA-only; not gigantic)
  const zipJson = JSON.parse(fs.readFileSync(zipPolygonsPath, "utf8"));
  const feats = zipJson?.features ?? [];
  if (!Array.isArray(feats) || feats.length === 0) die("zipPolygons has no features[]");

  // Build polygon list + bbox list
  let zipField = forcedZipField;
  if (!zipField) zipField = autoDetectZipField(feats[0]?.properties ?? {}, null);
  if (!zipField) die("Could not auto-detect zip field. Re-run with --zipField <FIELDNAME>.");

  const polygons = [];
  for (const f of feats) {
    const z = normStr(f?.properties?.[zipField]);
    const geom = f?.geometry;
    if (!z || !geom) continue;
    const bbox = bboxOfGeom(geom);
    if (!bbox) continue;
    polygons.push({ zip: z, geom, bbox });
  }
  if (polygons.length === 0) die("No usable polygons found after filtering.");

  console.log(`[zip] features total: ${feats.length.toLocaleString()}`);
  console.log(`[zip] usable polys:   ${polygons.length.toLocaleString()}`);
  console.log(`[zip] zipField used:  ${zipField}`);

  const gridIndex = buildGridIndex(polygons, cellSize);
  console.log(`[zip] grid cells:     ${gridIndex.grid.size.toLocaleString()}`);
  console.log("----------------------------------------------------");

  const meta = {
    script: "fillZipFromZipPolygons_PIP_v1_DROPIN.js",
    started_at: nowISO(),
    args: { in: inPath, zipPolygons: zipPolygonsPath, out: outPath, meta: metaPath, zipField, cellSize },
    counts: {
      total_rows: 0,
      missing_zip_in: 0,
      filled_zip: 0,
      still_missing_zip: 0,
      no_candidates_cell: 0,
      pip_miss: 0,
    },
  };

  const outWS = fs.createWriteStream(outPath, { encoding: "utf8" });
  const rl = readline.createInterface({
    input: fs.createReadStream(inPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    meta.counts.total_rows++;

    let row;
    try { row = JSON.parse(t); } catch { continue; }

    const currentZip = row.zip ?? row.ZIP ?? row.zip_code ?? row.zipCode ?? null;
    if (!isZipMissing(currentZip)) {
      outWS.write(JSON.stringify(row) + "\n");
      continue;
    }

    meta.counts.missing_zip_in++;

    const ll = getLatLng(row);
    if (!ll) {
      meta.counts.still_missing_zip++;
      outWS.write(JSON.stringify(row) + "\n");
      continue;
    }

    const pt = [ll.lng, ll.lat];
    const ix = gridIndex.toIx(pt[0]);
    const iy = gridIndex.toIy(pt[1]);
    const cellKey = gridIndex.key(ix, iy);
    const cand = gridIndex.grid.get(cellKey);

    if (!cand || cand.length === 0) {
      meta.counts.no_candidates_cell++;
      meta.counts.still_missing_zip++;
      outWS.write(JSON.stringify(row) + "\n");
      continue;
    }

    let found = null;

    for (const idx of cand) {
      const p = polygons[idx];
      const [minX, minY, maxX, maxY] = p.bbox;
      if (pt[0] < minX || pt[0] > maxX || pt[1] < minY || pt[1] > maxY) continue;
      if (pointInGeom(pt, p.geom)) { found = p.zip; break; }
    }

    if (!found) {
      meta.counts.pip_miss++;
      meta.counts.still_missing_zip++;
      outWS.write(JSON.stringify(row) + "\n");
      continue;
    }

    row.zip = found;
    row.zip_source = "zipPolygons:pip";
    row.zip_method = "point_in_polygon";
    row.zip_method_version = "v1_DROPIN";
    row.zip_patched_at = nowISO();
    row.zip_grid_cell = cellKey;

    meta.counts.filled_zip++;
    outWS.write(JSON.stringify(row) + "\n");
  }

  outWS.end();
  meta.finished_at = nowISO();
  fs.writeFileSync(metaPath, JSON.stringify(meta, null, 2), "utf8");

  console.log("====================================================");
  console.log("DONE — ZIP BACKFILL (P4)");
  console.log("----------------------------------------------------");
  console.log(`total_rows:        ${meta.counts.total_rows.toLocaleString()}`);
  console.log(`missing_zip_in:    ${meta.counts.missing_zip_in.toLocaleString()}`);
  console.log(`filled_zip:        ${meta.counts.filled_zip.toLocaleString()}`);
  console.log(`still_missing_zip: ${meta.counts.still_missing_zip.toLocaleString()}`);
  console.log(`no_candidates_cell:${meta.counts.no_candidates_cell.toLocaleString()}`);
  console.log(`pip_miss:          ${meta.counts.pip_miss.toLocaleString()}`);
  console.log("====================================================");
}

main().catch((e) => { console.error(e); process.exit(1); });
