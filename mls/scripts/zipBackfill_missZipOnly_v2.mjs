#!/usr/bin/env node
/**
 * zipBackfill_missZipOnly_v2.mjs
 * Stream NDJSON, and for rows missing zip, fill it via point-in-polygon against ZIP polygons (WGS84).
 *
 * Usage:
 *   node ./mls/scripts/zipBackfill_missZipOnly_v2.mjs --in <IN.ndjson> --zipGeo <ZIP.geojson> --out <OUT.ndjson> --report <REPORT.json>
 *
 * Notes:
 * - Assumes point coordinates in the input are WGS84 (lat/lng).
 * - Only modifies rows where zip is null/empty.
 * - No external deps (implements a basic PIP).
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
      const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
      out[k] = v;
    }
  }
  return out;
}

function isBlank(v) {
  return v === null || v === undefined || (typeof v === "string" && v.trim() === "");
}

function toNum(v) {
  const n = typeof v === "number" ? v : Number(String(v).trim());
  return Number.isFinite(n) ? n : null;
}

function getLatLng(row) {
  // try common keys
  const lat = toNum(row.lat ?? row.latitude ?? row.y ?? row.Y);
  const lng = toNum(row.lng ?? row.lon ?? row.longitude ?? row.x ?? row.X);
  return { lat, lng };
}

function bboxOfCoords(coords, bbox) {
  // coords can be nested arrays; recurse until [x,y]
  if (!Array.isArray(coords)) return bbox;
  if (coords.length === 0) return bbox;
  if (typeof coords[0] === "number" && typeof coords[1] === "number") {
    const x = coords[0], y = coords[1];
    if (x < bbox.minX) bbox.minX = x;
    if (x > bbox.maxX) bbox.maxX = x;
    if (y < bbox.minY) bbox.minY = y;
    if (y > bbox.maxY) bbox.maxY = y;
    return bbox;
  }
  for (const c of coords) bboxOfCoords(c, bbox);
  return bbox;
}

// Ray-casting for a ring (array of [x,y])
function pipRing(point, ring) {
  const x = point[0], y = point[1];
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0], yi = ring[i][1];
    const xj = ring[j][0], yj = ring[j][1];
    const intersect = ((yi > y) !== (yj > y)) && (x < ((xj - xi) * (y - yi)) / (yj - yi + 0.0) + xi);
    if (intersect) inside = !inside;
  }
  return inside;
}

function pipPolygon(point, polyCoords) {
  // polyCoords: [outerRing, hole1, hole2...]
  if (!polyCoords || polyCoords.length === 0) return false;
  const outer = polyCoords[0];
  if (!pipRing(point, outer)) return false;
  // exclude holes
  for (let i = 1; i < polyCoords.length; i++) {
    if (pipRing(point, polyCoords[i])) return false;
  }
  return true;
}

function pipGeometry(point, geom) {
  if (!geom) return false;
  if (geom.type === "Polygon") return pipPolygon(point, geom.coordinates);
  if (geom.type === "MultiPolygon") {
    for (const poly of geom.coordinates) {
      if (pipPolygon(point, poly)) return true;
    }
    return false;
  }
  return false;
}

function normalizeZipProps(props) {
  // try common prop names
  const v =
    props.ZIP ?? props.ZIPCODE ?? props.zip ?? props.zipcode ?? props.POSTCODE ?? props.postcode ?? props.ZCTA5CE10 ?? props.ZCTA5CE20;
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  if (!s) return null;
  // keep leading zeros
  const m = s.match(/\d{5}/);
  return m ? m[0] : s;
}

function loadZipIndex(zipGeoPath) {
  const raw = fs.readFileSync(zipGeoPath, "utf8");
  const gj = JSON.parse(raw);
  if (!gj || !Array.isArray(gj.features)) throw new Error("zipGeo must be a FeatureCollection");
  const feats = [];
  for (const f of gj.features) {
    const geom = f.geometry;
    const props = f.properties || {};
    const zip = normalizeZipProps(props);
    if (!zip || !geom) continue;
    const bbox = bboxOfCoords(geom.coordinates, { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity });
    feats.push({ zip, geom, bbox });
  }
  return feats;
}

function findZipForPoint(zipIndex, lng, lat) {
  const p = [lng, lat];
  for (const z of zipIndex) {
    const b = z.bbox;
    if (lng < b.minX || lng > b.maxX || lat < b.minY || lat > b.maxY) continue;
    if (pipGeometry(p, z.geom)) return z.zip;
  }
  return null;
}

async function main() {
  const args = parseArgs(process.argv);
  const inPath = args.in;
  const zipGeo = args.zipGeo;
  const outPath = args.out;
  const reportPath = args.report;

  if (!inPath || !zipGeo || !outPath || !reportPath) {
    console.error("Missing args. Required: --in --zipGeo --out --report");
    process.exit(1);
  }
  if (!fs.existsSync(inPath)) {
    console.error("Input not found:", inPath);
    process.exit(1);
  }
  if (!fs.existsSync(zipGeo)) {
    console.error("zipGeo not found:", zipGeo);
    process.exit(1);
  }
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.mkdirSync(path.dirname(reportPath), { recursive: true });

  console.log("=====================================");
  console.log("ZIP Backfill (missZip only)");
  console.log("=====================================");
  console.log("IN   :", inPath);
  console.log("ZIPG :", zipGeo);
  console.log("OUT  :", outPath);
  console.log("REP  :", reportPath);

  const zipIndex = loadZipIndex(zipGeo);
  console.log("Zip features indexed:", zipIndex.length);

  const rs = fs.createReadStream(inPath, { encoding: "utf8" });
  const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });
  const ws = fs.createWriteStream(outPath, { encoding: "utf8" });

  let total = 0;
  let parseErr = 0;
  let scannedMissingZip = 0;
  let filledZip = 0;
  let noCoords = 0;
  let noHit = 0;
  let sample = null;

  for await (const line of rl) {
    if (!line) continue;
    total++;
    let row;
    try {
      row = JSON.parse(line);
    } catch {
      parseErr++;
      continue;
    }

    const origZip = row.zip;
    if (isBlank(origZip)) {
      scannedMissingZip++;
      const { lat, lng } = getLatLng(row);
      if (lat === null || lng === null) {
        noCoords++;
      } else {
        const z = findZipForPoint(zipIndex, lng, lat);
        if (z) {
          row.zip = z;
          row.zip_source = row.zip_source ?? "zipcodes_poly_backfill";
          row.zip_fix = { method: "pip:zipGeo", asOf: new Date().toISOString() };
          filledZip++;
          if (!sample) {
            sample = {
              before: { zip: origZip ?? null, lat, lng, town: row.town ?? null, street: row.street_name ?? null, no: row.street_no ?? null },
              after: { zip: z },
            };
          }
        } else {
          noHit++;
        }
      }
    }

    ws.write(JSON.stringify(row) + "\n");

    if (total % 500000 === 0) console.log(`...processed ${total.toLocaleString()} rows`);
  }

  ws.end();

  const report = {
    created_at: new Date().toISOString(),
    in: inPath,
    zipGeo,
    out: outPath,
    total_rows: total,
    parseErr,
    missingZip_rows: scannedMissingZip,
    filledZip_rows: filledZip,
    noCoords_rows: noCoords,
    noHit_rows: noHit,
    sample,
  };

  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
  console.log("DONE.");
  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error("FATAL:", e?.stack || e);
  process.exit(1);
});
