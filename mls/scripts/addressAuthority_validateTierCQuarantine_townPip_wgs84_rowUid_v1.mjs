#!/usr/bin/env node
/**
 * addressAuthority_validateTierCQuarantine_townPip_wgs84_rowUid_v1.mjs
 *
 * Validate TierC quarantine rows (typically townMismatch) by checking whether the MAD candidate point
 * falls inside the BASE town polygon (town boundaries in WGS84).
 *
 * This is designed to salvage "wrong town label" cases while keeping strict geospatial evidence.
 *
 * Requires base rows to have `row_uid` so patches apply 1:1 (avoids property_id duplicate blast radius).
 *
 * Usage:
 *   node ./mls/scripts/addressAuthority_validateTierCQuarantine_townPip_wgs84_rowUid_v1.mjs `
 *     --base <BASE_with_row_uid.ndjson> `
 *     --quarantine <QUAR.ndjson> `
 *     --townsGeo <townBoundaries_wgs84.geojson> `
 *     --out <OUT.ndjson> `
 *     --report <REPORT.json>
 *
 * Behavior:
 * - Builds a patch map from quarantine keyed by row_uid when:
 *     candidate_point ∈ polygon(baseTown)
 * - Applies patches to base by row_uid (streaming).
 *
 * Candidate field detection:
 * - Looks for candidate lat/lng in common keys like `cand_lat/cand_lng`, `mad_lat/mad_lon`, `candidate.lat`, etc.
 * - Looks for candidate street fields similarly.
 *
 * If it can't find candidate point coords for a quarantine row, it will skip it.
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
function ensureDir(p) { fs.mkdirSync(path.dirname(p), { recursive: true }); }
function isBlank(v) { return v === null || v === undefined || (typeof v === "string" && v.trim() === ""); }
function toNum(v) { const n = typeof v === "number" ? v : Number(String(v).trim()); return Number.isFinite(n) ? n : null; }
function pickFirst(obj, keys) {
  for (const k of keys) {
    const v = obj?.[k];
    if (v !== undefined && v !== null && String(v).trim() !== "") return v;
  }
  return null;
}
function normTown(s) { return isBlank(s) ? "" : String(s).toUpperCase().replace(/\s+/g, " ").trim(); }

// PIP helpers (same as zip script)
function bboxOfCoords(coords, bbox) {
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
  if (!polyCoords || polyCoords.length === 0) return false;
  const outer = polyCoords[0];
  if (!pipRing(point, outer)) return false;
  for (let i = 1; i < polyCoords.length; i++) if (pipRing(point, polyCoords[i])) return false;
  return true;
}
function pipGeometry(point, geom) {
  if (!geom) return false;
  if (geom.type === "Polygon") return pipPolygon(point, geom.coordinates);
  if (geom.type === "MultiPolygon") {
    for (const poly of geom.coordinates) if (pipPolygon(point, poly)) return true;
  }
  return false;
}

function loadTownIndex(townsGeoPath) {
  const raw = fs.readFileSync(townsGeoPath, "utf8");
  const gj = JSON.parse(raw);
  if (!gj || !Array.isArray(gj.features)) throw new Error("townsGeo must be a FeatureCollection");
  const idx = new Map(); // TOWN -> {geom,bbox}
  let geomCounts = { Polygon: 0, MultiPolygon: 0 };
  for (const f of gj.features) {
    const props = f.properties || {};
    const town = normTown(props.TOWN ?? props.town ?? props.NAME ?? props.name);
    const geom = f.geometry;
    if (!town || !geom) continue;
    geomCounts[geom.type] = (geomCounts[geom.type] || 0) + 1;
    const bbox = bboxOfCoords(geom.coordinates, { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity });
    idx.set(town, { geom, bbox });
  }
  return { idx, geomCounts };
}

function pointInTown(townIndex, town, lng, lat) {
  const rec = townIndex.get(town);
  if (!rec) return false;
  const b = rec.bbox;
  if (lng < b.minX || lng > b.maxX || lat < b.minY || lat > b.maxY) return false;
  return pipGeometry([lng, lat], rec.geom);
}

function getCandidateLatLng(qrow) {
  // try nested and flat keys
  const cand = qrow.candidate ?? qrow.mad ?? qrow.nearest ?? null;
  const lat = toNum(
    pickFirst(qrow, ["cand_lat","candidate_lat","mad_lat","nearest_lat","match_lat","lat_cand"]) ??
    pickFirst(cand, ["lat","candidate_lat","mad_lat","nearest_lat","match_lat"])
  );
  const lng = toNum(
    pickFirst(qrow, ["cand_lng","cand_lon","candidate_lng","candidate_lon","mad_lon","mad_lng","nearest_lon","nearest_lng","match_lon","match_lng","lon_cand","lng_cand"]) ??
    pickFirst(cand, ["lng","lon","mad_lon","mad_lng","nearest_lon","nearest_lng","match_lon","match_lng"])
  );
  return { lat, lng };
}

function extractCandidateAddress(qrow) {
  const cand = qrow.candidate ?? qrow.mad ?? qrow.nearest ?? null;
  const street_no = pickFirst(qrow, ["cand_street_no","candidate_street_no","mad_street_no","match_street_no"]) ?? pickFirst(cand, ["street_no","house_number","ADDRNUM","number"]);
  const street_name = pickFirst(qrow, ["cand_street_name","candidate_street_name","mad_street_name","match_street_name"]) ?? pickFirst(cand, ["street_name","street","STREETNAME","road","ROAD","name"]);
  const zip = pickFirst(qrow, ["cand_zip","candidate_zip","mad_zip","match_zip"]) ?? pickFirst(cand, ["zip","postcode","ZIP","POSTCODE"]);
  return {
    street_no: isBlank(street_no) ? null : String(street_no).trim(),
    street_name: isBlank(street_name) ? null : String(street_name).trim(),
    zip: isBlank(zip) ? null : String(zip).trim(),
  };
}

async function main() {
  const args = parseArgs(process.argv);
  const basePath = args.base;
  const quarantinePath = args.quarantine;
  const townsGeoPath = args.townsGeo;
  const outPath = args.out;
  const reportPath = args.report;

  if (!basePath || !quarantinePath || !townsGeoPath || !outPath || !reportPath) {
    console.error("Missing args. Required: --base --quarantine --townsGeo --out --report");
    process.exit(1);
  }
  if (!fs.existsSync(basePath)) { console.error("Base not found:", basePath); process.exit(1); }
  if (!fs.existsSync(quarantinePath)) { console.error("Quarantine not found:", quarantinePath); process.exit(1); }
  if (!fs.existsSync(townsGeoPath)) { console.error("townsGeo not found:", townsGeoPath); process.exit(1); }

  ensureDir(outPath); ensureDir(reportPath);

  console.log("====================================================");
  console.log("Validate TierC Quarantine by Town PIP (WGS84) + row_uid apply");
  console.log("====================================================");
  console.log("BASE:", basePath);
  console.log("QUAR:", quarantinePath);
  console.log("TOWN:", townsGeoPath);
  console.log("OUT :", outPath);

  const { idx: townIndex, geomCounts } = loadTownIndex(townsGeoPath);
  console.log("Towns indexed:", townIndex.size, "geomCounts:", geomCounts);

  // Build patch map from quarantine keyed by row_uid
  const patchMap = new Map();
  const qr = fs.createReadStream(quarantinePath, { encoding: "utf8" });
  const qrl = readline.createInterface({ input: qr, crlfDelay: Infinity });

  let qTotal = 0, qParseErr = 0, qNoRowUid = 0, qNoCand = 0, qNoTown = 0, qNoPip = 0, qPromote = 0;
  let sample = null;

  for await (const line of qrl) {
    if (!line) continue;
    qTotal++;
    let qrow;
    try { qrow = JSON.parse(line); } catch { qParseErr++; continue; }

    const row_uid = qrow.row_uid ?? qrow._row_uid ?? null;
    if (isBlank(row_uid)) { qNoRowUid++; continue; }

    const baseTown = normTown(qrow.town ?? qrow.base_town ?? qrow.baseTown);
    if (!baseTown) { qNoTown++; continue; }

    const { lat, lng } = getCandidateLatLng(qrow);
    if (lat === null || lng === null) { qNoCand++; continue; }

    if (!pointInTown(townIndex, baseTown, lng, lat)) { qNoPip++; continue; }

    const candAddr = extractCandidateAddress(qrow);
    patchMap.set(String(row_uid), {
      street_no: candAddr.street_no,
      street_name: candAddr.street_name,
      zip: candAddr.zip,
      cand_lat: lat,
      cand_lng: lng,
    });
    qPromote++;

    if (!sample) sample = { row_uid, baseTown, cand: { lat, lng, ...candAddr } };
  }

  console.log("PatchMap built:", { qTotal, qParseErr, qNoRowUid, qNoTown, qNoCand, qNoPip, qPromote, patchKeys: patchMap.size });

  // Apply patches to base
  const br = fs.createReadStream(basePath, { encoding: "utf8" });
  const brl = readline.createInterface({ input: br, crlfDelay: Infinity });
  const ws = fs.createWriteStream(outPath, { encoding: "utf8" });

  let baseTotal = 0, baseParseErr = 0, applied = 0, baseMissingRowUid = 0;

  for await (const line of brl) {
    if (!line) continue;
    baseTotal++;
    let row;
    try { row = JSON.parse(line); } catch { baseParseErr++; continue; }

    const row_uid = row.row_uid ?? row._row_uid ?? null;
    if (isBlank(row_uid)) { baseMissingRowUid++; ws.write(JSON.stringify(row) + "\n"); continue; }

    const patch = patchMap.get(String(row_uid));
    if (patch) {
      if (!isBlank(patch.street_no)) row.street_no = patch.street_no;
      if (!isBlank(patch.street_name)) row.street_name = patch.street_name;
      if (isBlank(row.zip) && !isBlank(patch.zip)) row.zip = patch.zip;

      row.addr_quarantine_revalidated = {
        method: "townPip:wgs84",
        asOf: new Date().toISOString(),
        cand: { lat: patch.cand_lat, lng: patch.cand_lng },
      };
      applied++;
    }

    ws.write(JSON.stringify(row) + "\n");
    if (baseTotal % 500000 === 0) console.log(`...processed base ${baseTotal.toLocaleString()} rows`);
  }

  ws.end();

  const report = {
    created_at: new Date().toISOString(),
    in: { basePath, quarantinePath, townsGeoPath },
    out: outPath,
    counts: {
      base_total: baseTotal,
      base_parseErr: baseParseErr,
      base_missing_row_uid: baseMissingRowUid,
      quarantine_total: qTotal,
      quarantine_parseErr: qParseErr,
      quarantine_noRowUid: qNoRowUid,
      quarantine_noTown: qNoTown,
      quarantine_noCandidateLatLon: qNoCand,
      quarantine_noPipTown: qNoPip,
      quarantine_promoted: qPromote,
      patchKeys: patchMap.size,
      applied_to_base_rows: applied,
    },
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
