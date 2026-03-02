#!/usr/bin/env node
/**
 * Validate TierC quarantine suggestions using Town PIP in STATEPLANE (EPSG:26986),
 * and apply mad_suggest ONLY when:
 *   pipTown(x_sp,y_sp) === mad_suggest.town
 *
 * This is an "institutional" check (GIS truth) to safely promote quarantined rows.
 *
 * Inputs:
 *  --base      path to big NDJSON base (must include row_uid, x_sp, y_sp)
 *  --quarantine path to quarantine NDJSON (must include row_uid, mad_suggest{town,zip,street_no,street_name})
 *  --townsGeo  town boundaries GeoJSON in EPSG:26986 (your townBoundaries.geojson)
 *  --out       output base NDJSON with applied patches
 *  --report    output report JSON
 *
 * Optional:
 *  --outAuto       NDJSON of quarantine rows that were applied
 *  --outKeep       NDJSON of quarantine rows kept (not applied)
 *  --outUnresolved NDJSON of quarantine rows unresolved (missing fields, no PIP, etc.)
 */

import fs from "fs";
import path from "path";
import readline from "readline";

function argMap(argv) {
  const m = new Map();
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1];
    if (v && !v.startsWith("--")) {
      m.set(k, v);
      i++;
    } else {
      m.set(k, true);
    }
  }
  return m;
}

function die(msg) {
  console.error(msg);
  process.exit(1);
}

function exists(p) {
  try { fs.accessSync(p, fs.constants.R_OK); return true; } catch { return false; }
}

function normTown(s) {
  if (!s) return "";
  let t = String(s).toUpperCase().trim();
  // light normalization only (don’t get fancy)
  t = t.replace(/^TOWN OF\s+/, "");
  t = t.replace(/^CITY OF\s+/, "");
  t = t.replace(/\s+/g, " ");
  return t;
}

function normStreet(s) {
  if (!s) return "";
  return String(s).toUpperCase().trim().replace(/\s+/g, " ");
}

function bboxFromCoords(coords, bbox = [Infinity, Infinity, -Infinity, -Infinity]) {
  // bbox: [minX, minY, maxX, maxY]
  if (!coords) return bbox;
  if (typeof coords[0] === "number" && typeof coords[1] === "number") {
    const x = coords[0], y = coords[1];
    if (x < bbox[0]) bbox[0] = x;
    if (y < bbox[1]) bbox[1] = y;
    if (x > bbox[2]) bbox[2] = x;
    if (y > bbox[3]) bbox[3] = y;
    return bbox;
  }
  for (const c of coords) bboxFromCoords(c, bbox);
  return bbox;
}

function pointInRing(x, y, ring) {
  // ray casting; ring is array of [x,y]
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0], yi = ring[i][1];
    const xj = ring[j][0], yj = ring[j][1];
    const intersect =
      ((yi > y) !== (yj > y)) &&
      (x < ((xj - xi) * (y - yi)) / (yj - yi + 0.0) + xi);
    if (intersect) inside = !inside;
  }
  return inside;
}

function pointInPolygonGeom(x, y, geom) {
  // GeoJSON Polygon or MultiPolygon
  if (!geom) return false;
  if (geom.type === "Polygon") {
    const rings = geom.coordinates;
    if (!rings?.length) return false;
    if (!pointInRing(x, y, rings[0])) return false; // outside outer ring
    // holes
    for (let r = 1; r < rings.length; r++) {
      if (pointInRing(x, y, rings[r])) return false;
    }
    return true;
  }
  if (geom.type === "MultiPolygon") {
    for (const poly of geom.coordinates) {
      const rings = poly;
      if (!rings?.length) continue;
      if (!pointInRing(x, y, rings[0])) continue;
      let inHole = false;
      for (let r = 1; r < rings.length; r++) {
        if (pointInRing(x, y, rings[r])) { inHole = true; break; }
      }
      if (!inHole) return true;
    }
    return false;
  }
  return false;
}

function inBbox(x, y, b) {
  return x >= b[0] && x <= b[2] && y >= b[1] && y <= b[3];
}

async function readNdjsonStream(filePath, onObj) {
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });
  let n = 0;
  for await (const line of rl) {
    if (!line) continue;
    n++;
    let o;
    try { o = JSON.parse(line); }
    catch (e) { onObj(null, e, n); continue; }
    onObj(o, null, n);
  }
  return n;
}

(async function main() {
  const args = argMap(process.argv);

  const basePath = args.get("base");
  const quarantinePath = args.get("quarantine");
  const townsGeoPath = args.get("townsGeo");
  const outPath = args.get("out");
  const reportPath = args.get("report");

  if (!basePath || !quarantinePath || !townsGeoPath || !outPath || !reportPath) {
    die("Missing args. Required: --base --quarantine --townsGeo --out --report");
  }

  for (const p of [basePath, quarantinePath, townsGeoPath]) {
    if (!exists(p)) die(`Input not found: ${p}`);
  }

  const outAuto = args.get("outAuto");
  const outKeep = args.get("outKeep");
  const outUnresolved = args.get("outUnresolved");

  console.log("====================================================");
  console.log("Validate TierC Quarantine by Town PIP (STATEPLANE) + apply mad_suggest");
  console.log("====================================================");
  console.log("BASE:", basePath);
  console.log("QUAR:", quarantinePath);
  console.log("TOWN:", townsGeoPath);
  console.log("OUT :", outPath);

  // Load towns
  const towns = JSON.parse(fs.readFileSync(townsGeoPath, "utf8"));
if (!towns?.features?.length) die("townsGeo has no features");
const feats = towns.features || [];

  // Build town index by bbox
  const townIndex = [];
  const townGeomCounts = { Polygon: 0, MultiPolygon: 0, Other: 0 };
  for (const f of feats) {
    const town = normTown(f.properties?.TOWN ?? f.properties?.town ?? f.properties?.NAME ?? f.properties?.name);
    if (!town) continue;
    const geom = f.geometry;
    if (!geom) continue;
    if (geom.type === "Polygon") townGeomCounts.Polygon++;
    else if (geom.type === "MultiPolygon") townGeomCounts.MultiPolygon++;
    else townGeomCounts.Other++;
    const b = bboxFromCoords(geom.coordinates);
    townIndex.push({ town, geom, bbox: b });
  }

  console.log("Towns indexed:", townIndex.length, "geomCounts:", townGeomCounts);

  function pipTownForXY(x, y) {
    // bbox filter then pip
    for (const t of townIndex) {
      if (!inBbox(x, y, t.bbox)) continue;
      if (pointInPolygonGeom(x, y, t.geom)) return t.town;
    }
    return null;
  }

  // Build PatchMap from quarantine: row_uid -> patch fields
  const patchMap = new Map();

  const qCounts = {
    qTotal: 0,
    qParseErr: 0,
    qNoRowUid: 0,
    qNoSuggest: 0,
    qNoXY: 0,
    qNoPipTown: 0,
    qTownMismatch_keep: 0,
    qPromote: 0,
    patchKeys: 0,
  };

  const autoW = outAuto ? fs.createWriteStream(outAuto, { encoding: "utf8" }) : null;
  const keepW = outKeep ? fs.createWriteStream(outKeep, { encoding: "utf8" }) : null;
  const unrW = outUnresolved ? fs.createWriteStream(outUnresolved, { encoding: "utf8" }) : null;

  console.log("Building PatchMap from quarantine...");
  await readNdjsonStream(quarantinePath, (o, err) => {
    qCounts.qTotal++;
    if (err || !o) { qCounts.qParseErr++; return; }

    const row_uid = o.row_uid;
    if (!row_uid) {
      qCounts.qNoRowUid++;
      if (unrW) unrW.write(JSON.stringify(o) + "\n");
      return;
    }

    const ms = o.mad_suggest;
    if (!ms || !ms.town || !ms.street_no || !ms.street_name) {
      qCounts.qNoSuggest++;
      if (unrW) unrW.write(JSON.stringify(o) + "\n");
      return;
    }

    const x = o.x_sp;
    const y = o.y_sp;
    if (typeof x !== "number" || typeof y !== "number") {
      qCounts.qNoXY++;
      if (unrW) unrW.write(JSON.stringify(o) + "\n");
      return;
    }

    const pipTown = pipTownForXY(x, y);
    if (!pipTown) {
      qCounts.qNoPipTown++;
      if (unrW) unrW.write(JSON.stringify(o) + "\n");
      return;
    }

    const sugTown = normTown(ms.town);
    if (pipTown !== sugTown) {
      qCounts.qTownMismatch_keep++;
      if (keepW) keepW.write(JSON.stringify(o) + "\n");
      return;
    }

    // Promote: town confirmed by PIP matches suggested town
    qCounts.qPromote++;
    const patch = {
      row_uid,
      town: pipTown,
      zip: ms.zip ? String(ms.zip).trim() : (o.zip ?? null),
      street_no: String(ms.street_no).trim(),
      street_name: normStreet(ms.street_name),
      distM: ms.distM ?? null,
      at: new Date().toISOString(),
    };
    patchMap.set(row_uid, patch);
    if (autoW) autoW.write(JSON.stringify(o) + "\n");
  });

  qCounts.patchKeys = patchMap.size;

  console.log("PatchMap built:", {
    qTotal: qCounts.qTotal,
    qParseErr: qCounts.qParseErr,
    qNoRowUid: qCounts.qNoRowUid,
    qNoSuggest: qCounts.qNoSuggest,
    qNoXY: qCounts.qNoXY,
    qNoPipTown: qCounts.qNoPipTown,
    qTownMismatch_keep: qCounts.qTownMismatch_keep,
    qPromote: qCounts.qPromote,
    patchKeys: qCounts.patchKeys,
  });

  // Apply patches to base stream
  console.log("Applying patches to base...");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  const outW = fs.createWriteStream(outPath, { encoding: "utf8" });

  const applyCounts = {
    base_total: 0,
    base_parseErr: 0,
    applied_to_base_rows: 0,
    applied_unique_keys: patchMap.size,
  };

  let lastLog = 0;
  await readNdjsonStream(basePath, (o, err, n) => {
    applyCounts.base_total++;
    if (err || !o) { applyCounts.base_parseErr++; return; }

    const row_uid = o.row_uid;
    const patch = row_uid ? patchMap.get(row_uid) : null;

    if (patch) {
      applyCounts.applied_to_base_rows++;

      // Apply suggested fields
      o.street_no = patch.street_no;
      o.street_name = patch.street_name;
      o.town = patch.town;
      if (patch.zip) o.zip = patch.zip;

      // Normalize derived fields (keep MA constant)
      const full = `${o.street_no} ${o.street_name}`.trim();
      o.full_address = full;

      // Address label (safe + consistent)
      const zip = o.zip ? String(o.zip).trim() : "";
      o.address_label = zip
        ? `${full}, ${o.town}, MA ${zip}`
        : `${full}, ${o.town}, MA`;

      // Rebuild keys (matches your existing style)
      o.address_key_version = o.address_key_version ?? "v1";
      o.address_key = `A|${o.street_no}|${o.street_name}|${o.town}|${zip}`;
      o.site_key = `${o.street_name}|${o.town}|${zip}`;

      // Evidence / audit
      o.address_verified = o.address_verified || {};
      o.address_verified.town_pip_stateplane_mad_suggest = {
        verifiedTown: patch.town,
        distM: patch.distM,
        at: patch.at,
        rule: "pipTown(x_sp,y_sp) == mad_suggest.town",
      };
      o.address_tier_reason = "quarantine_promoted_by_town_pip_stateplane";
    }

    outW.write(JSON.stringify(o) + "\n");

    if (n && n - lastLog >= 500000) {
      lastLog = n;
      console.log(`...processed base ${n.toLocaleString()} rows`);
    }
  });

  outW.end();
  if (autoW) autoW.end();
  if (keepW) keepW.end();
  if (unrW) unrW.end();

  const report = {
    created_at: new Date().toISOString(),
    in: { basePath, quarantinePath, townsGeoPath },
    out: { outPath, reportPath, outAuto, outKeep, outUnresolved },
    counts: { ...qCounts, ...applyCounts },
    note:
      "Promotes only when GIS town PIP (stateplane) matches mad_suggest.town. No guessing.",
  };

  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
  console.log("DONE.");
  console.log(JSON.stringify(report.counts, null, 2));
})().catch((e) => {
  console.error("FATAL:", e);
  process.exit(1);
});
