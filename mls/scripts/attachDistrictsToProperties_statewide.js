import fs from "fs";
import path from "path";
import crypto from "crypto";
import readline from "readline";
import { fileURLToPath } from "url";

import booleanPointInPolygon from "@turf/boolean-point-in-polygon";
import area from "@turf/area";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const IN_PROPERTIES = path.resolve(
  __dirname,
  "../../publicData/properties/properties_statewide_geo_zip.ndjson"
);

const IN_ZONING = path.resolve(
  __dirname,
  "../../publicData/zoning/zoningBoundariesData_DISTRICTS_v7_wgs84.geojson"
);

const OUT_PROPERTIES = path.resolve(
  __dirname,
  "../../publicData/properties/properties_statewide_geo_zip_district.ndjson"
);

const OUT_META = path.resolve(
  __dirname,
  "../../publicData/properties/properties_statewide_geo_zip_district_meta.json"
);
if (!fs.existsSync(IN_PROPERTIES)) throw new Error(`Missing IN_PROPERTIES: ${IN_PROPERTIES}`);
if (!fs.existsSync(IN_ZONING)) throw new Error(`Missing IN_ZONING: ${IN_ZONING}`);



// grid sizing: keep consistent with what you already proved stable
const CELL_DEG = 0.02;

function sha256File(fp) {
  const h = crypto.createHash("sha256");
  const buf = fs.readFileSync(fp);
  h.update(buf);
  return h.digest("hex");
}

function stageRank(stageRaw) {
  const s = String(stageRaw ?? "").trim().toUpperCase();
  if (!s) return 2; // unknown-ish middle
  if (s.includes("ADOPT") || s.includes("CURRENT") || s.includes("EFFECT") || s.includes("IN FORCE")) return 3;
  if (s.includes("PROPOS") || s.includes("DRAFT") || s.includes("PENDING")) return 1;
  return 2;
}

function stableKey(props) {
  const city = String(props.__city ?? "");
  const norm = String(props.__norm ?? "");
  const file = String(props.__sourceFile ?? "");
  return `${city}||${norm}||${file}`;
}

function cellKey(lng, lat) {
  const x = Math.floor(lng / CELL_DEG);
  const y = Math.floor(lat / CELL_DEG);
  return `${x}|${y}`;
}

function bboxToCells([minX, minY, maxX, maxY]) {
  const x0 = Math.floor(minX / CELL_DEG);
  const x1 = Math.floor(maxX / CELL_DEG);
  const y0 = Math.floor(minY / CELL_DEG);
  const y1 = Math.floor(maxY / CELL_DEG);

  const keys = [];
  for (let x = x0; x <= x1; x++) {
    for (let y = y0; y <= y1; y++) keys.push(`${x}|${y}`);
  }
  return keys;
}

function getBbox(geom) {
  // minimal bbox calc (fast, no extra deps)
  // supports Polygon + MultiPolygon
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;

  const pushCoord = (c) => {
    const [x, y] = c;
    if (x < minX) minX = x;
    if (y < minY) minY = y;
    if (x > maxX) maxX = x;
    if (y > maxY) maxY = y;
  };

  if (!geom) return null;

  if (geom.type === "Polygon") {
    for (const ring of geom.coordinates) for (const c of ring) pushCoord(c);
  } else if (geom.type === "MultiPolygon") {
    for (const poly of geom.coordinates) for (const ring of poly) for (const c of ring) pushCoord(c);
  } else {
    return null;
  }

  if (!isFinite(minX)) return null;
  return [minX, minY, maxX, maxY];
}

function pickWinner(candidates) {
  // candidates are features with computed _rank and _area
  candidates.sort((a, b) => {
    const ra = a._rank ?? 0;
    const rb = b._rank ?? 0;
    if (rb !== ra) return rb - ra;         // higher stage rank wins
    const aa = a._area ?? Infinity;
    const ab = b._area ?? Infinity;
    if (aa !== ab) return aa - ab;         // smaller area wins
    const ka = a._stable ?? "";
    const kb = b._stable ?? "";
    return ka.localeCompare(kb);           // stable tie-break
  });
  return candidates[0];
}

async function main() {
  console.log("====================================================");
  console.log("  ATTACH DISTRICT ZONING -> PROPERTIES (STATEWIDE)");
  console.log("====================================================");
  console.log("IN_PROPERTIES:", IN_PROPERTIES);
  console.log("IN_ZONING:", IN_ZONING);
  console.log("OUT_PROPERTIES:", OUT_PROPERTIES);
  console.log("OUT_META:", OUT_META);
  console.log("CELL_DEG:", CELL_DEG);
  console.log("----------------------------------------------------");

  const zoningSha = sha256File(IN_ZONING);
  const builtAt = new Date().toISOString();

  console.log("[load] zoning geojson...");
  const zoning = JSON.parse(fs.readFileSync(IN_ZONING, "utf8"));

  const features = zoning.features ?? [];
  console.log(`[load] zoning features: ${features.length}`);

  // precompute bbox + area + stage rank + stable key
  console.log("[prep] precomputing bbox/area/rank...");
  for (const f of features) {
    const props = f.properties ?? {};
    f._bbox = getBbox(f.geometry);
    f._area = (f.geometry ? area(f) : Infinity);
    f._rank = stageRank(props.STAGE);
    f._stable = stableKey(props);
  }

  console.log("[index] building grid...");
  const grid = new Map(); // cellKey -> [featureIndex...]
  let indexed = 0;
  for (let i = 0; i < features.length; i++) {
    const f = features[i];
    if (!f._bbox) continue;
    const cells = bboxToCells(f._bbox);
    for (const ck of cells) {
      let arr = grid.get(ck);
      if (!arr) {
        arr = [];
        grid.set(ck, arr);
      }
      arr.push(i);
    }
    indexed++;
  }
  console.log(`[index] features indexed: ${indexed}`);
  console.log(`[index] grid cells: ${grid.size}`);

  // stream properties -> output
  const rl = readline.createInterface({
    input: fs.createReadStream(IN_PROPERTIES, "utf8"),
    crlfDelay: Infinity,
  });

  fs.mkdirSync(path.dirname(OUT_PROPERTIES), { recursive: true });
  const out = fs.createWriteStream(OUT_PROPERTIES, "utf8");

  let total = 0;
  let hasCoords = 0;
  let attached = 0;
  let multiHits = 0;
  let noHit = 0;

  const byCity = {};
  const stageCounts = {};

  console.log("[run] streaming properties...");
  for await (const line of rl) {
    if (!line.trim()) continue;
    total++;

    let row;
    try {
      row = JSON.parse(line);
    } catch {
      continue;
    }

    const lat = Number(row.lat);
    const lng = Number(row.lng);

    if (!isFinite(lat) || !isFinite(lng)) {
      row.zoning = row.zoning ?? {};
      row.zoning.district = null;
      row.zoning.attach = {
        method: "pip:grid",
        zoningSha256: zoningSha,
        asOf: builtAt,
        multiHit: false,
        candidateCount: 0,
        winnerRule: "stage_then_smallest_area_then_stable_tie",
        note: "missing_coords",
      };
      out.write(JSON.stringify(row) + "\n");
      continue;
    }

    hasCoords++;

    const ck = cellKey(lng, lat);
    const candIdx = grid.get(ck) ?? [];

    // quick skip if empty cell
    if (candIdx.length === 0) {
      noHit++;
      row.zoning = row.zoning ?? {};
      row.zoning.district = null;
      row.zoning.attach = {
        method: "pip:grid",
        zoningSha256: zoningSha,
        asOf: builtAt,
        multiHit: false,
        candidateCount: 0,
        winnerRule: "stage_then_smallest_area_then_stable_tie",
      };
      out.write(JSON.stringify(row) + "\n");
      continue;
    }

    const pt = { type: "Feature", geometry: { type: "Point", coordinates: [lng, lat] }, properties: {} };

    const hits = [];
    for (const i of candIdx) {
      const f = features[i];
      // bbox is already cell-filtered, now exact pip
      if (booleanPointInPolygon(pt, f)) hits.push(f);
    }

    if (hits.length === 0) {
      noHit++;
      row.zoning = row.zoning ?? {};
      row.zoning.district = null;
      row.zoning.attach = {
        method: "pip:grid",
        zoningSha256: zoningSha,
        asOf: builtAt,
        multiHit: false,
        candidateCount: 0,
        winnerRule: "stage_then_smallest_area_then_stable_tie",
      };
      out.write(JSON.stringify(row) + "\n");
      continue;
    }

    const winner = (hits.length === 1) ? hits[0] : pickWinner(hits);
    if (hits.length > 1) multiHits++;

    const p = winner.properties ?? {};
    const city = p.__city ?? null;
    const stage = p.STAGE ?? null;

    if (city) byCity[city] = (byCity[city] ?? 0) + 1;
    if (stage) stageCounts[String(stage)] = (stageCounts[String(stage)] ?? 0) + 1;

    attached++;

    row.zoning = row.zoning ?? {};
    row.zoning.district = {
      city: city,
      layer: p.__layer ?? "district",
      name: p.__label ?? null,
      codeRaw: p.DISTRICT ?? null,
      codeNorm: p.__norm ?? null,
      stage: stage,
      refs: {
        article: p.ARTICLE ?? null,
        mapNo: p.MAPNO ?? null,
        volume: p.VOLUME ?? null,
      },
      source: {
        file: p.__sourceFile ?? null,
        // path: p.__sourcePath ?? null, // enable only for internal builds
      },
    };

    row.zoning.attach = {
      method: "pip:grid",
      zoningSha256: zoningSha,
      asOf: builtAt,
      multiHit: hits.length > 1,
      candidateCount: hits.length,
      winnerRule: "stage_then_smallest_area_then_stable_tie",
    };

    out.write(JSON.stringify(row) + "\n");

    if (total % 250000 === 0) {
      console.log(`[progress] total=${total.toLocaleString()} attached=${attached.toLocaleString()} multiHits=${multiHits.toLocaleString()} noHit=${noHit.toLocaleString()}`);
    }
  }

  out.end();

  const meta = {
    builtAt,
    script: "attachDistrictsToProperties_statewide.js",
    inputs: {
      properties: IN_PROPERTIES,
      zoning: IN_ZONING,
      zoningSha256: zoningSha,
    },
    grid: { cellDeg: CELL_DEG, cells: grid.size, features: features.length },
    counts: { total, hasCoords, attached, noHit, multiHits },
    byCity,
    stageCounts,
  };

  fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2), "utf8");

  console.log("====================================================");
  console.log("[done]");
  console.log(meta.counts);
  console.log("OUT_PROPERTIES:", OUT_PROPERTIES);
  console.log("OUT_META:", OUT_META);
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ attachDistrictsToProperties failed:", err);
  process.exit(1);
});
