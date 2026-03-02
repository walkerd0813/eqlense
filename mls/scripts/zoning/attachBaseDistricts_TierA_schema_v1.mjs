import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import readline from "node:readline";

import booleanPointInPolygon from "@turf/boolean-point-in-polygon";
import bbox from "@turf/bbox";
import area from "@turf/area";
import { point } from "@turf/helpers";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
    out[k] = v;
  }
  return out;
}

async function sha256File(fp) {
  return new Promise((resolve, reject) => {
    const h = crypto.createHash("sha256");
    const s = fs.createReadStream(fp);
    s.on("data", d => h.update(d));
    s.on("error", reject);
    s.on("end", () => resolve(h.digest("hex")));
  });
}

function pickLatLon(row) {
  const lat =
    row.lat ?? row.latitude ?? row.y ??
    row?.location?.lat ?? row?.geo?.lat ?? row?.coords?.lat;

  const lon =
    row.lon ?? row.lng ?? row.longitude ?? row.x ??
    row?.location?.lon ?? row?.location?.lng ?? row?.geo?.lon ?? row?.geo?.lng ?? row?.coords?.lon ?? row?.coords?.lng;

  const latN = lat != null ? Number(lat) : null;
  const lonN = lon != null ? Number(lon) : null;

  if (!Number.isFinite(latN) || !Number.isFinite(lonN)) return null;
  if (latN < 41.0 || latN > 43.6) return null;
  if (lonN < -73.8 || lonN > -69.3) return null;

  return { lat: latN, lon: lonN };
}

function pickLabel(props) {
  if (!props) return "";
  const preferred = ["ZONING","ZONE","ZONEDIST","DISTRICT","DIST_NAME","NAME","ZONE_CODE","ZONING_CODE"];
  for (const k of preferred) {
    if (props[k] != null) return String(props[k]).trim();
    const found = Object.keys(props).find(pk => pk.toLowerCase() === k.toLowerCase());
    if (found && props[found] != null) return String(props[found]).trim();
  }
  return "";
}

function bboxContains(bb, lon, lat) {
  return lon >= bb[0] && lon <= bb[2] && lat >= bb[1] && lat <= bb[3];
}

// grid index for candidate pruning
function gridKey(lon, lat, cellDeg) {
  const gx = Math.floor(lon / cellDeg);
  const gy = Math.floor(lat / cellDeg);
  return `${gx}:${gy}`;
}
function cellsForBbox(bb, cellDeg) {
  const minX = Math.floor(bb[0] / cellDeg);
  const maxX = Math.floor(bb[2] / cellDeg);
  const minY = Math.floor(bb[1] / cellDeg);
  const maxY = Math.floor(bb[3] / cellDeg);
  const keys = [];
  for (let x = minX; x <= maxX; x++) for (let y = minY; y <= maxY; y++) keys.push(`${x}:${y}`);
  return keys;
}

function chooseWinner(hits) {
  // institutional tie-break: smallest area (more specific) -> label -> stable order
  let best = hits[0];
  for (let i = 1; i < hits.length; i++) {
    const h = hits[i];
    if (h.area < best.area) best = h;
    else if (h.area === best.area) {
      const a = (h.label || "").toLowerCase();
      const b = (best.label || "").toLowerCase();
      if (a && b && a < b) best = h;
    }
  }
  return best;
}

const args = parseArgs(process.argv);
const IN = args.in;
const OUT = args.out;
const META = args.meta;
const ZONING_FILE = args.zoningFile;

const TIER_FIELD = args.tierField;   // e.g. address_tier
const TIER_VALUE = args.tierValue;   // e.g. A

const CELL_DEG = Number(args.cellDeg || 0.02);

if (!IN || !OUT || !META || !ZONING_FILE || !TIER_FIELD || !TIER_VALUE) {
  console.error("Usage: node attachBaseDistricts_TierA_schema_v1.mjs --in <ndjson> --out <ndjson> --meta <json> --zoningFile <geojson> --tierField <field> --tierValue <value> [--cellDeg 0.02]");
  process.exit(1);
}

// ---- load zoning + index ----
const zoningRaw = await fsp.readFile(ZONING_FILE, "utf8");
const zoningGj = JSON.parse(zoningRaw);

const zoningHash = await sha256File(ZONING_FILE);
const zoningName = path.basename(ZONING_FILE);

const grid = new Map();
let zoningFeatures = 0;

for (const f of (zoningGj?.features || [])) {
  if (!f?.geometry) continue;
  const t = f.geometry.type;
  if (t !== "Polygon" && t !== "MultiPolygon") continue;

  const bb = bbox(f);
  const lbl = pickLabel(f.properties || {});
  const ar = area(f);

  const rec = { bbox: bb, geometry: f.geometry, label: lbl, area: ar };

  zoningFeatures++;
  for (const k of cellsForBbox(bb, CELL_DEG)) {
    if (!grid.has(k)) grid.set(k, []);
    grid.get(k).push(rec);
  }
}

const startedAt = new Date().toISOString();

console.log("====================================================");
console.log("ATTACH BASE ZONING DISTRICTS (Tier A only)");
console.log("====================================================");
console.log("IN:", IN);
console.log("OUT:", OUT);
console.log("META:", META);
console.log("ZONING:", ZONING_FILE);
console.log("zoning_sha256:", zoningHash);
console.log("zoning_features:", zoningFeatures.toLocaleString());
console.log("grid_cells:", grid.size.toLocaleString());
console.log("cellDeg:", CELL_DEG);
console.log("tier gate:", `${TIER_FIELD} == ${TIER_VALUE}`);
console.log("----------------------------------------------------");

// ---- stream attach ----
const rs = fs.createReadStream(IN, { encoding: "utf8" });
const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });
const ws = fs.createWriteStream(OUT, { encoding: "utf8" });

let total = 0, tierA = 0, gatedOut = 0, noCoords = 0;
let attached = 0, noMatch = 0, multiHit = 0;

for await (const line of rl) {
  const s = line.trim();
  if (!s) continue;

  let row;
  try { row = JSON.parse(s); } catch { continue; }
  total++;

  const coords = pickLatLon(row);

  const outRow = { ...row };
  outRow.zoning = outRow.zoning || {};
  outRow.zoning.attach = outRow.zoning.attach || {};

  if (!coords) {
    noCoords++;
    outRow.zoning.district = outRow.zoning.district ?? null;
    outRow.zoning.attach = {
      ...outRow.zoning.attach,
      status: "no_coords",
      method: "pip:grid",
      zoningSha256: zoningHash,
      zoningFile: zoningName,
      asOf: startedAt,
      tierField: TIER_FIELD,
      tierValue: TIER_VALUE,
      candidateCount: 0,
      multiHit: false,
      winnerRule: "smallest_area_then_label_then_stable",
    };
    ws.write(JSON.stringify(outRow) + "\n");
    continue;
  }

  const tv = String(outRow?.[TIER_FIELD] ?? "").trim();
  if (tv !== String(TIER_VALUE).trim()) {
    gatedOut++;
    outRow.zoning.district = outRow.zoning.district ?? null;
    outRow.zoning.attach = {
      ...outRow.zoning.attach,
      status: "gated_out",
      method: "pip:grid",
      zoningSha256: zoningHash,
      zoningFile: zoningName,
      asOf: startedAt,
      tierField: TIER_FIELD,
      tierValue: tv,
      candidateCount: 0,
      multiHit: false,
      winnerRule: "smallest_area_then_label_then_stable",
    };
    ws.write(JSON.stringify(outRow) + "\n");
    continue;
  }

  tierA++;
  const { lon, lat } = coords;
  const pt = point([lon, lat]);

  const key = gridKey(lon, lat, CELL_DEG);
  const cellCandidates = grid.get(key) || [];

  // bbox prune + pip
  let candidatesAfterBbox = 0;
  const hits = [];

  for (const z of cellCandidates) {
    if (!bboxContains(z.bbox, lon, lat)) continue;
    candidatesAfterBbox++;
    try {
      const poly = { type: "Feature", geometry: z.geometry, properties: {} };
      if (booleanPointInPolygon(pt, poly)) hits.push(z);
    } catch {}
  }

  if (!hits.length) {
    noMatch++;
    outRow.zoning.district = null;
    outRow.zoning.attach = {
      ...outRow.zoning.attach,
      status: "no_match",
      method: "pip:grid",
      zoningSha256: zoningHash,
      zoningFile: zoningName,
      asOf: startedAt,
      tierField: TIER_FIELD,
      tierValue: TIER_VALUE,
      candidateCount: candidatesAfterBbox,
      multiHit: false,
      winnerRule: "smallest_area_then_label_then_stable",
      point: { lat, lon },
    };
    ws.write(JSON.stringify(outRow) + "\n");
    continue;
  }

  const mh = hits.length > 1;
  if (mh) multiHit++;

  const winner = mh ? chooseWinner(hits) : hits[0];

  outRow.zoning.district = (winner.label || "").trim() || null;
  outRow.zoning.attach = {
    ...outRow.zoning.attach,
    status: "attached",
    method: "pip:grid",
    zoningSha256: zoningHash,
    zoningFile: zoningName,
    asOf: startedAt,
    tierField: TIER_FIELD,
    tierValue: TIER_VALUE,
    candidateCount: candidatesAfterBbox,
    multiHit: mh,
    winnerRule: "smallest_area_then_label_then_stable",
    point: { lat, lon },
  };

  attached++;
  ws.write(JSON.stringify(outRow) + "\n");
}

ws.end();

const meta = {
  created_at: new Date().toISOString(),
  join: {
    dataset: "base_zoning_districts",
    tier_gate: { field: TIER_FIELD, value: TIER_VALUE },
    method: "point_in_polygon",
    candidate_prune: `grid_${CELL_DEG}_deg_then_bbox`,
    winner_rule: "smallest_area_then_label_then_stable",
  },
  inputs: {
    in: IN,
    zoningFile: ZONING_FILE,
    zoningFileName: zoningName,
    zoningSha256: zoningHash,
  },
  counts: {
    total_rows: total,
    tierA_rows: tierA,
    gated_out: gatedOut,
    no_coords: noCoords,
    attached,
    no_match: noMatch,
    multi_hit: multiHit,
  },
};

await fsp.writeFile(META, JSON.stringify(meta, null, 2), "utf8");

console.log("----------------------------------------------------");
console.log("[done] total:", total.toLocaleString());
console.log("[done] tierA:", tierA.toLocaleString());
console.log("[done] attached:", attached.toLocaleString(), "no_match:", noMatch.toLocaleString(), "multi_hit:", multiHit.toLocaleString());
console.log("[done] wrote:", OUT);
console.log("[done] meta :", META);
