import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import readline from "node:readline";

import booleanPointInPolygon from "@turf/boolean-point-in-polygon";
import bbox from "@turf/bbox";
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

function pickTier(row) {
  const candidates = [
    row.addressTier,
    row.address_tier,
    row.addrTier,
    row.addr_tier,
    row.addressAuthorityTier,
    row.tier,
  ].filter(v => v != null);

  const v = candidates.length ? String(candidates[0]).trim().toUpperCase() : "";
  return ["A","B","C","D","E","F"].includes(v) ? v : "";
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

  // MA sanity bounds (defensive)
  if (latN < 41.0 || latN > 43.6) return null;
  if (lonN < -73.8 || lonN > -69.3) return null;

  return { lat: latN, lon: lonN };
}

function pickLabel(props) {
  if (!props) return "";
  const preferred = ["ZONING", "ZONE", "ZONEDIST", "DISTRICT", "NAME", "DIST_NAME", "ZONE_CODE", "ZONING_CODE"];
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

// Grid index for candidate pruning (fast + stable)
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
  for (let x = minX; x <= maxX; x++) {
    for (let y = minY; y <= maxY; y++) keys.push(`${x}:${y}`);
  }
  return keys;
}

const args = parseArgs(process.argv);
const IN = args.in;
const OUT = args.out;
const META = args.meta;
const ZONING_FILE = args.zoningFile;
const CELL_DEG = Number(args.cellDeg || 0.02);

if (!IN || !OUT || !META || !ZONING_FILE) {
  console.error("Usage: node attachBaseZoning_TierA_v1.mjs --in <ndjson> --out <ndjson> --meta <json> --zoningFile <geojson> [--cellDeg 0.02]");
  process.exit(1);
}

// ---- Load zoning + build grid index ----
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
  const rec = {
    bbox: bb,
    geometry: f.geometry,
    label: pickLabel(f.properties || {}),
  };

  zoningFeatures++;
  for (const k of cellsForBbox(bb, CELL_DEG)) {
    if (!grid.has(k)) grid.set(k, []);
    grid.get(k).push(rec);
  }
}

console.log("====================================================");
console.log("ATTACH BASE ZONING (Tier A only)");
console.log("====================================================");
console.log("IN:", IN);
console.log("OUT:", OUT);
console.log("META:", META);
console.log("ZONING:", ZONING_FILE);
console.log("zoning_sha256:", zoningHash);
console.log("zoning_features:", zoningFeatures.toLocaleString());
console.log("grid_cells:", grid.size.toLocaleString());
console.log("cellDeg:", CELL_DEG);
console.log("----------------------------------------------------");

// ---- Stream properties + attach ----
const rs = fs.createReadStream(IN, { encoding: "utf8" });
const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });
const ws = fs.createWriteStream(OUT, { encoding: "utf8" });

let total = 0, tierA = 0, noCoords = 0, gatedOut = 0;
let attached = 0, noMatch = 0, multiHit = 0;

for await (const line of rl) {
  const s = line.trim();
  if (!s) continue;

  let row;
  try { row = JSON.parse(s); } catch { continue; }
  total++;

  const tier = pickTier(row);
  const coords = pickLatLon(row);

  const outRow = { ...row };
  outRow.zoning = outRow.zoning || {};

  if (!coords) {
    noCoords++;
    outRow.zoning.base = { status: "no_coords", join_method: "pip" };
    ws.write(JSON.stringify(outRow) + "\n");
    continue;
  }

  if (tier !== "A") {
    gatedOut++;
    outRow.zoning.base = { status: "gated_out", tier, join_method: "pip" };
    ws.write(JSON.stringify(outRow) + "\n");
    continue;
  }

  tierA++;
  const { lon, lat } = coords;
  const pt = point([lon, lat]);

  const key = gridKey(lon, lat, CELL_DEG);
  const candidates = grid.get(key) || [];

  const hits = [];
  for (const z of candidates) {
    if (!bboxContains(z.bbox, lon, lat)) continue;
    try {
      const poly = { type: "Feature", geometry: z.geometry, properties: {} };
      if (booleanPointInPolygon(pt, poly)) hits.push(z);
    } catch {}
  }

  if (hits.length === 0) {
    noMatch++;
    outRow.zoning.base = {
      status: "no_match",
      tier: "A",
      join_method: "pip",
      point: { lat, lon },
      source_file: zoningName,
      source_sha256: zoningHash,
    };
    ws.write(JSON.stringify(outRow) + "\n");
    continue;
  }

  if (hits.length > 1) multiHit++;
  const primary = hits.find(h => (h.label || "").trim().length) || hits[0];
  const confidence = hits.length > 1 ? 0.40 : 0.90;

  outRow.zoning.base = {
    status: "attached",
    tier: "A",
    join_method: "pip",
    primary: (primary.label || "").trim(),
    confidence,
    point: { lat, lon },
    source_file: zoningName,
    source_sha256: zoningHash,
    hits: hits.slice(0, 10).map(h => ({ label: (h.label || "").trim() })),
    flags: hits.length > 1 ? ["multi_hit_possible_overlap"] : [],
  };

  attached++;
  ws.write(JSON.stringify(outRow) + "\n");
}

ws.end();

const meta = {
  created_at: new Date().toISOString(),
  join: { type: "base_zoning", method: "point_in_polygon", tier_gate: ["A"], cellDeg: CELL_DEG },
  inputs: { in: IN, zoning_file: ZONING_FILE, zoning_sha256: zoningHash },
  counts: {
    total_rows: total,
    tierA_rows: tierA,
    gated_out_nonA: gatedOut,
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
