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
  const lat = row.lat ?? row.latitude ?? row.y ?? row?.location?.lat ?? row?.geo?.lat ?? row?.coords?.lat;
  const lon = row.lon ?? row.lng ?? row.longitude ?? row.x ?? row?.location?.lon ?? row?.location?.lng ?? row?.geo?.lon ?? row?.geo?.lng ?? row?.coords?.lon ?? row?.coords?.lng;
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

function coordSourceAllowed(row) {
  const s = String(row.coord_source || "");
  // allow strong internal sources; block obvious external/fuzzy
  const allow = /addressIndex|addressPoint|parcel_direct|parcelIndex|parcelCentroid/i.test(s);
  const block = /fuzzy|external|nominatim|google|mapbox|bing/i.test(s);
  return allow && !block;
}

const args = parseArgs(process.argv);
const IN = args.in;
const OUT = args.out;
const META = args.meta;
const ZONING_FILE = args.zoningFile;

const TIER_FIELD = args.tierField || "address_tier";
const TIER_VALUE = args.tierValue || "B";
const CELL_DEG = Number(args.cellDeg || 0.02);

if (!IN || !OUT || !META || !ZONING_FILE) {
  console.error("Usage: node attachBaseDistricts_TierB_pass_v1.mjs --in <ndjson> --out <ndjson> --meta <json> --zoningFile <geojson> [--tierField address_tier] [--tierValue B] [--cellDeg 0.02]");
  process.exit(1);
}

// Load zoning + index
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
  const rec = { bbox: bb, geometry: f.geometry, label: pickLabel(f.properties || {}), area: area(f) };

  zoningFeatures++;
  for (const k of cellsForBbox(bb, CELL_DEG)) {
    if (!grid.has(k)) grid.set(k, []);
    grid.get(k).push(rec);
  }
}

const startedAt = new Date().toISOString();

console.log("====================================================");
console.log("ATTACH BASE ZONING DISTRICTS (Tier B pass)");
console.log("====================================================");
console.log("IN:", IN);
console.log("OUT:", OUT);
console.log("META:", META);
console.log("ZONING:", ZONING_FILE);
console.log("zoning_sha256:", zoningHash);
console.log("zoning_features:", zoningFeatures.toLocaleString());
console.log("tier gate:", `${TIER_FIELD} == ${TIER_VALUE}`);
console.log("coord_source gate: addressIndex/addressPoint/parcel* only");
console.log("----------------------------------------------------");

const rs = fs.createReadStream(IN, { encoding: "utf8" });
const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });
const ws = fs.createWriteStream(OUT, { encoding: "utf8" });

let total = 0, tierB = 0, skippedAlreadyAttached = 0, gatedOut = 0, badCoordSource = 0, noCoords = 0;
let attached = 0, noMatch = 0, multiHit = 0;

for await (const line of rl) {
  const s = line.trim();
  if (!s) continue;

  let row;
  try { row = JSON.parse(s); } catch { continue; }
  total++;

  const outRow = { ...row };
  outRow.zoning = outRow.zoning || {};
  outRow.zoning.attach = outRow.zoning.attach || {};

  // If it's already attached (Tier A pass), leave it alone
  if (outRow?.zoning?.attach?.status === "attached" && outRow?.zoning?.district) {
    skippedAlreadyAttached++;
    ws.write(JSON.stringify(outRow) + "\n");
    continue;
  }

  const tv = String(outRow?.[TIER_FIELD] ?? "").trim();
  if (tv !== String(TIER_VALUE).trim()) {
    gatedOut++;
    ws.write(JSON.stringify(outRow) + "\n");
    continue;
  }

  tierB++;

  const coords = pickLatLon(outRow);
  if (!coords) {
    noCoords++;
    outRow.zoning.district = outRow.zoning.district ?? null;
    outRow.zoning.attach = { ...outRow.zoning.attach, status: "no_coords", method: "pip:grid", zoningSha256: zoningHash, zoningFile: zoningName, asOf: startedAt, tierApplied: "B", flags: ["tierB_lower_trust"] };
    ws.write(JSON.stringify(outRow) + "\n");
    continue;
  }

  if (!coordSourceAllowed(outRow)) {
    badCoordSource++;
    outRow.zoning.district = outRow.zoning.district ?? null;
    outRow.zoning.attach = { ...outRow.zoning.attach, status: "gated_out_coord_source", method: "pip:grid", zoningSha256: zoningHash, zoningFile: zoningName, asOf: startedAt, tierApplied: "B", flags: ["tierB_lower_trust","coord_source_not_allowed"] };
    ws.write(JSON.stringify(outRow) + "\n");
    continue;
  }

  const { lon, lat } = coords;
  const pt = point([lon, lat]);

  const key = gridKey(lon, lat, CELL_DEG);
  const candidates = grid.get(key) || [];

  let candidatesAfterBbox = 0;
  const hits = [];

  for (const z of candidates) {
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
      tierApplied: "B",
      candidateCount: candidatesAfterBbox,
      multiHit: false,
      confidence: 0.0,
      flags: ["tierB_lower_trust"],
      point: { lat, lon }
    };
    ws.write(JSON.stringify(outRow) + "\n");
    continue;
  }

  const mh = hits.length > 1;
  if (mh) multiHit++;

  const winner = mh ? chooseWinner(hits) : hits[0];
  outRow.zoning.district = (winner.label || "").trim() || null;

  // Lower confidence for Tier B; even lower if multi-hit
  const conf = mh ? 0.30 : 0.70;

  outRow.zoning.attach = {
    ...outRow.zoning.attach,
    status: "attached",
    method: "pip:grid",
    zoningSha256: zoningHash,
    zoningFile: zoningName,
    asOf: startedAt,
    tierApplied: "B",
    candidateCount: candidatesAfterBbox,
    multiHit: mh,
    confidence: conf,
    flags: mh ? ["tierB_lower_trust","multi_hit_possible_overlap"] : ["tierB_lower_trust"],
    point: { lat, lon }
  };

  attached++;
  ws.write(JSON.stringify(outRow) + "\n");
}

ws.end();

const meta = {
  created_at: new Date().toISOString(),
  pass: "tierB_only",
  inputs: { in: IN, zoningFile: ZONING_FILE, zoningFileName: zoningName, zoningSha256: zoningHash },
  tier_gate: { field: TIER_FIELD, value: TIER_VALUE },
  coord_source_gate: "allow addressIndex/addressPoint/parcel_*; block fuzzy/external",
  counts: {
    total_rows: total,
    tierB_rows: tierB,
    skippedAlreadyAttached,
    gatedOut_nonB: gatedOut,
    badCoordSource,
    no_coords: noCoords,
    attached,
    no_match: noMatch,
    multi_hit: multiHit
  }
};

await fsp.writeFile(META, JSON.stringify(meta, null, 2), "utf8");

console.log("----------------------------------------------------");
console.log("[done] total:", total.toLocaleString());
console.log("[done] tierB:", tierB.toLocaleString());
console.log("[done] attached:", attached.toLocaleString(), "no_match:", noMatch.toLocaleString(), "multi_hit:", multiHit.toLocaleString());
console.log("[done] wrote:", OUT);
console.log("[done] meta :", META);
