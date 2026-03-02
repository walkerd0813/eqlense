import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

/**
 * Tier-C Quarantine Revalidation (MAD nearest + Town Boundary PIP)
 * ----------------------------------------------------------------
 * Goal:
 *   Take a Tier-C QUARANTINE subset (quarantined due to town mismatch),
 *   compute the "true" town from MA town boundary polygons (EPSG:26986),
 *   recompute nearest MAD address-point within maxDistM, and AUTO-ACCEPT
 *   ONLY when MAD.town == PIP.town (normalized).
 *
 * Why this exists:
 *   - Base `town` can be wrong for some parcels (bad upstream join / edge cases)
 *   - MAD nearest may be correct, but got quarantined due to town mismatch
 *   - Town boundary PIP is a cheap, defensible secondary validation
 *
 * Notes:
 *   - townsGeo is expected in EPSG:26986 (State Plane meters-ish), and we use x_sp/y_sp
 *   - MAD tiles are expected in WGS84 (lon/lat), and we use lng/lat
 *   - Patch key defaults to property_id; safer is row_uid if your base/quarantine have it.
 *
 * Outputs:
 *   --out           full base NDJSON with validated patches applied
 *   --outAuto       NDJSON of patch records that were AUTO_ACCEPTed
 *   --outKeep       NDJSON of patch records that remain quarantined
 *   --outUnresolved NDJSON of patch records that could not be resolved
 *   --report        JSON summary report
 */

function parseArgs(argv) {
  const a = {};
  for (let i = 2; i < argv.length; i++) {
    const tok = argv[i];
    if (!tok.startsWith("--")) continue;
    const key = tok.slice(2);
    const next = argv[i + 1];
    if (next == null || next.startsWith("--")) {
      a[key] = true;
    } else {
      a[key] = next;
      i++;
    }
  }
  return a;
}

function normTown(s) {
  return String(s ?? "")
    .toUpperCase()
    .trim()
    .replace(/\bTOWN OF\b/g, "")
    .replace(/\bCITY OF\b/g, "")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

function haversineMeters(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const toRad = (d) => (d * Math.PI) / 180;
  const p1 = toRad(lat1);
  const p2 = toRad(lat2);
  const dlat = toRad(lat2 - lat1);
  const dlon = toRad(lon2 - lon1);
  const a =
    Math.sin(dlat / 2) ** 2 +
    Math.cos(p1) * Math.cos(p2) * Math.sin(dlon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

// ---------------- Town PIP (EPSG:26986) ----------------
function bboxFromCoords(coords) {
  let minX = Infinity,
    minY = Infinity,
    maxX = -Infinity,
    maxY = -Infinity;
  const walk = (c) => {
    if (typeof c?.[0] === "number" && typeof c?.[1] === "number") {
      const x = c[0],
        y = c[1];
      if (x < minX) minX = x;
      if (y < minY) minY = y;
      if (x > maxX) maxX = x;
      if (y > maxY) maxY = y;
      return;
    }
    for (const child of c) walk(child);
  };
  walk(coords);
  return { minX, minY, maxX, maxY };
}

// Ray casting ring test
function pointInRing(x, y, ring) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0],
      yi = ring[i][1];
    const xj = ring[j][0],
      yj = ring[j][1];
    const intersect =
      yi > y !== yj > y &&
      x < ((xj - xi) * (y - yi)) / (yj - yi + 0.0) + xi;
    if (intersect) inside = !inside;
  }
  return inside;
}

function pointInPolygon(x, y, polyCoords) {
  const outer = polyCoords[0];
  if (!outer || outer.length < 4) return false;
  if (!pointInRing(x, y, outer)) return false;
  // holes
  for (let i = 1; i < polyCoords.length; i++) {
    const hole = polyCoords[i];
    if (hole && hole.length >= 4 && pointInRing(x, y, hole)) return false;
  }
  return true;
}

function pointInGeom(x, y, geom) {
  if (!geom) return false;
  if (geom.type === "Polygon") return pointInPolygon(x, y, geom.coordinates);
  if (geom.type === "MultiPolygon") {
    for (const poly of geom.coordinates) {
      if (pointInPolygon(x, y, poly)) return true;
    }
  }
  return false;
}

function looksLikeStatePlaneXY(x, y) {
  // MA State Plane (meters-ish): X ~ 40k-330k, Y ~ 800k-1,000k (roughly)
  return (
    typeof x === "number" &&
    typeof y === "number" &&
    Math.abs(x) > 1000 &&
    Math.abs(y) > 1000 &&
    Math.abs(x) < 2000000 &&
    Math.abs(y) < 2000000
  );
}

function extractTownFromMadPoint(p) {
  return normTown(
    p?.TOWN ??
      p?.town ??
      p?.MUNI ??
      p?.municipality ??
      p?.MUNICIPALITY ??
      p?.CITY ??
      p?.city ??
      p?.COMMUNITY ??
      p?.community ??
      p?.TOWN_NAME ??
      ""
  );
}

function extractLonLatFromMadPoint(p) {
  const lon =
    Number(p?.lon ?? p?.lng ?? p?.LON ?? p?.LONGITUDE ?? p?.X ?? p?.x);
  const lat =
    Number(p?.lat ?? p?.LAT ?? p?.LATITUDE ?? p?.Y ?? p?.y);
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return null;
  return { lon, lat };
}

function extractAddrFieldsFromMadPoint(p) {
  const streetNo = String(
    p?.street_no ??
      p?.STREET_NO ??
      p?.ADDR_NUM ??
      p?.HOUSE_NUM ??
      p?.NUM ??
      p?.NUMBER ??
      p?.STREETNUM ??
      ""
  ).trim();

  const streetName = String(
    p?.street_name ??
      p?.STREET_NAME ??
      p?.FULL_ST_NAME ??
      p?.FULL_STREET_NAME ??
      p?.ST_NAME ??
      p?.STREET ??
      ""
  )
    .trim()
    .replace(/\s+/g, " ");

  const unit = String(
    p?.unit ?? p?.UNIT ?? p?.APT ?? p?.APARTMENT ?? p?.SUITE ?? ""
  )
    .trim()
    .replace(/\s+/g, " ");

  const zip = String(p?.zip ?? p?.ZIP ?? p?.ZIP_CODE ?? p?.POSTCODE ?? "")
    .trim()
    .replace(/\D/g, "")
    .slice(0, 5);

  return { streetNo: streetNo || null, streetName: streetName || null, unit: unit || null, zip: zip || null };
}

// ---------------- MAD tiles ----------------
function parseLonLatFromFilename(filename) {
  // Try to extract 2 floats from the filename, e.g. tile_-69.94_41.72.ndjson
  const m = filename.match(/(-?\d+(?:\.\d+)?)[,_](-?\d+(?:\.\d+)?)/);
  if (!m) return null;
  const a = Number(m[1]);
  const b = Number(m[2]);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return null;

  // decide which is lon/lat by ranges
  const aIsLon = Math.abs(a) <= 180;
  const bIsLat = Math.abs(b) <= 90;
  const aIsLat = Math.abs(a) <= 90;
  const bIsLon = Math.abs(b) <= 180;

  if (aIsLon && bIsLat) return { lon: a, lat: b };
  if (aIsLat && bIsLon) return { lon: b, lat: a };

  // fallback: assume first is lon, second is lat (your tile naming convention)
  return { lon: a, lat: b };
}

function makeTileKey(lon, lat, tileSize, mode) {
  const bucket = (v) => {
    if (mode === "floor") return Math.floor(v / tileSize) * tileSize;
    return Math.round(v / tileSize) * tileSize; // default "round"
  };
  const tlon = bucket(lon);
  const tlat = bucket(lat);
  // keep stable formatting
  return `${tlon.toFixed(2)}|${tlat.toFixed(2)}`;
}

function neighborKeys(lon, lat, tileSize, mode) {
  const keys = new Set();
  const baseLon = (mode === "floor")
    ? Math.floor(lon / tileSize) * tileSize
    : Math.round(lon / tileSize) * tileSize;
  const baseLat = (mode === "floor")
    ? Math.floor(lat / tileSize) * tileSize
    : Math.round(lat / tileSize) * tileSize;

  for (let dx = -1; dx <= 1; dx++) {
    for (let dy = -1; dy <= 1; dy++) {
      const k = `${(baseLon + dx * tileSize).toFixed(2)}|${(baseLat + dy * tileSize).toFixed(2)}`;
      keys.add(k);
    }
  }
  return [...keys];
}

class LruCache {
  constructor(limit) {
    this.limit = limit;
    this.map = new Map();
  }
  get(k) {
    const v = this.map.get(k);
    if (v === undefined) return undefined;
    this.map.delete(k);
    this.map.set(k, v);
    return v;
  }
  set(k, v) {
    if (this.map.has(k)) this.map.delete(k);
    this.map.set(k, v);
    while (this.map.size > this.limit) {
      const first = this.map.keys().next().value;
      this.map.delete(first);
    }
  }
}

// ---------------- main ----------------
const a = parseArgs(process.argv);

const basePath = a.base;
const quarantinePath = a.quarantine;
const tilesDir = a.tilesDir;
const townsGeoPath = a.townsGeo;
const outPath = a.out;
const reportPath = a.report;

const maxDistM = Number(a.maxDistM ?? 60);
const tileSize = Number(a.tileSize ?? 0.01);
const tileCacheN = Number(a.tileCacheN ?? 250);
const tileKeyMode = (a.tileKeyMode ?? "round").toLowerCase() === "floor" ? "floor" : "round";
const keyField = String(a.keyField ?? "property_id");

const outAutoPath = a.outAuto ?? outPath.replace(/\.ndjson$/i, "_AUTO_ACCEPT.ndjson");
const outKeepPath = a.outKeep ?? outPath.replace(/\.ndjson$/i, "_KEEP_QUARANTINE.ndjson");
const outUnresolvedPath = a.outUnresolved ?? outPath.replace(/\.ndjson$/i, "_UNRESOLVED.ndjson");

if (!basePath || !quarantinePath || !tilesDir || !townsGeoPath || !outPath || !reportPath) {
  console.error(
    "Usage:\n" +
      "  node addressAuthority_revalidateTierCQuarantine_madNearest_townPip_v2_PATCHED.mjs \\\n" +
      "    --base <base.ndjson> \\\n" +
      "    --quarantine <tierC_QUARANTINE.ndjson> \\\n" +
      "    --tilesDir <mad_tiles_0p01> \\\n" +
      "    --townsGeo <townBoundaries.geojson> \\\n" +
      "    --out <out.ndjson> \\\n" +
      "    --report <report.json> \\\n" +
      "    [--maxDistM 60] [--tileSize 0.01] [--tileCacheN 250] [--tileKeyMode round|floor] [--keyField property_id|row_uid]\n"
  );
  process.exit(1);
}

for (const p of [basePath, quarantinePath, townsGeoPath, tilesDir]) {
  if (!fs.existsSync(p)) {
    console.error("Missing:", p);
    process.exit(1);
  }
}
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.mkdirSync(path.dirname(reportPath), { recursive: true });
fs.mkdirSync(path.dirname(outAutoPath), { recursive: true });
fs.mkdirSync(path.dirname(outKeepPath), { recursive: true });
fs.mkdirSync(path.dirname(outUnresolvedPath), { recursive: true });

const NOW = new Date().toISOString();

console.log("Loading towns geojson (expected EPSG:26986):", townsGeoPath);
const townsGeo = JSON.parse(fs.readFileSync(townsGeoPath, "utf8"));
const towns = [];
const geomCounts = {};
for (const f of townsGeo.features || []) {
  const g = f.geometry;
  if (!g) continue;
  geomCounts[g.type] = (geomCounts[g.type] || 0) + 1;
  const name = normTown(f.properties?.TOWN ?? "");
  if (!name) continue;
  const bb = bboxFromCoords(g.coordinates);
  towns.push({ name, bb, geom: g });
}
console.log("Towns loaded:", towns.length, "geomCounts:", geomCounts);

function townForPointXY(x, y) {
  for (const t of towns) {
    const bb = t.bb;
    if (x < bb.minX || x > bb.maxX || y < bb.minY || y > bb.maxY) continue;
    if (pointInGeom(x, y, t.geom)) return t.name;
  }
  return null;
}

// Build tile map (key -> filepath)
console.log("Building tile map from:", tilesDir);
const tileMap = new Map();
let collisions = 0;
for (const f of fs.readdirSync(tilesDir)) {
  if (!f.endsWith(".ndjson")) continue;
  const ll = parseLonLatFromFilename(f);
  if (!ll) continue;
  const key = `${Number(ll.lon).toFixed(2)}|${Number(ll.lat).toFixed(2)}`;
  const full = path.join(tilesDir, f);
  if (tileMap.has(key) && tileMap.get(key) !== full) collisions++;
  tileMap.set(key, full);
}
console.log("Tile map:", { files: fs.readdirSync(tilesDir).length, mapped: tileMap.size, collisions });
const sampleKeys = [...tileMap.keys()].slice(0, 5);
console.log("Sample tile keys:", sampleKeys);

const tileCache = new LruCache(tileCacheN);

async function loadTilePoints(tileKey) {
  const cached = tileCache.get(tileKey);
  if (cached) return cached;
  const fp = tileMap.get(tileKey);
  if (!fp) return null;

  const pts = [];
  const rl = readline.createInterface({
    input: fs.createReadStream(fp, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const s = line.trim();
    if (!s) continue;
    try {
      const obj = JSON.parse(s);
      pts.push(obj);
    } catch {
      // ignore bad lines; tile generator should not produce these
    }
  }

  tileCache.set(tileKey, pts);
  return pts;
}

async function nearestMadPoint(lat, lon) {
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;

  const keys = neighborKeys(lon, lat, tileSize, tileKeyMode);
  let best = null;

  for (const k of keys) {
    const pts = await loadTilePoints(k);
    if (!pts || pts.length === 0) continue;

    for (const p of pts) {
      const ll = extractLonLatFromMadPoint(p);
      if (!ll) continue;
      const d = haversineMeters(lat, lon, ll.lat, ll.lon);
      if (d > maxDistM) continue;
      if (!best || d < best.distM) {
        best = { point: p, distM: d };
      }
    }
  }
  return best;
}

function getRowKey(row) {
  const v = row?.[keyField] ?? row?.property_id ?? row?.row_uid ?? row?.parcel_id;
  return String(v ?? "").trim() || null;
}

// Step 1: Build patchMap from quarantine targets
console.log("Targets loaded from quarantine...");
const targets = new Map();
const counts = {
  qTotal: 0,
  qParseErr: 0,
  qNoLatLon: 0,
  qNoXY: 0,
  qNoPipTown: 0,
  qNoTile: 0,
  qNoCandidate: 0,
  qTooFar: 0,
  qTownMismatch: 0,
  qMissingCandidateFields: 0,
  qAutoAccept: 0,
  qKeep: 0,
  qUnresolved: 0,
};

const outAuto = fs.createWriteStream(outAutoPath, { encoding: "utf8" });
const outKeep = fs.createWriteStream(outKeepPath, { encoding: "utf8" });
const outUnresolved = fs.createWriteStream(outUnresolvedPath, { encoding: "utf8" });

const qrl = readline.createInterface({
  input: fs.createReadStream(quarantinePath, { encoding: "utf8" }),
  crlfDelay: Infinity,
});

let qSeen = 0;
for await (const line of qrl) {
  const s = line.trim();
  if (!s) continue;
  counts.qTotal++;
  qSeen++;
  if (qSeen % 5000 === 0) console.log(`...quarantine scanned ${qSeen.toLocaleString()} rows`);

  let row;
  try {
    row = JSON.parse(s);
  } catch {
    counts.qParseErr++;
    continue;
  }

  const key = getRowKey(row);
  if (!key) continue;

  const lat = Number(row.lat);
  const lon = Number(row.lng ?? row.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    counts.qNoLatLon++;
    continue;
  }

  const x = Number(row.x_sp);
  const y = Number(row.y_sp);
  if (!looksLikeStatePlaneXY(x, y)) {
    counts.qNoXY++;
    continue;
  }

  const pipTown = townForPointXY(x, y);
  if (!pipTown) {
    counts.qNoPipTown++;
    continue;
  }

  // one target record per key (but keep first; base may still have duplicates, that's fine)
  if (!targets.has(key)) {
    targets.set(key, { key, lat, lon, x, y, pipTown, baseTown: normTown(row.town ?? ""), parcel_id: row.parcel_id ?? null, property_id: row.property_id ?? null });
  }
}

// Resolve each target to a MAD nearest point and decide auto/keep/unresolved
console.log("Resolving nearest MAD points for targets:", targets.size.toLocaleString());
let tDone = 0;

const patchMap = new Map(); // key -> patch
const keysByOutcome = { AUTO_ACCEPT: 0, KEEP_QUARANTINE: 0, UNRESOLVED: 0 };

for (const t of targets.values()) {
  tDone++;
  if (tDone % 5000 === 0) console.log(`...target nearest ${tDone.toLocaleString()} / ${targets.size.toLocaleString()}`);

  const nearest = await nearestMadPoint(t.lat, t.lon);
  if (!nearest) {
    counts.qNoCandidate++;
    keysByOutcome.UNRESOLVED++;
    outUnresolved.write(JSON.stringify({ key: t.key, reason: "NO_CANDIDATE", pipTown: t.pipTown }) + "\n");
    continue;
  }

  const candTown = extractTownFromMadPoint(nearest.point);
  if (!candTown) {
    counts.qMissingCandidateFields++;
    keysByOutcome.UNRESOLVED++;
    outUnresolved.write(JSON.stringify({ key: t.key, reason: "CANDIDATE_MISSING_TOWN", pipTown: t.pipTown, distM: nearest.distM }) + "\n");
    continue;
  }

  if (nearest.distM > maxDistM) {
    counts.qTooFar++;
    keysByOutcome.UNRESOLVED++;
    outUnresolved.write(JSON.stringify({ key: t.key, reason: "TOO_FAR", pipTown: t.pipTown, candTown, distM: nearest.distM }) + "\n");
    continue;
  }

  if (candTown !== t.pipTown) {
    counts.qTownMismatch++;
    keysByOutcome.KEEP_QUARANTINE++;
    outKeep.write(JSON.stringify({ key: t.key, reason: "TOWN_MISMATCH", pipTown: t.pipTown, candTown, distM: nearest.distM }) + "\n");
    continue;
  }

  const addr = extractAddrFieldsFromMadPoint(nearest.point);
  if (!addr.streetNo || !addr.streetName) {
    counts.qMissingCandidateFields++;
    keysByOutcome.UNRESOLVED++;
    outUnresolved.write(JSON.stringify({ key: t.key, reason: "CANDIDATE_MISSING_ADDR", pipTown: t.pipTown, candTown, distM: nearest.distM }) + "\n");
    continue;
  }

  // AUTO-ACCEPT patch
  const confidence = nearest.distM <= 30 ? "HIGH" : "MED_HIGH";
  const patch = {
    key: t.key,
    distM: Number(nearest.distM.toFixed(3)),
    pipTown: t.pipTown,
    candTown,
    street_no: addr.streetNo,
    street_name: addr.streetName,
    unit: addr.unit,
    zip: addr.zip,
    confidence,
  };

  patchMap.set(t.key, patch);
  keysByOutcome.AUTO_ACCEPT++;
  outAuto.write(JSON.stringify(patch) + "\n");
}

outAuto.end();
outKeep.end();
outUnresolved.end();

console.log("PatchMap built:", {
  targets: targets.size,
  patchKeys: patchMap.size,
  AUTO_ACCEPT: keysByOutcome.AUTO_ACCEPT,
  KEEP_QUARANTINE: keysByOutcome.KEEP_QUARANTINE,
  UNRESOLVED: keysByOutcome.UNRESOLVED,
});

// Step 2: Stream base and apply patches
const out = fs.createWriteStream(outPath, { encoding: "utf8" });

const brl = readline.createInterface({
  input: fs.createReadStream(basePath, { encoding: "utf8" }),
  crlfDelay: Infinity,
});

let baseTotal = 0;
let baseParseErr = 0;
let appliedRows = 0;
const appliedKeys = new Set();

for await (const line of brl) {
  const s = line.trim();
  if (!s) continue;
  baseTotal++;
  if (baseTotal % 500000 === 0) console.log(`...processed base ${baseTotal.toLocaleString()} rows`);

  let row;
  try {
    row = JSON.parse(s);
  } catch {
    baseParseErr++;
    continue;
  }

  const key = getRowKey(row);
  const patch = key ? patchMap.get(key) : null;
  if (patch) {
    appliedRows++;
    appliedKeys.add(key);

    // Apply patch fields
    row.town = patch.candTown;
    row.street_no = patch.street_no;
    row.street_name = patch.street_name;
    if (patch.unit) row.unit = patch.unit;

    // Only set zip if present; avoid overwriting a good zip with empty
    if (patch.zip) row.zip = patch.zip;

    // Rebuild labels (match your existing style)
    const parts = [];
    parts.push(`${patch.street_no} ${patch.street_name}`.trim());
    if (row.unit) parts[0] = `${parts[0]} ${row.unit}`.trim();
    if (row.town) parts.push(row.town);
    parts.push("MA");
    if (row.zip) parts.push(row.zip);
    row.address_label = parts.join(", ").replace(/\s+/g, " ").trim();

    // Optional: full_address without town/state/zip
    row.full_address = `${patch.street_no} ${patch.street_name}`.replace(/\s+/g, " ").trim();

    row.address_authority = row.address_authority ?? {};
    row.address_authority.tierC_quarantine_revalidated = {
      method: "mad_nearest + town_pip",
      keyField,
      maxDistM,
      tileSize,
      tileKeyMode,
      distM: patch.distM,
      pipTown: patch.pipTown,
      confidence: patch.confidence,
      asOf: NOW,
    };
  }

  out.write(JSON.stringify(row) + "\n");
}

out.end();

const report = {
  created_at: NOW,
  in: { basePath, quarantinePath, tilesDir, townsGeoPath },
  out: { outPath, outAutoPath, outKeepPath, outUnresolvedPath, reportPath },
  params: { maxDistM, tileSize, tileCacheN, tileKeyMode, keyField },
  counts: {
    base_total: baseTotal,
    base_parseErr: baseParseErr,
    quarantine_total: counts.qTotal,
    quarantine_parseErr: counts.qParseErr,
    quarantine_noLatLon: counts.qNoLatLon,
    quarantine_noXY: counts.qNoXY,
    quarantine_noPipTown: counts.qNoPipTown,
    targets: targets.size,
    patchKeys: patchMap.size,
    applied_to_base_rows: appliedRows,
    applied_unique_keys: appliedKeys.size,
    keep_quarantine: keysByOutcome.KEEP_QUARANTINE,
    unresolved: keysByOutcome.UNRESOLVED,
    townMismatch_keep: counts.qTownMismatch,
    missingCandidateFields: counts.qMissingCandidateFields,
    noCandidate: counts.qNoCandidate,
    tooFar: counts.qTooFar,
  },
};

fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");

console.log("DONE.");
console.log(JSON.stringify(report.counts, null, 2));
