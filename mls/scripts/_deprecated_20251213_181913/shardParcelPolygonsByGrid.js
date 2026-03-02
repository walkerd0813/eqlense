// backend/mls/scripts/shardParcelPolygonsByGrid.js
// ---------------------------------------------------
// Stream-shard statewide parcel polygons (GeoJSONSeq / NDJSON) into grid tiles.
// INPUT:  backend/publicData/parcels/parcels.ndjson
// OUTPUT: backend/publicData/parcels/tiles_0p02/tile_<latIdx>_<lonIdx>.ndjson
//
// Run (from C:\seller-app\backend):
//   node mls/scripts/shardParcelPolygonsByGrid.js
//
// Optional flags:
//   --tileDeg 0.02
//   --maxOpen 128
//   --in  <path>
//   --out <dir>

import fs from "fs";
import fsp from "fs/promises";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function getArg(name, fallback = null) {
  const idx = process.argv.indexOf(name);
  if (idx === -1) return fallback;
  return process.argv[idx + 1] ?? fallback;
}
function numArg(name, fallback) {
  const v = getArg(name, null);
  if (v == null) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

const INPUT_PATH = path.resolve(__dirname, getArg("--in", "../../publicData/parcels/parcels.ndjson"));
const OUT_DIR = path.resolve(__dirname, getArg("--out", "../../publicData/parcels/tiles_0p02"));

const TILE_DEG = numArg("--tileDeg", 0.02);
const MAX_OPEN = Math.max(8, Math.floor(numArg("--maxOpen", 128)));

function tileIndex(v) {
  return Math.floor(v / TILE_DEG); // works for negatives too
}
function tileKeyFromIdx(latIdx, lonIdx) {
  return `${latIdx}_${lonIdx}`;
}
function tilePath(key) {
  return path.join(OUT_DIR, `tile_${key}.ndjson`);
}

function walkCoords(geom, fn) {
  if (!geom) return;
  const t = geom.type;
  const c = geom.coordinates;

  if (t === "Polygon") {
    for (const ring of c || []) for (const p of ring || []) fn(p);
  } else if (t === "MultiPolygon") {
    for (const poly of c || []) for (const ring of poly || []) for (const p of ring || []) fn(p);
  } else if (t === "GeometryCollection") {
    for (const g of geom.geometries || []) walkCoords(g, fn);
  }
}

function bboxOfGeometry(geom) {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;

  walkCoords(geom, (p) => {
    if (!Array.isArray(p) || p.length < 2) return;
    const x = Number(p[0]);
    const y = Number(p[1]);
    if (!Number.isFinite(x) || !Number.isFinite(y)) return;
    if (x < minX) minX = x;
    if (y < minY) minY = y;
    if (x > maxX) maxX = x;
    if (y > maxY) maxY = y;
  });

  if (!Number.isFinite(minX) || !Number.isFinite(minY)) return null;
  return { minX, minY, maxX, maxY };
}

class LRUWriters {
  constructor(maxOpen) {
    this.maxOpen = maxOpen;
    this.map = new Map(); // key -> {stream,lastUsed}
  }

  async get(key) {
    const existing = this.map.get(key);
    if (existing) {
      existing.lastUsed = Date.now();
      return existing;
    }

    if (this.map.size >= this.maxOpen) {
      let oldestKey = null;
      let oldestTs = Infinity;
      for (const [k, v] of this.map.entries()) {
        if (v.lastUsed < oldestTs) {
          oldestTs = v.lastUsed;
          oldestKey = k;
        }
      }
      if (oldestKey) {
        const w = this.map.get(oldestKey);
        if (w) await new Promise((res) => w.stream.end(res));
        this.map.delete(oldestKey);
      }
    }

    await fsp.mkdir(OUT_DIR, { recursive: true });
    const stream = fs.createWriteStream(tilePath(key), { flags: "a" });
    const obj = { stream, lastUsed: Date.now() };
    this.map.set(key, obj);
    return obj;
  }

  async closeAll() {
    const closers = [];
    for (const [, v] of this.map.entries()) closers.push(new Promise((res) => v.stream.end(res)));
    await Promise.all(closers);
    this.map.clear();
  }
}

async function main() {
  console.log("====================================================");
  console.log(" SHARDING PARCEL POLYGONS BY GRID (NDJSON)");
  console.log("====================================================");
  console.log("Input :", INPUT_PATH);
  console.log("OutDir:", OUT_DIR);
  console.log("Tile° :", TILE_DEG);
  console.log("MaxOpen:", MAX_OPEN);
  console.log("----------------------------------------------------");

  if (!fs.existsSync(INPUT_PATH)) {
    console.error("❌ Missing input NDJSON:", INPUT_PATH);
    console.error("Create it with: ogr2ogr -f GeoJSONSeq parcels.ndjson parcels.gpkg parcels");
    process.exit(1);
  }

  const writers = new LRUWriters(MAX_OPEN);
  const rl = readline.createInterface({
    input: fs.createReadStream(INPUT_PATH),
    crlfDelay: Infinity,
  });

  let total = 0;
  let writes = 0;
  const tileCounts = new Map();

  for await (const line of rl) {
    if (!line) continue;
    total++;

    let feat;
    try {
      feat = JSON.parse(line);
    } catch (e) {
      console.error("❌ Bad JSON line at parcel", total);
      throw e;
    }

    const bbox = bboxOfGeometry(feat?.geometry);
    if (!bbox) continue;

    const latMinIdx = tileIndex(bbox.minY);
    const latMaxIdx = tileIndex(bbox.maxY);
    const lonMinIdx = tileIndex(bbox.minX);
    const lonMaxIdx = tileIndex(bbox.maxX);

    const latSpan = Math.abs(latMaxIdx - latMinIdx) + 1;
    const lonSpan = Math.abs(lonMaxIdx - lonMinIdx) + 1;
    const cells = latSpan * lonSpan;

    // prevent “giant parcel spans 500 tiles” explosions
    const MAX_CELLS_PER_FEATURE = 25;

    if (cells > MAX_CELLS_PER_FEATURE) {
      const centerLat = (bbox.minY + bbox.maxY) / 2;
      const centerLon = (bbox.minX + bbox.maxX) / 2;
      const key = tileKeyFromIdx(tileIndex(centerLat), tileIndex(centerLon));
      const w = await writers.get(key);
      w.stream.write(line + "\n");
      tileCounts.set(key, (tileCounts.get(key) ?? 0) + 1);
      writes++;
    } else {
      for (let lati = Math.min(latMinIdx, latMaxIdx); lati <= Math.max(latMinIdx, latMaxIdx); lati++) {
        for (let loni = Math.min(lonMinIdx, lonMaxIdx); loni <= Math.max(lonMinIdx, lonMaxIdx); loni++) {
          const key = tileKeyFromIdx(lati, loni);
          const w = await writers.get(key);
          w.stream.write(line + "\n");
          tileCounts.set(key, (tileCounts.get(key) ?? 0) + 1);
          writes++;
        }
      }
    }

    if (total % 50_000 === 0) {
      console.log(`[shard] parcels=${total.toLocaleString()} | writes=${writes.toLocaleString()} | tiles=${tileCounts.size.toLocaleString()}`);
    }
  }

  await writers.closeAll();

  await fsp.writeFile(
    path.join(OUT_DIR, "tileIndex.json"),
    JSON.stringify(
      {
        tileDeg: TILE_DEG,
        tiles: Object.fromEntries([...tileCounts.entries()].sort((a, b) => b[1] - a[1])),
        stats: { parcelsRead: total, writes, tilesCreated: tileCounts.size },
      },
      null,
      2
    ),
    "utf8"
  );

  console.log("====================================================");
  console.log("✅ Sharding complete");
  console.log("Tiles:", tileCounts.size.toLocaleString());
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ Sharding failed:", err);
  process.exit(1);
});
