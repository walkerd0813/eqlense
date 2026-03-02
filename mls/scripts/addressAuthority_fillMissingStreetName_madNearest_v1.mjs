#!/usr/bin/env node
/**
 * addressAuthority_fillMissingStreetName_madNearest_v1.mjs
 *
 * For rows missing street_name (or blank), attempt to fill from nearest MAD tile candidate within maxDistM
 * under a strict town guard (candidate town must match base town).
 *
 * Usage:
 *   node ./mls/scripts/addressAuthority_fillMissingStreetName_madNearest_v1.mjs `
 *     --in <BASE.ndjson> `
 *     --tilesDir <mad_tiles_0p01> `
 *     --out <OUT.ndjson> `
 *     --report <REPORT.json> `
 *     --maxDistM 60 `
 *     --tileSize 0.01 `
 *     --tileCacheN 250
 *
 * Notes:
 * - Designed as a surgical pass to reduce tierC where street_name is missing.
 * - Flexible about MAD tile point field names; tries many common variants.
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

function pickFirst(obj, keys) {
  for (const k of keys) {
    if (obj && obj[k] !== undefined && obj[k] !== null && String(obj[k]).trim() !== "") return obj[k];
  }
  return null;
}

function normTown(s) {
  if (s === null || s === undefined) return "";
  return String(s).toUpperCase().replace(/\s+/g, " ").trim();
}

function normStreet(s) {
  if (s === null || s === undefined) return "";
  let t = String(s).toUpperCase();
  t = t.replace(/[.,]/g, " ");
  t = t.replace(/\s+/g, " ").trim();
  // common expansions (keep conservative)
  t = t.replace(/\bSTREET\b/g, "ST");
  t = t.replace(/\bROAD\b/g, "RD");
  t = t.replace(/\bAVENUE\b/g, "AVE");
  t = t.replace(/\bBOULEVARD\b/g, "BLVD");
  t = t.replace(/\bDRIVE\b/g, "DR");
  t = t.replace(/\bLANE\b/g, "LN");
  t = t.replace(/\bCOURT\b/g, "CT");
  return t;
}

function getLatLng(row) {
  const lat = toNum(row.lat ?? row.latitude ?? row.y ?? row.Y);
  const lng = toNum(row.lng ?? row.lon ?? row.longitude ?? row.x ?? row.X);
  return { lat, lng };
}

function haversineM(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function ensureDir(p) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
}

function extractTileKeyFromFilename(name) {
  // try to extract two floats from filename; assumes (lon,lat) appear in that order
  const nums = name.match(/-?\d+\.\d+/g);
  if (!nums || nums.length < 2) return null;
  const lon = Number(nums[0]);
  const lat = Number(nums[1]);
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return null;
  return `${lon.toFixed(2)}|${lat.toFixed(2)}`;
}

function tileKeyForPoint(lon, lat, tileSize) {
  const x = Math.floor(lon / tileSize) * tileSize;
  const y = Math.floor(lat / tileSize) * tileSize;
  // keep 2 decimal places for 0.01 tiles; if tileSize differs, this is still ok-ish.
  return `${x.toFixed(2)}|${y.toFixed(2)}`;
}

function getNeighborKeys(lon, lat, tileSize) {
  const keys = new Set();
  const baseLon = Math.floor(lon / tileSize) * tileSize;
  const baseLat = Math.floor(lat / tileSize) * tileSize;
  for (let dx = -1; dx <= 1; dx++) {
    for (let dy = -1; dy <= 1; dy++) {
      keys.add(`${(baseLon + dx * tileSize).toFixed(2)}|${(baseLat + dy * tileSize).toFixed(2)}`);
    }
  }
  return [...keys];
}

class LRUCache {
  constructor(limit) {
    this.limit = limit;
    this.map = new Map();
  }
  get(k) {
    if (!this.map.has(k)) return null;
    const v = this.map.get(k);
    this.map.delete(k);
    this.map.set(k, v);
    return v;
  }
  set(k, v) {
    if (this.map.has(k)) this.map.delete(k);
    this.map.set(k, v);
    if (this.map.size > this.limit) {
      const first = this.map.keys().next().value;
      this.map.delete(first);
    }
  }
}

function loadTilePoints(tilePath) {
  const text = fs.readFileSync(tilePath, "utf8");
  const lines = text.split(/\r?\n/).filter(Boolean);
  const pts = [];
  for (const ln of lines) {
    try {
      const o = JSON.parse(ln);
      const lon = toNum(pickFirst(o, ["lon", "lng", "x", "X", "LONG", "LONGITUDE", "longitude"]));
      const lat = toNum(pickFirst(o, ["lat", "y", "Y", "LAT", "LATITUDE", "latitude"]));
      if (lon === null || lat === null) continue;
      pts.push({ o, lon, lat });
    } catch {
      // skip parse errors inside tiles
    }
  }
  return pts;
}

async function main() {
  const args = parseArgs(process.argv);
  const inPath = args.in;
  const tilesDir = args.tilesDir;
  const outPath = args.out;
  const reportPath = args.report;
  const maxDistM = Number(args.maxDistM ?? 60);
  const tileSize = Number(args.tileSize ?? 0.01);
  const tileCacheN = Number(args.tileCacheN ?? 250);

  if (!inPath || !tilesDir || !outPath || !reportPath) {
    console.error("Missing args. Required: --in --tilesDir --out --report");
    process.exit(1);
  }
  if (!fs.existsSync(inPath)) {
    console.error("Input not found:", inPath);
    process.exit(1);
  }
  if (!fs.existsSync(tilesDir)) {
    console.error("tilesDir not found:", tilesDir);
    process.exit(1);
  }

  ensureDir(outPath);
  ensureDir(reportPath);

  console.log("====================================================");
  console.log("Fill Missing street_name via MAD nearest (strict town guard)");
  console.log("====================================================");
  console.log("IN :", inPath);
  console.log("TIL:", tilesDir);
  console.log("OUT:", outPath);
  console.log("maxDistM:", maxDistM, "tileSize:", tileSize, "tileCacheN:", tileCacheN);

  // build tile map (key -> filepath)
  const tileFiles = fs.readdirSync(tilesDir).filter((f) => f.toLowerCase().endsWith(".ndjson"));
  const tileMap = new Map();
  let collisions = 0;
  for (const f of tileFiles) {
    const key = extractTileKeyFromFilename(f);
    if (!key) continue;
    const full = path.join(tilesDir, f);
    if (tileMap.has(key)) collisions++;
    tileMap.set(key, full);
  }
  console.log("Tile map:", { files: tileFiles.length, mapped: tileMap.size, collisions });

  const cache = new LRUCache(tileCacheN);

  const rs = fs.createReadStream(inPath, { encoding: "utf8" });
  const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });
  const ws = fs.createWriteStream(outPath, { encoding: "utf8" });

  let total = 0;
  let parseErr = 0;
  let targets = 0;
  let noCoords = 0;
  let noTile = 0;
  let noCandidate = 0;
  let tooFar = 0;
  let townMismatch = 0;
  let patched = 0;

  const examples = { patched: null, mismatch: null, tooFar: null };

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

    const streetName = row.street_name;
    const baseTown = normTown(row.town ?? row.city ?? row.municipality);

    const isTarget = isBlank(streetName);
    if (!isTarget) {
      ws.write(JSON.stringify(row) + "\n");
      if (total % 500000 === 0) console.log(`...processed ${total.toLocaleString()} rows`);
      continue;
    }

    targets++;
    const { lat, lng } = getLatLng(row);
    if (lat === null || lng === null) {
      noCoords++;
      ws.write(JSON.stringify(row) + "\n");
      continue;
    }

    const neighborKeys = getNeighborKeys(lng, lat, tileSize);
    let candidates = [];
    for (const k of neighborKeys) {
      const tilePath = tileMap.get(k);
      if (!tilePath) continue;
      let pts = cache.get(tilePath);
      if (!pts) {
        pts = loadTilePoints(tilePath);
        cache.set(tilePath, pts);
      }
      candidates = candidates.concat(pts);
    }
    if (candidates.length === 0) {
      noTile++;
      ws.write(JSON.stringify(row) + "\n");
      continue;
    }

    let best = null;
    for (const c of candidates) {
      const d = haversineM(lat, lng, c.lat, c.lon);
      if (d > maxDistM) continue;
      if (!best || d < best.d) best = { ...c, d };
    }

    if (!best) {
      tooFar++;
      if (!examples.tooFar) examples.tooFar = { lat, lng, town: row.town ?? null, address_label: row.address_label ?? null };
      ws.write(JSON.stringify(row) + "\n");
      continue;
    }

    // extract candidate fields
    const candTown = normTown(pickFirst(best.o, ["town", "TOWN", "city", "CITY", "municipality", "MUNICIPALITY", "COMMUNITY", "community"]));
    if (baseTown && candTown && baseTown !== candTown) {
      townMismatch++;
      if (!examples.mismatch) {
        examples.mismatch = {
          baseTown,
          candTown,
          base: { lat, lng, address_label: row.address_label ?? null },
          cand: { lat: best.lat, lng: best.lon },
        };
      }
      ws.write(JSON.stringify(row) + "\n");
      continue;
    }

    const candStreet = pickFirst(best.o, [
      "street_name",
      "STREETNAME",
      "street",
      "STREET",
      "road",
      "ROAD",
      "FULLSTREET",
      "full_street",
      "name",
      "NAME",
    ]);
    const candNo = pickFirst(best.o, ["street_no", "ADDRNUM", "addrnum", "house_number", "NUMBER", "number"]);

    // Apply patch
    const before = { street_name: row.street_name ?? null, street_no: row.street_no ?? null };
    if (!isBlank(candStreet)) row.street_name = String(candStreet).trim();
    if (isBlank(row.street_no) && !isBlank(candNo)) row.street_no = String(candNo).trim();

    row.addr_fill = row.addr_fill ?? {};
    row.addr_fill.missStreetName = {
      method: "madNearest:tiles",
      maxDistM,
      distM: Number(best.d.toFixed(2)),
      asOf: new Date().toISOString(),
      confidence: best.d <= 30 ? "HIGH" : "MED",
    };

    if (!examples.patched) {
      examples.patched = {
        before,
        after: { street_name: row.street_name ?? null, street_no: row.street_no ?? null },
        town: row.town ?? null,
        zip: row.zip ?? null,
        distM: Number(best.d.toFixed(2)),
      };
    }

    patched++;
    ws.write(JSON.stringify(row) + "\n");

    if (total % 500000 === 0) console.log(`...processed ${total.toLocaleString()} rows`);
  }

  ws.end();

  const report = {
    created_at: new Date().toISOString(),
    in: inPath,
    tilesDir,
    out: outPath,
    params: { maxDistM, tileSize, tileCacheN },
    counts: {
      total_rows: total,
      parseErr,
      targets_missingStreetName: targets,
      patched,
      noCoords,
      noTileCandidates: noTile,
      tooFar,
      townMismatch_kept: townMismatch,
      noCandidate: noCandidate,
    },
    examples,
  };

  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
  console.log("DONE.");
  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error("FATAL:", e?.stack || e);
  process.exit(1);
});
