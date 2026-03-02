import fs from "fs";
import path from "path";
import readline from "readline";

// =====================
// PATCH: StreetNo “0” is missing
// Paste this block RIGHT HERE (after imports)
// =====================
function isMissingStreetNo(v) {
  const s = String(v ?? "").trim();
  if (!s) return true;
  if (s === "0" || s === "00") return true;
  return false;
}

// (optional but recommended if you also gate on street name)
function isMissingStreetName(v) {
  const s = String(v ?? "").trim();
  return !s || s === "-" || s.toLowerCase() === "null";
}

// =====================
// END PATCH BLOCK
// =====================

// your existing code continues here:
// const args = parseArgs(...)
// function main() { ... }


function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : "true";
    out[k] = v;
  }
  return out;
}

function isBlank(v) {
  if (v === null || v === undefined) return true;
  const s = String(v).trim();
  return s === "" || s === "-" || s.toLowerCase() === "null" || s.toLowerCase() === "undefined";
}

function normZip(v) {
  if (isBlank(v)) return "";
  const s = String(v).replace(/[^\d]/g, "").slice(0, 5);
  return s.length === 5 ? s : "";
}

function normStreetName(v) {
  if (isBlank(v)) return "";
  return String(v)
    .toUpperCase()
    .replace(/[^A-Z0-9 ]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function haversineM(lon1, lat1, lon2, lat2) {
  const R = 6371000;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

function tileKey(lon, lat, cellSize) {
  const ix = Math.floor(lon / cellSize);
  const iy = Math.floor(lat / cellSize);
  return { ix, iy, key: `${ix}_${iy}` };
}

class LRUCache {
  constructor(max) { this.max = max; this.map = new Map(); }
  get(k) {
    const v = this.map.get(k);
    if (!v) return null;
    this.map.delete(k); this.map.set(k, v);
    return v;
  }
  set(k, v) {
    if (this.map.has(k)) this.map.delete(k);
    this.map.set(k, v);
    while (this.map.size > this.max) {
      const oldest = this.map.keys().next().value;
      this.map.delete(oldest);
    }
  }
}

async function loadTilePoints(tilePath) {
  if (!fs.existsSync(tilePath)) return [];
  const pts = [];
  const rl = readline.createInterface({ input: fs.createReadStream(tilePath, "utf8"), crlfDelay: Infinity });
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    pts.push(JSON.parse(t));
  }
  return pts;
}

function classifyStreetNo(raw) {
  const s = isBlank(raw) ? "" : String(raw).trim().toUpperCase();

  if (!s) return { ok: false, kind: "MISSING" };
  if (s === "0" || /^0+$/.test(s)) return { ok: false, kind: "ZERO" };

  // strip common formatting
  const t = s.replace(/\s+/g, "");

  // Accept: 123, 123A
  if (/^\d{1,6}[A-Z]?$/.test(t)) return { ok: true, kind: "OK", norm: t };

  // Common "bad but explainable" forms (don’t treat as OK)
  if (/^\d{1,6}-\d{1,6}$/.test(t)) return { ok: false, kind: "RANGE" };
  if (/^\d{1,6}\/\d{1,6}$/.test(t)) return { ok: false, kind: "FRACTION" };

  return { ok: false, kind: "NONSTANDARD" };
}

function isBadStreetNo(raw) {
  return !classifyStreetNo(raw).ok;
}


function needsAddressFix(p) {
  const streetNo = isBlank(p.street_no) ? "" : String(p.street_no).trim();
  const streetName = isBlank(p.street_name) ? "" : String(p.street_name).trim();
  const fullAddr = isBlank(p.full_address) ? "" : String(p.full_address).trim();
  const zip = isBlank(p.zip) ? "" : String(p.zip).trim();

  return (
    !streetName ||
    !fullAddr ||
    !zip ||
    !streetNo ||
    isBadStreetNo(streetNo)
  );
}


function buildFullAddress(no, name, unit) {
  const n = isBlank(no) ? "" : String(no).trim();
  const s = isBlank(name) ? "" : String(name).trim();
  if (!n || !s) return "";
  const u = isBlank(unit) ? "" : String(unit).trim();
  return u ? `${n} ${s} #${u}`.replace(/\s+/g, " ").trim() : `${n} ${s}`.replace(/\s+/g, " ").trim();
}

async function main() {
  const args = parseArgs(process.argv);

  const IN = args.in;
  const TILE_DIR = args.madTiles;
  const OUT = args.out;
  const META = args.meta;

  const cellSize = Number(args.cellSize || 0.01);
  const maxDistM = Number(args.maxDistM || 60);       // strict
  const tileCacheN = Number(args.tileCacheN || 250);  // in-memory tiles

  if (!IN || !TILE_DIR || !OUT) {
    console.log("USAGE: node addressAuthority_applyMadNearest_v1_DROPIN.js --in <v27.ndjson> --madTiles <tileDir> --out <v28.ndjson> [--meta <meta.json>]");
    process.exit(1);
  }
  if (!fs.existsSync(IN)) throw new Error(`IN not found: ${IN}`);
  if (!fs.existsSync(TILE_DIR)) throw new Error(`madTiles dir not found: ${TILE_DIR}`);

  console.log("ADDRESS AUTHORITY UPGRADE V1 — APPLY (NEAREST MAD POINT)");
  console.log("====================================================");
  console.log("in:", IN);
  console.log("madTiles:", TILE_DIR);
  console.log("out:", OUT);
  console.log("cellSize:", cellSize, "maxDistM:", maxDistM);
  console.log("----------------------------------------------------");

  const tileCache = new LRUCache(tileCacheN);

  const inRL = readline.createInterface({ input: fs.createReadStream(IN, "utf8"), crlfDelay: Infinity });
  const tmpOut = OUT + ".tmp";
  const outStream = fs.createWriteStream(tmpOut, { encoding: "utf8" });

  let total = 0;
  let candidate = 0;
  let patched = 0;
  let stillMissing = 0;
  let tooFar = 0;

  for await (const line of inRL) {
    const t = line.trim();
    if (!t) continue;
    total++;

    const p = JSON.parse(t);

    // require coords
    const lon = Number(p.lng ?? p.lon ?? p.longitude);
    const lat = Number(p.lat ?? p.latitude);
    if (!Number.isFinite(lon) || !Number.isFinite(lat) || !needsAddressFix(p)) {
      outStream.write(JSON.stringify(p) + "\n");
      continue;
    }

    candidate++;

    const pZip = normZip(p.zip);
    const pTown = isBlank(p.city_town) ? (isBlank(p.city) ? "" : String(p.city).trim()) : String(p.city_town).trim();
    const pTownN = normStreetName(pTown);
    const pStreetNameN = normStreetName(p.street_name);

    const { ix, iy } = tileKey(lon, lat, cellSize);

    // load 3x3 neighbor tiles
    let pts = [];
    for (let dx = -1; dx <= 1; dx++) {
      for (let dy = -1; dy <= 1; dy++) {
        const k = `${ix + dx}_${iy + dy}`;
        let tilePts = tileCache.get(k);
        if (!tilePts) {
          const tp = path.join(TILE_DIR, `tile_${k}.ndjson`);
          tilePts = await loadTilePoints(tp);
          tileCache.set(k, tilePts);
        }
        if (tilePts.length) pts = pts.concat(tilePts);
      }
    }

    if (!pts.length) {
      stillMissing++;
      outStream.write(JSON.stringify(p) + "\n");
      continue;
    }

    // Prefer: same ZIP first, else same town, else all
    let pool = pts;
    if (pZip) {
      const z = pool.filter((q) => normZip(q.zip) === pZip);
      if (z.length) pool = z;
    }
    if (pTownN) {
      const tt = pool.filter((q) => normStreetName(q.town) === pTownN);
      if (tt.length) pool = tt;
    }
    if (pStreetNameN) {
      const sn = pool.filter((q) => normStreetName(q.street_name) === pStreetNameN);
      if (sn.length) pool = sn;
    }

    let best = null;
    let bestD = Infinity;
    for (const q of pool) {
      const d = haversineM(lon, lat, q.lon, q.lat);
      if (d < bestD) { bestD = d; best = q; }
    }

    if (!best || !Number.isFinite(bestD)) {
      stillMissing++;
      outStream.write(JSON.stringify(p) + "\n");
      continue;
    }

    if (bestD > maxDistM) {
      // too risky for V1 auto-fill
      tooFar++;
      outStream.write(JSON.stringify(p) + "\n");
      continue;
    }

    // Apply only missing fields
    let changed = false;


    // Street number: fill if missing OR clearly bad (000/0/etc)
    const curNoClass = classifyStreetNo(p.street_no);
    const candNoClass = classifyStreetNo(best?.street_no);

    if ((curNoClass.isMissing || curNoClass.isBad) && candNoClass.norm && !candNoClass.isBad) {
      p.street_no = candNoClass.norm;
      changed = true;
    }

    if (isBlank(p.street_name) && !isBlank(best.street_name)) { p.street_name = normStreetName(best.street_name); changed = true; }

    // Town + ZIP are optional to patch here (your canonical already has town for all rows)
    if (("city_town" in p) && isBlank(p.city_town) && !isBlank(best.town)) { p.city_town = String(best.town).trim(); changed = true; }
    if (isBlank(p.town) && !isBlank(best.town)) { p.town = String(best.town).trim(); changed = true; }
    if (isBlank(p.zip) && !isBlank(best.zip)) { const z = normZip(best.zip); if (z) { p.zip = z; changed = true; } }

    if (isBlank(p.full_address)) {
      const fa = buildFullAddress(p.street_no, p.street_name, p.unit);
      if (fa) { p.full_address = fa; changed = true; }
    }

    if (changed) {
      patched++;
      p.addr_authority = {
        ...(p.addr_authority || {}),
        mad_nearest_v1: {
          source: "MassGIS MAD_ADDRESS_POINTS_GC",
          method: "NEAREST_POINT_TO_PARCEL_CENTROID",
          maxDistM,
          distM: Math.round(bestD * 10) / 10,
          cellSize,
          matched_on: {
            zip: pZip || null,
            town: pTown || null,
            street_name: isBlank(p.street_name) ? null : p.street_name,
          },
          matched_point: {
            centroid_id: best.centroid_id || null,
            point_type: best.point_type || null,
          },
          applied_at: new Date().toISOString(),
        },
      };
    } else {
      stillMissing++;
    }

    outStream.write(JSON.stringify(p) + "\n");
  }

  outStream.end();
  await new Promise((res) => outStream.on("finish", res));
  fs.renameSync(tmpOut, OUT);

  const meta = {
    created_at: new Date().toISOString(),
    in: IN,
    madTiles: TILE_DIR,
    out: OUT,
    params: { cellSize, maxDistM, tileCacheN },
    counts: { total_rows: total, candidate_rows: candidate, patched_rows: patched, still_missing_rows: stillMissing, too_far_rows: tooFar },
  };

  if (META) fs.writeFileSync(META, JSON.stringify(meta, null, 2), "utf8");
  console.log("[done]", meta);
}

main().catch((e) => {
  console.error("❌ applyMadNearest failed:", e);
  process.exit(1);
});
