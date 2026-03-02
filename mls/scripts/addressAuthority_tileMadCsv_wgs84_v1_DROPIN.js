import fs from "fs";
import path from "path";
import readline from "readline";

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

function parseCSVLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; }
      else inQ = !inQ;
      continue;
    }
    if (c === "," && !inQ) { out.push(cur); cur = ""; continue; }
    cur += c;
  }
  out.push(cur);
  return out;
}

function tileKey(lon, lat, cellSize) {
  const ix = Math.floor(lon / cellSize);
  const iy = Math.floor(lat / cellSize);
  return { ix, iy, key: `${ix}_${iy}` };
}

class LRUStreams {
  constructor(maxOpen) {
    this.maxOpen = maxOpen;
    this.map = new Map(); // key -> {stream, last}
  }
  get(key, filePath) {
    const now = Date.now();
    const hit = this.map.get(key);
    if (hit) { hit.last = now; return hit.stream; }

    while (this.map.size >= this.maxOpen) {
      let oldestK = null, oldestT = Infinity;
      for (const [k, v] of this.map.entries()) {
        if (v.last < oldestT) { oldestT = v.last; oldestK = k; }
      }
      if (oldestK) {
        const v = this.map.get(oldestK);
        try { v.stream.end(); } catch {}
        this.map.delete(oldestK);
      } else break;
    }

    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    const stream = fs.createWriteStream(filePath, { flags: "a", encoding: "utf8" });
    this.map.set(key, { stream, last: now });
    return stream;
  }
  async closeAll() {
    for (const v of this.map.values()) {
      await new Promise((res) => v.stream.end(res));
    }
    this.map.clear();
  }
}

async function main() {
  const args = parseArgs(process.argv);

  const CSV = args.csv;
  const OUTDIR = args.outDir;
  const cellSize = Number(args.cellSize || 0.01); // ~1km lat; good for MA
  const maxOpen = Number(args.maxOpen || 80);

  if (!CSV || !OUTDIR) {
    console.log("USAGE: node addressAuthority_tileMadCsv_wgs84_v1_DROPIN.js --csv <mad.csv> --outDir <dir> [--cellSize 0.01]");
    process.exit(1);
  }
  if (!fs.existsSync(CSV)) throw new Error(`CSV not found: ${CSV}`);

  console.log("ADDRESS AUTHORITY UPGRADE V1 — TILE MAD CSV (WGS84)");
  console.log("====================================================");
  console.log("csv:", CSV);
  console.log("outDir:", OUTDIR);
  console.log("cellSize:", cellSize);
  console.log("----------------------------------------------------");

  const rl = readline.createInterface({ input: fs.createReadStream(CSV, "utf8"), crlfDelay: Infinity });

  let header = null;
  let idx = null;

  const streams = new LRUStreams(maxOpen);

  let total = 0;
  let written = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;

    if (!header) {
      header = parseCSVLine(line).map((s) => s.trim());
      idx = new Map(header.map((k, i) => [k, i]));
      continue;
    }

    total++;
    const row = parseCSVLine(line);

    const lon = Number(row[idx.get("X")]);
    const lat = Number(row[idx.get("Y")]);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;

    const { key } = tileKey(lon, lat, cellSize);
    const outPath = path.join(OUTDIR, `tile_${key}.ndjson`);

    const rec = {
      lon, lat,
      centroid_id: (row[idx.get("CENTROID_ID")] || "").trim(),
      street_no: (row[idx.get("FULL_NUMBER_STANDARDIZED")] || row[idx.get("ADDRESS_NUMBER")] || "").toString().trim(),
      street_name: (row[idx.get("STREET_NAME")] || "").trim(),
      unit: (row[idx.get("UNIT")] || "").trim(),
      town: (row[idx.get("GEOGRAPHIC_TOWN")] || row[idx.get("COMMUNITY_NAME")] || "").trim(),
      zip: (row[idx.get("POSTCODE")] || "").trim(),
      point_type: (row[idx.get("POINT_TYPE")] || "").trim(),
    };

    const s = streams.get(key, outPath);
    s.write(JSON.stringify(rec) + "\n");
    written++;
  }

  await streams.closeAll();

  const meta = {
    created_at: new Date().toISOString(),
    csv: CSV,
    outDir: OUTDIR,
    cellSize,
    counts: { csv_rows_scanned: total, points_written: written },
  };

  const metaPath = path.join(OUTDIR, "_meta.json");
  fs.writeFileSync(metaPath, JSON.stringify(meta, null, 2), "utf8");

  console.log("[done]", meta);
}

main().catch((e) => {
  console.error("❌ tileMadCsv failed:", e);
  process.exit(1);
});
