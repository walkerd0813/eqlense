import fs from "fs";
import path from "path";
import readline from "readline";

function arg(name, def = null) {
  const k = `--${name}`;
  const i = process.argv.indexOf(k);
  if (i >= 0 && i + 1 < process.argv.length) return process.argv[i + 1];
  return def;
}

function argInt(name, def) {
  const v = arg(name, null);
  if (v === null) return def;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : def;
}

async function loadBucketsToMap(bucketsDir, logEveryBuckets = 25) {
  const files = fs.readdirSync(bucketsDir)
    .filter(f => f.toLowerCase().endsWith(".ndjson"))
    .sort();

  const m = new Map();
  let parseErr = 0;
  let loaded = 0;

  for (let idx = 0; idx < files.length; idx++) {
    const f = files[idx];
    const full = path.join(bucketsDir, f);

    const rl = readline.createInterface({
      input: fs.createReadStream(full, { encoding: "utf8" }),
      crlfDelay: Infinity,
    });

    for await (const line of rl) {
      const s = line.trim();
      if (!s) continue;
      try {
        const o = JSON.parse(s);
        if (o && o.property_id) {
          m.set(o.property_id, o);
          loaded++;
        }
      } catch {
        parseErr++;
      }
    }

    if ((idx + 1) % logEveryBuckets === 0 || idx === files.length - 1) {
      console.log(`[INDEX] buckets=${idx + 1}/${files.length} records=${loaded.toLocaleString()} parseErr=${parseErr}`);
    }
  }

  return { map: m, loaded, parseErr, bucketFiles: files.length };
}

function ensureDir(p) {
  const d = path.dirname(p);
  if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
}

function normalizePropertyId(row) {
  // Prefer existing property_id
  if (row.property_id && typeof row.property_id === "string" && row.property_id.trim()) {
    const pid = row.property_id.trim();
    return pid.startsWith("ma:parcel:") ? pid : pid;
  }

  // Fallback: parcel_id style fields (if present in MLS)
  const raw =
    row.parcel_id ?? row.parcelId ?? row.MAP_PAR_ID ?? row.map_par_id ?? row.parcelID;

  if (raw === undefined || raw === null) return null;

  const s = String(raw).trim();
  if (!s) return null;

  return s.startsWith("ma:parcel:") ? s : `ma:parcel:${s}`;
}

async function main() {
  const inFile = arg("in");
  const outFile = arg("out");
  const reportFile = arg("report", ".\\publicData\\_audit\\listings_base_zoning_join_report.json");
  const logEvery = argInt("logEvery", 250000);

  if (!inFile || !outFile) {
    console.error("Usage: node join_listings_with_base_zoning_v1_DROPIN.mjs --in <listings.ndjson> --out <out.ndjson> [--report <json>] [--logEvery 250000]");
    process.exit(1);
  }

  const pointerPath = ".\\publicData\\properties\\_frozen\\CURRENT_BASE_ZONING_INDEX.txt";
  if (!fs.existsSync(pointerPath)) {
    throw new Error(`Missing index pointer: ${pointerPath}`);
  }

  const indexDir = fs.readFileSync(pointerPath, "utf8").trim();
  if (!indexDir) throw new Error("CURRENT_BASE_ZONING_INDEX.txt is empty.");

  const manifestPath = path.join(indexDir, "INDEX_MANIFEST.json");
  const bucketsDir = path.join(indexDir, "buckets");

  if (!fs.existsSync(bucketsDir)) throw new Error(`Missing buckets dir: ${bucketsDir}`);
  if (!fs.existsSync(manifestPath)) throw new Error(`Missing manifest: ${manifestPath}`);

  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));

  console.log("====================================================");
  console.log("[START] Join MLS listings with Base Zoning (frozen index)");
  console.log("[INFO ] listings in :", path.resolve(inFile));
  console.log("[INFO ] listings out:", path.resolve(outFile));
  console.log("[INFO ] indexDir    :", indexDir);
  console.log("[INFO ] as_of       :", manifest.as_of);
  console.log("====================================================");

  // 1) Load index into memory
  const { map: indexMap, loaded: indexLoaded, parseErr: indexParseErr, bucketFiles } =
    await loadBucketsToMap(bucketsDir);

  console.log(`[INDEX] loaded=${indexLoaded.toLocaleString()} bucketFiles=${bucketFiles} parseErr=${indexParseErr}`);

  // 2) Stream listings and attach snapshot
  ensureDir(outFile);
  ensureDir(reportFile);

  const out = fs.createWriteStream(outFile, { encoding: "utf8" });

  const rl = readline.createInterface({
    input: fs.createReadStream(inFile, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let lines = 0;
  let parseErr = 0;
  let missingProp = 0;
  let found = 0;
  let notFound = 0;

  for await (const line of rl) {
    const s = line.trim();
    if (!s) continue;

    lines++;
    let row;
    try {
      row = JSON.parse(s);
    } catch {
      parseErr++;
      continue;
    }

    const propId = normalizePropertyId(row);
    if (!propId) {
      missingProp++;
      row.base_zoning_snapshot = null;
      out.write(JSON.stringify(row) + "\n");
      if (lines % logEvery === 0) {
        console.log(`[PROG] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingProp=${missingProp.toLocaleString()} parseErr=${parseErr}`);
      }
      continue;
    }

    // preserve if we derived it
    if (!row.property_id) row.property_id = propId;

    const rec = indexMap.get(propId);
    if (rec) {
      found++;
      row.base_zoning_snapshot = {
        as_of: rec.as_of ?? manifest.as_of ?? null,
        base_district_code: rec.base_district_code ?? null,
        base_district_name: rec.base_district_name ?? null,
        base_zone_attach_method: rec.base_zone_attach_method ?? null,
        base_zone_confidence: rec.base_zone_confidence ?? null,
        base_zone_evidence: rec.base_zone_evidence ?? null,
      };
    } else {
      notFound++;
      row.base_zoning_snapshot = null;
    }

    out.write(JSON.stringify(row) + "\n");

    if (lines % logEvery === 0) {
      console.log(`[PROG] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingProp=${missingProp.toLocaleString()} parseErr=${parseErr}`);
    }
  }

  out.end();

  const report = {
    created_at: new Date().toISOString(),
    in: path.resolve(inFile),
    out: path.resolve(outFile),
    indexDir,
    as_of: manifest.as_of ?? null,
    counts: {
      lines,
      parseErr,
      missingProp,
      found,
      notFound,
      indexLoaded,
      indexParseErr,
      bucketFiles,
    },
  };

  fs.writeFileSync(reportFile, JSON.stringify(report, null, 2), "utf8");

  console.log("----------------------------------------------------");
  console.log("[DONE] Join complete.");
  console.log(`[DONE] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingProp=${missingProp.toLocaleString()} parseErr=${parseErr}`);
  console.log(`[DONE] report -> ${path.resolve(reportFile)}`);
  console.log("====================================================");
}

main().catch(err => {
  console.error("[FAIL]", err?.stack || err);
  process.exit(1);
});
