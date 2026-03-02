import fs from "fs";
import path from "path";
import readline from "readline";
import crypto from "crypto";

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
      args[k] = v;
    }
  }
  return args;
}

function safeJsonParse(line) {
  try { return JSON.parse(line); } catch { return null; }
}

function normalizePropertyId(raw) {
  if (raw === null || raw === undefined) return null;
  let s = String(raw).trim();
  if (!s) return null;

  // If embedded "ma:parcel:" appears anywhere, extract from there
  const idx = s.toLowerCase().indexOf("ma:parcel:");
  if (idx >= 0) {
    s = s.slice(idx);
  }

  // Case-normalize prefix if present
  if (/^ma:parcel:/i.test(s)) {
    const rest = s.slice(s.indexOf(":parcel:") + ":parcel:".length);
    return "ma:parcel:" + rest.trim();
  }

  // If "TOWN|PARCEL" or similar, keep the last segment
  if (s.includes("|")) {
    const parts = s.split("|").map(p => p.trim()).filter(Boolean);
    if (parts.length) s = parts[parts.length - 1];
  }

  s = s.trim();
  if (!s) return null;

  // Treat as parcel id and prefix
  return "ma:parcel:" + s;
}

function bucketKey(propertyId) {
  // 256-bucket: first byte of sha256 hex
  const h = crypto.createHash("sha256").update(propertyId).digest("hex");
  return h.slice(0, 2);
}

function loadIndex(indexDir) {
  const manifestPath = path.join(indexDir, "INDEX_MANIFEST.json");
  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));

  const bucketsDir = path.join(indexDir, "buckets");
  const files = fs.readdirSync(bucketsDir).filter(f => f.endsWith(".ndjson"));
  files.sort();

  const map = new Map();
  let parseErr = 0;
  for (const f of files) {
    const full = path.join(bucketsDir, f);
    const lines = fs.readFileSync(full, "utf8").split(/\r?\n/);
    for (const line of lines) {
      if (!line) continue;
      const o = safeJsonParse(line);
      if (!o) { parseErr++; continue; }
      if (o.property_id) map.set(o.property_id, o);
    }
  }
  return { manifest, map, parseErr, bucketFiles: files.length };
}

async function main() {
  const args = parseArgs(process.argv);

  const infile = args.in;
  const outfile = args.out;
  const reportPath = args.report || null;
  const logEvery = Number(args.logEvery || 100000);

  if (!infile || !outfile) {
    console.error("Usage: node join_listings_with_base_zoning_v2_DROPIN.mjs --in <listings.ndjson> --out <out.ndjson> [--report <json>] [--logEvery N]");
    process.exit(1);
  }

  const backendRoot = process.cwd();
  const pointerPath = path.join(backendRoot, "publicData", "properties", "_frozen", "CURRENT_BASE_ZONING_INDEX.txt");
  if (!fs.existsSync(pointerPath)) throw new Error("Missing pointer: " + pointerPath);

  const indexDir = fs.readFileSync(pointerPath, "utf8").trim();
  const { manifest, map, parseErr: indexParseErr, bucketFiles } = loadIndex(indexDir);

  console.log("====================================================");
  console.log("[START] Join MLS listings with Base Zoning (frozen index) v2");
  console.log("[INFO ] listings in : " + path.resolve(infile));
  console.log("[INFO ] listings out: " + path.resolve(outfile));
  console.log("[INFO ] indexDir    : " + indexDir);
  console.log("[INFO ] as_of       : " + manifest.as_of);
  console.log("====================================================");
  console.log(`[INDEX] loaded=${map.size.toLocaleString()} bucketFiles=${bucketFiles} parseErr=${indexParseErr}`);

  fs.mkdirSync(path.dirname(outfile), { recursive: true });
  const out = fs.createWriteStream(outfile, { encoding: "utf8" });

  const rl = readline.createInterface({
    input: fs.createReadStream(infile, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let lines = 0, found = 0, notFound = 0, missingProp = 0, parseErr = 0;

  const idSourceCounts = {
    top_property_id: 0,
    link_property_id: 0,
    link_parcel_id: 0,
    raw_property_id: 0,
    raw_parcel_id: 0,
    none: 0
  };

  const sampleNotFound = [];
  const sampleMissing = [];

  for await (const line of rl) {
    if (!line) continue;
    lines++;

    const o = safeJsonParse(line);
    if (!o) { parseErr++; continue; }

    // pick raw id from best candidates
    let rawId = null;
    if (o.property_id) { rawId = o.property_id; idSourceCounts.top_property_id++; }
    else if (o.link?.property_id) { rawId = o.link.property_id; idSourceCounts.link_property_id++; }
    else if (o.link?.propertyId) { rawId = o.link.propertyId; idSourceCounts.link_property_id++; }
    else if (o.link?.parcel_id) { rawId = o.link.parcel_id; idSourceCounts.link_parcel_id++; }
    else if (o.link?.parcelId) { rawId = o.link.parcelId; idSourceCounts.link_parcel_id++; }
    else if (o.raw?.property_id) { rawId = o.raw.property_id; idSourceCounts.raw_property_id++; }
    else if (o.raw?.parcel_id) { rawId = o.raw.parcel_id; idSourceCounts.raw_parcel_id++; }
    else { idSourceCounts.none++; }

    const normId = normalizePropertyId(rawId);
    if (!normId) {
      missingProp++;
      // Always output required headers (UNKNOWN state)
      o.base_district_code = o.base_district_code ?? "";
      o.base_district_name = o.base_district_name ?? "";
      o.base_zone_attach_method = o.base_zone_attach_method ?? "UNKNOWN";
      o.base_zone_confidence = o.base_zone_confidence ?? 0;
      o.base_zone_evidence = o.base_zone_evidence ?? null;
      o.as_of = o.as_of ?? manifest.as_of;

      if (sampleMissing.length < 25) sampleMissing.push({ listingId: o.listingId, property_id: o.property_id ?? null });
      out.write(JSON.stringify(o) + "\n");
      continue;
    }

    const rec = map.get(normId);
    if (rec) {
      found++;

      // Canonicalize property_id safely (preserve raw)
      if (o.property_id !== normId) {
        o.property_id_raw = o.property_id ?? null;
        o.property_id = normId;
      }

      o.base_district_code = rec.base_district_code ?? "";
      o.base_district_name = rec.base_district_name ?? "";
      o.base_zone_attach_method = rec.base_zone_attach_method ?? "UNKNOWN";
      o.base_zone_confidence = rec.base_zone_confidence ?? 0;
      o.base_zone_evidence = rec.base_zone_evidence ?? null;
      o.as_of = rec.as_of ?? manifest.as_of;

      out.write(JSON.stringify(o) + "\n");
    } else {
      notFound++;

      // Always output required headers (UNKNOWN state)
      o.base_district_code = o.base_district_code ?? "";
      o.base_district_name = o.base_district_name ?? "";
      o.base_zone_attach_method = o.base_zone_attach_method ?? "UNKNOWN";
      o.base_zone_confidence = o.base_zone_confidence ?? 0;
      o.base_zone_evidence = o.base_zone_evidence ?? null;
      o.as_of = o.as_of ?? manifest.as_of;

      if (sampleNotFound.length < 25) {
        sampleNotFound.push({ listingId: o.listingId, rawId: rawId ?? null, normId });
      }

      out.write(JSON.stringify(o) + "\n");
    }

    if (logEvery && lines % logEvery === 0) {
      console.log(`[PROG] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingProp=${missingProp.toLocaleString()} parseErr=${parseErr}`);
    }
  }

  out.end();

  const report = {
    created_at: new Date().toISOString(),
    infile: path.resolve(infile),
    outfile: path.resolve(outfile),
    indexDir,
    as_of: manifest.as_of,
    counts: { lines, found, notFound, missingProp, parseErr },
    idSourceCounts,
    sampleNotFound,
    sampleMissing
  };

  if (reportPath) {
    fs.mkdirSync(path.dirname(reportPath), { recursive: true });
    fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
  }

  console.log("----------------------------------------------------");
  console.log("[DONE] Join complete.");
  console.log(`[DONE] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingProp=${missingProp.toLocaleString()} parseErr=${parseErr}`);
  if (reportPath) console.log("[DONE] report -> " + path.resolve(reportPath));
  console.log("====================================================");
}

main().catch(err => {
  console.error("[FAIL]", err?.stack || err);
  process.exit(1);
});
