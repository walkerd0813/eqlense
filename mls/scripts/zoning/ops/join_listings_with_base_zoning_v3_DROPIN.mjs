import fs from "fs";
import path from "path";
import readline from "readline";
import crypto from "crypto";

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
    args[k] = v;
  }
  return args;
}

function stripBom(s) {
  if (!s) return s;
  return String(s).replace(/^\uFEFF/, "");
}

function safeJsonParse(line) {
  try { return JSON.parse(line); } catch { return null; }
}

function sha256hex(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}
function bucketForKey(key) {
  return sha256hex(key).slice(0, 2);
}

function normJurisdictionKey(s) {
  if (!s) return "";
  return String(s)
    .trim()
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function getDeep(o, pathArr) {
  let cur = o;
  for (const k of pathArr) {
    if (!cur || typeof cur !== "object") return undefined;
    cur = cur[k];
  }
  return cur;
}

function firstNonEmpty(...vals) {
  for (const v of vals) {
    if (v === undefined || v === null) continue;
    const s = String(v).trim();
    if (s.length) return s;
  }
  return "";
}

function deriveParcelId(listing) {
  // Prefer explicit parcel_id fields
  const fromLink =
    firstNonEmpty(
      getDeep(listing, ["link", "parcel_id"]),
      getDeep(listing, ["link", "parcelId"]),
      getDeep(listing, ["link", "parcelID"]),
      getDeep(listing, ["link", "MAP_PAR_ID"]),
      getDeep(listing, ["link", "map_par_id"])
    );

  if (fromLink) return fromLink;

  // Otherwise extract from property_id if it matches ma:parcel:<parcel_id>
  const pid = firstNonEmpty(listing.property_id, getDeep(listing, ["link", "property_id"]));
  if (pid && pid.startsWith("ma:parcel:")) return pid.slice("ma:parcel:".length);

  return "";
}

function deriveJurisdictionName(listing) {
  // Prefer jurisdiction-like keys
  const j =
    firstNonEmpty(
      listing.jurisdiction_name,
      listing.jurisdiction,
      getDeep(listing, ["link", "jurisdiction_name"]),
      getDeep(listing, ["link", "jurisdiction"]),
      getDeep(listing, ["address", "town"]),
      getDeep(listing, ["address", "city"]),
      getDeep(listing, ["address", "municipality"]),
      getDeep(listing, ["physical", "town"]),
      getDeep(listing, ["physical", "city"]),
      getDeep(listing, ["raw", "TOWN"]),
      getDeep(listing, ["raw", "town"]),
      getDeep(listing, ["raw", "City"]),
      getDeep(listing, ["raw", "CITY"])
    );
  return j;
}

function betterPick(a, b) {
  // Choose best zoning record when duplicate property_uid exists in index
  // Priority: higher confidence, has code, has name, prefer point_in_poly
  const aConf = Number(a?.base_zone_confidence || 0);
  const bConf = Number(b?.base_zone_confidence || 0);
  if (bConf !== aConf) return bConf > aConf ? b : a;

  const aHasCode = !!a?.base_district_code;
  const bHasCode = !!b?.base_district_code;
  if (bHasCode !== aHasCode) return bHasCode ? b : a;

  const aHasName = !!a?.base_district_name;
  const bHasName = !!b?.base_district_name;
  if (bHasName !== aHasName) return bHasName ? b : a;

  const aMeth = String(a?.base_zone_attach_method || "");
  const bMeth = String(b?.base_zone_attach_method || "");
  if (aMeth !== bMeth) {
    if (bMeth === "point_in_poly") return b;
    if (aMeth === "point_in_poly") return a;
  }

  // default: keep existing
  return a;
}

async function loadUidIndex(indexDir) {
  const bucketsDir = path.join(indexDir, "buckets");
  const files = fs.readdirSync(bucketsDir).filter(f => f.toLowerCase().endsWith(".ndjson"));
  const map = new Map();
  let parseErr = 0;
  let lines = 0;
  let dup = 0;

  for (const f of files) {
    const full = path.join(bucketsDir, f);
    const rl = readline.createInterface({
      input: fs.createReadStream(full, { encoding: "utf8" }),
      crlfDelay: Infinity
    });

    for await (const line of rl) {
      if (!line) continue;
      lines++;
      const o = safeJsonParse(line);
      if (!o) { parseErr++; continue; }
      const key = o.property_uid;
      if (!key) continue;

      if (map.has(key)) {
        dup++;
        const cur = map.get(key);
        map.set(key, betterPick(cur, o));
      } else {
        map.set(key, o);
      }
    }
  }

  return { map, lines, parseErr, dup, files: files.length };
}

async function main() {
  const args = parseArgs(process.argv);
  const backendRoot = process.cwd();

  const infile = args.in;
  const outfile = args.out;
  const reportPath = args.report || null;
  const logEvery = Number(args.logEvery || 100000);

  if (!infile || !outfile) {
    throw new Error("Usage: --in <listings.ndjson> --out <out.ndjson> [--report <report.json>] [--logEvery N]");
  }

  // Use UID index pointer by default
  let indexDir = args.indexDir;
  if (!indexDir) {
    const ptr = path.join(backendRoot, "publicData", "properties", "_frozen", "CURRENT_BASE_ZONING_INDEX_UID.txt");
    if (!fs.existsSync(ptr)) throw new Error("Missing UID index pointer: " + ptr);
    indexDir = stripBom(fs.readFileSync(ptr, "utf8")).trim();
  }

  const manifestPath = path.join(indexDir, "INDEX_MANIFEST.json");
  const indexManifest = fs.existsSync(manifestPath) ? JSON.parse(stripBom(fs.readFileSync(manifestPath, "utf8"))) : null;
  const asOf = indexManifest?.as_of || indexManifest?.asOf || null;

  console.log("====================================================");
  console.log("[START] Join MLS listings with Base Zoning (UID index) v3");
  console.log("[INFO ] listings in : " + path.resolve(infile));
  console.log("[INFO ] listings out: " + path.resolve(outfile));
  console.log("[INFO ] uidIndexDir  : " + path.resolve(indexDir));
  console.log("[INFO ] as_of        : " + (asOf || "(null)"));
  console.log("====================================================");

  // Load UID index
  const idx = await loadUidIndex(indexDir);
  console.log(`[INDEX] loadedUnique=${idx.map.size.toLocaleString()} bucketFiles=${idx.files} linesRead=${idx.lines.toLocaleString()} dupMerged=${idx.dup.toLocaleString()} parseErr=${idx.parseErr}`);

  // Stream join
  fs.mkdirSync(path.dirname(outfile), { recursive: true });
  const out = fs.createWriteStream(outfile, { flags: "w", encoding: "utf8" });

  const rl = readline.createInterface({
    input: fs.createReadStream(infile, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  let lines = 0, found = 0, notFound = 0, missingProp = 0, parseErr = 0;
  let missingParcel = 0, missingJurisdiction = 0;

  for await (const line of rl) {
    if (!line) continue;
    lines++;

    const o = safeJsonParse(line);
    if (!o) { parseErr++; continue; }

    // property_id missing means we can still try to build uid from parcel_id derived from property_id; so track separately
    const parcel_id = deriveParcelId(o);
    const jName = deriveJurisdictionName(o);
    const jKey = normJurisdictionKey(jName);

    if (!parcel_id) missingParcel++;
    if (!jKey) missingJurisdiction++;

    let property_uid = "";
    if (parcel_id && jKey) {
      property_uid = `${jKey}|${parcel_id}`;
    }

    let z = null;
    if (property_uid) {
      z = idx.map.get(property_uid) || null;
    }

    if (!property_uid) {
      missingProp++;
    } else if (z) {
      found++;
      // attach fields at top-level to match your current v2 output expectations
      o.property_uid = property_uid;
      o.base_district_code = z.base_district_code || "";
      o.base_district_name = z.base_district_name || "";
      o.base_zone_attach_method = z.base_zone_attach_method || "";
      o.base_zone_confidence = Number(z.base_zone_confidence || 0);
      o.base_zone_evidence = z.base_zone_evidence || null;
      o.as_of = z.as_of || asOf || null;
    } else {
      notFound++;
      o.property_uid = property_uid || "";
      // Do NOT invent zoning on misses. Keep clean + honest.
    }

    out.write(JSON.stringify(o) + "\n");

    if (logEvery && lines % logEvery === 0) {
      console.log(`[PROG] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingUid=${missingProp.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
    }
  }

  out.end();

  const report = {
    created_at: new Date().toISOString(),
    infile: path.resolve(infile),
    outfile: path.resolve(outfile),
    uid_index_dir: path.resolve(indexDir),
    as_of: asOf,
    index_loaded_unique: idx.map.size,
    index_bucket_files: idx.files,
    index_lines_read: idx.lines,
    index_dup_merged: idx.dup,
    index_parseErr: idx.parseErr,
    lines,
    found,
    notFound,
    missingUid: missingProp,
    missingParcel,
    missingJurisdiction,
    parseErr
  };

  if (reportPath) {
    fs.mkdirSync(path.dirname(reportPath), { recursive: true });
    fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
  }

  console.log("----------------------------------------------------");
  console.log("[DONE] Join complete.");
  console.log(`[DONE] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingUid=${missingProp.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
  if (reportPath) console.log("[DONE] report -> " + path.resolve(reportPath));
  console.log("====================================================");
}

main().catch(err => {
  console.error("[FAIL]", err?.stack || err);
  process.exit(1);
});
