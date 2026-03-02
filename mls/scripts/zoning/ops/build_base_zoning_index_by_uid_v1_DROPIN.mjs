import fs from "fs";
import path from "path";
import crypto from "crypto";
import readline from "readline";

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

function safeJsonParse(line) {
  try { return JSON.parse(line); } catch { return null; }
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

function sha256hex(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}

function bucketForKey(key) {
  return sha256hex(key).slice(0, 2);
}

async function main() {
  const args = parseArgs(process.argv);

  const backendRoot = process.cwd();

  // Default to CURRENT_BASE_ZONING pointer if --in not provided
  let infile = args.in;
  if (!infile) {
    const ptr = path.join(backendRoot, "publicData", "properties", "_frozen", "CURRENT_BASE_ZONING.txt");
    if (!fs.existsSync(ptr)) throw new Error("Missing pointer: " + ptr);
    const p = fs.readFileSync(ptr, "utf8").trim();
    if (fs.existsSync(p) && fs.statSync(p).isDirectory()) {
      const nd = fs.readdirSync(p).filter(f => f.toLowerCase().endsWith(".ndjson"));
      if (!nd.length) throw new Error("No .ndjson found in frozen dir: " + p);
      infile = path.join(p, nd[0]);
    } else {
      infile = p;
    }
  }

  const onlyMatched = String(args.onlyMatched ?? "true").toLowerCase() !== "false";
  const logEvery = Number(args.logEvery || 250000);

  // Try to pull as_of + sha from freeze manifest if available
  let asOf = args.asOf || null;
  let infileSha256 = null;

  const freezeDir = path.dirname(infile);
  const freezeManifestPath = path.join(freezeDir, "MANIFEST.json");
  if (fs.existsSync(freezeManifestPath)) {
    const m = JSON.parse(fs.readFileSync(freezeManifestPath, "utf8"));
    asOf = asOf || m?.asOf || m?.as_of || null;
    infileSha256 = m?.sha256 || m?.dataset_sha256 || null;
  }

  // If still missing asOf, fall back to pointer manifest inside zoning evidence later; but keep null if unknown.
  const ts = new Date().toISOString().replace(/[-:]/g, "").slice(0, 15);
  const outDir = args.outDir || path.join(
    backendRoot,
    "publicData", "properties", "_frozen", "_indexes",
    `base_zoning_index_uid__${ts}`
  );

  const bucketsDir = path.join(outDir, "buckets");
  fs.mkdirSync(bucketsDir, { recursive: true });

  console.log("====================================================");
  console.log("[indexUID] START " + new Date().toISOString());
  console.log("[indexUID] in         : " + path.resolve(infile));
  console.log("[indexUID] outDir     : " + path.resolve(outDir));
  console.log("[indexUID] asOf       : " + (asOf || "(null)"));
  console.log("[indexUID] onlyMatched: " + onlyMatched);
  console.log("[indexUID] logEvery   : " + logEvery);
  console.log("====================================================");

  const writers = new Map(); // bucket -> writeStream
  function getWriter(bucket) {
    let ws = writers.get(bucket);
    if (!ws) {
      const f = path.join(bucketsDir, `${bucket}.ndjson`);
      ws = fs.createWriteStream(f, { flags: "a", encoding: "utf8" });
      writers.set(bucket, ws);
    }
    return ws;
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(infile, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  let lines = 0, written = 0, parseErr = 0;
  const uidSet = new Set(); // track uniqueness to measure collisions in UID space (should be ~written)
  const pidSet = new Set(); // measure collisions in property_id space

  for await (const line of rl) {
    if (!line) continue;
    lines++;

    const o = safeJsonParse(line);
    if (!o) { parseErr++; continue; }

    if (onlyMatched) {
      const conf = Number(o.base_zone_confidence || 0);
      if (!(conf >= 1 && o.base_district_code)) continue;
    }

    const parcel_id = o.parcel_id ? String(o.parcel_id).trim() : "";
    const jRaw = o.jurisdiction_name || o.jurisdiction || o.town || "";
    const jKey = normJurisdictionKey(jRaw);

    if (!parcel_id || !jKey) continue;

    const property_uid = `${jKey}|${parcel_id}`;

    const rec = {
      property_uid,
      property_id: o.property_id || "",
      parcel_id,
      town: o.town || "",
      base_district_code: o.base_district_code || "",
      base_district_name: o.base_district_name || "",
      base_zone_attach_method: o.base_zone_attach_method || "",
      base_zone_confidence: Number(o.base_zone_confidence || 0),
      base_zone_evidence: o.base_zone_evidence || null,
      as_of: asOf || (o.base_zone_evidence?.zoning_as_of ?? o.as_of ?? null)
    };

    const bucket = bucketForKey(property_uid);
    getWriter(bucket).write(JSON.stringify(rec) + "\n");

    written++;
    uidSet.add(property_uid);
    if (o.property_id) pidSet.add(o.property_id);

    if (logEvery && lines % logEvery === 0) {
      console.log(`[PROG] lines=${lines.toLocaleString()} written=${written.toLocaleString()} parseErr=${parseErr}`);
    }
  }

  // close writers
  for (const ws of writers.values()) ws.end();

  // build manifest
  const schema = [
    "property_uid",
    "property_id",
    "parcel_id",
    "town",
    "base_district_code",
    "base_district_name",
    "base_zone_attach_method",
    "base_zone_confidence",
    "base_zone_evidence",
    "as_of"
  ];

  const manifest = {
    created_at: new Date().toISOString(),
    as_of: asOf,
    infile: path.resolve(infile),
    infile_sha256: infileSha256,
    onlyMatched,
    buckets: 256,
    lines,
    written,
    parseErr,
    unique_property_uid: uidSet.size,
    unique_property_id: pidSet.size,
    schema
  };

  const manifestPath = path.join(outDir, "INDEX_MANIFEST.json");
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), "utf8");

  // update pointer
  const pointer = path.join(backendRoot, "publicData", "properties", "_frozen", "CURRENT_BASE_ZONING_INDEX_UID.txt");
  fs.writeFileSync(pointer, outDir, "utf8");

  console.log("----------------------------------------------------");
  console.log("[DONE] UID index built.");
  console.log("[DONE] lines   : " + lines.toLocaleString());
  console.log("[DONE] written : " + written.toLocaleString());
  console.log("[DONE] parseErr: " + parseErr.toLocaleString());
  console.log("[DONE] unique_property_uid: " + uidSet.size.toLocaleString());
  console.log("[DONE] unique_property_id : " + pidSet.size.toLocaleString());
  console.log("[DONE] manifest: " + manifestPath);
  console.log("[OK ] updated UID index pointer -> " + pointer);
  console.log("====================================================");
}

main().catch(err => {
  console.error("[FAIL]", err?.stack || err);
  process.exit(1);
});
