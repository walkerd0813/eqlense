import fs from "fs";
import path from "path";
import readline from "readline";
import crypto from "crypto";

function arg(name, defVal = null) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : defVal;
}
function has(name) {
  return process.argv.includes(name);
}

const infile = arg("--in");
const outDir = arg("--outDir");
const asOf = arg("--asOf", "");
const onlyMatched = has("--onlyMatched");
const logEvery = Number(arg("--logEvery", "250000"));

if (!infile || !outDir) {
  console.error("Usage: node build_base_zoning_index_buckets_v1_DROPIN.mjs --in <ndjson> --outDir <dir> [--asOf YYYY-MM-DD] [--onlyMatched] [--logEvery N]");
  process.exit(1);
}

fs.mkdirSync(outDir, { recursive: true });
const bucketsDir = path.join(outDir, "buckets");
fs.mkdirSync(bucketsDir, { recursive: true });

const sha = crypto.createHash("sha256");
const streams = new Map();

function bucketKey(propertyId) {
  const h = crypto.createHash("sha1").update(propertyId).digest("hex");
  return h.slice(0, 2); // 256 buckets
}
function getStream(key) {
  let s = streams.get(key);
  if (!s) {
    const p = path.join(bucketsDir, key + ".ndjson");
    s = fs.createWriteStream(p, { flags: "a" });
    streams.set(key, s);
  }
  return s;
}

let lines = 0,
  written = 0,
  parseErr = 0;

console.log("====================================================");
console.log("[index] START " + new Date().toISOString());
console.log("[index] in         : " + infile);
console.log("[index] outDir     : " + outDir);
console.log("[index] asOf       : " + (asOf || ""));
console.log("[index] onlyMatched: " + (onlyMatched ? "true" : "false"));
console.log("[index] logEvery   : " + logEvery);
console.log("====================================================");

const rl = readline.createInterface({
  input: fs.createReadStream(infile, { encoding: "utf8" }),
  crlfDelay: Infinity
});

for await (const line of rl) {
  if (!line) continue;
  lines++;
  sha.update(line);
  sha.update("\n");

  let o;
  try {
    o = JSON.parse(line);
  } catch {
    parseErr++;
    continue;
  }

  const property_id = o.property_id;
  if (!property_id) continue;

  const code = o.base_district_code ?? null;

  // keep index small: by default write ONLY matched rows (code present)
  if (onlyMatched && !code) {
    if (lines % logEvery === 0) {
      console.log("[PROG] lines=" + lines.toLocaleString() + " written=" + written.toLocaleString() + " parseErr=" + parseErr.toLocaleString());
    }
    continue;
  }

  const rec = {
    property_id,
    parcel_id: o.parcel_id ?? null,
    town: o.town ?? null,
    base_district_code: code,
    base_district_name: o.base_district_name ?? null,
    base_zone_attach_method: o.base_zone_attach_method ?? null,
    base_zone_confidence: o.base_zone_confidence ?? null,
    base_zone_evidence: o.base_zone_evidence ?? null,
    as_of: asOf || null
  };

  const key = bucketKey(property_id);
  getStream(key).write(JSON.stringify(rec) + "\n");
  written++;

  if (lines % logEvery === 0) {
    console.log("[PROG] lines=" + lines.toLocaleString() + " written=" + written.toLocaleString() + " parseErr=" + parseErr.toLocaleString());
  }
}

// close streams
for (const s of streams.values()) {
  await new Promise((res) => s.end(res));
}

const inputSha256 = sha.digest("hex").toUpperCase();
const done = new Date().toISOString();

const manifest = {
  created_at: done,
  as_of: asOf || null,
  infile,
  infile_sha256: inputSha256,
  onlyMatched: !!onlyMatched,
  buckets: 256,
  lines,
  written,
  parseErr,
  schema: [
    "property_id","parcel_id","town",
    "base_district_code","base_district_name",
    "base_zone_attach_method","base_zone_confidence","base_zone_evidence",
    "as_of"
  ]
};

fs.writeFileSync(path.join(outDir, "INDEX_MANIFEST.json"), JSON.stringify(manifest, null, 2));

console.log("----------------------------------------------------");
console.log("[DONE] index built.");
console.log("[DONE] lines   : " + lines.toLocaleString());
console.log("[DONE] written : " + written.toLocaleString());
console.log("[DONE] parseErr: " + parseErr.toLocaleString());
console.log("[DONE] infile_sha256: " + inputSha256);
console.log("[DONE] manifest: " + path.join(outDir, "INDEX_MANIFEST.json"));
console.log("====================================================");
