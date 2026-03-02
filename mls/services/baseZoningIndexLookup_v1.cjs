const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PTR_INDEX = path.resolve(process.cwd(), "publicData", "properties", "_frozen", "CURRENT_BASE_ZONING_INDEX.txt");

let _cachedIndexDir = null;
let _cachedManifest = null;

function readPointerFile() {
  if (!fs.existsSync(PTR_INDEX)) {
    throw new Error("Missing pointer: " + PTR_INDEX);
  }
  const p = fs.readFileSync(PTR_INDEX, "utf8").trim();
  if (!p) throw new Error("Empty pointer file: " + PTR_INDEX);
  return p;
}

function getIndexDir() {
  if (_cachedIndexDir) return _cachedIndexDir;
  const dir = readPointerFile();
  if (!fs.existsSync(dir)) throw new Error("Index dir not found: " + dir);
  const man = path.join(dir, "INDEX_MANIFEST.json");
  if (!fs.existsSync(man)) throw new Error("Index manifest missing: " + man);
  const bucketsDir = path.join(dir, "buckets");
  if (!fs.existsSync(bucketsDir)) throw new Error("Buckets dir missing: " + bucketsDir);
  _cachedIndexDir = dir;
  return dir;
}

function getManifest() {
  if (_cachedManifest) return _cachedManifest;
  const dir = getIndexDir();
  const manPath = path.join(dir, "INDEX_MANIFEST.json");
  _cachedManifest = JSON.parse(fs.readFileSync(manPath, "utf8"));
  return _cachedManifest;
}

function bucketKey(propertyId) {
  const h = crypto.createHash("sha1").update(propertyId).digest("hex");
  return h.slice(0, 2); // 256 buckets
}

function bucketPathFor(propertyId) {
  const dir = getIndexDir();
  const key = bucketKey(propertyId);
  return { key, file: path.join(dir, "buckets", key + ".ndjson") };
}

/**
 * Lookup base zoning by property_id.
 * Returns:
 *  { found, record, meta }
 */
function lookupBaseZoningByPropertyId(propertyId, opts = {}) {
  if (!propertyId || typeof propertyId !== "string") {
    throw new Error("propertyId required");
  }

  const includeMeta = !!opts.includeMeta;
  const { key, file } = bucketPathFor(propertyId);

  if (!fs.existsSync(file)) {
    return {
      found: false,
      record: null,
      meta: includeMeta ? { bucket: key, bucketFile: file, manifest: getManifest() } : { bucket: key, bucketFile: file }
    };
  }

  const lines = fs.readFileSync(file, "utf8").split(/\r?\n/);
  for (const line of lines) {
    if (!line) continue;
    let o;
    try {
      o = JSON.parse(line);
    } catch {
      continue;
    }
    if (o && o.property_id === propertyId) {
      return {
        found: true,
        record: o,
        meta: includeMeta ? { bucket: key, bucketFile: file, manifest: getManifest() } : { bucket: key, bucketFile: file }
      };
    }
  }

  return {
    found: false,
    record: null,
    meta: includeMeta ? { bucket: key, bucketFile: file, manifest: getManifest() } : { bucket: key, bucketFile: file }
  };
}

module.exports = {
  getIndexDir,
  getManifest,
  bucketKey,
  lookupBaseZoningByPropertyId
};
