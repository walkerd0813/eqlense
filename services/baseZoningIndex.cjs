"use strict";

const fs = require("fs");
const path = require("path");
const readline = require("readline");
const crypto = require("crypto");

function isAbs(p) {
  return path.isAbsolute(p) || /^[A-Za-z]:\\/.test(p);
}

function sha1hex(s) {
  return crypto.createHash("sha1").update(String(s), "utf8").digest("hex");
}

function readText(p) {
  return fs.readFileSync(p, "utf8");
}

function resolveIndexDir(backendRoot) {
  const ptr = path.join(backendRoot, "publicData", "properties", "_frozen", "CURRENT_BASE_ZONING_INDEX.txt");
  if (!fs.existsSync(ptr)) {
    throw new Error(`Missing index pointer: ${ptr}`);
  }
  const raw = readText(ptr).trim();
  if (!raw) throw new Error(`Empty index pointer: ${ptr}`);

  // pointer might be relative or absolute
  const idxDir = isAbs(raw) ? raw : path.resolve(backendRoot, raw);
  const manifestPath = path.join(idxDir, "INDEX_MANIFEST.json");
  if (!fs.existsSync(manifestPath)) {
    throw new Error(`Missing INDEX_MANIFEST.json at: ${manifestPath}`);
  }
  return { idxDir, manifestPath, pointerPath: ptr, pointerValue: raw };
}

async function lookupBaseZoningByPropertyId(propertyId, opts = {}) {
  const includeMeta = !!opts.includeMeta;
  const backendRoot = path.resolve(__dirname, "..");
  const { idxDir, manifestPath, pointerPath, pointerValue } = resolveIndexDir(backendRoot);

  const bucket = sha1hex(propertyId).slice(0, 2);
  const bucketFile = path.join(idxDir, "buckets", `${bucket}.ndjson`);
  if (!fs.existsSync(bucketFile)) {
    return {
      found: false,
      record: null,
      meta: includeMeta ? { bucket, bucketFile, idxDir, manifestPath, pointerPath, pointerValue } : undefined
    };
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(bucketFile, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let found = null;

  for await (const line of rl) {
    const s = (line || "").trim();
    if (!s) continue;
    let o;
    try {
      o = JSON.parse(s);
    } catch {
      continue;
    }
    if (o && o.property_id === propertyId) {
      found = o;
      break;
    }
  }

  rl.close();

  if (!includeMeta) {
    return { found: !!found, record: found || null };
  }

  let manifest = null;
  try {
    manifest = JSON.parse(readText(manifestPath));
  } catch {
    manifest = null;
  }

  return {
    found: !!found,
    record: found || null,
    meta: {
      bucket,
      bucketFile,
      idxDir,
      manifestPath,
      pointerPath,
      pointerValue,
      manifest
    }
  };
}

module.exports = {
  lookupBaseZoningByPropertyId
};
