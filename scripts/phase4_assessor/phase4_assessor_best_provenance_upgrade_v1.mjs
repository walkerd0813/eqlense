#!/usr/bin/env node
/**
 * Phase 4 — Upgrade assessor_best to provenance objects
 * Reads a properties NDJSON (typically CURRENT_PROPERTIES_WITH_ASSESSOR_GLOBAL_BEST.json pointer),
 * and upgrades assessor_best leaf nodes from:
 *   { "value": 123 }
 * to:
 *   { "value": 123, "source": "city_assessor|massgis_statewide|unknown", "as_of": "...", "dataset_hash": "...", "confidence": "A|B|C", "flags": [...] }
 *
 * Idempotent: if leaf already has "source", leaves it as-is.
 * Memory safe: streaming NDJSON in/out.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import readline from "node:readline";

function nowIsoSafe() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function sha256File(p) {
  const h = crypto.createHash("sha256");
  const fd = fs.openSync(p, "r");
  try {
    const buf = Buffer.alloc(1024 * 1024);
    while (true) {
      const n = fs.readSync(fd, buf, 0, buf.length, null);
      if (!n) break;
      h.update(buf.subarray(0, n));
    }
  } finally {
    fs.closeSync(fd);
  }
  return h.digest("hex");
}

function readJson(p) {
  const txt = fs.readFileSync(p, "utf8").replace(/^\uFEFF/, "");
  return JSON.parse(txt);
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function parseArgs(argv) {
  const out = { config: null };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--config") out.config = argv[++i];
  }
  if (!out.config) {
    console.log("usage: node phase4_assessor_best_provenance_upgrade_v1.mjs --config <config.json>");
    process.exit(2);
  }
  return out;
}

function getEvidenceForSource(rec, sourceKey) {
  if (!rec?.assessor_by_source) return null;
  if (sourceKey === "city_assessor") return rec.assessor_by_source?.city_assessor_raw?.evidence ?? null;
  if (sourceKey === "massgis_statewide") return rec.assessor_by_source?.massgis_statewide_raw?.evidence ?? null;
  return null;
}

function confidenceForSource(sourceKey) {
  if (sourceKey === "city_assessor") return "A";
  if (sourceKey === "massgis_statewide") return "B";
  return "C";
}

function upgradeLeaf(leaf, meta) {
  if (!leaf || typeof leaf !== "object") return leaf;
  if ("source" in leaf) return leaf; // already upgraded
  return {
    value: leaf.value ?? null,
    source: meta.sourceKey ?? "unknown",
    as_of: meta.evidence?.as_of ?? null,
    dataset_hash: meta.evidence?.dataset_hash ?? null,
    confidence: confidenceForSource(meta.sourceKey),
    flags: meta.flags ?? []
  };
}

function isValueLeaf(obj) {
  return obj && typeof obj === "object" && !Array.isArray(obj) && ("value" in obj);
}

function walkUpgrade(node, basePath, rec) {
  let touched = 0;

  if (isValueLeaf(node)) {
    const map = rec?.assessor_source_map ?? {};
    const src = map[basePath] ?? "unknown";
    const sourceKey = (src === "city_assessor" || src === "massgis_statewide") ? src : "unknown";
    const evidence = getEvidenceForSource(rec, sourceKey);
    const flags = [];
    if (sourceKey !== "city_assessor" && sourceKey !== "unknown") flags.push("FALLBACK_USED");
    if (sourceKey === "unknown") flags.push("SOURCE_UNKNOWN");
    const upgraded = upgradeLeaf(node, { sourceKey, evidence, flags });
    if (upgraded !== node) touched++;
    return { node: upgraded, touched };
  }

  if (!node || typeof node !== "object") return { node, touched };
  if (Array.isArray(node)) return { node, touched };

  const out = {};
  for (const [k, v] of Object.entries(node)) {
    const nextPath = basePath ? `${basePath}.${k}` : k;
    const r = walkUpgrade(v, nextPath, rec);
    out[k] = r.node;
    touched += r.touched;
  }
  return { node: out, touched };
}

function normalizeSourceMapKeys(rec) {
  if (!rec.assessor_source_map || typeof rec.assessor_source_map !== "object") rec.assessor_source_map = {};
  return rec;
}

async function main() {
  const { config } = parseArgs(process.argv);
  const cfg = readJson(config);

  const root = cfg.root || process.cwd();
  const ptrPath = path.resolve(root, cfg.properties_current_ptr);
  const outDir = path.resolve(root, cfg.out_dir);
  ensureDir(outDir);

  if (!fs.existsSync(ptrPath)) throw new Error(`[err] properties_current_ptr not found: ${ptrPath}`);

  const ptr = readJson(ptrPath);
  const inFile = ptr.properties_ndjson ? ptr.properties_ndjson : (cfg.properties_in_ndjson || null);
  if (!inFile) throw new Error(`[err] could not resolve input properties NDJSON from pointer: ${ptrPath}`);

  const inPath = path.resolve(inFile);
  if (!fs.existsSync(inPath)) throw new Error(`[err] input NDJSON not found: ${inPath}`);

  const stamp = nowIsoSafe();
  const outFile = path.join(outDir, `properties__with_assessor_best_provenance__${stamp}__V1.ndjson`);
  const auditPath = path.resolve(root, cfg.audit_dir, `phase4_assessor_best_provenance_upgrade__${stamp}__V1.json`);
  const currentPtr = path.join(outDir, "CURRENT_PROPERTIES_WITH_ASSESSOR_BEST_PROVENANCE.json");

  ensureDir(path.dirname(auditPath));

  console.log("[info] input:", inPath);
  console.log("[info] output:", outFile);

  const rl = readline.createInterface({
    input: fs.createReadStream(inPath, { encoding: "utf8" }),
    crlfDelay: Infinity
  });
  const out = fs.createWriteStream(outFile, { encoding: "utf8" });

  let n = 0;
  let upgradedRecords = 0;
  let upgradedLeaves = 0;
  let skippedNoAssessorBest = 0;
  let skippedNoSourceMap = 0;

  for await (const line of rl) {
    n++;
    if (!line.trim()) continue;
    let rec;
    rec = JSON.parse(line);

    if (!rec.assessor_best) {
      skippedNoAssessorBest++;
      out.write(line + "\n");
      if (n % 500000 === 0) console.log("[progress] processed", n);
      continue;
    }

    normalizeSourceMapKeys(rec);
    const hasMap = rec.assessor_source_map && Object.keys(rec.assessor_source_map).length > 0;
    if (!hasMap) {
      skippedNoSourceMap++;
      out.write(line + "\n");
      if (n % 500000 === 0) console.log("[progress] processed", n);
      continue;
    }

    const r = walkUpgrade(rec.assessor_best, "", rec);
    rec.assessor_best = r.node;

    if (r.touched > 0) {
      upgradedRecords++;
      upgradedLeaves += r.touched;
    }

    out.write(JSON.stringify(rec) + "\n");
    if (n % 500000 === 0) console.log("[progress] processed", n, "upgradedRecords", upgradedRecords);
  }

  out.end();
  await new Promise((res) => out.on("finish", res));

  const sha = sha256File(outFile);

  const audit = {
    created_at: new Date().toISOString(),
    config: path.resolve(config),
    inputs: { properties_in: inPath, properties_ptr: ptrPath },
    outputs: { properties_out: outFile, current_ptr: currentPtr, audit: auditPath },
    stats: { processed: n, upgradedRecords, upgradedLeaves, skippedNoAssessorBest, skippedNoSourceMap },
    hashes: { properties_out_sha256: sha },
    notes: [
      "Upgrades assessor_best leaf nodes to include provenance: source, as_of, dataset_hash, confidence, flags.",
      "Idempotent: leaves already containing `source` untouched.",
      "Uses assessor_source_map plus assessor_by_source.*.evidence to populate provenance."
    ]
  };

  fs.writeFileSync(auditPath, JSON.stringify(audit, null, 2));
  fs.writeFileSync(currentPtr, JSON.stringify({
    updated_at: new Date().toISOString(),
    note: "AUTO: Phase4 assessor_best provenance upgrade v1",
    properties_ndjson: outFile,
    audit: auditPath,
    input_ptr: ptrPath
  }, null, 2));

  console.log("[ok] wrote audit:", auditPath);
  console.log("[ok] wrote CURRENT pointer:", currentPtr);
  console.log("[ok] output sha256:", sha);
  console.log("[done] Phase4 assessor_best provenance upgrade v1 complete.");
}

main().catch((e) => {
  console.error(String(e && e.stack ? e.stack : e));
  process.exit(1);
});
