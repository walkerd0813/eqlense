import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import crypto from "node:crypto";

function readJSON(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function isoNow() {
  return new Date().toISOString();
}

function sha256File(filePath) {
  const h = crypto.createHash("sha256");
  const fd = fs.openSync(filePath, "r");
  try {
    const buf = Buffer.alloc(1024 * 1024);
    while (true) {
      const n = fs.readSync(fd, buf, 0, buf.length, null);
      if (n <= 0) break;
      h.update(buf.subarray(0, n));
    }
  } finally {
    fs.closeSync(fd);
  }
  return h.digest("hex");
}

function safeParse(line) {
  try {
    return JSON.parse(line);
  } catch {
    return null;
  }
}

function isFiniteNumber(x) {
  return typeof x === "number" && Number.isFinite(x);
}

function getFiscalYearCandidate(rec) {
  const v1 = rec?.assessor_best?.valuation?.assessment_year?.value;
  if (isFiniteNumber(v1)) return v1;

  const aCity = rec?.assessor_by_source?.city_assessor_raw?.assessed_year;
  if (isFiniteNumber(aCity)) return aCity;

  const aMass = rec?.assessor_by_source?.massgis_statewide_raw?.assessed_year;
  if (isFiniteNumber(aMass)) return aMass;

  return null;
}

function makeFYLabel(fy) {
  if (!isFiniteNumber(fy)) return null;
  return `FY${Math.trunc(fy)}`;
}

function addFYMetaToLeaf(leaf, fyLabel) {
  if (!leaf || typeof leaf !== "object") return;
  if (!fyLabel) return;
  // Don't clobber if already present
  if (!leaf.data_as_of) leaf.data_as_of = fyLabel;
  if (!leaf.data_as_of_kind) leaf.data_as_of_kind = "fiscal_year";
}

function upgradeLeaf(rawValue, meta, fieldPath, fyLabel) {
  // meta: { source, as_of, dataset_hash, confidence, flags }
  if (rawValue === null || rawValue === undefined) return null;

  const flags = Array.isArray(meta?.flags) ? [...meta.flags] : [];

  // IMPORTANT: assessed_year in assessor data is typically a fiscal year (FY), not a calendar year.
  if (fieldPath === "valuation.assessment_year") {
    if (!flags.includes("FISCAL_YEAR")) flags.push("FISCAL_YEAR");
  }

  const leaf = {
    value: rawValue,
    source: meta?.source ?? "unknown",
    as_of: meta?.as_of ?? null,
    dataset_hash: meta?.dataset_hash ?? null,
    confidence: meta?.confidence ?? "C",
    flags
  };

  addFYMetaToLeaf(leaf, fyLabel);
  return leaf;
}

function getSourceMeta(rec, sourceName) {
  // Pull evidence/meta from whichever raw block exists
  let raw = null;
  if (sourceName === "city_assessor") raw = rec?.assessor_by_source?.city_assessor_raw;
  if (sourceName === "massgis_statewide") raw = rec?.assessor_by_source?.massgis_statewide_raw;
  if (!raw) return { as_of: null, dataset_hash: null };
  return {
    as_of: raw?.evidence?.as_of ?? null,
    dataset_hash: raw?.evidence?.dataset_hash ?? null
  };
}

function computeLotSqftFromRaw(raw, lotSizeField) {
  if (!raw) return null;
  const lot = raw?.lot_size_raw;
  const units = raw?.lot_units_raw;
  if (!isFiniteNumber(lot)) return null;

  const unitNorm = (units ?? "").toString().trim().toLowerCase();
  // Common: "Acres", "A" for acres in MassGIS.
  const isAcres = unitNorm === "acres" || unitNorm === "a";
  if (isAcres) {
    // If already looks like sqft (e.g., 11000) but unit says acres, we still treat as acres.
    const sqft = lot * 43560;
    if (!Number.isFinite(sqft)) return null;
    return {
      value: sqft,
      computed: true,
      reason: `computed_from_${lotSizeField}_acres`
    };
  }

  // If not acres, assume it's already sqft-ish if it's plausible.
  // We'll only accept values >= 50 as sqft; smaller values are probably not sqft.
  if (lot >= 50) {
    return { value: lot, computed: false, reason: `copied_from_${lotSizeField}` };
  }

  return null;
}

function findLotSqft(rec, fyLabel) {
  // Use the same winning source as site.lot_size (if present)
  const src = rec?.assessor_source_map?.["site.lot_size"];
  if (!src) return null;

  const raw = src === "city_assessor"
    ? rec?.assessor_by_source?.city_assessor_raw
    : (src === "massgis_statewide" ? rec?.assessor_by_source?.massgis_statewide_raw : null);

  const lotSizeField = src === "city_assessor" ? "city_assessor" : "massgis_statewide";
  const calc = computeLotSqftFromRaw(raw, lotSizeField);
  if (!calc) return null;

  const baseMeta = getSourceMeta(rec, src);
  const flags = [];
  if (calc.computed) flags.push("COMPUTED_FROM_ACRES");

  const leaf = upgradeLeaf(calc.value, {
    source: src,
    as_of: baseMeta.as_of,
    dataset_hash: baseMeta.dataset_hash,
    confidence: src === "city_assessor" ? "A" : "B",
    flags
  }, "site.lot_size_sqft", fyLabel);

  return leaf;
}

function ensureNested(obj, parts) {
  let cur = obj;
  for (const p of parts) {
    if (!cur[p] || typeof cur[p] !== "object") cur[p] = {};
    cur = cur[p];
  }
  return cur;
}

function deepClone(x) {
  return x ? JSON.parse(JSON.stringify(x)) : x;
}

function upgradeRecord(rec, createdAtIso) {
  if (!rec || typeof rec !== "object") return { rec, upgraded: false };

  const fy = getFiscalYearCandidate(rec);
  const fyLabel = makeFYLabel(fy);

  let upgraded = false;

  // assessor_best exists but may have "value-only" leaves (v0 style) or provenance leaves (v1 style)
  const best = rec?.assessor_best;
  if (!best || typeof best !== "object") return { rec, upgraded: false };

  // Detect if leaf objects already have "source"; if not, upgrade.
  const alreadyProvenanced = typeof best?.valuation?.total_value?.source === "string";

  if (!alreadyProvenanced) {
    // We can't safely rebuild provenance without source_map + raw blocks.
    // v1 pack already handled that case. If we ever hit this, leave it.
  } else {
    // 1) Add FY metadata fields across all assessor_best leaves (non-breaking)
    // 2) Add valuation.assessment_fy alias leaf (same as assessment_year) to clarify FY-vs-calendar.
    // 3) Add site.lot_size_sqft computed leaf (if possible)

    // 1) add FY meta to each existing leaf
    const walk = (node) => {
      if (!node || typeof node !== "object") return;
      // leaf object shape: { value, source, as_of, dataset_hash, confidence, flags, ... }
      if (Object.prototype.hasOwnProperty.call(node, "value") && Object.prototype.hasOwnProperty.call(node, "source")) {
        addFYMetaToLeaf(node, fyLabel);
        return;
      }
      for (const k of Object.keys(node)) walk(node[k]);
    };
    walk(best);

    // 2) valuation.tax_fy alias (preferred)
    //    Many assessor datasets store a Fiscal Year (FY2026) while pulled in late 2025.
    //    "assessment_year" is internally derived and can be confusing; we keep it if present,
    //    but we ALSO expose a clearer alias: valuation.tax_fy.
    const assessmentYearLeaf = best?.valuation?.assessment_year;
    if (assessmentYearLeaf && typeof assessmentYearLeaf === "object" && Object.prototype.hasOwnProperty.call(assessmentYearLeaf, "value")) {
      const fyLeaf = deepClone(assessmentYearLeaf);
      fyLeaf.flags = Array.isArray(fyLeaf.flags) ? fyLeaf.flags : [];
      if (!fyLeaf.flags.includes("FISCAL_YEAR")) fyLeaf.flags.push("FISCAL_YEAR");
      addFYMetaToLeaf(fyLeaf, fyLabel);
      ensureNested(best, ["valuation"]);
      best.valuation.tax_fy = fyLeaf;
      upgraded = true;

      // Tag assessment_year leaf as FY (non-destructive)
      assessmentYearLeaf.flags = Array.isArray(assessmentYearLeaf.flags) ? assessmentYearLeaf.flags : [];
      if (!assessmentYearLeaf.flags.includes("FISCAL_YEAR")) assessmentYearLeaf.flags.push("FISCAL_YEAR");
      addFYMetaToLeaf(assessmentYearLeaf, fyLabel);
    }

    // 3) site.lot_size_sqft
    const lotSqftLeaf = findLotSqft(rec, fyLabel);
    if (lotSqftLeaf) {
      ensureNested(best, ["site"]);
      best.site.lot_size_sqft = lotSqftLeaf;
      // Update source map
      rec.assessor_source_map = rec.assessor_source_map || {};
      rec.assessor_source_map["site.lot_size_sqft"] = lotSqftLeaf.source;
      // If computed from acres, note it for QA.
      rec.assessor_fallback_fields = Array.isArray(rec.assessor_fallback_fields) ? rec.assessor_fallback_fields : [];
      upgraded = true;
    }

    // Add a tiny meta block to make FY semantics explicit.
    rec.assessor_best_meta = rec.assessor_best_meta || {};
    if (fyLabel) {
      rec.assessor_best_meta.tax_fy_kind = "fiscal_year";
      rec.assessor_best_meta.tax_fy_label = fyLabel;
    }
    rec.assessor_best_meta.upgraded_at = createdAtIso;
    rec.assessor_best_meta.upgrade_version = "v3";
    upgraded = true;
  }

  rec.assessor_best = best;
  return { rec, upgraded };
}

async function main() {
  const ROOT = process.cwd();
  const configPath = path.resolve(ROOT, process.argv[2] || "phase4_assessor_best_provenance_upgrade_config_v3.json");
  const cfg = readJSON(configPath);

  const input = path.resolve(ROOT, cfg.in_properties_ndjson);
  const outDir = path.resolve(ROOT, cfg.out_dir);
  ensureDir(outDir);

  const createdAt = isoNow();
  const outFile = path.join(outDir, `properties__with_assessor_best_provenance__${createdAt.replace(/[:.]/g, "-")}__V3.ndjson`);
  const auditPath = path.resolve(ROOT, cfg.audit_path_dir || "publicData/_audit/phase4_assessor");
  ensureDir(auditPath);
  const auditFile = path.join(auditPath, `phase4_assessor_best_provenance_upgrade__${createdAt.replace(/[:.]/g, "-")}__V3.json`);

  console.log("[start] Phase4 assessor_best provenance upgrade (v3)");
  console.log("[info] config:", configPath);
  console.log("[info] input:", input);
  console.log("[info] output:", outFile);

  const rl = readline.createInterface({
    input: fs.createReadStream(input, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  const out = fs.createWriteStream(outFile, { encoding: "utf8" });

  let processed = 0;
  let upgradedRecords = 0;
  let parseErrors = 0;

  for await (const line of rl) {
    processed++;
    const rec = safeParse(line);
    if (!rec) {
      parseErrors++;
      continue;
    }

    const { rec: up, upgraded } = upgradeRecord(rec, createdAt);
    if (upgraded) upgradedRecords++;

    out.write(JSON.stringify(up) + "\n");

    if (processed % 500000 === 0) {
      console.log("[progress] processed", processed, "upgradedRecords", upgradedRecords, "parseErrors", parseErrors);
    }
  }

  await new Promise((r) => out.end(r));

  const outSha = sha256File(outFile);

  const audit = {
    created_at: createdAt,
    version: "v3",
    config: configPath,
    input,
    outFile,
    output_sha256: outSha,
    stats: { processed, upgradedRecords, parseErrors },
    notes: [
      "v3: treats valuation.assessment_year as Fiscal Year (FY) and also writes valuation.tax_fy as the UI-friendly alias.",
      "v3: adds data_as_of + data_as_of_kind=fiscal_year to all assessor_best provenance leaves when FY can be inferred.",
      "v3: adds site.lot_size_sqft computed from acres when source units indicate acres.",
      "Non-breaking: retains existing assessor_best paths and adds new fields only."
    ]
  };

  fs.writeFileSync(auditFile, JSON.stringify(audit, null, 2), "utf8");

  const currentPtr = path.join(outDir, "CURRENT_PROPERTIES_WITH_ASSESSOR_BEST_TAX_PROVENANCE.json");
  fs.writeFileSync(currentPtr, JSON.stringify({
    updated_at: isoNow(),
    note: "AUTO: Phase4 assessor_best provenance upgrade v3 (tax_fy alias + FY semantics + lot_size_sqft)",
    properties_ndjson: outFile,
    audit: auditFile
  }, null, 2), "utf8");

  console.log("[ok] wrote audit:", auditFile);
  console.log("[ok] wrote CURRENT pointer:", currentPtr);
  console.log("[ok] output sha256:", outSha);
  console.log("[done] Phase4 assessor_best provenance upgrade v2 complete.");
}

main().catch((e) => {
  console.error("[fatal]", e);
  process.exit(1);
});
