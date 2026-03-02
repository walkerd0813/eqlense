#!/usr/bin/env node
import fs from "fs";
import path from "path";
import readline from "readline";
import crypto from "crypto";

function nowStamp() {
  const d = new Date();
  const pad = (n, w=2) => String(n).padStart(w, "0");
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}-${pad(d.getMinutes())}-${pad(d.getSeconds())}-${pad(d.getMilliseconds(),3)}`;
}
function readJSON(p) {
  const buf = fs.readFileSync(p);
  const s = buf.toString("utf8").replace(/^\uFEFF/, "");
  return JSON.parse(s);
}
function sha256File(p) {
  const h = crypto.createHash("sha256");
  const fd = fs.openSync(p, "r");
  const buf = Buffer.alloc(1024*1024);
  try {
    let bytes = 0;
    while ((bytes = fs.readSync(fd, buf, 0, buf.length, null)) > 0) h.update(buf.subarray(0, bytes));
  } finally { fs.closeSync(fd); }
  return h.digest("hex");
}
function ensureDir(p) { fs.mkdirSync(p, { recursive: true }); }

function parcelPatternFlags(parcelIdRaw) {
  const s = (parcelIdRaw ?? "").toString().trim();
  const flags = {
    contains_space: /\s/.test(s),
    contains_hyphen: /-/.test(s),
    digits_only: /^[0-9]+$/.test(s),
    leading_zeros: /^0+/.test(s),
    looks_maplot: false,
  };
  const maplotA = /^[0-9]{1,4}[-][0-9A-Za-z]{1,6}[-][0-9A-Za-z]{1,6}$/.test(s);
  const maplotB = /^[0-9]{3,6}\s+[0-9A-Za-z]{1,6}$/.test(s);
  flags.looks_maplot = maplotA || maplotB || flags.contains_hyphen || flags.contains_space;
  return flags;
}

function classifyMissingReason(rec) {
  const parcel = (rec.parcel_id_norm ?? rec.parcel_id ?? rec.parcelId ?? "").toString().trim();
  const town = (rec.town ?? rec.jurisdiction_name ?? "").toString().trim();
  if (!parcel) return "PARCEL_ID_MALFORMED_OR_EMPTY";
  const f = parcelPatternFlags(parcel);
  if (f.digits_only) return "PARCEL_ID_PATTERN_DIGITS_ONLY";
  if (f.leading_zeros) return "PARCEL_ID_PATTERN_LEADING_ZEROS";
  if (f.contains_hyphen) return "PARCEL_ID_PATTERN_HAS_HYPHEN";
  if (f.contains_space) return "PARCEL_ID_PATTERN_HAS_SPACE";
  if (f.looks_maplot) return "PARCEL_ID_PATTERN_MAPLOT_LIKE";
  if (!town) return "TOWN_OR_JURISDICTION_MISSING";
  return "OTHER_UNCLASSIFIED";
}

async function main() {
  const args = process.argv.slice(2);
  const cfgIdx = args.indexOf("--config");
  if (cfgIdx === -1 || !args[cfgIdx+1]) {
    console.error("usage: node scripts/phase4_assessor/phase4_assessor_unknown_classify_v1.mjs --config <config.json>");
    process.exit(2);
  }
  const configPath = path.resolve(args[cfgIdx+1]);
  const cfg = readJSON(configPath);

  const root = path.resolve(cfg.root ?? ".");
  const phase4Ptr = path.resolve(root, cfg.phase4_pointer ?? "publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL.json");
  if (!fs.existsSync(phase4Ptr)) throw new Error("Phase4 canonical pointer missing: " + phase4Ptr);

  const ptr = readJSON(phase4Ptr);
  const inputNd = path.resolve(ptr.properties_ndjson ?? ptr.ndjson ?? ptr.file ?? "");
  if (!inputNd || !fs.existsSync(inputNd)) throw new Error("Input NDJSON missing (from pointer): " + inputNd);

  const outDir = path.resolve(root, cfg.out_dir ?? "publicData/properties/_attached/phase4_assessor_unknown_classify_v1");
  ensureDir(outDir);

  const stamp = nowStamp();
  const outNd = path.join(outDir, `properties__phase4_assessor_canonical_unknown_classified__${stamp}__V1.ndjson`);
  const auditPath = path.resolve(root, (cfg.audit_out ?? `publicData/_audit/phase4_assessor/phase4_assessor_unknown_classify__${stamp}__V1.json`).replace("__AUTO__", stamp));
  ensureDir(path.dirname(auditPath));

  console.log("[start] Phase4 assessor UNKNOWN classify v1");
  console.log("[info] root:", root);
  console.log("[info] input:", inputNd);
  console.log("[info] out:", outNd);

  const rl = readline.createInterface({ input: fs.createReadStream(inputNd, { encoding: "utf8" }), crlfDelay: Infinity });
  const out = fs.createWriteStream(outNd, { encoding: "utf8" });

  const counts = {
    rows: 0,
    unknown: 0,
    ok: 0,
    reasons: {},
    patterns: { contains_space: 0, contains_hyphen: 0, digits_only: 0, leading_zeros: 0, looks_maplot: 0 }
  };

  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    counts.rows++;
    let rec;
    try { rec = JSON.parse(line); } catch { continue; }

    const by = rec.assessor_by_source ?? {};
    const hasCity = !!by.city_assessor_raw;
    const hasMass = !!by.massgis_statewide_raw;
    const any = hasCity || hasMass;

    if (!any) {
      const reason = classifyMissingReason(rec);
      counts.unknown++;
      counts.reasons[reason] = (counts.reasons[reason] ?? 0) + 1;

      const parcel = (rec.parcel_id_norm ?? rec.parcel_id ?? "").toString().trim();
      const pf = parcelPatternFlags(parcel);
      for (const k of Object.keys(counts.patterns)) if (pf[k]) counts.patterns[k] += 1;

      rec.assessor_status = "UNKNOWN";
      rec.assessor_quality_grade = "NONE";
      rec.assessor_missing_reason = reason;
      rec.flags = Array.isArray(rec.flags) ? rec.flags : [];
      if (!rec.flags.includes("ASSESSOR_MISSING_BOTH_SOURCES")) rec.flags.push("ASSESSOR_MISSING_BOTH_SOURCES");

      rec.assessor_best = rec.assessor_best ?? {};
      rec.assessor_best.meta = rec.assessor_best.meta ?? {};
      rec.assessor_best.meta.assessor_missing_reason = reason;
      rec.assessor_best.meta.assessor_status = "UNKNOWN";
    } else {
      counts.ok++;
      rec.assessor_status = "OK";
      rec.assessor_quality_grade = hasCity ? "A" : "B";
      rec.assessor_best = rec.assessor_best ?? {};
      rec.assessor_best.meta = rec.assessor_best.meta ?? {};
      rec.assessor_best.meta.assessor_status = "OK";
      rec.assessor_best.meta.assessor_quality_grade = rec.assessor_quality_grade;
      if (!hasCity) rec.assessor_best.meta.assessor_quality_note = "City assessor not present; using statewide parcels fallback where available.";
    }

    out.write(JSON.stringify(rec) + "\n");
    if (counts.rows % 500000 === 0) console.log("[progress] processed", counts.rows, "unknown", counts.unknown);
  }

  out.end();
  await new Promise((res) => out.on("finish", res));

  const outSha = sha256File(outNd);
  const audit = {
    created_at: new Date().toISOString(),
    phase4_pointer: phase4Ptr,
    input: inputNd,
    output: outNd,
    output_sha256: outSha,
    rows: counts.rows,
    ok: counts.ok,
    unknown: counts.unknown,
    unknown_rate: counts.rows ? (counts.unknown / counts.rows) : null,
    missing_reason_counts: counts.reasons,
    parcel_pattern_counts_among_unknown: counts.patterns,
    note: "UNKNOWN classification is pattern-based; does not infer external data availability."
  };
  fs.writeFileSync(auditPath, JSON.stringify(audit, null, 2), "utf8");
  console.log("[ok] wrote audit:", auditPath);
  console.log("[ok] output sha256:", outSha);

  const currentDir = path.resolve(root, "publicData/properties/_attached/CURRENT");
  ensureDir(currentDir);
  const currentV2 = path.join(currentDir, "CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json");
  const ptrV2 = { updated_at: new Date().toISOString(), note: "AUTO: Phase4 assessor UNKNOWN classify v1", properties_ndjson: outNd, audit: auditPath };
  fs.writeFileSync(currentV2, JSON.stringify(ptrV2, null, 2), "utf8");
  console.log("[ok] wrote canonical v2 pointer:", currentV2);
  console.log("[done] Phase4 assessor UNKNOWN classify v1 complete.");
}

main().catch((e) => { console.error("[fatal]", e?.stack ?? String(e)); process.exit(1); });
