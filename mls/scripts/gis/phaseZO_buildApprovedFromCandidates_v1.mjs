#!/usr/bin/env node
/**
 * phaseZO_buildApprovedFromCandidates_v1.mjs
 *
 * Purpose:
 *  Reads the latest Phase ZO overlay scan output and produces:
 *    - PHASE_ZO__APPROVE_STRONG.csv
 *    - PHASE_ZO__REVIEW.csv
 *    - PHASE_ZO__AUTO_EXCLUDE.csv
 *    - PHASE_ZO__approved_layers_AUTO.json   (starter manifest: approved true/false)
 *
 * Default behavior:
 *  If --auditDir is NOT provided, it will read:
 *    <root>/publicData/_audit/phaseZO_overlay_scan/CURRENT_RUN.txt
 *  where <root> defaults to process.cwd()
 *
 * Usage (from backend/):
 *  node .\mls\scripts\gis\phaseZO_buildApprovedFromCandidates_v1.mjs
 *
 * Optional:
 *  --root "C:\seller-app\backend"
 *  --auditDir "C:\seller-app\backend\publicData\_audit\phaseZO_overlay_scan\20251223_024314Z"
 */

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

function info(m){ console.log(`[info] ${m}`); }
function warn(m){ console.log(`[warn] ${m}`); }
function done(m){ console.log(`[done] ${m}`); }

function parseArgs(argv){
  const out = {};
  for (let i=2;i<argv.length;i++){
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = (argv[i+1] && !argv[i+1].startsWith("--")) ? argv[++i] : true;
    out[k]=v;
  }
  return out;
}

async function exists(p){
  try { await fsp.access(p); return true; } catch { return false; }
}

function csvParse(text){
  // Minimal CSV parser w/ quotes support.
  const rows = [];
  let i = 0;
  let field = "";
  let row = [];
  let inQuotes = false;

  const pushField = () => { row.push(field); field=""; };
  const pushRow = () => { rows.push(row); row=[]; };

  while (i < text.length){
    const c = text[i];

    if (inQuotes){
      if (c === '"'){
        const next = text[i+1];
        if (next === '"'){ field += '"'; i += 2; continue; }
        inQuotes = false; i++; continue;
      }
      field += c; i++; continue;
    }

    if (c === '"'){ inQuotes = true; i++; continue; }

    if (c === ","){ pushField(); i++; continue; }

    if (c === "\r"){
      // ignore, handle on \n
      i++; continue;
    }

    if (c === "\n"){
      pushField(); pushRow(); i++; continue;
    }

    field += c; i++;
  }

  // flush last row if needed
  if (field.length > 0 || row.length > 0){
    pushField(); pushRow();
  }

  // If last row is empty (common trailing newline), drop it
  if (rows.length && rows[rows.length-1].length===1 && rows[rows.length-1][0]===""){
    rows.pop();
  }

  return rows;
}

function toCsv(rows, headers){
  const esc = (v) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    if (/[",\n]/.test(s)) return `"${s.replaceAll('"','""')}"`;
    return s;
  };
  const out = [];
  out.push(headers.join(","));
  for (const r of rows){
    out.push(headers.map(h => esc(r[h])).join(","));
  }
  return out.join("\n");
}

function scoreOverlay(rel){
  const p = rel.toLowerCase();

  // Hard excludes
  if (/police|fire|school|schools|ward|precinct|admin|administrative|voting|census|tract|block/.test(p)){
    return { bucket: "auto_exclude", score: -999, reason: "admin_or_junk" };
  }

  // Env duplicate-ish patterns (keep visible, but not auto-approve)
  if (/flood|fema|wetland|wetlands|open[_\s]?space|conservation|aquifer|groundwater|hazard|buffer|river|coastal|shore|storm|sea[_\s]?level/.test(p)){
    return { bucket: "review", score: 0, reason: "env_duplicate_like" };
  }

  let score = 0;
  const reasons = [];

  // Strong overlay/subdistrict signals
  const strong = [
    "overlay","overlays","ipod","subdistrict","sub_district","special_district","specialdistrict",
    "planned","pod","spd","pdd","pdod","interim","district_overlay","design_review"
  ];
  if (strong.some(k => p.includes(k))){
    score += 3; reasons.push("strong_overlay_keyword");
  }

  // MA city zoning overlay themes (common)
  const mid = ["mbta","multifamily","mf_overlay","transit","village","center_overlay","waterfront","harbor","downtown","station"];
  if (mid.some(k => p.includes(k))){
    score += 2; reasons.push("common_overlay_theme");
  }

  // Historic districts are legal overlays (often relevant; keep as review/approve depending on naming)
  if (/historic|landmark|preservation|lhd|local[_\s]?historic/.test(p)){
    score += 2; reasons.push("historic_overlay");
  }

  // If it looks like pure base zoning, don't auto-approve from overlays folder
  if (/zoning_base|base_zoning|zoningdistrict|zoning_district/.test(p)){
    return { bucket: "auto_exclude", score: -50, reason: "base_zoning_not_overlay" };
  }

  // Decide bucket
  if (score >= 4) return { bucket: "approve_strong", score, reason: reasons.join("|") || "high_score" };
  if (score >= 2) return { bucket: "review", score, reason: reasons.join("|") || "medium_score" };

  // Low score: still might be relevant if it's a real GIS layer (we don't know), so keep in review
  return { bucket: "review", score, reason: reasons.join("|") || "low_signal_keep_review" };
}

async function main(){
  const args = parseArgs(process.argv);
  const root = args.root ? String(args.root) : process.cwd();

  let auditDir = args.auditDir ? String(args.auditDir) : null;
  if (!auditDir){
    const ptr = path.join(root, "publicData", "_audit", "phaseZO_overlay_scan", "CURRENT_RUN.txt");
    if (!(await exists(ptr))){
      throw new Error(`No --auditDir provided and CURRENT_RUN.txt not found: ${ptr}`);
    }
    auditDir = (await fsp.readFile(ptr, "utf8")).trim();
  }

  const candCsv = path.join(auditDir, "PHASE_ZO__CANDIDATES.csv");
  if (!(await exists(candCsv))){
    throw new Error(`Missing candidates CSV: ${candCsv}`);
  }

  info(`Root:     ${root}`);
  info(`AuditDir:  ${auditDir}`);
  info(`Input:     ${candCsv}`);

  const text = await fsp.readFile(candCsv, "utf8");
  const parsed = csvParse(text);
  if (parsed.length < 2){
    warn("Candidates CSV has no data rows.");
  }

  const headers = parsed[0];
  const idx = (name) => headers.indexOf(name);

  const rows = [];
  for (let r=1;r<parsed.length;r++){
    const cols = parsed[r];
    const obj = {};
    for (let c=0;c<headers.length;c++){
      obj[headers[c]] = cols[c] ?? "";
    }
    rows.push(obj);
  }

  const approveStrong = [];
  const review = [];
  const autoExclude = [];

  const manifest = {
    meta: {
      created_utc: new Date().toISOString(),
      audit_dir: auditDir,
      source_candidates_csv: candCsv,
      rules_version: "phaseZO_buildApprovedFromCandidates_v1",
      notes: [
        "This is an AUTO starter manifest. Review before running Normalize+Attach+Freeze.",
        "Env-duplicate-like overlays are kept in REVIEW by default.",
        "Admin/junk are auto-excluded."
      ]
    },
    cities: {}
  };

  for (const r of rows){
    const city = String(r.city || "").toLowerCase();
    const rel = String(r.rel_path || "");
    const layerKey = String(r.layer_key || "");

    const { bucket, score, reason } = scoreOverlay(rel);

    const entry = {
      city,
      layer_key: layerKey,
      rel_path: rel,
      bucket,
      score,
      reason,
      approved: bucket === "approve_strong"
    };

    if (!manifest.cities[city]) manifest.cities[city] = [];
    manifest.cities[city].push(entry);

    if (bucket === "approve_strong") approveStrong.push(entry);
    else if (bucket === "auto_exclude") autoExclude.push(entry);
    else review.push(entry);
  }

  const outApproveCsv = path.join(auditDir, "PHASE_ZO__APPROVE_STRONG.csv");
  const outReviewCsv = path.join(auditDir, "PHASE_ZO__REVIEW.csv");
  const outExcludeCsv = path.join(auditDir, "PHASE_ZO__AUTO_EXCLUDE.csv");
  const outManifest = path.join(auditDir, "PHASE_ZO__approved_layers_AUTO.json");

  const outHeaders = ["city","layer_key","rel_path","bucket","score","reason","approved"];

  await fsp.writeFile(outApproveCsv, toCsv(approveStrong, outHeaders), "utf8");
  await fsp.writeFile(outReviewCsv, toCsv(review, outHeaders), "utf8");
  await fsp.writeFile(outExcludeCsv, toCsv(autoExclude, outHeaders), "utf8");
  await fsp.writeFile(outManifest, JSON.stringify(manifest, null, 2), "utf8");

  done(`Wrote: ${outApproveCsv}`);
  done(`Wrote: ${outReviewCsv}`);
  done(`Wrote: ${outExcludeCsv}`);
  done(`Wrote: ${outManifest}`);

  console.log("====================================================");
  done("Approve-builder complete.");
  console.log("NEXT:");
  console.log("  1) Open PHASE_ZO__approved_layers_AUTO.json and flip approved=true/false as needed.");
  console.log("  2) Then we run Normalize+Attach+Freeze using that approved manifest.");
  console.log("====================================================");
}

main().catch(e => {
  console.error("[fatal]", e?.stack || e);
  process.exit(1);
});
