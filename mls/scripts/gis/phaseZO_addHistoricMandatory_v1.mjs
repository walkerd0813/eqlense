#!/usr/bin/env node
/**
 * phaseZO_addHistoricMandatory_v1.mjs
 *
 * Goal:
 *  Ensure "historic/landmark/preservation" overlays are ALWAYS included for every city if present,
 *  even when they live under districts/ instead of overlays/.
 *
 * What it does:
 *  1) Loads the overlay-scan auditDir (default: .../phaseZO_overlay_scan/CURRENT_RUN.txt)
 *  2) Reads PHASE_ZO__approved_layers_AUTO.json
 *  3) For each city under publicData/zoning/<city>/:
 *      - searches BOTH:
 *          overlays/**
 *          districts/**
 *        for "historic/landmark/preservation" layers (GeoJSON preferred)
 *      - picks the best match per city (prefers overlays > districts, then GeoJSON, then larger file)
 *      - inserts/updates manifest entry with approved=true and bucket="mandatory_historic"
 *        including source_subdir ("overlays"|"districts") and rel_path.
 *  4) Writes:
 *      - PHASE_ZO__HISTORIC_MANDATORY.csv
 *      - PHASE_ZO__approved_layers_AUTO.json  (UPDATED in-place, with a backup)
 *
 * Usage (from backend/):
 *  node .\mls\scripts\gis\phaseZO_addHistoricMandatory_v1.mjs
 *
 * Optional:
 *  --root "C:\seller-app\backend"
 *  --auditDir "C:\seller-app\backend\publicData\_audit\phaseZO_overlay_scan\20251223_024314Z"
 *  --manifest "...\PHASE_ZO__approved_layers_AUTO.json"
 *  --dryRun true
 */

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";

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
    out[k] = v;
  }
  return out;
}

async function exists(p){ try { await fsp.access(p); return true; } catch { return false; } }

async function walkFiles(dir){
  const out = [];
  const stack = [dir];
  while (stack.length){
    const cur = stack.pop();
    let ents = [];
    try { ents = await fsp.readdir(cur, { withFileTypes: true }); }
    catch { continue; }
    for (const e of ents){
      const full = path.join(cur, e.name);
      if (e.isDirectory()) stack.push(full);
      else if (e.isFile()) out.push(full);
    }
  }
  return out;
}

function relFrom(base, full){
  const rp = path.relative(base, full);
  return rp.split(path.sep).join("/");
}

function layerKeyFrom(relPath, sourceSubdir){
  const s = `${sourceSubdir}:${relPath}`.toLowerCase();
  const h = crypto.createHash("sha1").update(s).digest("hex").slice(0, 10);
  const base = path.basename(relPath).toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  return `${base}__${h}`;
}

function isGeoJSON(p){
  const ext = path.extname(p).toLowerCase();
  return ext === ".geojson" || ext === ".json";
}

function historicScore(relLower, sourceSubdir){
  // Historic is mandatory; score used only to choose best file among several.
  let score = 0;
  const reasons = [];

  // Keyword strength
  if (/(local[_\s-]?historic|historic[_\s-]?district|historicdistricts|lhd)/.test(relLower)){ score += 6; reasons.push("historic_district_strong"); }
  if (/landmark|preservation|conservation[_\s-]?district/.test(relLower)){ score += 4; reasons.push("landmark_or_preservation"); }
  if (/historic/.test(relLower)){ score += 3; reasons.push("historic"); }

  // Prefer overlays folder when both exist (but districts acceptable)
  if (sourceSubdir === "overlays"){ score += 2; reasons.push("prefer_overlays"); }
  if (sourceSubdir === "districts"){ score += 1; reasons.push("districts_ok"); }

  // Penalize obvious junk/admin (still might contain "historic"? very unlikely)
  if (/police|fire|school|ward|precinct|admin|census|tract|block/.test(relLower)){ score -= 10; reasons.push("admin_penalty"); }

  return { score, reason: reasons.join("|") || "scored" };
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

async function main(){
  const args = parseArgs(process.argv);
  const root = args.root ? String(args.root) : process.cwd();
  const dryRun = String(args.dryRun ?? "false").toLowerCase() === "true";

  let auditDir = args.auditDir ? String(args.auditDir) : null;
  if (!auditDir){
    const ptr = path.join(root, "publicData", "_audit", "phaseZO_overlay_scan", "CURRENT_RUN.txt");
    if (!(await exists(ptr))){
      throw new Error(`No --auditDir provided and CURRENT_RUN.txt not found: ${ptr}`);
    }
    auditDir = (await fsp.readFile(ptr, "utf8")).trim();
  }

  const manifestPath = args.manifest
    ? String(args.manifest)
    : path.join(auditDir, "PHASE_ZO__approved_layers_AUTO.json");

  if (!(await exists(manifestPath))){
    throw new Error(`Missing manifest: ${manifestPath}`);
  }

  const zoningRoot = path.join(root, "publicData", "zoning");
  if (!(await exists(zoningRoot))){
    throw new Error(`Missing zoning root: ${zoningRoot}`);
  }

  console.log("====================================================");
  console.log(" Phase ZO — Add Mandatory Historic Layers (v1)");
  console.log(` Root:      ${root}`);
  console.log(` AuditDir:   ${auditDir}`);
  console.log(` Manifest:   ${manifestPath}`);
  console.log(` DryRun:     ${dryRun}`);
  console.log("====================================================");

  const manifest = JSON.parse(await fsp.readFile(manifestPath, "utf8"));
  if (!manifest.cities) manifest.cities = {};

  // Cities from zoning folder
  const cityDirs = (await fsp.readdir(zoningRoot, { withFileTypes: true }))
    .filter(d => d.isDirectory())
    .map(d => d.name)
    .filter(name => !name.startsWith("_")); // skip internal folders

  const report = [];

  for (const city of cityDirs){
    const cityLower = city.toLowerCase();

    // Search in overlays + districts
    const candidates = [];
    for (const subdir of ["overlays", "districts"]){
      const base = path.join(zoningRoot, city, subdir);
      if (!(await exists(base))) continue;
      const files = await walkFiles(base);
      for (const fp of files){
        const rel = relFrom(base, fp);
        const relLower = rel.toLowerCase();

        // Must look like historic/landmark/preservation
        if (!/(historic|landmark|preservation|lhd)/.test(relLower)) continue;

        const st = await fsp.stat(fp);
        const { score, reason } = historicScore(relLower, subdir);

        candidates.push({
          city: cityLower,
          source_subdir: subdir,
          rel_path: rel,
          abs_path: fp,
          ext: path.extname(fp).toLowerCase(),
          is_geojson: isGeoJSON(fp),
          size_bytes: st.size,
          modified_utc: new Date(st.mtimeMs).toISOString(),
          score,
          score_reason: reason
        });
      }
    }

    // Choose best match
    let pick = null;
    if (candidates.length){
      candidates.sort((a,b) => {
        // score desc
        if (b.score !== a.score) return b.score - a.score;
        // GeoJSON preferred
        if (b.is_geojson !== a.is_geojson) return (b.is_geojson ? 1 : 0) - (a.is_geojson ? 1 : 0);
        // bigger preferred
        if (b.size_bytes !== a.size_bytes) return b.size_bytes - a.size_bytes;
        // newer preferred
        return (b.modified_utc || "").localeCompare(a.modified_utc || "");
      });
      pick = candidates[0];
    }

    const cityArr = manifest.cities[cityLower] || [];
    // find existing historic entry (bucket mandatory_historic OR rel_path includes historic)
    const existingIdx = cityArr.findIndex(e => {
      const rp = String(e?.rel_path || "").toLowerCase();
      const b = String(e?.bucket || "");
      return b === "mandatory_historic" || /(historic|landmark|preservation|lhd)/.test(rp);
    });

    if (!pick){
      report.push({
        city: cityLower,
        found: false,
        source_subdir: "",
        rel_path: "",
        layer_key: "",
        action: "MISSING_HISTORIC_LAYER",
        notes: "No historic/landmark/preservation layer found in overlays/ or districts/."
      });
      continue;
    }

    const layer_key = layerKeyFrom(pick.rel_path, pick.source_subdir);

    const entry = {
      city: cityLower,
      layer_key,
      rel_path: pick.rel_path,
      source_subdir: pick.source_subdir, // IMPORTANT for later freeze/attach runner
      bucket: "mandatory_historic",
      score: pick.score,
      reason: `MANDATORY_HISTORIC:${pick.score_reason}`,
      approved: true
    };

    let action = "ADD";
    if (existingIdx >= 0){
      cityArr[existingIdx] = { ...cityArr[existingIdx], ...entry };
      action = "UPDATE";
    } else {
      cityArr.push(entry);
    }

    manifest.cities[cityLower] = cityArr;

    report.push({
      city: cityLower,
      found: true,
      source_subdir: pick.source_subdir,
      rel_path: pick.rel_path,
      layer_key,
      action,
      notes: `picked score=${pick.score}, geojson=${pick.is_geojson}, size_bytes=${pick.size_bytes}`
    });
  }

  const reportCsv = path.join(auditDir, "PHASE_ZO__HISTORIC_MANDATORY.csv");
  await fsp.writeFile(reportCsv, toCsv(report, ["city","found","source_subdir","rel_path","layer_key","action","notes"]), "utf8");
  done(`Wrote: ${reportCsv}`);

  if (!dryRun){
    // Backup manifest first
    const backup = manifestPath.replace(/\.json$/i, `.bak_${Date.now()}.json`);
    await fsp.copyFile(manifestPath, backup);
    await fsp.writeFile(manifestPath, JSON.stringify(manifest, null, 2), "utf8");
    done(`Updated manifest in-place: ${manifestPath}`);
    info(`Backup: ${backup}`);
  } else {
    warn("DryRun=true: manifest not modified.");
  }

  console.log("====================================================");
  done("Historic mandatory injection complete.");
  console.log("NEXT:");
  console.log("  1) Open PHASE_ZO__HISTORIC_MANDATORY.csv and confirm each city has a pick.");
  console.log("  2) Then run Normalize+Freeze (v2) which supports source_subdir (overlays/districts).");
  console.log("====================================================");
}

main().catch(e => {
  console.error("[fatal]", e?.stack || e);
  process.exit(1);
});
