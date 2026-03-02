#!/usr/bin/env node
/**
 * scanCityOverlaysForPhaseZO_v1.mjs
 *
 * Phase ZO — fast overlay folder scan:
 *   Scans:  backend/publicData/zoning/<city>/overlays/**
 *   Writes: backend/publicData/_audit/phaseZO_overlay_scan/<timestamp>/**
 *
 * Outputs:
 *   - PHASE_ZO__CANDIDATES.csv          (Phase ZO candidate overlays)
 *   - PHASE_ZO__EXCLUDED.csv            (excluded + why)
 *   - SUMMARY__by_city.csv              (counts)
 *   - PHASE_ZO__approved_layers_TEMPLATE.json (approval checklist per city)
 *
 * Usage (from backend/):
 *   node .\mls\scripts\gis\scanCityOverlaysForPhaseZO_v1.mjs
 *
 * Optional:
 *   --root "C:\seller-app\backend"
 *   --cities "boston,cambridge,somerville,chelsea"
 *   --hash small|all|none      (default: small)
 *   --hashMaxMB 512            (default: 512)
 */

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";

function logInfo(msg) { console.log(`[info] ${msg}`); }
function logWarn(msg) { console.log(`[warn] ${msg}`); }
function logDone(msg) { console.log(`[done] ${msg}`); }

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const key = a.slice(2);
    const val = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
    out[key] = val;
  }
  return out;
}

function utcStamp() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}_${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}Z`;
}

async function ensureDir(p) {
  await fsp.mkdir(p, { recursive: true });
}

async function pathExists(p) {
  try { await fsp.access(p); return true; } catch { return false; }
}

function toCsv(rows, headers) {
  const esc = (v) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    if (/[",\n]/.test(s)) return `"${s.replaceAll(`"`, `""`)}"`;
    return s;
  };
  const lines = [];
  lines.push(headers.join(","));
  for (const r of rows) lines.push(headers.map(h => esc(r[h])).join(","));
  return lines.join("\n");
}

function relFrom(base, full) {
  const rp = path.relative(base, full);
  return rp.split(path.sep).join("/");
}

function looksLikeOverlay(relLower) {
  // Strong ZO signals (tune over time)
  return /overlay|overlays|ipod|subdistrict|sub_district|special_district|specialdistrict|planned|pod|spd|pdd|pdod|interim|design_review|village|center_overlay|mbta|multifamily|mf_overlay|transit|district_overlay/.test(relLower);
}

function looksAdminOrJunk(relLower) {
  return /police|fire|school|schools|ward|precinct|admin|administrative|voting|census|tract|block|neighborhoods?(_admin)?/.test(relLower);
}

function looksEnvDuplicate(relLower) {
  // Phase 1A env/legal statewide canonical only — city copies flagged as duplicates
  return /flood|fema|wetland|wetlands|open[_\s]?space|conservation|aquifer|groundwater|hazard|buffer|river|coastal|shore|storm|sea[_\s]?level/.test(relLower);
}

function shouldExcludeFolder(relLower) {
  // Prevent trash folders from polluting lists (still logged as excluded)
  return /base\s?only|normalized|_normalized|tmp|temp|old|archive|backup/.test(relLower);
}

function isGeoDataExt(extLower) {
  return [
    ".geojson", ".json",
    ".shp", ".dbf", ".shx", ".prj", ".cpg",
    ".gpkg",
    ".zip"
  ].includes(extLower);
}

async function sha256File(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash("sha256");
    const stream = fs.createReadStream(filePath);
    stream.on("data", (d) => hash.update(d));
    stream.on("error", reject);
    stream.on("end", () => resolve(hash.digest("hex")));
  });
}

function shouldHash(sizeBytes, mode, maxMB) {
  if (mode === "none") return false;
  if (mode === "all") return true;
  return sizeBytes <= maxMB * 1024 * 1024;
}

async function walkFiles(dir) {
  const out = [];
  const stack = [dir];
  while (stack.length) {
    const cur = stack.pop();
    let ents = [];
    try { ents = await fsp.readdir(cur, { withFileTypes: true }); }
    catch { continue; }

    for (const e of ents) {
      const full = path.join(cur, e.name);
      if (e.isDirectory()) stack.push(full);
      else if (e.isFile()) out.push(full);
    }
  }
  return out;
}

function layerKeyFrom(relPath) {
  const s = relPath.toLowerCase();
  const h = crypto.createHash("sha1").update(s).digest("hex").slice(0, 10);
  const base = path.basename(relPath).toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  return `${base}__${h}`;
}

async function main() {
  const args = parseArgs(process.argv);

  const root = args.root ? String(args.root) : process.cwd();
  const citiesArg = args.cities ? String(args.cities) : null;
  const citiesFilter = citiesArg ? new Set(citiesArg.split(",").map(s => s.trim().toLowerCase()).filter(Boolean)) : null;

  const hashMode = String(args.hash ?? "small").toLowerCase();
  const hashMaxMB = Number(args.hashMaxMB ?? 512);

  const zoningRoot = path.join(root, "publicData", "zoning");
  const auditRootBase = path.join(root, "publicData", "_audit", "phaseZO_overlay_scan");
  const ts = utcStamp();
  const auditRoot = path.join(auditRootBase, ts);

  console.log("====================================================");
  console.log(" Phase ZO — Overlay Scan (all cities)");
  console.log(` Root:     ${root}`);
  console.log(` Scan:     publicData/zoning/<city>/overlays/**`);
  console.log(` Out:      ${auditRoot}`);
  console.log(` Hash:     ${hashMode} (small max ${hashMaxMB}MB)`);
  console.log("====================================================");

  if (!(await pathExists(zoningRoot))) {
    throw new Error(`Missing zoning root: ${zoningRoot}`);
  }

  await ensureDir(auditRoot);

  // CURRENT pointer
  await ensureDir(auditRootBase);
  await fsp.writeFile(path.join(auditRootBase, "CURRENT_RUN.txt"), auditRoot, "utf8");

  // Cities
  const cityDirs = (await fsp.readdir(zoningRoot, { withFileTypes: true }))
    .filter(d => d.isDirectory())
    .map(d => d.name);

  const cities = cityDirs.filter(c => !citiesFilter || citiesFilter.has(c.toLowerCase()));
  logInfo(`Cities found: ${cityDirs.length}. Scanning: ${cities.length}.`);

  const allRows = [];
  const byCitySummary = [];

  for (const city of cities) {
    const overlaysDir = path.join(zoningRoot, city, "overlays");
    if (!(await pathExists(overlaysDir))) {
      logWarn(`No overlays folder: ${overlaysDir}`);
      byCitySummary.push({ city, overlays_folder_exists: false, files: 0, candidates: 0 });
      continue;
    }

    logInfo(`Scanning city overlays: ${city}`);
    const files = await walkFiles(overlaysDir);

    const cityRows = [];
    let candCount = 0;

    let idx = 0;
    for (const fullPath of files) {
      idx++;
      if (idx % 300 === 0) logInfo(`  progress: ${idx}/${files.length} files`);

      const st = await fsp.stat(fullPath);
      const rel = relFrom(overlaysDir, fullPath);
      const relLower = rel.toLowerCase();
      const ext = path.extname(fullPath).toLowerCase();

      let excludeReason = null;
      if (shouldExcludeFolder(relLower)) excludeReason = "excluded_folder_pattern";
      if (looksAdminOrJunk(relLower)) excludeReason = excludeReason ?? "admin_or_junk";

      const envDup = looksEnvDuplicate(relLower);
      const overlaySignal = looksLikeOverlay(relLower);

      const geoish = isGeoDataExt(ext);
      const isCandidate =
        !excludeReason &&
        !envDup &&
        (overlaySignal || geoish) &&
        !/zoning_base|base_zoning|zoningdistrict|zoning_district/.test(relLower);

      if (isCandidate) candCount++;

      let sha256 = null;
      if (shouldHash(st.size, hashMode, hashMaxMB)) {
        try { sha256 = await sha256File(fullPath); }
        catch { sha256 = null; }
      } else if (hashMode !== "none") {
        sha256 = `SKIPPED_SIZE_GT_${hashMaxMB}MB`;
      }

      const row = {
        city,
        overlays_root: overlaysDir,
        rel_path: rel,
        layer_key: layerKeyFrom(rel),
        ext,
        size_bytes: st.size,
        size_mb: Math.round((st.size / (1024 * 1024)) * 1000) / 1000,
        modified_utc: new Date(st.mtimeMs).toISOString(),
        overlay_signal: overlaySignal,
        env_duplicate_flag: envDup,
        exclude_reason: excludeReason,
        phaseZO_candidate: isCandidate,
        sha256
      };

      cityRows.push(row);
      allRows.push(row);
    }

    const cityOut = path.join(auditRoot, city);
    await ensureDir(cityOut);

    const headers = Object.keys(cityRows[0] ?? {
      city: "", overlays_root: "", rel_path: "", layer_key: "", ext: "", size_bytes: 0, size_mb: 0,
      modified_utc: "", overlay_signal: false, env_duplicate_flag: false, exclude_reason: "", phaseZO_candidate: false, sha256: ""
    });

    await fsp.writeFile(path.join(cityOut, `OVERLAYS__${city}.json`), JSON.stringify(cityRows, null, 2), "utf8");
    await fsp.writeFile(path.join(cityOut, `OVERLAYS__${city}.csv`), toCsv(cityRows, headers), "utf8");

    byCitySummary.push({
      city,
      overlays_folder_exists: true,
      files: cityRows.length,
      candidates: candCount
    });

    logDone(`City ${city}: files=${cityRows.length}, candidates=${candCount}`);
  }

  const headersAll = Object.keys(allRows[0] ?? {
    city: "", overlays_root: "", rel_path: "", layer_key: "", ext: "", size_bytes: 0, size_mb: 0,
    modified_utc: "", overlay_signal: false, env_duplicate_flag: false, exclude_reason: "", phaseZO_candidate: false, sha256: ""
  });

  await fsp.writeFile(path.join(auditRoot, "ROLLUP__ALL_OVERLAYS.json"), JSON.stringify(allRows, null, 2), "utf8");
  await fsp.writeFile(path.join(auditRoot, "ROLLUP__ALL_OVERLAYS.csv"), toCsv(allRows, headersAll), "utf8");

  const candidates = allRows.filter(r => r.phaseZO_candidate);
  const excluded = allRows.filter(r => !r.phaseZO_candidate);

  await fsp.writeFile(path.join(auditRoot, "PHASE_ZO__CANDIDATES.csv"), toCsv(candidates, headersAll), "utf8");
  await fsp.writeFile(path.join(auditRoot, "PHASE_ZO__EXCLUDED.csv"), toCsv(excluded, headersAll), "utf8");

  const approvedTemplate = {};
  for (const r of candidates) {
    const c = r.city.toLowerCase();
    if (!approvedTemplate[c]) approvedTemplate[c] = [];
    approvedTemplate[c].push({
      layer_key: r.layer_key,
      rel_path: r.rel_path,
      notes: "REVIEW_ME",
      approved: false
    });
  }
  await fsp.writeFile(
    path.join(auditRoot, "PHASE_ZO__approved_layers_TEMPLATE.json"),
    JSON.stringify(approvedTemplate, null, 2),
    "utf8"
  );

  await fsp.writeFile(path.join(auditRoot, "SUMMARY__by_city.json"), JSON.stringify(byCitySummary, null, 2), "utf8");
  await fsp.writeFile(
    path.join(auditRoot, "SUMMARY__by_city.csv"),
    toCsv(byCitySummary, Object.keys(byCitySummary[0] ?? { city: "", overlays_folder_exists: false, files: 0, candidates: 0 })),
    "utf8"
  );

  const manifest = {
    created_utc: new Date().toISOString(),
    root,
    scan: "publicData/zoning/<city>/overlays/**",
    out: auditRoot,
    hash_mode: hashMode,
    hash_max_mb: hashMaxMB,
    notes: [
      "Phase ZO Step B: inventory + auto-sorting only. No approvals made.",
      "Env duplicate layers are flagged (flood/wetlands/open space etc.) and excluded from Phase ZO candidates.",
      "Admin/junk patterns excluded. Review candidates manually before Normalize+Attach+Freeze."
    ]
  };
  await fsp.writeFile(path.join(auditRoot, "MANIFEST.json"), JSON.stringify(manifest, null, 2), "utf8");

  console.log("====================================================");
  logDone("Overlay scan complete.");
  console.log("Open this folder in VS Code:");
  console.log(`  ${auditRoot}`);
  console.log("Key files:");
  console.log("  PHASE_ZO__CANDIDATES.csv");
  console.log("  PHASE_ZO__approved_layers_TEMPLATE.json");
  console.log("====================================================");
}

main().catch((e) => {
  console.error("[fatal]", e?.stack || e);
  process.exit(1);
});
