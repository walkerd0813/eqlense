import fs from 'node:fs';
import path from 'node:path';

function parseArgs(argv) {
  const o = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith('--')) {
      const k = a.slice(2);
      const v = (argv[i + 1] && !argv[i + 1].startsWith('--')) ? argv[++i] : 'true';
      o[k] = v;
    }
  }
  return o;
}

function safeStat(p) {
  try { return fs.statSync(p); } catch { return null; }
}

function existsDir(p) {
  const st = safeStat(p);
  return !!(st && st.isDirectory());
}

function walk(dir, out) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const e of entries) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) {
      // Skip giant derived/frozen trees
      const nm = e.name.toLowerCase();
      if (nm === '_frozen' || nm === '_work' || nm === '_audit' || nm === 'normalized' || nm === 'base only...' || nm === 'base-only') {
        continue;
      }
      walk(p, out);
    } else if (e.isFile()) {
      out.push(p);
    }
  }
}

function classify(relLower, baseLower) {
  const tags = [];
  const add = (t) => { if (!tags.includes(t)) tags.push(t); };

  // hard excludes
  if (baseLower.includes('zoning_base') || baseLower.includes('basezoning') || baseLower.includes('base_zoning')) {
    return { cls: 'base_zoning', tags: ['exclude'] };
  }

  // civic boundaries (deferred Phase 2)
  if (baseLower.includes('police') || baseLower.includes('fire') || baseLower.includes('school') || baseLower.includes('ward') || baseLower.includes('precinct') || baseLower.includes('neighborhood') || baseLower.includes('trash') || baseLower.includes('snow') || baseLower.includes('sweeping')) {
    return { cls: 'civic_candidate', tags: ['phase2_deferred'] };
  }

  // env duplicates (Phase1 statewide canonical)
  if (
    baseLower.includes('fema') || baseLower.includes('nfhl') || baseLower.includes('flood') ||
    baseLower.includes('wetland') || baseLower.includes('aquifer') || baseLower.includes('zoneii') ||
    baseLower.includes('iwpa') || baseLower.includes('swsp') || baseLower.includes('water_supply') ||
    baseLower.includes('surface_water') || baseLower.includes('pros') || baseLower.includes('open_space')
  ) {
    return { cls: 'env_duplicate_candidate', tags: ['phase1_statewide_duplicate'] };
  }

  // local legal patches (Phase 1B)
  if (baseLower.includes('historic') || baseLower.includes('landmark') || baseLower.includes('preservation') || baseLower.includes('local_historic')) {
    return { cls: 'local_legal_patch_candidate', tags: ['phase1b_local'] };
  }

  // zoning overlays (Phase ZO)
  if (
    baseLower.includes('overlay') || baseLower.includes('subdistrict') || baseLower.includes('sub_district') ||
    baseLower.includes('special') || baseLower.includes('district') || baseLower.includes('design') ||
    baseLower.includes('tod') || baseLower.includes('transit') || baseLower.includes('mbta') ||
    baseLower.includes('multifamily') || baseLower.includes('multi_family') || baseLower.includes('mf') ||
    baseLower.includes('village') || baseLower.includes('center') || baseLower.includes('cdd') || baseLower.includes('sesa')
  ) {
    add('phasezo');
    return { cls: 'zoning_overlay_candidate', tags };
  }

  // might still be zoning-related
  if (baseLower.includes('zoning') && (baseLower.includes('overlay') || baseLower.includes('district'))) {
    add('phasezo');
    return { cls: 'zoning_overlay_candidate', tags };
  }

  return { cls: 'unknown', tags: [] };
}

const args = parseArgs(process.argv);
const root = path.resolve(args.root || process.cwd());
const cityRaw = (args.city || '').trim();
if (!cityRaw) {
  console.error('Usage: node scan_city_overlay_candidates_v1.mjs --root <backendRoot> --city <CityName> --out <out.json>');
  process.exit(2);
}
const city = cityRaw.toLowerCase();
const outPath = path.resolve(args.out || path.join(root, 'publicData', '_audit', `phasezo_inventory__${city}__${Date.now()}.json`));

const searchRoots = [
  path.join(root, 'publicData', 'zoning', city),
  path.join(root, 'publicData', 'boundaries', city),
  path.join(root, 'publicData', 'gis', 'cities', city),
  path.join(root, 'publicData', 'gis', 'cities', city, 'raw'),
  path.join(root, 'publicData', 'gis', 'cities', city, 'normalized')
].filter(existsDir);

const allowedExt = new Set(['.geojson', '.geojsons', '.json', '.shp', '.gdb', '.gpkg']);
const files = [];
for (const r of searchRoots) {
  walk(r, files);
}

const candidates = [];
for (const abs of files) {
  const ext = path.extname(abs).toLowerCase();
  if (!allowedExt.has(ext)) continue;

  const rel = path.relative(root, abs);
  const relLower = rel.toLowerCase();

  // exclude folders known to be noise
  if (relLower.includes('base only') || relLower.includes('base-only') || relLower.includes('normalized\\base') || relLower.includes('\\_frozen\\') || relLower.includes('\\_work\\')) {
    continue;
  }

  const base = path.basename(abs);
  const baseLower = base.toLowerCase();

  const cls = classify(relLower, baseLower);
  if (cls.tags.includes('exclude')) continue;

  const st = safeStat(abs);
  candidates.push({
    city: cityRaw,
    abs_path: abs,
    rel_path: rel,
    ext,
    size_bytes: st ? st.size : null,
    mtime_iso: st ? st.mtime.toISOString() : null,
    class: cls.cls,
    tags: cls.tags
  });
}

const out = {
  created_at: new Date().toISOString(),
  root,
  city: cityRaw,
  search_roots: searchRoots.map((p) => path.relative(root, p)),
  candidates_count: candidates.length,
  candidates
};

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(out, null, 2), 'utf8');
console.error(`[ok] wrote ${outPath}`);
console.error(`[info] candidates_count=${candidates.length}`);
