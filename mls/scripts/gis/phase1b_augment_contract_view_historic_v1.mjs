#!/usr/bin/env node
/*
Phase1B -> Contract View augmentation (Historic split)

Goal
- Keep Phase1B attachments as the source of truth.
- Add conservative, legally-safer split fields to contract view:
  Enforceable historic controls (district-based):
    historic_district_name
    historic_designation_type (local|state|federal)
    review_required_flag
    demolition_restricted_flag
    exterior_change_review_flag
    regulatory_body

  Informational historic inventory (non-enforceable):
    historic_inventory_flag
    historic_significance_level
    inventory_source

Notes
- This script intentionally avoids claiming detailed ordinance rules.
- If we can’t confidently extract a value, we emit null (UNKNOWN).
*/

import fs from 'fs';
import path from 'path';
import readline from 'readline';
import crypto from 'crypto';

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    const key = a.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      args[key] = true;
    } else {
      args[key] = next;
      i++;
    }
  }
  return args;
}

function sha256File(filePath) {
  const h = crypto.createHash('sha256');
  const buf = fs.readFileSync(filePath);
  h.update(buf);
  return h.digest('hex').toUpperCase();
}

// Accept either a contract NDJSON file OR a directory that contains it.
function resolveContractInPath(p) {
  if (!p) throw new Error('--contractIn is required');
  if (!fs.existsSync(p)) throw new Error(`--contractIn must exist: ${p}`);

  const st = fs.statSync(p);
  if (st.isFile()) return p;

  if (st.isDirectory()) {
    const entries = fs
      .readdirSync(p)
      .filter((fn) => fn.toLowerCase().endsWith('.ndjson'))
      .map((fn) => ({
        fn,
        full: path.join(p, fn),
        mtime: fs.statSync(path.join(p, fn)).mtimeMs,
      }));

    if (!entries.length) {
      throw new Error(`--contractIn is a directory but contains no .ndjson files: ${p}`);
    }

    const priority = (fn) => {
      const low = fn.toLowerCase();
      if (low.includes('contract_view') && low.endsWith('.ndjson')) return 0;
      return 1;
    };

    entries.sort((a, b) => priority(a.fn) - priority(b.fn) || b.mtime - a.mtime);
    return entries[0].full;
  }

  throw new Error(`--contractIn must be a file or directory: ${p}`);
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function readJsonFile(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

function toLowerSafe(v) {
  return (v ?? '').toString().trim().toLowerCase();
}

function isObject(x) {
  return x && typeof x === 'object' && !Array.isArray(x);
}

function pickFirstString(obj, fields) {
  for (const f of fields) {
    if (obj && obj[f] != null) {
      const v = obj[f];
      if (typeof v === 'string') {
        const s = v.trim();
        if (s) return s;
      }
      if (typeof v === 'number') return String(v);
    }
  }
  return null;
}

function tryExtractFromNested(row, fields) {
  // Common places a freeze/attach pipeline may stash feature properties.
  const candidates = [
    row,
    row.properties,
    row.feature_properties,
    row.featureProperties,
    row.feature,
    row.feature?.properties,
    row.overlay_properties,
    row.overlayProperties
  ].filter(Boolean);

  for (const c of candidates) {
    if (!isObject(c)) continue;
    const got = pickFirstString(c, fields);
    if (got) return got;
  }
  return null;
}

function normalizeOverlayKey(row) {
  const k = row.overlay_key ?? row.overlayKey ?? row.layer_key ?? row.layerKey ?? row.key ?? row.overlay_id ?? row.overlayId;
  if (typeof k === 'string' && k.trim()) return k.trim();
  // fallback: some pipelines store a "layer_id" or "layer" label
  const k2 = row.layer_id ?? row.layerId ?? row.layer ?? row.layer_name ?? row.layerName;
  if (typeof k2 === 'string' && k2.trim()) return k2.trim();
  return null;
}

function overlayKeyMatches(key, matchAnyList) {
  const k = toLowerSafe(key);
  for (const m of matchAnyList) {
    if (!m) continue;
    if (k.includes(toLowerSafe(m))) return true;
  }
  return false;
}

function buildHistoricIndex(attachmentsPath, mapping) {
  const enforceableMatch = mapping?.enforceable?.match_any ?? [];
  const inventoryMatch = mapping?.inventory?.match_any ?? [];

  const nameFields = mapping?.enforceable?.name_fields ?? ['name'];
  const sigFields = mapping?.inventory?.significance_fields ?? ['significance'];
  const srcFields = mapping?.inventory?.source_fields ?? ['source'];

  const designationType = mapping?.enforceable?.designation_type ?? 'local';
  const defaultRegBody = mapping?.enforceable?.default_regulatory_body ?? 'local commission';
  const cityRegBody = mapping?.enforceable?.city_regulatory_body ?? {};

  // Legal-safe defaults: unknown unless the mapping explicitly asserts the flag.
  const defaultReview = mapping?.enforceable?.default_review_required_flag ?? null;
  const defaultExterior = mapping?.enforceable?.default_exterior_change_review_flag ?? null;
  const defaultDemo = (mapping?.enforceable?.default_demolition_restricted_flag ?? null);

  const defaultInvSource = mapping?.inventory?.default_inventory_source ?? 'city_gis';

  const index = new Map(); // property_id -> agg

  const input = fs.createReadStream(attachmentsPath, { encoding: 'utf8' });
  const rl = readline.createInterface({ input, crlfDelay: Infinity });

  let rows = 0;

  return new Promise((resolve, reject) => {
    rl.on('line', (line) => {
      const t = line.trim();
      if (!t) return;
      rows++;
      let row;
      try {
        row = JSON.parse(t);
      } catch {
        return; // ignore bad line
      }

      const propertyId = row.property_id ?? row.propertyId ?? row.pid;
      if (!propertyId) return;

      const overlayKey = normalizeOverlayKey(row);
      if (!overlayKey) return;

      const city = toLowerSafe(row.city ?? row.source_city ?? row.sourceCity ?? row.municipality ?? row.source_muni ?? row.source_municipality);

      const isEnf = overlayKeyMatches(overlayKey, enforceableMatch);
      const isInv = !isEnf && overlayKeyMatches(overlayKey, inventoryMatch);

      if (!isEnf && !isInv) return;

      let agg = index.get(propertyId);
      if (!agg) {
        agg = {
          enforceable: {
            names: new Set(),
            designation_type: null,
            regulatory_body: null,
            review_required_flag: null,
            demolition_restricted_flag: null,
            exterior_change_review_flag: null
          },
          inventory: {
            flag: false,
            significance_level: null,
            source: null
          }
        };
        index.set(propertyId, agg);
      }

      if (isEnf) {
        const name = tryExtractFromNested(row, nameFields);
        if (name) agg.enforceable.names.add(name);

        agg.enforceable.designation_type = designationType;
        agg.enforceable.regulatory_body = (city && cityRegBody[city]) ? cityRegBody[city] : defaultRegBody;
        agg.enforceable.review_required_flag = defaultReview;
        agg.enforceable.exterior_change_review_flag = defaultExterior;
        agg.enforceable.demolition_restricted_flag = defaultDemo;
      } else if (isInv) {
        agg.inventory.flag = true;
        const sig = tryExtractFromNested(row, sigFields);
        if (sig && !agg.inventory.significance_level) agg.inventory.significance_level = sig;
        const src = tryExtractFromNested(row, srcFields);
        if (src && !agg.inventory.source) agg.inventory.source = src;
        if (!agg.inventory.source) agg.inventory.source = defaultInvSource;
      }
    });

    rl.on('close', () => {
      resolve({ index, rows });
    });

    rl.on('error', reject);
  });
}

async function main() {
  const args = parseArgs(process.argv);

  const contractIn = resolveContractInPath(args.contractIn);
  const attachments = args.attachments;
  const out = args.out;
  const mappingPath = args.mapping;
  const metaOut = args.metaOut;

  // contractIn validated by resolveContractInPath
  if (!attachments) throw new Error('Missing --attachments');
  if (!out) throw new Error('Missing --out');
  if (!mappingPath) throw new Error('Missing --mapping');

  if (!fs.existsSync(attachments) || fs.statSync(attachments).isDirectory()) {
    throw new Error(`--attachments must be an existing FILE: ${attachments}`);
  }

  const outDir = path.dirname(out);
  ensureDir(outDir);

  const mapping = readJsonFile(mappingPath);

  const t0 = Date.now();
  console.log(`[info] contractIn: ${contractIn}`);
  console.log(`[info] attachments: ${attachments}`);
  console.log(`[info] mapping: ${mappingPath}`);

  const { index, rows } = await buildHistoricIndex(attachments, mapping);
  console.log(`[info] attachments parsed: rows=${rows} unique_properties=${index.size}`);

  const input = fs.createReadStream(contractIn, { encoding: 'utf8' });
  const rl = readline.createInterface({ input, crlfDelay: Infinity });

  const outStream = fs.createWriteStream(out, { encoding: 'utf8' });

  let read = 0;
  let wrote = 0;
  let anyEnf = 0;
  let anyInv = 0;

  await new Promise((resolve, reject) => {
    rl.on('line', (line) => {
      const t = line.trim();
      if (!t) return;
      read++;

      let row;
      try {
        row = JSON.parse(t);
      } catch {
        return;
      }

      const pid = row.property_id;
      const agg = pid ? index.get(pid) : null;

      // Default nulls (UNKNOWN) unless we have evidence
      row.historic_district_name = null;
      row.historic_designation_type = null;
      row.review_required_flag = null;
      row.demolition_restricted_flag = null;
      row.exterior_change_review_flag = null;
      row.regulatory_body = null;

      row.historic_inventory_flag = false;
      row.historic_significance_level = null;
      row.inventory_source = null;

      if (agg) {
        const enfNames = Array.from(agg.enforceable.names);
        const hasEnf = enfNames.length > 0;
        if (hasEnf) {
          anyEnf++;
          row.historic_district_name = enfNames.length === 1 ? enfNames[0] : enfNames.join('; ');
          row.historic_designation_type = agg.enforceable.designation_type ?? null;
          row.review_required_flag = agg.enforceable.review_required_flag ?? null;
          row.demolition_restricted_flag = (agg.enforceable.demolition_restricted_flag ?? null);
          row.exterior_change_review_flag = agg.enforceable.exterior_change_review_flag ?? null;
          row.regulatory_body = agg.enforceable.regulatory_body ?? null;
        }

        if (agg.inventory.flag) {
          anyInv++;
          row.historic_inventory_flag = true;
          row.historic_significance_level = agg.inventory.significance_level ?? null;
          row.inventory_source = agg.inventory.source ?? null;
        }
      }

      outStream.write(JSON.stringify(row) + '\n');
      wrote++;

      if (read % 200000 === 0) {
        console.log(`[prog] read=${read} wrote=${wrote} anyEnforceable=${anyEnf} anyInventory=${anyInv}`);
      }
    });

    rl.on('close', () => {
      outStream.end();
      resolve();
    });

    rl.on('error', reject);
  });

  const durationS = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`[done] read=${read} wrote=${wrote} anyEnforceable=${anyEnf} anyInventory=${anyInv} sec=${durationS}`);

  if (metaOut) {
    const meta = {
      created_at: new Date().toISOString(),
      contract_in: contractIn,
      contract_in_sha256: sha256File(contractIn),
      attachments_in: attachments,
      attachments_in_sha256: sha256File(attachments),
      mapping: mappingPath,
      mapping_sha256: sha256File(mappingPath),
      out,
      out_sha256: sha256File(out),
      stats: {
        read_lines: read,
        wrote_lines: wrote,
        any_enforceable: anyEnf,
        any_inventory: anyInv
      }
    };
    ensureDir(path.dirname(metaOut));
    fs.writeFileSync(metaOut, JSON.stringify(meta, null, 2));
    console.log(`[ok] wrote meta: ${metaOut}`);
  }
}

main().catch((err) => {
  console.error('[fatal]', err);
  process.exit(1);
});
