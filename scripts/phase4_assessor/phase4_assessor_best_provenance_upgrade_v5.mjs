import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import crypto from 'node:crypto';

function readJSON(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

function writeJSON(p, obj) {
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), 'utf8');
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function sha256File(filePath) {
  const h = crypto.createHash('sha256');
  const fd = fs.openSync(filePath, 'r');
  try {
    const buf = Buffer.allocUnsafe(1024 * 1024);
    let bytes = 0;
    while ((bytes = fs.readSync(fd, buf, 0, buf.length, null)) > 0) {
      h.update(buf.subarray(0, bytes));
    }
  } finally {
    fs.closeSync(fd);
  }
  return h.digest('hex');
}

function isoNowForFilename() {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

function pickInputNdjson(root, cfg) {
  // 1) explicit path
  if (cfg.properties_in_ndjson) return path.resolve(root, cfg.properties_in_ndjson);

  // 2) resolve via CURRENT pointer
  const ptrRel = cfg.properties_current_ptr;
  if (ptrRel) {
    const ptrPath = path.resolve(root, ptrRel);
    if (!fs.existsSync(ptrPath)) {
      throw new Error(`[err] properties_current_ptr not found: ${ptrPath}`);
    }
    const ptr = readJSON(ptrPath);
    const nd = ptr.properties_ndjson || ptr.propertiesOut || ptr.properties_in_ndjson;
    if (!nd) {
      throw new Error(`[err] CURRENT pointer missing properties_ndjson: ${ptrPath}`);
    }
    return path.resolve(root, nd);
  }

  throw new Error('[err] No input specified. Set properties_in_ndjson or properties_current_ptr in config.');
}

function resolveOutputs(root, cfg, inputPathAbs) {
  const outDirRel = cfg.outputs?.out_dir || cfg.outputs?.outDir || 'publicData/properties/_attached/phase4_assessor_global_best_provenance_taxfy';
  const auditDirRel = cfg.outputs?.audit_dir || cfg.outputs?.auditDir || 'publicData/_audit/phase4_assessor';
  const currentPtrRel = cfg.outputs?.current_ptr || cfg.outputs?.currentPtr || 'publicData/properties/_attached/phase4_assessor_global_best_provenance_taxfy/CURRENT_PROPERTIES_WITH_ASSESSOR_BEST_PROVENANCE_TAXFY.json';

  const outDir = path.resolve(root, outDirRel);
  const auditDir = path.resolve(root, auditDirRel);
  ensureDir(outDir);
  ensureDir(auditDir);

  const stamp = isoNowForFilename();
  const outFile = path.resolve(outDir, `properties__with_assessor_best_provenance_taxfy__${stamp}__V5.ndjson`);
  const auditPath = path.resolve(auditDir, `phase4_assessor_best_provenance_taxfy_upgrade__${stamp}__V5.json`);
  const currentPtr = path.resolve(root, currentPtrRel);

  return { outDir, auditDir, outFile, auditPath, currentPtr, stamp, inputPathAbs };
}

function normalizeFiscalYear(value, asOfIso) {
  // Some cities publish next fiscal year values before calendar year flips.
  // We do not "fix" it; we label it and optionally compute a calendar_year_hint.
  if (value == null) return { tax_fy: null, calendar_year_hint: null, flags: [] };
  const fy = Number(value);
  if (!Number.isFinite(fy)) return { tax_fy: null, calendar_year_hint: null, flags: ['INVALID_FY'] };

  const asOfYear = asOfIso ? new Date(asOfIso).getUTCFullYear() : null;
  const flags = [];
  if (asOfYear != null) {
    if (fy === asOfYear + 1) flags.push('FISCAL_YEAR_NEXT');
    if (fy === asOfYear) flags.push('FISCAL_YEAR_CURRENT');
    if (fy < asOfYear - 1 || fy > asOfYear + 2) flags.push('FISCAL_YEAR_OUTLIER');
  }
  return { tax_fy: fy, calendar_year_hint: asOfYear, flags };
}

function upgradeAssessorBest(obj) {
  if (!obj || typeof obj !== 'object') return obj;
  if (!obj.assessor_best || typeof obj.assessor_best !== 'object') return obj;

  const best = obj.assessor_best;
  const valuation = best.valuation || {};
  const assessmentYear = valuation.assessment_year;

  // Only upgrade when we have the new {value, source, as_of, dataset_hash, confidence, flags[]} objects.
  if (!assessmentYear || typeof assessmentYear !== 'object' || assessmentYear.value == null) {
    return obj;
  }

  const asOf = assessmentYear.as_of || null;
  const { tax_fy, calendar_year_hint, flags } = normalizeFiscalYear(assessmentYear.value, asOf);

  // Add tax_fy alongside (do not delete assessment_year to remain backward compatible).
  valuation.tax_fy = {
    value: tax_fy,
    source: assessmentYear.source || null,
    as_of: asOf,
    dataset_hash: assessmentYear.dataset_hash || null,
    confidence: assessmentYear.confidence || null,
    flags: Array.isArray(assessmentYear.flags) ? [...new Set([...assessmentYear.flags, ...flags])] : flags
  };

  // Keep the original, but mark deprecated semantics.
  valuation.assessment_year = {
    ...assessmentYear,
    flags: Array.isArray(assessmentYear.flags)
      ? [...new Set([...assessmentYear.flags, 'DEPRECATED_SEMANTICS_USE_tax_fy'])]
      : ['DEPRECATED_SEMANTICS_USE_tax_fy']
  };

  // Add meta block for UI clarity
  best.meta = {
    ...(best.meta || {}),
    fiscal_year_note: 'Assessor values are typically published by Fiscal Year (FY). Use assessor_best.valuation.tax_fy for UI/analytics. assessment_year is retained for backward compatibility only.',
    calendar_year_hint,
    upgraded_at: new Date().toISOString()
  };

  best.valuation = valuation;
  obj.assessor_best = best;
  return obj;
}

async function main() {
  const args = process.argv.slice(2);
  const cfgIdx = args.indexOf('--config');
  if (cfgIdx === -1 || !args[cfgIdx + 1]) {
    console.log('usage: node scripts/phase4_assessor/phase4_assessor_best_provenance_upgrade_v5.mjs --config <config.json>');
    process.exit(2);
  }

  const configPath = args[cfgIdx + 1];
  const cfg = readJSON(configPath);
  const root = cfg.root || process.cwd();

  const inputAbs = pickInputNdjson(root, cfg);
  if (!fs.existsSync(inputAbs)) throw new Error(`[err] input not found: ${inputAbs}`);

  const out = resolveOutputs(root, cfg, inputAbs);

  console.log('[start] Phase4 assessor_best tax_fy provenance upgrade (v5)');
  console.log('[info] root:', root);
  console.log('[info] config:', configPath);
  console.log('[info] input:', inputAbs);
  console.log('[info] output:', out.outFile);

  const rl = readline.createInterface({
    input: fs.createReadStream(inputAbs, { encoding: 'utf8' }),
    crlfDelay: Infinity
  });

  const ws = fs.createWriteStream(out.outFile, { encoding: 'utf8' });

  let n = 0;
  let upgraded = 0;
  for await (const line of rl) {
    if (!line) continue;
    n++;
    let obj;
    try {
      obj = JSON.parse(line);
    } catch {
      // Pass through invalid lines (shouldn't happen), but keep pipeline running.
      ws.write(line + '\n');
      continue;
    }

    const before = obj.assessor_best?.valuation?.tax_fy?.value;
    const afterObj = upgradeAssessorBest(obj);
    const after = afterObj.assessor_best?.valuation?.tax_fy?.value;
    if (before == null && after != null) upgraded++;

    ws.write(JSON.stringify(afterObj) + '\n');
    if (n % 500000 === 0) console.log('[progress] processed', n, 'upgradedRecords', upgraded);
  }

  ws.end();
  await new Promise((res) => ws.on('finish', res));

  const sha = sha256File(out.outFile);
  const audit = {
    created_at: new Date().toISOString(),
    version: 'v5',
    config: path.resolve(configPath),
    inputs: { input: inputAbs },
    outputs: {
      output: out.outFile,
      currentPtr: out.currentPtr,
      auditPath: out.auditPath
    },
    stats: { processed: n, upgradedRecords: upgraded },
    hashes: { output_sha256: sha },
    notes: [
      'v5: adds assessor_best.valuation.tax_fy with full provenance, preserves assessment_year (deprecated semantics).',
      'v5: resolves input via properties_current_ptr when properties_in_ndjson is null.'
    ]
  };

  writeJSON(out.auditPath, audit);

  // CURRENT pointer
  ensureDir(path.dirname(out.currentPtr));
  writeJSON(out.currentPtr, {
    updated_at: new Date().toISOString(),
    note: 'AUTO: Phase4 assessor_best tax_fy provenance upgrade v5',
    properties_ndjson: out.outFile,
    audit: out.auditPath
  });

  console.log('[ok] wrote audit:', out.auditPath);
  console.log('[ok] wrote CURRENT pointer:', out.currentPtr);
  console.log('[ok] output sha256:', sha);
  console.log('[done] Phase4 assessor_best tax_fy provenance upgrade v5 complete.');
}

main().catch((e) => {
  console.error('[fatal]', e);
  process.exit(1);
});
