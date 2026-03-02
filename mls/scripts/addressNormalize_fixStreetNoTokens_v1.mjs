#!/usr/bin/env node
/**
 * addressNormalize_fixStreetNoTokens_v1.mjs
 *
 * Purpose
 *   Normalize "street_no" tokens so your "strict mail-like" validators can agree.
 *   SAFE pass: it does NOT invent a new house number. It only extracts a leading
 *   numeric base number (when present) and moves suffix/range/decimal/alpha parts
 *   into helper fields and/or unit.
 *
 * Typical fixes
 *   - "6 8"        -> street_no="6"   street_no_range_end="8"
 *   - "6-8"        -> street_no="6"   street_no_range_end="8"
 *   - "200.1"      -> street_no="200" street_no_sub="1"
 *   - "12A"        -> street_no="12"  street_no_suffix_alpha="A"
 *   - "12 1/2"     -> street_no="12"  street_no_fraction="1/2"
 *
 * Output
 *   NDJSON with added fields + audit trail under address_norm.street_no_fix
 *
 * Usage (PowerShell)
 *   node .\mls\scripts\addressNormalize_fixStreetNoTokens_v1.mjs `
 *     --in  "C:\seller-app\backend\publicData\properties\v38_addressTierBadged.ndjson" `
 *     --out "C:\seller-app\backend\publicData\properties\v39_streetNoNormalized.ndjson" `
 *     --report "C:\seller-app\backend\publicData\properties\v39_streetNoNormalized_report.json"
 */
import fs from "fs";
import path from "path";
import readline from "readline";

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
      args[k] = v;
    }
  }
  return args;
}

function safeAppendUnit(existingUnit, extra) {
  const u = (existingUnit ?? "").toString().trim();
  const e = (extra ?? "").toString().trim();
  if (!e) return u || null;
  if (!u) return e;
  if (u.toUpperCase().includes(e.toUpperCase())) return u;
  return `${u}; ${e}`;
}

function normalizeStreetNo(raw) {
  if (raw == null) return { changed: false };
  const s0 = String(raw).trim();
  if (!s0) return { changed: false };
  const s = s0.toUpperCase();

  // already clean numeric
  if (/^\d+$/.test(s)) return { changed: false };

  // 12A
  let m = s.match(/^(\d+)([A-Z])$/);
  if (m) {
    return { changed: true, street_no: m[1], street_no_suffix_alpha: m[2], method: "NUM_ALPHA_SUFFIX", evidence: s0 };
  }

  // 200.1
  m = s.match(/^(\d+)\.(\d+)$/);
  if (m) {
    return { changed: true, street_no: m[1], street_no_sub: m[2], method: "DECIMAL_SUB", evidence: s0 };
  }

  // 6 8
  m = s.match(/^(\d+)\s+(\d+)$/);
  if (m) {
    return { changed: true, street_no: m[1], street_no_range_end: m[2], method: "SPACE_RANGE", evidence: s0 };
  }

  // 6-8
  m = s.match(/^(\d+)\s*-\s*(\d+)$/);
  if (m) {
    return { changed: true, street_no: m[1], street_no_range_end: m[2], method: "DASH_RANGE", evidence: s0 };
  }

  // 12/14
  m = s.match(/^(\d+)\s*\/\s*(\d+)$/);
  if (m) {
    return { changed: true, street_no: m[1], street_no_range_end: m[2], method: "SLASH_RANGE", evidence: s0 };
  }

  // 12 1/2
  m = s.match(/^(\d+)\s+(1\/2|1\/4|3\/4)$/);
  if (m) {
    return { changed: true, street_no: m[1], street_no_fraction: m[2], method: "FRACTION_SUFFIX", evidence: s0 };
  }

  // 12 & 14
  m = s.match(/^(\d+)\s*&\s*(\d+)$/);
  if (m) {
    return { changed: true, street_no: m[1], street_no_range_end: m[2], method: "AMP_RANGE", evidence: s0 };
  }

  // leading digits + tail
  m = s.match(/^(\d+)\s*(.*)$/);
  if (m && m[1] && m[2] && m[2].trim()) {
    const tail = m[2].trim();
    return { changed: true, street_no: m[1], street_no_tail: tail, method: "LEADING_NUM_TAIL", evidence: s0 };
  }

  return { changed: false };
}

async function main() {
  const args = parseArgs(process.argv);
  const inPath = args.in;
  const outPath = args.out;
  const reportPath = args.report;

  if (!inPath || !outPath || !reportPath) {
    console.error("Usage: --in <in.ndjson> --out <out.ndjson> --report <report.json>");
    process.exit(1);
  }
  if (!fs.existsSync(inPath)) {
    console.error("Input not found:", inPath);
    process.exit(1);
  }

  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.mkdirSync(path.dirname(reportPath), { recursive: true });

  const rl = readline.createInterface({
    input: fs.createReadStream(inPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });
  const out = fs.createWriteStream(outPath, { encoding: "utf8" });

  const counts = {
    total_rows: 0,
    parseErr: 0,
    changed_rows: 0,
    methods: {},
    examples: {},
  };

  const start = Date.now();
  const logEvery = 500000;

  for await (const line of rl) {
    if (!line) continue;
    counts.total_rows++;

    let r;
    try {
      r = JSON.parse(line);
    } catch {
      counts.parseErr++;
      continue;
    }

    const fix = normalizeStreetNo(r.street_no);
    if (fix.changed) {
      counts.changed_rows++;
      counts.methods[fix.method] = (counts.methods[fix.method] ?? 0) + 1;
      if (!counts.examples[fix.method]) {
        counts.examples[fix.method] = {
          before: fix.evidence,
          after: {
            street_no: fix.street_no,
            street_no_range_end: fix.street_no_range_end ?? null,
            street_no_sub: fix.street_no_sub ?? null,
            street_no_suffix_alpha: fix.street_no_suffix_alpha ?? null,
            street_no_fraction: fix.street_no_fraction ?? null,
            street_no_tail: fix.street_no_tail ?? null,
          },
        };
      }

      r.street_no = fix.street_no;
      if (fix.street_no_range_end && r.street_no_range_end == null) r.street_no_range_end = fix.street_no_range_end;
      if (fix.street_no_sub && r.street_no_sub == null) r.street_no_sub = fix.street_no_sub;
      if (fix.street_no_suffix_alpha && r.street_no_suffix_alpha == null) r.street_no_suffix_alpha = fix.street_no_suffix_alpha;
      if (fix.street_no_fraction && r.street_no_fraction == null) r.street_no_fraction = fix.street_no_fraction;
      if (fix.street_no_tail && r.street_no_tail == null) r.street_no_tail = fix.street_no_tail;

      let unitExtra = null;
      if (fix.method === "DECIMAL_SUB") unitExtra = `.${fix.street_no_sub}`;
      else if (fix.method === "NUM_ALPHA_SUFFIX") unitExtra = fix.street_no_suffix_alpha;
      else if (fix.method === "FRACTION_SUFFIX") unitExtra = fix.street_no_fraction;
      else if (fix.method === "LEADING_NUM_TAIL") unitExtra = fix.street_no_tail;

      if (unitExtra) r.unit = safeAppendUnit(r.unit, unitExtra);

      r.address_norm = r.address_norm ?? {};
      r.address_norm.street_no_fix = { version: "v1", method: fix.method, before: fix.evidence, at: new Date().toISOString() };
    }

    out.write(JSON.stringify(r) + "\n");

    if (counts.total_rows % logEvery === 0) {
      const mins = ((Date.now() - start) / 60000).toFixed(1);
      console.log(`...processed ${counts.total_rows.toLocaleString()} rows (${mins} min)`);
    }
  }

  await new Promise((resolve) => out.end(resolve));

  const report = { created_at: new Date().toISOString(), in: inPath, out: outPath, counts };
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
  console.log("DONE.", report);
  process.exit(0);
}

main().catch((e) => {
  console.error("Fatal:", e);
  process.exit(1);
});
