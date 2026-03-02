import fs from "node:fs";
import path from "node:path";

function arg(name) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : null;
}

const inFile = arg("in");
const outFile = arg("out");
const city = arg("city") || "";
const kind = arg("kind") || "";
const sourceUrl = arg("sourceUrl") || "";

if (!inFile || !outFile) {
  console.error("usage: node geojsonFieldScan_v1.mjs --in <file> --out <file> [--city x] [--kind y] [--sourceUrl url]");
  process.exit(1);
}

const st = fs.statSync(inFile);
const report = {
  ok: true,
  created_at: new Date().toISOString(),
  in: path.resolve(inFile),
  out: path.resolve(outFile),
  file_bytes: st.size,
  city,
  kind,
  sourceUrl,
  scan: { status: "ok" }
};

const MAX_PARSE_BYTES = 150 * 1024 * 1024; // 150MB safety
if (st.size > MAX_PARSE_BYTES) {
  report.scan.status = "skipped_large_file";
  report.scan.note = `Skipped deep scan because file > ${MAX_PARSE_BYTES} bytes`;
  fs.writeFileSync(outFile, JSON.stringify(report, null, 2), "utf8");
  console.error(`[warn] field scan skipped (large file): ${inFile}`);
  process.exit(0);
}

let gj;
try {
  gj = JSON.parse(fs.readFileSync(inFile, "utf8"));
} catch (e) {
  report.ok = false;
  report.scan.status = "parse_failed";
  report.scan.error = String(e?.message || e);
  fs.writeFileSync(outFile, JSON.stringify(report, null, 2), "utf8");
  process.exit(2);
}

const feats = Array.isArray(gj?.features) ? gj.features : [];
report.feature_count = feats.length;

const keyStats = new Map(); // key -> { nonNull, examples:Set }
const geomTypes = new Map();

for (const f of feats) {
  const gt = f?.geometry?.type || "null";
  geomTypes.set(gt, (geomTypes.get(gt) || 0) + 1);

  const props = f?.properties && typeof f.properties === "object" ? f.properties : {};
  for (const [k, v] of Object.entries(props)) {
    if (!keyStats.has(k)) keyStats.set(k, { nonNull: 0, examples: new Set() });
    const s = keyStats.get(k);

    const isNullish = v === null || v === undefined || v === "";
    if (!isNullish) s.nonNull += 1;

    if (s.examples.size < 6 && !isNullish) {
      const sv = typeof v === "string" ? v : JSON.stringify(v);
      s.examples.add(sv.length > 120 ? sv.slice(0, 120) + "…" : sv);
    }
  }
}

report.geometry_types = Object.fromEntries([...geomTypes.entries()].sort((a,b)=>b[1]-a[1]));

const keys = [...keyStats.entries()].map(([k, s]) => ({
  key: k,
  nonNull: s.nonNull,
  examples: [...s.examples.values()]
})).sort((a,b)=>b.nonNull - a.nonNull);

report.fields = {
  total_keys: keys.length,
  top: keys.slice(0, 80)
};

fs.mkdirSync(path.dirname(outFile), { recursive: true });
fs.writeFileSync(outFile, JSON.stringify(report, null, 2), "utf8");
console.error(`[ok] field scan -> ${outFile}`);
