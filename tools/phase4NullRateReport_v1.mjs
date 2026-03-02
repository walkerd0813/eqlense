// tools/phase4NullRateReport_v1.mjs
// Usage: node tools/phase4NullRateReport_v1.mjs <in.ndjson> <out.json>
// Streams NDJSON and computes presence/null rates for key normalized fields.

import fs from "node:fs";
import readline from "node:readline";

const infile = process.argv[2];
const outfile = process.argv[3];

if (!infile || !outfile) {
  console.log("usage: node tools/phase4NullRateReport_v1.mjs <in.ndjson> <out.json>");
  process.exit(2);
}

const rl = readline.createInterface({
  input: fs.createReadStream(infile, { encoding: "utf8" }),
  crlfDelay: Infinity
});

let n = 0;

const c = {
  records: 0,
  has_assessor_best: 0,
  has_tax_fy: 0,
  has_city_raw: 0,
  has_massgis_raw: 0
};

const fields = [
  "assessor_best.valuation.total_value.value",
  "assessor_best.valuation.land_value.value",
  "assessor_best.valuation.building_value.value",
  "assessor_best.valuation.other_value.value",
  "assessor_best.valuation.tax_fy.value",
  "assessor_best.transaction.last_sale_date.value",
  "assessor_best.transaction.last_sale_price.value",
  "assessor_best.structure.year_built.value",
  "assessor_best.structure.building_area_sqft.value",
  "assessor_best.site.lot_size.value"
];

const stat = {};
for (const f of fields) stat[f] = { present: 0, nullish: 0 };

function getPath(o, path) {
  const parts = path.split(".");
  let cur = o;
  for (const p of parts) {
    if (cur == null) return undefined;
    cur = cur[p];
  }
  return cur;
}

rl.on("line", (line) => {
  if (!line) return;
  n++;
  let o;
  try { o = JSON.parse(line); } catch { return; }

  c.records++;

  if (o.assessor_best) c.has_assessor_best++;

  if (getPath(o, "assessor_best.valuation.tax_fy.value") !== undefined) c.has_tax_fy++;

  if (getPath(o, "assessor_by_source.city_assessor_raw") != null) c.has_city_raw++;
  if (getPath(o, "assessor_by_source.massgis_statewide_raw") != null) c.has_massgis_raw++;

  for (const f of fields) {
    const v = getPath(o, f);
    if (v === undefined) continue;
    stat[f].present++;
    if (v === null || v === "" || (typeof v === "number" && Number.isNaN(v))) stat[f].nullish++;
  }

  if (n % 500000 === 0) console.log("[progress] scanned", n);
});

rl.on("close", () => {
  const pct = (x) => (c.records ? (100 * x / c.records) : 0);

  const out = {
    created_at: new Date().toISOString(),
    input: infile,
    totals: {
      records: c.records,
      has_assessor_best: { count: c.has_assessor_best, pct: pct(c.has_assessor_best) },
      has_tax_fy: { count: c.has_tax_fy, pct: pct(c.has_tax_fy) },
      has_city_assessor_raw: { count: c.has_city_raw, pct: pct(c.has_city_raw) },
      has_massgis_statewide_raw: { count: c.has_massgis_raw, pct: pct(c.has_massgis_raw) }
    },
    fields: {}
  };

  for (const [k, v] of Object.entries(stat)) {
    out.fields[k] = {
      present_count: v.present,
      present_pct_of_records: pct(v.present),
      nullish_count_within_present: v.nullish,
      nullish_pct_within_present: v.present ? (100 * v.nullish / v.present) : 0
    };
  }

  fs.writeFileSync(outfile, JSON.stringify(out, null, 2), "utf8");
  console.log("[done] wrote", outfile);
});
