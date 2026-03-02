import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

const OK = process.argv[2];
const BAD = process.argv[3];
const BOUNDARIES = process.argv[4] || "publicData/zoning/zoningBoundariesData.geojson";
const OUT_JSON = process.argv[5] || "mls/normalized/zoningQA_FINAL.json";
const OUT_CSV  = process.argv[6] || "mls/normalized/zoningQA_FINAL.csv";

if (!OK || !BAD) {
  console.log("Usage:");
  console.log("  node mls/scripts/zoningQA.js <OK.ndjson> <BAD.ndjson> [boundaries.geojson] [out.json] [out.csv]");
  process.exit(1);
}

const okAbs = path.resolve(OK);
const badAbs = path.resolve(BAD);
const bAbs = path.resolve(BOUNDARIES);
const outJsonAbs = path.resolve(OUT_JSON);
const outCsvAbs  = path.resolve(OUT_CSV);

for (const p of [okAbs, badAbs, bAbs]) {
  if (!fs.existsSync(p)) {
    console.error("❌ Missing:", p);
    process.exit(1);
  }
}

fs.mkdirSync(path.dirname(outJsonAbs), { recursive: true });
fs.mkdirSync(path.dirname(outCsvAbs),  { recursive: true });

function normTown(s) {
  if (!s) return "UNKNOWN";
  const base = String(s).split(",")[0].trim();
  return base
    .replace(/^TOWN OF\s+/i, "")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

function pickTownFromProps(props = {}) {
  const keys = [
    "TOWN", "TOWN_DESC", "MUNI", "MUNICIPALITY", "CITY", "TOWN_NAME", "COMMUNITY"
  ];
  for (const k of keys) {
    const v = props[k];
    if (v != null && String(v).trim() !== "") return normTown(v);
  }
  return "UNKNOWN";
}

function inc(map, k, field, n = 1) {
  let o = map.get(k);
  if (!o) { o = { city: k, total: 0, matched: 0, unmatched: 0 }; map.set(k, o); }
  o[field] += n;
  return o;
}

console.log("====================================================");
console.log(" ZONING Q&A — COVERAGE BY CITY");
console.log("====================================================");
console.log("OK file:       ", okAbs);
console.log("Unmatched file:", badAbs);
console.log("Boundaries:    ", bAbs);
console.log("Out JSON:      ", outJsonAbs);
console.log("Out CSV:       ", outCsvAbs);
console.log("----------------------------------------------------");

// Load boundaries towns + feature counts
const boundaries = JSON.parse(fs.readFileSync(bAbs, "utf8"));
const feats = Array.isArray(boundaries?.features) ? boundaries.features : [];

const zoningTowns = new Map(); // town -> {features}
for (const f of feats) {
  const t = pickTownFromProps(f?.properties || {});
  const cur = zoningTowns.get(t) || { town: t, features: 0 };
  cur.features++;
  zoningTowns.set(t, cur);
}

console.log(`[qa] zoning features=${feats.length.toLocaleString()} towns=${zoningTowns.size.toLocaleString()}`);

// Stream both NDJSONs
const byCity = new Map();
let total = 0, matched = 0, unmatched = 0, badJson = 0;

async function scanFile(file, isMatched) {
  const rl = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity });
  for await (const line of rl) {
    const l = line.trim();
    if (!l) continue;
    let o;
    try { o = JSON.parse(l); } catch { badJson++; continue; }

    const c = normTown(o?.address?.city);
    total++;

    inc(byCity, c, "total", 1);
    if (isMatched) { matched++; inc(byCity, c, "matched", 1); }
    else { unmatched++; inc(byCity, c, "unmatched", 1); }

    if (total % 100000 === 0) console.log(`[qa] scanned=${total.toLocaleString()} matched=${matched.toLocaleString()} unmatched=${unmatched.toLocaleString()}`);
  }
}

await scanFile(okAbs, true);
await scanFile(badAbs, false);

// Build report rows
const rows = [];
for (const v of byCity.values()) {
  const pct = v.total ? (v.matched / v.total) * 100 : 0;
  rows.push({ ...v, pctMatched: +pct.toFixed(2) });
}
rows.sort((a,b) => b.matched - a.matched);

// Build “do we have zoning but got 0 matched” checks
const zoningTownChecks = [];
for (const z of zoningTowns.values()) {
  const c = byCity.get(z.town);
  zoningTownChecks.push({
    town: z.town,
    zoningFeatures: z.features,
    listingsTotal: c?.total || 0,
    listingsMatched: c?.matched || 0,
    pctMatched: c?.total ? +(((c.matched / c.total) * 100).toFixed(2)) : 0
  });
}
zoningTownChecks.sort((a,b) => (b.listingsMatched - a.listingsMatched));

const townsWithZoningButZeroMatches = zoningTownChecks.filter(x => x.listingsTotal > 0 && x.listingsMatched === 0);

// Write JSON
const report = {
  generatedAt: new Date().toISOString(),
  inputs: { okAbs, badAbs, boundariesAbs: bAbs },
  totals: { total, matched, unmatched, badJson, pctMatched: +(total ? ((matched/total)*100).toFixed(2) : 0) },
  zoningBoundaries: {
    featureCount: feats.length,
    townCount: zoningTowns.size,
    towns: zoningTownChecks
  },
  coverageByCity: rows,
  flags: {
    townsWithZoningButZeroMatches
  }
};

fs.writeFileSync(outJsonAbs, JSON.stringify(report, null, 2), "utf8");

// Write CSV
const header = "city,total,matched,unmatched,pctMatched\n";
const csv = header + rows.map(r => `${r.city},${r.total},${r.matched},${r.unmatched},${r.pctMatched}`).join("\n");
fs.writeFileSync(outCsvAbs, csv, "utf8");

// Console summary
console.log("====================================================");
console.log("TOTAL:", total.toLocaleString(), "MATCHED:", matched.toLocaleString(), `(${report.totals.pctMatched}%)`, "UNMATCHED:", unmatched.toLocaleString(), "badJson:", badJson.toLocaleString());
console.log("----------------------------------------------------");
console.log("TOP 20 CITIES BY MATCHED:");
for (const r of rows.slice(0, 20)) {
  console.log(`  ${r.city.padEnd(20)} matched=${String(r.matched).padStart(7)} / total=${String(r.total).padStart(7)} (${String(r.pctMatched).padStart(6)}%)`);
}
console.log("----------------------------------------------------");
console.log("CITIES WE HAVE ZONING FOR (boundaries) BUT 0 MATCHES (and listings exist):", townsWithZoningButZeroMatches.length);
for (const x of townsWithZoningButZeroMatches.slice(0, 50)) {
  console.log(`  ${x.town.padEnd(20)} features=${String(x.zoningFeatures).padStart(5)} listings=${String(x.listingsTotal).padStart(7)} matched=${String(x.listingsMatched).padStart(6)}`);
}
console.log("----------------------------------------------------");
console.log("✅ WROTE:");
console.log(" ", outJsonAbs);
console.log(" ", outCsvAbs);
console.log("====================================================");
