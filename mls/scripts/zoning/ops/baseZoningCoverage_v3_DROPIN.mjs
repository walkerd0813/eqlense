import fs from "fs";
import path from "path";
import readline from "readline";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const next = argv[i + 1];
    const v = (next && !next.startsWith("--")) ? (i++, next) : true;
    out[k] = v;
  }
  return out;
}

function normTown(t) {
  return String(t || "").trim().toLowerCase().replace(/\s+/g, "_");
}

async function main() {
  const args = parseArgs(process.argv);
  const input = args.in;
  const zoningRoot = args.zoningRoot || ".\\publicData\\zoning";
  const progressEvery = Number(args.progressEvery || 250000);

  if (!input) throw new Error("Missing --in <ndjson>");
  const inAbs = path.resolve(input);
  const zoningAbs = path.resolve(zoningRoot);

  console.log("====================================================");
  console.log(`[coverage] START ${new Date().toISOString()}`);
  console.log(`[coverage] in: ${inAbs}`);
  console.log(`[coverage] zoningRoot: ${zoningAbs}`);
  console.log("====================================================");

  const townsWithFile = new Set();
  if (fs.existsSync(zoningAbs)) {
    const dirs = fs.readdirSync(zoningAbs, { withFileTypes: true }).filter(d => d.isDirectory());
    for (const d of dirs) {
      const p = path.join(zoningAbs, d.name, "districts", "zoning_base.geojson");
      if (fs.existsSync(p)) townsWithFile.add(normTown(d.name));
    }
  }

  const townStats = new Map(); // town -> {total, matched}
  let rows = 0;
  let parseErr = 0;

  const rl = readline.createInterface({
    input: fs.createReadStream(inAbs, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if (!line) continue;
    rows++;
    let obj;
    try {
      obj = JSON.parse(line);
    } catch {
      parseErr++;
      continue;
    }

    const town = normTown(obj?.town || obj?.jurisdiction_name || "");
    if (!town) continue;

    const s = townStats.get(town) || { total: 0, matched: 0 };
    s.total++;

    // "Real match" = we hit a polygon AND we got a usable district_code
    const conf = Number(obj?.base_zone_confidence || 0);
    const code = obj?.base_district_code ?? obj?.base_district_code_norm ?? null;

    if (conf >= 1 && code) s.matched++;

    townStats.set(town, s);

    if (rows % progressEvery === 0) {
      const matchedSoFar = Array.from(townStats.values()).reduce((a, b) => a + b.matched, 0);
      console.log(`[coverage] progress rows=${rows.toLocaleString()} matched=${matchedSoFar.toLocaleString()} rate=${(matchedSoFar / rows * 100).toFixed(2)}% parseErr=${parseErr}`);
    }
  }

  // Summaries
  const totals = { rows, parseErr, matched: 0 };
  for (const v of townStats.values()) totals.matched += v.matched;

  console.log("====================================================");
  console.log(`[coverage] DONE ${new Date().toISOString()}`);
  console.log(`[coverage] match_rate=${(totals.matched / rows * 100).toFixed(2)}% matched=${totals.matched.toLocaleString()} / rows=${rows.toLocaleString()}`);
  console.log("====================================================");

  // Print top towns by total
  const top = Array.from(townStats.entries())
    .sort((a, b) => b[1].total - a[1].total)
    .slice(0, 15)
    .map(([town, s]) => ({
      town,
      total: s.total,
      matched: s.matched,
      rate: s.total ? (s.matched / s.total) : 0,
      hasFile: townsWithFile.has(town),
    }));

  console.log("[coverage] Top towns by total (first 15):");
  for (const r of top) {
    console.log(`- ${r.town}: total=${r.total.toLocaleString()} matched=${r.matched.toLocaleString()} rate=${(r.rate * 100).toFixed(2)}% hasFile=${r.hasFile}`);
  }
}

main().catch((e) => {
  console.error("[FATAL]", e);
  process.exit(1);
});
