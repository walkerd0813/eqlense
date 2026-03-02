import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

function argMap(argv) {
  const m = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = (i + 1 < argv.length && !argv[i + 1].startsWith("--")) ? argv[++i] : true;
    m[k] = v;
  }
  return m;
}

function normTown(s) {
  return String(s || "").trim().toLowerCase();
}

function normTownFolderKey(s) {
  return normTown(s).replace(/\s+/g, "_");
}

function pickTown(row) {
  return row.town ?? row.TOWN ?? row.municipality ?? row.city ?? row.jurisdiction_name ?? row.jurisdictionName ?? "";
}

function hasBaseZoning(row) {
  if (!row || typeof row !== "object") return false;

  // common direct keys
  const directKeys = [
    "base_district_code","base_district_name","base_zone_code","zoning_base_code",
    "base_zone_confidence","base_zone_attach_method"
  ];
  for (const k of directKeys) if (row[k]) return true;

  // common nested containers
  const containers = [row.zoning, row.zoning_attach, row.zoningAttach, row.zoning_base, row.base_zoning];
  for (const c of containers) {
    if (c && typeof c === "object") {
      for (const k of directKeys) if (c[k]) return true;
      if (c.base && typeof c.base === "object") {
        if (c.base.code || c.base.district_code || c.base.districtCode) return true;
      }
      if (c.code || c.district_code || c.districtCode) return true;
    }
  }

  // fallback: any key name hint
  for (const k of Object.keys(row)) {
    const lk = k.toLowerCase();
    if ((lk.includes("base_district") || lk.includes("base_zone") || lk.includes("zoning_base")) && row[k]) return true;
  }

  return false;
}

function listTownsWithZoningBase(zoningRoot) {
  const out = new Set();
  if (!fs.existsSync(zoningRoot)) return out;

  for (const ent of fs.readdirSync(zoningRoot, { withFileTypes: true })) {
    if (!ent.isDirectory()) continue;
    const townDir = path.join(zoningRoot, ent.name);
    const p = path.join(townDir, "districts", "zoning_base.geojson");
    if (fs.existsSync(p)) out.add(ent.name.toLowerCase());
  }
  return out;
}

async function main() {
  const args = argMap(process.argv);

  const inPath = args.in || args.input;
  const zoningRoot = args.zoningRoot || ".\\publicData\\zoning";
  const outPath = args.out || args.output || null;
  const progressEvery = Number(args.progressEvery || 250000);

  if (!inPath) {
    console.error("Missing --in <ndjson>");
    process.exit(2);
  }
  if (!fs.existsSync(inPath)) {
    console.error("Input not found:", inPath);
    process.exit(2);
  }

  const townsWithZoning = listTownsWithZoningBase(zoningRoot);

  console.log("=====================================================");
  console.log("[coverage] START", new Date().toISOString());
  console.log("[coverage] in:", inPath);
  console.log("[coverage] zoningRoot:", zoningRoot);
  console.log("[coverage] townsWithZoningBase:", townsWithZoning.size);
  console.log("[coverage] progressEvery:", progressEvery);
  console.log("=====================================================");

  const totals = new Map();    // town -> total
  const matched = new Map();   // town -> matched

  let rows = 0, matchedRows = 0, parseErr = 0;

  const rl = readline.createInterface({
    input: fs.createReadStream(inPath, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    if (!line) continue;
    rows++;
    let row;
    try { row = JSON.parse(line); } catch { parseErr++; continue; }

    const townRaw = pickTown(row);
    const townKey = normTown(townRaw) || "(missing)";
    totals.set(townKey, (totals.get(townKey) || 0) + 1);

    const ok = hasBaseZoning(row);
    if (ok) {
      matchedRows++;
      matched.set(townKey, (matched.get(townKey) || 0) + 1);
    }

    if (progressEvery > 0 && (rows % progressEvery) === 0) {
      const rate = rows ? (matchedRows / rows) : 0;
      console.log(`[coverage] progress rows=${rows.toLocaleString()} matched=${matchedRows.toLocaleString()} rate=${(rate*100).toFixed(2)}% parseErr=${parseErr}`);
    }
  }

  const townStats = [];
  for (const [town, total] of totals.entries()) {
    const m = matched.get(town) || 0;
    const miss = total - m;

    // check if we *have* a zoning_base.geojson folder for this town (try both naming styles)
    const t1 = town;
    const t2 = normTownFolderKey(town);
    const hasFile =
      townsWithZoning.has(t1) ||
      townsWithZoning.has(t2) ||
      townsWithZoning.has(t1.replace(/\s+/g, "_")) ||
      townsWithZoning.has(t1.replace(/_/g, " "));

    townStats.push({
      town,
      total,
      matched: m,
      missing: miss,
      match_rate: total ? (m / total) : 0,
      has_zoning_base_file: !!hasFile
    });
  }

  townStats.sort((a,b) => (b.missing - a.missing) || (b.total - a.total));

  const overall = {
    ran_at: new Date().toISOString(),
    in: path.resolve(inPath),
    zoningRoot: path.resolve(zoningRoot),
    rows,
    matched: matchedRows,
    match_rate: rows ? (matchedRows / rows) : 0,
    parse_errors: parseErr,
    towns_seen: totals.size,
    towns_with_zoning_base_geojson: townsWithZoning.size,
    top_missing_towns: townStats.slice(0, 25),
    top_matched_towns: [...townStats].sort((a,b)=> (b.matched-a.matched)).slice(0,25)
  };

  const defaultOut = path.resolve(".\\publicData\\_audit", `baseZoning_coverage__${new Date().toISOString().replace(/[:.]/g,"")}.json`);
  const finalOut = outPath ? path.resolve(outPath) : defaultOut;

  fs.mkdirSync(path.dirname(finalOut), { recursive: true });
  fs.writeFileSync(finalOut, JSON.stringify(overall, null, 2), "utf8");

  console.log("=====================================================");
  console.log("[coverage] DONE", new Date().toISOString());
  console.log("[coverage] wrote:", finalOut);
  console.log("=====================================================");
  console.log("[coverage] Top missing towns (first 10):");
  for (const r of overall.top_missing_towns.slice(0,10)) {
    console.log(`- ${r.town}: missing=${r.missing.toLocaleString()} total=${r.total.toLocaleString()} matchRate=${(r.match_rate*100).toFixed(2)}% hasFile=${r.has_zoning_base_file}`);
  }
}

main().catch(e => {
  console.error("[FATAL]", e);
  process.exit(1);
});
