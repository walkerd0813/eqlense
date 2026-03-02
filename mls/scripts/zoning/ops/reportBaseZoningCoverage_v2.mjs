// mls/scripts/zoning/ops/reportBaseZoningCoverage_v2.mjs
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import crypto from "node:crypto";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
    out[k] = v;
  }
  return out;
}

function normTown(t) {
  return String(t || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_");
}

function listTownsWithZoningBase(zoningRoot) {
  const towns = new Set();
  if (!fs.existsSync(zoningRoot)) return towns;

  for (const ent of fs.readdirSync(zoningRoot, { withFileTypes: true })) {
    if (!ent.isDirectory()) continue;
    const town = ent.name;
    const p = path.join(zoningRoot, town, "districts", "zoning_base.geojson");
    if (fs.existsSync(p)) towns.add(normTown(town));
  }
  return towns;
}

function extractBaseCode(row) {
  // Try the most likely placements (support both flat and nested schemas)
  const candidates = [
    row?.base_district_code,
    row?.baseDistrictCode,
    row?.zoning?.base_district_code,
    row?.zoning?.baseDistrictCode,
    row?.zoning_attach?.base_district_code,
    row?.zoningAttach?.base_district_code,
  ];

  for (const c of candidates) {
    if (c === null || c === undefined) continue;
    const s = String(c).trim();
    if (s.length) return s;
  }
  return "";
}

function extractTown(row) {
  // same idea: support multiple schema locations
  return (
    row?.town ||
    row?.city ||
    row?.jurisdiction_name ||
    row?.jurisdiction?.name ||
    row?.address?.town ||
    row?.address?.city ||
    ""
  );
}

function isMeaningfulCode(code) {
  const c = String(code || "").trim().toLowerCase();
  if (!c) return false;

  // treat these as "not a real zoning match"
  const bad = new Set([
    "unknown",
    "unassigned",
    "none",
    "null",
    "n/a",
    "na",
    "missing",
    "missing_town_zoning_file",
    "no_zoning_file",
  ]);
  if (bad.has(c)) return false;

  // if your pipeline uses prefixed placeholders, catch them too
  if (c.startsWith("missing_")) return false;
  if (c.startsWith("no_")) return false;

  return true;
}

function sha1File(p) {
  const h = crypto.createHash("sha1");
  const fd = fs.openSync(p, "r");
  const buf = Buffer.allocUnsafe(1024 * 1024);
  try {
    let bytes;
    while ((bytes = fs.readSync(fd, buf, 0, buf.length, null)) > 0) {
      h.update(buf.subarray(0, bytes));
    }
  } finally {
    fs.closeSync(fd);
  }
  return h.digest("hex");
}

async function main() {
  const args = parseArgs(process.argv);
  const inPath = args.in;
  const zoningRoot = args.zoningRoot || "./publicData/zoning";
  const progressEvery = Number(args.progressEvery || 250000);

  if (!inPath) {
    console.error("Missing --in <ndjson>");
    process.exit(1);
  }
  if (!fs.existsSync(inPath)) {
    console.error(`Missing input: ${inPath}`);
    process.exit(1);
  }

  const townsWithZoningBase = listTownsWithZoningBase(zoningRoot);

  console.log("====================================================");
  console.log(`[coverage] START ${new Date().toISOString()}`);
  console.log(`[coverage] in: ${inPath}`);
  console.log(`[coverage] zoningRoot: ${zoningRoot}`);
  console.log(`[coverage] townsWithZoningBase: ${townsWithZoningBase.size}`);
  console.log(`[coverage] progressEvery: ${progressEvery}`);
  console.log("=====================================================");

  const rl = readline.createInterface({
    input: fs.createReadStream(inPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let rows = 0;
  let parseErr = 0;

  // GLOBAL counters
  let matched = 0;          // meaningful base district code
  let unknownLike = 0;      // code present but looks like placeholder
  let noCode = 0;           // empty/null
  const byTown = new Map(); // town -> { total, matched, noCode, unknownLike, hasFile }

  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    rows++;

    let row;
    try {
      row = JSON.parse(line);
    } catch {
      parseErr++;
      continue;
    }

    const townRaw = extractTown(row);
    const town = normTown(townRaw || "UNKNOWN_TOWN");
    const code = extractBaseCode(row);

    const entry =
      byTown.get(town) ||
      {
        town,
        total: 0,
        matched: 0,
        noCode: 0,
        unknownLike: 0,
        hasFile: townsWithZoningBase.has(town),
      };

    entry.total++;

    if (!code) {
      noCode++;
      entry.noCode++;
    } else if (isMeaningfulCode(code)) {
      matched++;
      entry.matched++;
    } else {
      unknownLike++;
      entry.unknownLike++;
    }

    byTown.set(town, entry);

    if (rows % progressEvery === 0) {
      const rate = rows ? (matched / rows) * 100 : 0;
      console.log(
        `[coverage] progress rows=${rows.toLocaleString()} matched=${matched.toLocaleString()} rate=${rate.toFixed(
          2
        )}% parseErr=${parseErr}`
      );
    }
  }

  const outDir = path.resolve("./publicData/_audit");
  fs.mkdirSync(outDir, { recursive: true });

  const outPath = path.join(
    outDir,
    `baseZoning_coverage_v2__${new Date().toISOString().replace(/[:.]/g, "")}.json`
  );

  const byTownArr = Array.from(byTown.values()).sort((a, b) => b.total - a.total);

  const payload = {
    ran_at: new Date().toISOString(),
    in: path.resolve(inPath),
    in_sha1: sha1File(inPath),
    zoningRoot: path.resolve(zoningRoot),
    townsWithZoningBase: townsWithZoningBase.size,
    rows,
    matched,
    match_rate: rows ? matched / rows : 0,
    noCode,
    unknownLike,
    parseErr,
    byTownTop50: byTownArr.slice(0, 50),
  };

  fs.writeFileSync(outPath, JSON.stringify(payload, null, 2), "utf8");

  console.log("=====================================================");
  console.log(`[coverage] DONE ${new Date().toISOString()}`);
  console.log(`[coverage] wrote: ${outPath}`);
  console.log("=====================================================");
  console.log(`[coverage] match_rate=${(payload.match_rate * 100).toFixed(2)}%  matched=${matched.toLocaleString()} / rows=${rows.toLocaleString()}`);
  console.log("[coverage] Top towns by total (first 10):");
  for (const t of byTownArr.slice(0, 10)) {
    const r = t.total ? (t.matched / t.total) * 100 : 0;
    console.log(`- ${t.town}: total=${t.total.toLocaleString()} matched=${t.matched.toLocaleString()} rate=${r.toFixed(2)}% hasFile=${t.hasFile}`);
  }

  console.log("[coverage] Top missing towns (first 10, hasFile=false):");
  const missing = byTownArr.filter((t) => !t.hasFile).slice(0, 10);
  for (const t of missing) {
    const r = t.total ? (t.matched / t.total) * 100 : 0;
    console.log(`- ${t.town}: total=${t.total.toLocaleString()} matched=${t.matched.toLocaleString()} rate=${r.toFixed(2)}% hasFile=${t.hasFile}`);
  }
}

main().catch((e) => {
  console.error("[FATAL]", e);
  process.exit(1);
});
