import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = (argv[i + 1] && !argv[i + 1].startsWith("--")) ? argv[++i] : true;
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

function pct(n, d) {
  if (!d) return 0;
  return Math.round((n / d) * 10000) / 100;
}

function csvEscape(x) {
  const s = String(x ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

async function main() {
  const args = parseArgs(process.argv);

  const inFile = args.in;
  const outDir = args.outDir || ".";
  const logEvery = Number(args.logEvery || 250000);
  const heartbeatSec = Number(args.heartbeatSec || 15);

  const townField = args.townField || "town";
  const confidenceField = args.confidenceField || "base_zone_confidence";
  const methodField = args.methodField || "base_zone_attach_method";
  const codeField = args.codeField || "base_district_code";

  if (!inFile) {
    console.error("Usage: node summarize_base_zoning_coverage_v1.mjs --in <ndjson> --outDir <dir> [--logEvery N] [--heartbeatSec N]");
    process.exit(1);
  }

  fs.mkdirSync(outDir, { recursive: true });

  console.log("=====================================================");
  console.log("[START] Base zoning coverage (schema-aware) v1");
  console.log("[INFO ] in      :", inFile);
  console.log("[INFO ] outDir  :", outDir);
  console.log("[INFO ] fields  :", { townField, confidenceField, methodField, codeField });
  console.log("[INFO ] logEvery:", logEvery);
  console.log("[INFO ] heartbeat:", `${heartbeatSec}s`);
  console.log("=====================================================");

  const towns = new Map();
  let lines = 0;
  let parseErr = 0;

  let lastBeat = Date.now();
  const beatMs = heartbeatSec * 1000;

  const rl = readline.createInterface({
    input: fs.createReadStream(inFile, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    if (!line) continue;
    lines++;

    let row;
    try {
      row = JSON.parse(line);
    } catch {
      parseErr++;
      continue;
    }

    const townRaw = row[townField];
    const town = normTown(townRaw);
    if (!town) continue;

    let rec = towns.get(town);
    if (!rec) {
      rec = {
        town,
        seen: 0,
        baseHit: 0,
        codePresent: 0,
        methods: new Map(),
        codes: new Map()
      };
      towns.set(town, rec);
    }

    rec.seen++;

    const conf = Number(row[confidenceField] || 0);
    const method = String(row[methodField] || "");
    const hit = conf > 0 || method === "point_in_poly";

    if (hit) rec.baseHit++;

    if (method) {
      rec.methods.set(method, (rec.methods.get(method) || 0) + 1);
    }

    const code = row[codeField];
    if (code !== null && code !== undefined && String(code).trim() !== "") {
      rec.codePresent++;
      const c = String(code).trim();
      rec.codes.set(c, (rec.codes.get(c) || 0) + 1);
    }

    if (logEvery && (lines % logEvery === 0)) {
      console.log(`[PROG] ${new Date().toISOString()} lines=${lines.toLocaleString()} towns=${towns.size.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
    }

    const now = Date.now();
    if (now - lastBeat >= beatMs) {
      lastBeat = now;
      console.log(`[BEAT] ${new Date().toISOString()} lines=${lines.toLocaleString()} towns=${towns.size.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
    }
  }

  // Build CSV
  const rows = [];
  for (const rec of towns.values()) {
    // top 5 codes
    const topCodes = [...rec.codes.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([c, n]) => `${c}:${n}`)
      .join(" | ");

    const topMethods = [...rec.methods.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([m, n]) => `${m}:${n}`)
      .join(" | ");

    rows.push({
      town: rec.town,
      seen: rec.seen,
      baseHit: rec.baseHit,
      baseRatePct: pct(rec.baseHit, rec.seen),
      codePresent: rec.codePresent,
      codeRatePct: pct(rec.codePresent, rec.seen),
      topMethods,
      topBaseCodes: topCodes
    });
  }

  rows.sort((a, b) => b.seen - a.seen);

  const csvPath = path.join(outDir, "base_zoning_coverage_by_town.csv");
  const jsonPath = path.join(outDir, "base_zoning_summary.json");

  const header = ["town","seen","baseHit","baseRatePct","codePresent","codeRatePct","topMethods","topBaseCodes"];
  const csvLines = [header.join(",")];

  for (const r of rows) {
    csvLines.push([
      r.town,
      r.seen,
      r.baseHit,
      r.baseRatePct,
      r.codePresent,
      r.codeRatePct,
      csvEscape(r.topMethods),
      csvEscape(r.topBaseCodes)
    ].join(","));
  }

  fs.writeFileSync(csvPath, csvLines.join("\n"), "utf8");

  const summary = {
    ran_at: new Date().toISOString(),
    in: inFile,
    outDir,
    lines,
    parseErr,
    towns: towns.size,
    totals: {
      seen: rows.reduce((s, r) => s + r.seen, 0),
      baseHit: rows.reduce((s, r) => s + r.baseHit, 0),
      codePresent: rows.reduce((s, r) => s + r.codePresent, 0)
    },
    rows
  };

  fs.writeFileSync(jsonPath, JSON.stringify(summary, null, 2), "utf8");

  console.log("-----------------------------------------------------");
  console.log("[DONE] Base zoning coverage complete.");
  console.log("[DONE] lines  :", lines.toLocaleString());
  console.log("[DONE] towns  :", towns.size.toLocaleString());
  console.log("[DONE] wrote  :", csvPath);
  console.log("[DONE] wrote  :", jsonPath);
  console.log("=====================================================");
}

main().catch((e) => {
  console.error("[FATAL]", e);
  process.exit(1);
});
