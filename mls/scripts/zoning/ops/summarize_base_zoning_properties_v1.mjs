import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
    args[k] = v;
  }
  return args;
}

function normTown(t) {
  return String(t || "").trim().toLowerCase().replace(/\s+/g, "_");
}

function pct(n, d) {
  if (!d) return 0;
  return (n / d) * 100;
}

// Define what "has base zoning" means for THIS properties schema.
function hasBaseZoning(rec) {
  const conf = Number(rec?.base_zone_confidence ?? 0);
  const method = String(rec?.base_zone_attach_method ?? "");
  const code = rec?.base_district_code;
  // Positive if we have confidence OR a code OR the attach method indicates a hit.
  return conf > 0 || !!code || method === "point_in_poly";
}

function baseCode(rec) {
  const c = rec?.base_district_code;
  if (c === null || c === undefined) return null;
  const s = String(c).trim();
  return s.length ? s : null;
}

async function main() {
  const args = parseArgs(process.argv);

  const inPath = args.in;
  const outDir = args.outDir || "publicData/_audit";
  const logEvery = Number(args.logEvery || 250000);
  const heartbeatSec = Number(args.heartbeatSec || 15);

  if (!inPath) throw new Error("Usage: node summarize_base_zoning_properties_v1.mjs --in <ndjson> --outDir <dir> [--logEvery N] [--heartbeatSec N]");
  fs.mkdirSync(outDir, { recursive: true });

  console.log("=====================================================");
  console.log("[START] Summarize BASE zoning (properties schema) v1");
  console.log("[INFO ] in        :", inPath);
  console.log("[INFO ] outDir    :", outDir);
  console.log("[INFO ] logEvery  :", logEvery);
  console.log("[INFO ] heartbeat :", `${heartbeatSec}s`);
  console.log("=====================================================");

  const byTown = new Map(); // town -> { seen, baseHit, topCodes: Map(code->count) }
  let lines = 0;

  const rl = readline.createInterface({
    input: fs.createReadStream(inPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let lastBeat = Date.now();

  for await (const line of rl) {
    if (!line) continue;
    lines++;

    let rec;
    try {
      rec = JSON.parse(line);
    } catch {
      continue;
    }

    const town = normTown(rec.town);
    if (!byTown.has(town)) {
      byTown.set(town, { town, seen: 0, baseHit: 0, topCodes: new Map() });
    }
    const row = byTown.get(town);
    row.seen++;

    if (hasBaseZoning(rec)) {
      row.baseHit++;
      const c = baseCode(rec);
      if (c) row.topCodes.set(c, (row.topCodes.get(c) || 0) + 1);
    }

    if (logEvery && (lines % logEvery === 0)) {
      console.log(`[PROG] lines=${lines.toLocaleString()} towns=${byTown.size}`);
    }

    const now = Date.now();
    if (heartbeatSec > 0 && now - lastBeat >= heartbeatSec * 1000) {
      console.log(`[BEAT] ${new Date().toISOString()} lines=${lines.toLocaleString()} towns=${byTown.size}`);
      lastBeat = now;
    }
  }

  // Write CSV
  const rows = Array.from(byTown.values())
    .sort((a, b) => b.seen - a.seen)
    .map((t) => {
      const top = Array.from(t.topCodes.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 8)
        .map(([code, cnt]) => `${code}:${cnt}`)
        .join("|");
      return {
        town: t.town,
        seen: t.seen,
        baseHit: t.baseHit,
        baseRatePct: pct(t.baseHit, t.seen).toFixed(2),
        topBaseCodes: top,
      };
    });

  const csvPath = path.join(outDir, "base_zoning_coverage_by_town.csv");
  const header = "town,seen,baseHit,baseRatePct,topBaseCodes\n";
  const csv = header + rows.map(r =>
    `${r.town},${r.seen},${r.baseHit},${r.baseRatePct},"${String(r.topBaseCodes).replaceAll('"','""')}"`
  ).join("\n") + "\n";
  fs.writeFileSync(csvPath, csv, "utf8");

  const summary = {
    ran_at: new Date().toISOString(),
    in: inPath,
    outDir,
    lines,
    towns: byTown.size,
    total_seen: rows.reduce((s, r) => s + r.seen, 0),
    total_baseHit: rows.reduce((s, r) => s + r.baseHit, 0),
  };
  const jsonPath = path.join(outDir, "base_zoning_summary.json");
  fs.writeFileSync(jsonPath, JSON.stringify(summary, null, 2), "utf8");

  console.log("-----------------------------------------------------");
  console.log("[DONE] Base zoning summary complete.");
  console.log("[DONE] lines  :", lines.toLocaleString());
  console.log("[DONE] towns  :", byTown.size.toLocaleString());
  console.log("[DONE] csv    :", csvPath.replaceAll("\\", "/"));
  console.log("[DONE] summary:", jsonPath.replaceAll("\\", "/"));
  console.log("=====================================================");
}

main().catch((e) => {
  console.error("[FATAL]", e);
  process.exit(1);
});
