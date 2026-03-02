import fs from "fs";
import path from "path";
import readline from "readline";

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
function safeParse(line) { try { return JSON.parse(line); } catch { return null; } }

function inc(map, key, n=1) {
  const k = key ?? "undefined";
  map.set(k, (map.get(k) || 0) + n);
}

async function main() {
  const args = parseArgs(process.argv);
  const infile = args.in;
  const report = args.report || null;
  const logEvery = Number(args.logEvery || 100000);

  if (!infile) throw new Error("Usage: --in <ndjson> [--report <json>] [--logEvery N]");

  console.log("====================================================");
  console.log("[START] Summarize Base Zoning Join Keys v1");
  console.log("[INFO ] in: " + path.resolve(infile));
  console.log("====================================================");

  const byKey = new Map();
  const byKeyHits = new Map();

  let lines = 0, parseErr = 0, hits = 0;

  const rl = readline.createInterface({
    input: fs.createReadStream(infile, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    if (!line) continue;
    lines++;

    const o = safeParse(line);
    if (!o) { parseErr++; continue; }

    const k = o.base_zone_join_key || "none";
    inc(byKey, k, 1);

    const conf = Number(o.base_zone_confidence || 0);
    const hasCode = !!o.base_district_code;
    if (conf >= 1 && hasCode) {
      hits++;
      inc(byKeyHits, k, 1);
    }

    if (logEvery && lines % logEvery === 0) {
      console.log(`[PROG] lines=${lines.toLocaleString()} hits=${hits.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
    }
  }

  const toSortedArr = (m) =>
    Array.from(m.entries())
      .sort((a,b) => b[1]-a[1])
      .map(([key,count]) => ({ key, count, pct: lines ? +(100*count/lines).toFixed(3) : 0 }));

  const out = {
    created_at: new Date().toISOString(),
    infile: path.resolve(infile),
    lines,
    hits,
    hit_rate_pct: lines ? +(100*hits/lines).toFixed(3) : 0,
    parseErr,
    by_join_key: toSortedArr(byKey),
    hits_by_join_key: toSortedArr(byKeyHits),
  };

  console.log("----------------------------------------------------");
  console.log("[DONE] Summary:");
  console.log(JSON.stringify({
    lines: out.lines,
    hits: out.hits,
    hit_rate_pct: out.hit_rate_pct,
    parseErr: out.parseErr
  }, null, 2));

  if (report) {
    fs.mkdirSync(path.dirname(report), { recursive: true });
    fs.writeFileSync(report, JSON.stringify(out, null, 2), "utf8");
    console.log("[DONE] wrote report -> " + path.resolve(report));
  }
  console.log("====================================================");
}

main().catch(err => {
  console.error("[FAIL]", err?.stack || err);
  process.exit(1);
});
