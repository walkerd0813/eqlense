import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

function nowIso(){ return new Date().toISOString(); }
function log(msg){ process.stdout.write(msg + "\n"); }

function parseArgs(argv){
  const args = {};
  for(let i=2;i<argv.length;i++){
    const a = argv[i];
    if(a.startsWith("--")){
      const k = a.slice(2);
      const v = argv[i+1];
      if(v && !v.startsWith("--")){ args[k]=v; i++; }
      else args[k]=true;
    }
  }
  return args;
}

function pick(obj, keys){
  if(!obj || typeof obj !== "object") return "";
  for(const k of keys){
    const v = obj[k];
    if(typeof v === "string" && v.trim()) return v.trim();
    if(typeof v === "number") return String(v);
  }
  return "";
}

function baseCode(x){
  if(!x) return "";
  if(typeof x === "string") return x.trim();
  if(typeof x === "object") return pick(x, ["code","zone_code","district_code","district","zone","label","name","ZONE","ZONING"]);
  return "";
}

function overlayCodes(x){
  if(!x) return [];
  if(Array.isArray(x)){
    const out = [];
    for(const it of x){
      if(typeof it === "string" && it.trim()) out.push(it.trim());
      else if(it && typeof it === "object"){
        const c = pick(it, ["code","overlay_code","zone_code","district","label","name"]);
        if(c) out.push(c);
      }
    }
    return out;
  }
  if(typeof x === "string") return x.trim() ? [x.trim()] : [];
  if(typeof x === "object"){
    const c = pick(x, ["code","overlay_code","zone_code","district","label","name"]);
    return c ? [c] : [];
  }
  return [];
}

function bumpMap(map, key, n=1, maxKeys=600){
  if(!key) return;
  if(map.has(key)){ map.set(key, map.get(key)+n); return; }
  if(map.size >= maxKeys){
    map.set("__other__", (map.get("__other__")||0)+n);
    return;
  }
  map.set(key, n);
}

function topK(map, k){
  const arr = Array.from(map.entries()).sort((a,b)=>b[1]-a[1]).slice(0,k);
  return arr.map(([name,count])=>({name,count}));
}

async function main(){
  const args = parseArgs(process.argv);
  const infile = args.in;
  const outDir = args.outDir;
  const townField = args.townField || "town";
  const baseField = args.baseField || "zoning_base";
  const overlaysField = args.overlaysField || "zoning_overlays";
  const logEvery = Number(args.logEvery || 500000);
  const heartbeatSec = Number(args.heartbeatSec || 10);

  if(!infile || !outDir){
    throw new Error("Usage: node summarize_zoning_attach_output_v1.mjs --in <ndjson> --outDir <dir> [--logEvery N] [--heartbeatSec N]");
  }
  fs.mkdirSync(outDir, { recursive: true });

  log("=====================================================");
  log("[START] Summarize zoning attach output (streaming) v1");
  log(`[INFO ] in        : ${infile}`);
  log(`[INFO ] outDir    : ${outDir}`);
  log(`[INFO ] townField : ${townField}`);
  log(`[INFO ] baseField : ${baseField}`);
  log(`[INFO ] overlays  : ${overlaysField}`);
  log(`[INFO ] logEvery  : ${logEvery}`);
  log(`[INFO ] heartbeat : ${heartbeatSec}s`);
  log("=====================================================");

  const inStream = fs.createReadStream(infile, { encoding: "utf8" });
  const rl = readline.createInterface({ input: inStream, crlfDelay: Infinity });

  let seen = 0;
  const towns = new Map(); // town -> stats
  const globalOverlays = new Map();

  let lastBeat = Date.now();
  for await (const line of rl){
    if(!line) continue;
    let obj;
    try { obj = JSON.parse(line); } catch { continue; }

    const town = (obj[townField] || "").toString().trim().toLowerCase() || "__no_town__";
    const base = baseCode(obj[baseField]);
    const overlays = overlayCodes(obj[overlaysField]);

    if(!towns.has(town)){
      towns.set(town, {
        seen: 0,
        basePresent: 0,
        overlaysAny: 0,
        overlaysTotal: 0,
        topBase: new Map(),
        topOverlay: new Map()
      });
    }
    const t = towns.get(town);
    t.seen++;
    if(base){
      t.basePresent++;
      bumpMap(t.topBase, base, 1);
    }
    if(overlays.length){
      t.overlaysAny++;
      t.overlaysTotal += overlays.length;
      for(const o of overlays){
        bumpMap(t.topOverlay, o, 1);
        bumpMap(globalOverlays, o, 1, 5000);
      }
    }

    seen++;
    if(seen % logEvery === 0){
      log(`[PROG] ${nowIso()} lines=${seen.toLocaleString()}`);
    }
    const now = Date.now();
    if(now - lastBeat >= heartbeatSec*1000){
      log(`[BEAT] ${nowIso()} lines=${seen.toLocaleString()} towns=${towns.size}`);
      lastBeat = now;
    }
  }

  // Build CSV rows
  const rows = [];
  for(const [town, t] of towns.entries()){
    const baseRate = t.seen ? (t.basePresent / t.seen) * 100 : 0;
    const overlayRate = t.seen ? (t.overlaysAny / t.seen) * 100 : 0;
    const avgOverlays = t.seen ? (t.overlaysTotal / t.seen) : 0;
    rows.push({
      town,
      seen: t.seen,
      basePresent: t.basePresent,
      baseRatePct: Number(baseRate.toFixed(2)),
      overlaysAny: t.overlaysAny,
      overlaysAnyRatePct: Number(overlayRate.toFixed(2)),
      avgOverlaysPerParcel: Number(avgOverlays.toFixed(4)),
      topBaseCodes: topK(t.topBase, 8).map(x=>`${x.name}:${x.count}`).join(" | "),
      topOverlays: topK(t.topOverlay, 8).map(x=>`${x.name}:${x.count}`).join(" | ")
    });
  }

  rows.sort((a,b)=>b.seen - a.seen);

  const covCsv = path.join(outDir, "attach_output_coverage_by_town.csv");
  const covJson = path.join(outDir, "attach_output_coverage_by_town.json");
  const globalCsv = path.join(outDir, "attach_output_overlays_global.csv");
  const summaryJson = path.join(outDir, "attach_output_summary.json");

  // write CSV helper
  function writeCsv(fp, data){
    const cols = Object.keys(data[0] || {});
    const esc = (v)=>{
      const s = (v===null || v===undefined) ? "" : String(v);
      return /[,"\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
    };
    const lines = [cols.join(",")];
    for(const r of data){
      lines.push(cols.map(c=>esc(r[c])).join(","));
    }
    fs.writeFileSync(fp, lines.join("\n"), "utf8");
  }

  if(rows.length){
    writeCsv(covCsv, rows);
    fs.writeFileSync(covJson, JSON.stringify(rows, null, 2), "utf8");
  } else {
    fs.writeFileSync(covCsv, "town,seen\n", "utf8");
    fs.writeFileSync(covJson, "[]", "utf8");
  }

  const gRows = topK(globalOverlays, 2000).map(x=>({ overlay: x.name, count: x.count }));
  if(gRows.length){
    writeCsv(globalCsv, gRows);
  } else {
    fs.writeFileSync(globalCsv, "overlay,count\n", "utf8");
  }

  const summary = {
    created_at: nowIso(),
    infile,
    total_lines_parsed: seen,
    towns: towns.size,
    notes: [
      "basePresent counts records where zoning_base is non-empty",
      "overlaysAny counts records where zoning_overlays has at least 1 overlay",
      "Rates are computed per-town from the output NDJSON (ground truth)"
    ]
  };
  fs.writeFileSync(summaryJson, JSON.stringify(summary, null, 2), "utf8");

  log("-----------------------------------------------------");
  log("[DONE] Output attach summary complete.");
  log(`[DONE] lines   : ${seen.toLocaleString()}`);
  log(`[DONE] towns   : ${towns.size}`);
  log(`[DONE] coverage: ${covCsv}`);
  log(`[DONE] overlays: ${globalCsv}`);
  log(`[DONE] summary : ${summaryJson}`);
  log("=====================================================");
}

main().catch(e=>{
  console.error("[FAIL] " + (e && e.stack ? e.stack : e));
  process.exit(1);
});
