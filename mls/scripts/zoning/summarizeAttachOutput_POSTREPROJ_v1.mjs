// ESM (Node) — streaming NDJSON summary for zoning_base + zoning_overlays
import fs from "fs";
import path from "path";
import readline from "readline";

function arg(name, def=null){
  const ix = process.argv.indexOf(name);
  if(ix === -1) return def;
  const v = process.argv[ix+1];
  if(!v || v.startsWith("--")) return def;
  return v;
}

function log(msg){
  const ts = new Date().toISOString();
  process.stdout.write(`[${ts}] ${msg}\n`);
}

function toTown(v){
  return ((v ?? "") + "").trim().toLowerCase();
}

function basePresent(b){
  if(!b) return false;
  if(typeof b === "string") return b.trim().length > 0;
  if(typeof b === "object"){
    for(const k of ["code","zone_code","district","Zoning","ZONE_TYPE","ZONE","Zone","ZONE_CODE","DISTRICT"]){
      if(typeof b[k] === "string" && b[k].trim().length > 0) return true;
    }
  }
  return false;
}

function overlayList(v){
  if(!Array.isArray(v)) return [];
  const out = [];
  for(const it of v){
    if(!it) continue;
    if(typeof it === "string"){
      const s = it.trim();
      if(s) out.push(s);
      continue;
    }
    if(typeof it === "object"){
      for(const k of ["name","overlay_name","code","overlay","OVERLAY","district","DISTRICT"]){
        if(typeof it[k] === "string" && it[k].trim()){
          out.push(it[k].trim());
          break;
        }
      }
    }
  }
  return out;
}

async function main(){
  const inPath = arg("--in");
  const outDir = arg("--outDir");
  const townField = arg("--townField","town");
  const baseField = arg("--baseField","zoning_base");
  const overlaysField = arg("--overlaysField","zoning_overlays");
  const logEvery = parseInt(arg("--logEvery","500000"),10);
  const heartbeatSec = parseInt(arg("--heartbeatSec","10"),10);

  if(!inPath || !outDir) throw new Error("Missing --in or --outDir");

  log("=====================================================");
  log("[START] Summarize zoning attach output (POST-REPROJ) v1");
  log(`in         : ${inPath}`);
  log(`outDir     : ${outDir}`);
  log(`townField  : ${townField}`);
  log(`baseField  : ${baseField}`);
  log(`overlays   : ${overlaysField}`);
  log(`logEvery   : ${logEvery}`);
  log(`heartbeat  : ${heartbeatSec}s`);
  log("=====================================================");

  const byTown = new Map();
  const overlayCounts = new Map();

  let lines = 0;
  let lastBeat = Date.now();

  const rl = readline.createInterface({
    input: fs.createReadStream(inPath, {encoding:"utf8"}),
    crlfDelay: Infinity
  });

  for await (const line of rl){
    if(!line) continue;
    lines++;
    let o;
    try { o = JSON.parse(line); } catch { continue; }

    const town = toTown(o[townField]);
    if(!town){
      continue;
    }

    let t = byTown.get(town);
    if(!t){
      t = { town, seen:0, baseHit:0, overlaysAny:0, overlaysTotal:0 };
      byTown.set(town, t);
    }

    t.seen++;

    if(basePresent(o[baseField])) t.baseHit++;

    const ovs = overlayList(o[overlaysField]);
    if(ovs.length > 0){
      t.overlaysAny++;
      t.overlaysTotal += ovs.length;
      for(const name of ovs){
        overlayCounts.set(name, (overlayCounts.get(name) || 0) + 1);
      }
    }

    if(logEvery > 0 && lines % logEvery === 0){
      log(`[PROG] lines=${lines.toLocaleString()}`);
    }

    if(heartbeatSec > 0 && (Date.now() - lastBeat) > heartbeatSec*1000){
      lastBeat = Date.now();
      log(`[BEAT] lines=${lines.toLocaleString()} towns=${byTown.size}`);
    }
  }

  const covRows = Array.from(byTown.values()).sort((a,b)=>b.seen-a.seen);
  const covCsv = path.join(outDir, "attach_output_coverage_by_town.csv");
  const covJson = path.join(outDir, "attach_output_coverage_by_town.json");
  const ovCsv  = path.join(outDir, "attach_output_overlays_global.csv");
  const sumJson = path.join(outDir, "attach_output_summary.json");

  const toPct = (num, den) => den ? (100.0 * num / den) : 0;

  const covOut = covRows.map(r => ({
    town: r.town,
    seen: r.seen,
    baseHit: r.baseHit,
    baseRatePct: +toPct(r.baseHit, r.seen).toFixed(2),
    overlaysAny: r.overlaysAny,
    overlaysAnyRatePct: +toPct(r.overlaysAny, r.seen).toFixed(2),
    overlaysTotal: r.overlaysTotal,
    avgOverlaysPerParcel: r.seen ? +(r.overlaysTotal / r.seen).toFixed(4) : 0
  }));

  fs.writeFileSync(covJson, JSON.stringify(covOut, null, 2), "utf8");

  const covHeader = Object.keys(covOut[0] || {town:1,seen:1});
  const covLines = [covHeader.join(",")].concat(covOut.map(r => covHeader.map(k => r[k]).join(",")));
  fs.writeFileSync(covCsv, covLines.join("\n"), "utf8");

  const ovArr = Array.from(overlayCounts.entries())
    .map(([overlay,hits])=>({overlay,hits}))
    .sort((a,b)=>b.hits-a.hits);

  fs.writeFileSync(ovCsv,
    ["overlay,hits"].concat(ovArr.map(x => `"${(x.overlay||"").replace(/"/g,'""')}",${x.hits}`)).join("\n"),
    "utf8"
  );

  const summary = {
    lines,
    towns: byTown.size,
    coverageCsv: covCsv,
    overlaysCsv: ovCsv
  };
  fs.writeFileSync(sumJson, JSON.stringify(summary, null, 2), "utf8");

  log("-----------------------------------------------------");
  log("[DONE] Output attach summary complete (POST-REPROJ).");
  log(`lines   : ${lines.toLocaleString()}`);
  log(`towns   : ${byTown.size.toLocaleString()}`);
  log(`coverage: ${covCsv}`);
  log(`overlays: ${ovCsv}`);
  log(`summary : ${sumJson}`);
  log("=====================================================");
}

main().catch(err => {
  console.error("[FAIL]", err && err.stack ? err.stack : err);
  process.exit(1);
});
