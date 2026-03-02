import fs from "node:fs";
import readline from "node:readline";
import path from "node:path";

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

async function main(){
  const args = parseArgs(process.argv);
  const infile = args.in;
  const town = (args.town || "boston").toString().trim().toLowerCase();
  const n = Number(args.n || 25);
  const outPath = args.out || "";

  if(!infile){
    throw new Error("Usage: node sample_zoning_attach_output_v1.mjs --in <ndjson> [--town boston] [--n 25] [--out <json>]");
  }

  const outFile = outPath || path.join(process.cwd(), `zoning_attach_sample_${town}_${Date.now()}.json`);

  log("=====================================================");
  log("[START] Sample zoning attach output v1");
  log(`[INFO ] in   : ${infile}`);
  log(`[INFO ] town : ${town}`);
  log(`[INFO ] n    : ${n}`);
  log(`[INFO ] out  : ${outFile}`);
  log("=====================================================");

  const inStream = fs.createReadStream(infile, { encoding: "utf8" });
  const rl = readline.createInterface({ input: inStream, crlfDelay: Infinity });

  let seen = 0;
  let matched = 0;
  const sample = [];

  // Reservoir sampling on matches
  for await (const line of rl){
    if(!line) continue;
    let obj;
    try { obj = JSON.parse(line); } catch { continue; }
    seen++;
    const t = (obj.town || "").toString().trim().toLowerCase();
    if(t !== town) continue;

    matched++;
    const slim = {
      property_id: obj.property_id,
      parcel_id: obj.parcel_id,
      full_address: obj.full_address,
      town: obj.town,
      zip: obj.zip,
      lat: obj.lat,
      lng: obj.lng,
      zoning_base: obj.zoning_base,
      zoning_overlays: obj.zoning_overlays,
      zoning_attach: obj.zoning_attach,
      zoning: obj.zoning
    };

    if(sample.length < n){
      sample.push(slim);
    } else {
      const j = Math.floor(Math.random() * matched);
      if(j < n) sample[j] = slim;
    }

    if(seen % 500000 === 0){
      log(`[PROG] ${nowIso()} scanned=${seen.toLocaleString()} matched=${matched.toLocaleString()} sample=${sample.length}`);
    }
  }

  fs.writeFileSync(outFile, JSON.stringify({
    created_at: nowIso(),
    infile,
    town,
    scanned: seen,
    matched,
    sample_count: sample.length,
    sample
  }, null, 2), "utf8");

  log("-----------------------------------------------------");
  log("[DONE] Sample complete.");
  log(`[DONE] scanned : ${seen.toLocaleString()}`);
  log(`[DONE] matched : ${matched.toLocaleString()}`);
  log(`[DONE] wrote   : ${outFile}`);
  log("=====================================================");
}

main().catch(e=>{
  console.error("[FAIL] " + (e && e.stack ? e.stack : e));
  process.exit(1);
});
