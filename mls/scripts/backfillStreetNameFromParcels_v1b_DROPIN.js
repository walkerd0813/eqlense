import fs from "fs";
import path from "path";
import readline from "readline";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1];
      if (!v || v.startsWith("--")) out[k] = true;
      else { out[k] = v; i++; }
    }
  }
  return out;
}

function nowISO(){ return new Date().toISOString(); }
function die(msg){ console.error(`\n❌ ${msg}\n`); process.exit(1); }
function exists(fp){ return fs.existsSync(fp); }
function norm(v){ if(v===null||v===undefined) return null; const s=String(v).trim(); return s.length?s:null; }
function isBlank(v){ return !norm(v); }
function isStreetNoMissing(v){
  const s = norm(v);
  if(!s) return true;
  const t = s.replace(/\s+/g,"").toUpperCase();
  return t === "0" || t === "00" || t === "000";
}
function getPid(row){ return norm(row.parcel_id ?? row.parcelId ?? row.MAP_PAR_ID ?? row.LOC_ID); }

function pick(props, keys){
  for(const k of keys){
    const v = norm(props?.[k]);
    if(v) return { key:k, value:v };
  }
  return { key:null, value:null };
}

function cleanSiteAddr(s){
  if(!s) return null;
  let x = s.split(",")[0].trim();          // remove ", VILLAGE" tails
  x = x.replace(/\s*\(.*?\)\s*$/g,"").trim(); // drop trailing "(OFF)" etc (kept in original fields anyway)
  x = x.replace(/\s+/g," ");
  return x || null;
}

// Parse street number if present at start, including decimals and ranges: "105.1", "270-6"
function parseLeadingNoAndName(siteAddr){
  const s = cleanSiteAddr(siteAddr);
  if(!s) return { no:null, name:null };
  const m = s.match(/^(\d+(?:\.\d+)?(?:-\d+)?)\s+(.*)$/);
  if(!m) return { no:null, name:s }; // no number, but the whole string is the street name
  const no = m[1];
  const name = m[2]?.trim() || null;
  if(no === "0") return { no:null, name: name ?? null };
  return { no, name };
}

async function collectNeededPids(inPath){
  const needed = new Set();
  const counts = { total:0, candidates:0, missingPid:0 };
  const rl = readline.createInterface({ input: fs.createReadStream(inPath,"utf8"), crlfDelay: Infinity });
  for await (const line of rl){
    const t = line.trim(); if(!t) continue;
    counts.total++;
    let row; try{ row = JSON.parse(t);}catch{continue;}
    const streetName = row.street_name ?? row.streetName ?? null;
    const streetNo = row.street_no ?? row.streetNo ?? null;

    const needs = isBlank(streetName) || isStreetNoMissing(streetNo);
    if(!needs) continue;
    counts.candidates++;

    const pid = getPid(row);
    if(!pid){ counts.missingPid++; continue; }
    needed.add(pid);
  }
  return { needed, counts };
}

async function buildParcelLookup(parcelsPath, needed){
  const byId = new Map();
  const stats = { parcels_seen:0, hits:0 };
  const rl = readline.createInterface({ input: fs.createReadStream(parcelsPath,"utf8"), crlfDelay: Infinity });
  for await (const line of rl){
    const t = line.trim(); if(!t) continue;
    stats.parcels_seen++;
    let obj; try{ obj = JSON.parse(t);}catch{continue;}
    const props = obj.properties ?? obj;

    const mapPar = pick(props, ["MAP_PAR_ID","map_par_id","PARCEL_ID","parcel_id"]).value;
    const locId  = pick(props, ["LOC_ID","loc_id"]).value;

    const addrNum = pick(props, ["ADDR_NUM","addr_num","ADDRNUM","ST_NUM","HOUSE_NUM"]);
    const siteAddr= pick(props, ["SITE_ADDR","site_addr","SITEADDR","ADDRESS"]);
    const fullStr = pick(props, ["FULL_STR","full_str","FULLADDR","FULL_ADDRESS"]);
    const city    = pick(props, ["CITY","city","TOWN","town","MUNICIPALITY"]);
    const zip     = pick(props, ["ZIP","zip","ZIPCODE","POSTCODE"]);

    const ids = [mapPar, locId].filter(Boolean);
    let matched = false;
    for(const id of ids){
      if(needed.has(id)){
        byId.set(id, { addrNum, siteAddr, fullStr, city, zip });
        matched = true;
      }
    }
    if(matched) stats.hits++;
    if(byId.size >= needed.size) break;
  }
  return { byId, stats };
}

async function main(){
  const args = parseArgs(process.argv);
  const inPath = args.in;
  const parcelsPath = args.parcels;
  const outPath = args.out;
  const metaPath = args.meta;

  if(!inPath || !parcelsPath || !outPath || !metaPath){
    console.log(`Usage:
node mls/scripts/backfillStreetNameFromParcels_v1b_DROPIN.js ^
  --in <properties.ndjson> ^
  --parcels <parcels.ndjson> ^
  --out <out.ndjson> ^
  --meta <meta.json>`);
    process.exit(1);
  }
  if(!exists(inPath)) die(`--in not found: ${inPath}`);
  if(!exists(parcelsPath)) die(`--parcels not found: ${parcelsPath}`);
  if(path.resolve(inPath) === path.resolve(outPath)) die("--out must differ from --in");

  console.log("====================================================");
  console.log(" P3b — STREET NAME/NO BACKFILL FROM PARCEL ATTRS");
  console.log("====================================================");
  console.log(`[run] started_at: ${nowISO()}`);
  console.log(`[run] in:        ${inPath}`);
  console.log(`[run] parcels:   ${parcelsPath}`);
  console.log(`[run] out:       ${outPath}`);
  console.log(`[run] meta:      ${metaPath}`);
  console.log("----------------------------------------------------");

  const { needed, counts: pass1 } = await collectNeededPids(inPath);
  const { byId, stats: pass2 } = await buildParcelLookup(parcelsPath, needed);

  const meta = {
    script: "backfillStreetNameFromParcels_v1b_DROPIN.js",
    started_at: nowISO(),
    args: { in: inPath, parcels: parcelsPath, out: outPath, meta: metaPath },
    pass1,
    pass2,
    counts: {
      total_rows: 0,
      candidate_rows: 0,
      parcel_hit: 0,
      filled_street_name: 0,
      filled_street_no: 0,
    },
  };

  const outWS = fs.createWriteStream(outPath, { encoding:"utf8" });
  const rl = readline.createInterface({ input: fs.createReadStream(inPath,"utf8"), crlfDelay: Infinity });

  for await (const line of rl){
    const t = line.trim(); if(!t) continue;
    meta.counts.total_rows++;
    let row; try{ row = JSON.parse(t);}catch{continue;}

    const streetNameKey = "street_name";
    const streetNoKey = "street_no";

    const streetName = row.street_name ?? row.streetName ?? null;
    const streetNo = row.street_no ?? row.streetNo ?? null;

    const needs = isBlank(streetName) || isStreetNoMissing(streetNo);
    if(!needs){
      outWS.write(JSON.stringify(row) + "\n");
      continue;
    }
    meta.counts.candidate_rows++;

    const pid = getPid(row);
    const hit = pid ? byId.get(pid) : null;
    if(hit) meta.counts.parcel_hit++;

    let did = false;

    if(hit){
      const siteAddr = hit.siteAddr.value;
      const fullStr  = hit.fullStr.value;
      const parsed = parseLeadingNoAndName(siteAddr);

      // street_name: prefer FULL_STR, else parsed.name (which could be whole SITE_ADDR when no number)
      if(isBlank(streetName)){
        const candidateName = fullStr ?? parsed.name ?? null;
        if(!isBlank(candidateName)){
          row[streetNameKey] = candidateName;
          row.street_name_source = fullStr ? `parcels:${hit.fullStr.key}` : (siteAddr ? `parcels:${hit.siteAddr.key}` : "parcels:parsed");
          meta.counts.filled_street_name++;
          did = true;
        }
      }

      // street_no: prefer ADDR_NUM if not 0, else parsed.no (if not 0)
      if(isStreetNoMissing(streetNo)){
        const addrNum = hit.addrNum.value;
        const candidateNo = (!isStreetNoMissing(addrNum) ? addrNum : parsed.no);
        if(!isStreetNoMissing(candidateNo)){
          row[streetNoKey] = candidateNo;
          row.street_no_source = (!isStreetNoMissing(addrNum) ? `parcels:${hit.addrNum.key}` : `parcels:${hit.siteAddr.key}`);
          meta.counts.filled_street_no++;
          did = true;
        }
      }

      if(did){
        row.address_method_version = "P3b_v1_DROPIN";
        row.address_patched_at = nowISO();
      }
    }

    outWS.write(JSON.stringify(row) + "\n");
  }

  outWS.end();
  meta.finished_at = nowISO();
  fs.writeFileSync(metaPath, JSON.stringify(meta,null,2), "utf8");

  console.log("====================================================");
  console.log("DONE — P3b STREET FILL");
  console.log("----------------------------------------------------");
  console.log(meta.counts);
  console.log("====================================================");
}

main().catch(e => { console.error(e); process.exit(1); });
