import fs from "fs";
import readline from "readline";

function norm(v){ return v==null? null : (String(v).trim()||null); }
function zip5(v){ if(v==null) return null; const d=String(v).replace(/\D/g,""); if(d.length<5) return null; return d.slice(0,5).padStart(5,"0"); }
function getKey(row){ return norm(row.property_id ?? row.propertyId ?? row.id ?? row.parcel_id ?? row.parcelId); }

const patchPath = process.argv[2];
const inPath = process.argv[3];
const outPath = process.argv[4];

if(!patchPath||!inPath||!outPath){
  console.log("Usage: node applyZipPatchesByKey_v1_DROPIN.js <patched_missing.ndjson> <in_full.ndjson> <out_full.ndjson>");
  process.exit(1);
}

(async ()=>{
  const patches = new Map();
  const rlP = readline.createInterface({ input: fs.createReadStream(patchPath,"utf8"), crlfDelay: Infinity });
  for await (const line of rlP){
    const t=line.trim(); if(!t) continue;
    let r; try{ r=JSON.parse(t);}catch{continue;}
    const k=getKey(r);
    const z=zip5(r.zip);
    if(k && z) patches.set(k, z);
  }
  console.log("[patches]", patches.size);

  const ws = fs.createWriteStream(outPath,"utf8");
  const rl = readline.createInterface({ input: fs.createReadStream(inPath,"utf8"), crlfDelay: Infinity });

  let total=0, applied=0;
  for await (const line of rl){
    const t=line.trim(); if(!t) continue;
    total++;
    let row; try{ row=JSON.parse(t);}catch{continue;}
    const cur = zip5(row.zip ?? row.ZIP ?? row.zip_code ?? row.zipCode);
    if(cur){ ws.write(JSON.stringify(row)+"\n"); continue; }

    const k=getKey(row);
    const z= k ? patches.get(k) : null;
    if(z){
      row.zip = z;
      row.zip_source = row.zip_source ?? "zipPolygons:pip_inclusive";
      row.zip_method_version = "P4_merge_v1";
      row.zip_patched_at = new Date().toISOString();
      applied++;
    }
    ws.write(JSON.stringify(row)+"\n");
  }
  ws.end();
  console.log("[done]", { total, applied });
})();
