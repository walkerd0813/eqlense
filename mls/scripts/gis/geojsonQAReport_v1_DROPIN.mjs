import fs from "node:fs";
import crypto from "node:crypto";

function args() {
  const out = {};
  for (let i=2;i<process.argv.length;i++){
    const a=process.argv[i]; if(!a.startsWith("--")) continue;
    const k=a.slice(2); const v=process.argv[i+1] && !process.argv[i+1].startsWith("--") ? process.argv[++i] : true;
    out[k]=v;
  }
  return out;
}
function sha256File(p){
  return new Promise((res, rej)=>{
    const h=crypto.createHash("sha256");
    const s=fs.createReadStream(p);
    s.on("data", d=>h.update(d));
    s.on("error", rej);
    s.on("end", ()=>res(h.digest("hex")));
  });
}
function bboxOfCoords(coords, bbox){
  if(!coords) return;
  if(typeof coords[0] === "number"){
    const x=coords[0], y=coords[1];
    bbox[0]=Math.min(bbox[0], x); bbox[1]=Math.min(bbox[1], y);
    bbox[2]=Math.max(bbox[2], x); bbox[3]=Math.max(bbox[3], y);
    return;
  }
  for(const c of coords) bboxOfCoords(c, bbox);
}
async function main(){
  const a=args();
  const input=a.in, out=a.out, name=a.name || "geojson";
  if(!input||!out) throw new Error("Usage: --in <file.geojson> --out <report.json> [--name label]");
  const raw=fs.readFileSync(input,"utf8");
  const json=JSON.parse(raw);
  const feats = Array.isArray(json.features) ? json.features : [];
  const geomCounts = {};
  const keyCounts = {};
  const zoneCounts = { zone_code:0, zone_name:0, zone_label:0 };
  const bbox=[Infinity,Infinity,-Infinity,-Infinity];
  const zoneCodeFreq = new Map();

  for(const f of feats){
    const g=f?.geometry?.type || "null";
    geomCounts[g]=(geomCounts[g]||0)+1;

    const props=f?.properties || {};
    for(const k of Object.keys(props)) keyCounts[k]=(keyCounts[k]||0)+1;

    if(props.zone_code) zoneCounts.zone_code++;
    if(props.zone_name) zoneCounts.zone_name++;
    if(props.zone_label) zoneCounts.zone_label++;

    if(props.zone_code){
      zoneCodeFreq.set(props.zone_code, (zoneCodeFreq.get(props.zone_code)||0)+1);
    }

    bboxOfCoords(f?.geometry?.coordinates, bbox);
  }

  const topKeys = Object.entries(keyCounts).sort((a,b)=>b[1]-a[1]).slice(0,40);
  const topZoneCodes = Array.from(zoneCodeFreq.entries()).sort((a,b)=>b[1]-a[1]).slice(0,30);

  const report = {
    name,
    file: input,
    sha256: await sha256File(input),
    featureCount: feats.length,
    geometryTypes: geomCounts,
    bbox: (bbox[0]===Infinity? null : { minX:bbox[0], minY:bbox[1], maxX:bbox[2], maxY:bbox[3] }),
    topPropertyKeys: topKeys,
    standardizedFieldPresence: zoneCounts,
    topZoneCodes
  };

  fs.writeFileSync(out, JSON.stringify(report,null,2), "utf8");
  console.log(`[ok] QA -> ${out}`);
}
main().catch(e=>{ console.error("[ERR]", e.message||e); process.exit(1); });
