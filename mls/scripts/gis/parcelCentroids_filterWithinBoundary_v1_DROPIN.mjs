import fs from "node:fs";
import readline from "node:readline";
import crypto from "node:crypto";
import * as turf from "@turf/turf";

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
function pickLatLon(obj){
  // supports common centroid schemas
  const lat = obj.lat ?? obj.latitude ?? obj.y ?? obj.centroid?.lat ?? obj.centroid?.latitude ?? obj.centroid?.y;
  const lon = obj.lon ?? obj.lng ?? obj.longitude ?? obj.x ?? obj.centroid?.lon ?? obj.centroid?.lng ?? obj.centroid?.longitude ?? obj.centroid?.x;
  if(typeof lat === "number" && typeof lon === "number") return {lat, lon};
  return null;
}
function pickParcelId(obj){
  return obj.parcel_id ?? obj.parcelId ?? obj.LOC_ID ?? obj.loc_id ?? obj.MAP_PAR_ID ?? obj.map_par_id ?? obj.PARCEL_ID ?? obj.par_id ?? obj.id;
}
function loadBoundary(boundaryPath){
  const b = JSON.parse(fs.readFileSync(boundaryPath, "utf8"));
  if(!b.features?.length) throw new Error("Boundary has no features.");
  // dissolve-ish: combine to MultiPolygon if needed
  const combined = turf.combine(b);
  const geom = combined?.features?.[0]?.geometry || b.features[0].geometry;
  return turf.feature(geom);
}

async function main(){
  const a=args();
  const input=a.in, boundaryPath=a.boundary, out=a.out, reportPath=a.report;
  if(!input||!boundaryPath||!out||!reportPath) throw new Error("Usage: --in <centroids.ndjson> --boundary <boundary.geojson> --out <out.ndjson> --report <report.json>");

  const boundary = loadBoundary(boundaryPath);

  const rl = readline.createInterface({ input: fs.createReadStream(input, "utf8"), crlfDelay: Infinity });
  const ws = fs.createWriteStream(out, { encoding:"utf8" });

  let total=0, kept=0, bad=0, noid=0, nocoord=0;

  for await (const line of rl){
    if(!line.trim()) continue;
    total++;
    let obj;
    try { obj=JSON.parse(line); } catch { bad++; continue; }

    const pid = pickParcelId(obj);
    if(pid==null){ noid++; continue; }

    const ll = pickLatLon(obj);
    if(!ll){ nocoord++; continue; }

    const pt = turf.point([ll.lon, ll.lat]);
    if(turf.booleanPointInPolygon(pt, boundary)){
      ws.write(JSON.stringify(obj) + "\n");
      kept++;
    }
  }

  ws.end();

  const report = {
    input, input_sha256: await sha256File(input),
    boundary: boundaryPath,
    out,
    totals: { total, kept, bad_json: bad, missing_parcel_id: noid, missing_coords: nocoord }
  };
  fs.writeFileSync(reportPath, JSON.stringify(report,null,2), "utf8");
  console.log(`[ok] centroids filtered -> ${out} kept=${kept}/${total}`);
}
main().catch(e=>{ console.error("[ERR]", e.message||e); process.exit(1); });
