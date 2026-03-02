import fs from "node:fs";
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
function loadBoundary(boundaryPath){
  const b = JSON.parse(fs.readFileSync(boundaryPath, "utf8"));
  if(!b.features?.length) throw new Error("Boundary has no features.");
  const combined = turf.combine(b);
  const geom = combined?.features?.[0]?.geometry || b.features[0].geometry;
  return turf.feature(geom);
}
function chooseParcelIdKey(sampleProps){
  const preferred = ["LOC_ID","loc_id","parcel_id","PARCEL_ID","MAP_PAR_ID","map_par_id","OBJECTID","objectid"];
  for(const k of preferred) if(sampleProps && Object.prototype.hasOwnProperty.call(sampleProps,k)) return k;
  return null;
}

async function main(){
  const a=args();
  const parcelsPath=a.parcelsIn, boundaryPath=a.boundary, out=a.out, reportPath=a.report;
  if(!parcelsPath||!boundaryPath||!out||!reportPath) throw new Error("Usage: --parcelsIn <parcels.geojson> --boundary <boundary.geojson> --out <out.ndjson> --report <report.json>");

  const parcels = JSON.parse(fs.readFileSync(parcelsPath, "utf8"));
  const feats = parcels.features || [];
  const boundary = loadBoundary(boundaryPath);

  const sampleProps = feats[0]?.properties || {};
  const idKey = chooseParcelIdKey(sampleProps);

  const ws = fs.createWriteStream(out, { encoding:"utf8" });

  let total=0, kept=0, missingId=0, missingGeom=0;

  for(const f of feats){
    total++;
    if(!f?.geometry){ missingGeom++; continue; }

    const props = f.properties || {};
    const pid = idKey ? props[idKey] : (props.parcel_id ?? props.PARCEL_ID ?? props.LOC_ID ?? f.id);
    if(pid==null){ missingId++; continue; }

    // centerOfMass tends to land inside polygon more reliably than centroid
    const pt = turf.centerOfMass(f);
    if(!pt?.geometry?.coordinates) continue;

    if(turf.booleanPointInPolygon(pt, boundary)){
      const lon = pt.geometry.coordinates[0];
      const lat = pt.geometry.coordinates[1];
      ws.write(JSON.stringify({ parcel_id: pid, lat, lon }) + "\n");
      kept++;
    }
  }

  ws.end();

  const report = {
    parcelsIn: parcelsPath, parcels_sha256: await sha256File(parcelsPath),
    boundary: boundaryPath,
    idKeyPicked: idKey,
    out,
    totals: { total, kept, missingId, missingGeom }
  };
  fs.writeFileSync(reportPath, JSON.stringify(report,null,2), "utf8");
  console.log(`[ok] centroids built -> ${out} kept=${kept}/${total}`);
}
main().catch(e=>{ console.error("[ERR]", e.message||e); process.exit(1); });
