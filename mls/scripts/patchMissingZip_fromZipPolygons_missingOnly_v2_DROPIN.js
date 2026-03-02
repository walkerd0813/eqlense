import fs from "fs";
import readline from "readline";

function parseArgs(argv){ const o={}; for(let i=2;i<argv.length;i++){ const a=argv[i]; if(a.startsWith("--")){ const k=a.slice(2); const v=argv[i+1]; if(!v||v.startsWith("--")) o[k]=true; else {o[k]=v; i++;}}} return o; }
const norm = (v)=> v==null? null : (String(v).trim()||null);
const zip5 = (v)=>{ if(v==null) return null; const d=String(v).replace(/\D/g,""); if(d.length<5) return null; return d.slice(0,5).padStart(5,"0"); };

function getLatLng(row){
  const lat = row.lat ?? row.latitude;
  const lng = row.lng ?? row.lon ?? row.longitude;
  const la = Number(lat), lo = Number(lng);
  if(!Number.isFinite(la)||!Number.isFinite(lo)) return null;
  return { lat: la, lng: lo };
}

function getKey(row){
  return norm(row.property_id ?? row.propertyId ?? row.id ?? row.parcel_id ?? row.parcelId);
}

function bboxOfGeom(geom){
  let minX=Infinity,minY=Infinity,maxX=-Infinity,maxY=-Infinity;
  const scan=(ring)=>{ for(const p of ring){ const x=p[0],y=p[1]; if(x<minX)minX=x; if(y<minY)minY=y; if(x>maxX)maxX=x; if(y>maxY)maxY=y; } };
  if(geom?.type==="Polygon"){ for(const r of geom.coordinates) scan(r); }
  else if(geom?.type==="MultiPolygon"){ for(const poly of geom.coordinates) for(const r of poly) scan(r); }
  else return null;
  if(!Number.isFinite(minX)) return null;
  return [minX,minY,maxX,maxY];
}

function pointOnSeg(px,py, ax,ay, bx,by, eps=1e-10){
  // colinear + within bounds
  const cross = (px-ax)*(by-ay) - (py-ay)*(bx-ax);
  if(Math.abs(cross) > eps) return false;
  const dot = (px-ax)*(bx-ax) + (py-ay)*(by-ay);
  if(dot < -eps) return false;
  const len2 = (bx-ax)*(bx-ax) + (by-ay)*(by-ay);
  if(dot - len2 > eps) return false;
  return true;
}
function ringHasPoint(pt, ring){
  const x=pt[0], y=pt[1];
  for(let i=0;i<ring.length-1;i++){
    const a=ring[i], b=ring[i+1];
    if(pointOnSeg(x,y, a[0],a[1], b[0],b[1])) return true;
  }
  return false;
}
function pointInRing(pt, ring){
  // ray casting (non-inclusive)
  let inside=false;
  const x=pt[0], y=pt[1];
  for(let i=0,j=ring.length-1;i<ring.length;j=i++){
    const xi=ring[i][0], yi=ring[i][1];
    const xj=ring[j][0], yj=ring[j][1];
    const intersect = (yi>y)!==(yj>y) && x < ((xj-xi)*(y-yi))/((yj-yi)+0.0)+xi;
    if(intersect) inside=!inside;
  }
  return inside;
}
function pointInPolygonInclusive(pt, polyCoords){
  const outer = polyCoords?.[0];
  if(!outer || outer.length<3) return false;

  // boundary-inclusive
  if(ringHasPoint(pt, outer)) return true;

  if(!pointInRing(pt, outer)) return false;

  // holes: if on boundary of a hole, treat as outside hole? safest: consider inside parcel boundary => false if in hole boundary
  for(let h=1; h<polyCoords.length; h++){
    const hole = polyCoords[h];
    if(!hole || hole.length<3) continue;
    if(ringHasPoint(pt, hole)) return false;
    if(pointInRing(pt, hole)) return false;
  }
  return true;
}
function pointInGeomInclusive(pt, geom){
  if(!geom) return false;
  if(geom.type==="Polygon") return pointInPolygonInclusive(pt, geom.coordinates);
  if(geom.type==="MultiPolygon"){
    for(const poly of geom.coordinates){
      if(pointInPolygonInclusive(pt, poly)) return true;
    }
    return false;
  }
  return false;
}

function buildGrid(polys, cellSize){
  let gminX=Infinity,gminY=Infinity;
  for(const p of polys){ gminX=Math.min(gminX,p.bbox[0]); gminY=Math.min(gminY,p.bbox[1]); }
  const key=(ix,iy)=>`${ix}|${iy}`;
  const toIx=(x)=>Math.floor((x-gminX)/cellSize);
  const toIy=(y)=>Math.floor((y-gminY)/cellSize);
  const grid=new Map();
  polys.forEach((p,idx)=>{
    const [minX,minY,maxX,maxY]=p.bbox;
    const ix0=toIx(minX), ix1=toIx(maxX);
    const iy0=toIy(minY), iy1=toIy(maxY);
    for(let ix=ix0; ix<=ix1; ix++){
      for(let iy=iy0; iy<=iy1; iy++){
        const k=key(ix,iy);
        if(!grid.has(k)) grid.set(k, []);
        grid.get(k).push(idx);
      }
    }
  });
  return {grid, key, toIx, toIy};
}

async function main(){
  const args=parseArgs(process.argv);
  const inMissing=args.in;
  const zipPolysPath=args.zipPolygons;
  const zipField=args.zipField ?? "POSTCODE";
  const outPath=args.out;
  const metaPath=args.meta;
  const cellSize=Number(args.cellSize ?? 0.05);

  if(!inMissing||!zipPolysPath||!outPath||!metaPath){
    console.log(`Usage:
node mls/scripts/patchMissingZip_fromZipPolygons_missingOnly_v2_DROPIN.js ^
  --in <missing_zip.ndjson> ^
  --zipPolygons <zip_polys.geojson> ^
  --zipField POSTCODE ^
  --out <patched_missing_zip.ndjson> ^
  --meta <meta.json>`);
    process.exit(1);
  }

  const zipJson=JSON.parse(fs.readFileSync(zipPolysPath,"utf8"));
  const feats=zipJson.features||[];
  const polygons=[];
  for(const f of feats){
    const z=zip5(f?.properties?.[zipField]);
    const geom=f?.geometry;
    if(!z||!geom) continue;
    const bbox=bboxOfGeom(geom);
    if(!bbox) continue;
    polygons.push({zip:z, geom, bbox});
  }
  if(polygons.length===0) throw new Error("No usable polygons");

  const grid=buildGrid(polygons, cellSize);

  const meta={ counts:{ total:0, filled:0, still_missing:0, no_cell:0, miss:0 } };
  const ws=fs.createWriteStream(outPath,"utf8");
  const rl=readline.createInterface({ input: fs.createReadStream(inMissing,"utf8"), crlfDelay: Infinity });

  for await (const line of rl){
    const t=line.trim(); if(!t) continue;
    meta.counts.total++;
    let row; try{ row=JSON.parse(t);}catch{continue;}

    const ll=getLatLng(row);
    const k=getKey(row);

    if(!ll || !k){ meta.counts.still_missing++; ws.write(JSON.stringify(row)+"\n"); continue; }

    const pt=[ll.lng, ll.lat];
    const cellKey = grid.key(grid.toIx(pt[0]), grid.toIy(pt[1]));
    const cand = grid.grid.get(cellKey);
    if(!cand){ meta.counts.no_cell++; meta.counts.still_missing++; ws.write(JSON.stringify(row)+"\n"); continue; }

    let found=null;
    for(const idx of cand){
      const p=polygons[idx];
      const [minX,minY,maxX,maxY]=p.bbox;
      if(pt[0]<minX||pt[0]>maxX||pt[1]<minY||pt[1]>maxY) continue;
      if(pointInGeomInclusive(pt,p.geom)){ found=p.zip; break; }
    }

    if(!found){ meta.counts.miss++; meta.counts.still_missing++; ws.write(JSON.stringify(row)+"\n"); continue; }

    row.zip = found;
    row.zip_source = row.zip_source ?? "zipPolygons:pip_inclusive";
    row.zip_method_version = "P4_missingOnly_v2_inclusive";
    row.zip_patched_at = new Date().toISOString();
    ws.write(JSON.stringify(row)+"\n");
    meta.counts.filled++;
  }

  ws.end();
  fs.writeFileSync(metaPath, JSON.stringify(meta,null,2), "utf8");
  console.log("[done]", meta.counts);
}

main().catch(e=>{ console.error(e); process.exit(1); });
