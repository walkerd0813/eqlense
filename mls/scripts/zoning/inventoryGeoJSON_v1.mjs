import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

function getArg(name, d=null){
  const k=`--${name}`; const i=process.argv.indexOf(k);
  return i>=0 ? process.argv[i+1] : d;
}

const root = getArg("root");
const out = getArg("out");
if(!root || !out){
  console.error("Usage: node inventoryGeoJSON_v1.mjs --root <dir> --out <json>");
  process.exit(1);
}

const IGNORE_RE = /[\\\/]_build[\\\/]|zoningMaster_v\d|zoningBoundariesData_/i;

function walk(dir, acc=[]){
  for(const ent of fs.readdirSync(dir,{withFileTypes:true})){
    const p = path.join(dir, ent.name);
    if(IGNORE_RE.test(p)) continue;
    if(ent.isDirectory()) walk(p, acc);
    else if(ent.isFile() && p.toLowerCase().endsWith(".geojson")) acc.push(p);
  }
  return acc;
}

function sha256File(fp){
  const h=crypto.createHash("sha256");
  h.update(fs.readFileSync(fp));
  return h.digest("hex");
}

function geomBbox(g){
  let minX=Infinity,minY=Infinity,maxX=-Infinity,maxY=-Infinity, ok=false;

  function scanCoords(c){
    if(!c) return;
    if(typeof c[0] === "number" && typeof c[1] === "number"){
      const x=c[0], y=c[1];
      if(Number.isFinite(x) && Number.isFinite(y)){
        ok=true;
        if(x<minX) minX=x; if(y<minY) minY=y;
        if(x>maxX) maxX=x; if(y>maxY) maxY=y;
      }
      return;
    }
    if(Array.isArray(c)){
      for(const z of c) scanCoords(z);
    }
  }

  scanCoords(g.coordinates);
  if(!ok) return null;
  return [minX,minY,maxX,maxY];
}

function mergeBbox(a,b){
  if(!a) return b;
  if(!b) return a;
  return [Math.min(a[0],b[0]),Math.min(a[1],b[1]),Math.max(a[2],b[2]),Math.max(a[3],b[3])];
}

function looksWgs84MA(b){
  if(!b) return false;
  const [minLon,minLat,maxLon,maxLat]=b;
  const lonOk = (minLon>-74 && maxLon<-69);
  const latOk = (minLat>40.5 && maxLat<43.8);
  return lonOk && latOk;
}

const files = walk(root, []);
const rows = [];

for(const fp of files){
  const rel = path.relative(process.cwd(), fp);
  const bytes = fs.statSync(fp).size;
  const sha = sha256File(fp);

  let featureCount=0;
  let bbox=null;
  let parseError=null;

  try{
    const j = JSON.parse(fs.readFileSync(fp,"utf8"));
    const feats = Array.isArray(j.features) ? j.features : [];
    featureCount = feats.length;

    for(const f of feats){
      const g = f && f.geometry;
      if(!g) continue;
      bbox = mergeBbox(bbox, geomBbox(g));
    }
  }catch(e){
    parseError = String(e?.message || e);
  }

  const wgs84MA = parseError ? false : looksWgs84MA(bbox);

  rows.push({
    rel, abs: fp, bytes, sha256: sha,
    featureCount, bbox, wgs84MA,
    parseError
  });
}

rows.sort((a,b)=>b.featureCount-a.featureCount);

fs.mkdirSync(path.dirname(out), {recursive:true});
fs.writeFileSync(out, JSON.stringify({root, generatedAt: new Date().toISOString(), files: rows}, null, 2));

console.log("[done] wrote inventory:", out);
console.log("Top 15 by featureCount:");
console.table(rows.slice(0,15).map(r=>({rel:r.rel, featureCount:r.featureCount, wgs84MA:r.wgs84MA, bytes:r.bytes})));
