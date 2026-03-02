#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

function die(msg){ console.error("[fatal] " + msg); process.exit(1); }
function ensureDir(p){ fs.mkdirSync(p,{recursive:true}); }
function isFile(p){ try{ return fs.statSync(p).isFile(); }catch{ return false; } }
function isDir(p){ try{ return fs.statSync(p).isDirectory(); }catch{ return false; } }

function sha256FileStream(filePath){
  return new Promise((resolve,reject)=>{
    const h=crypto.createHash("sha256");
    const s=fs.createReadStream(filePath);
    s.on("data",(c)=>h.update(c));
    s.on("error",reject);
    s.on("end",()=>resolve(h.digest("hex").toUpperCase()));
  });
}

function walkFiles(root){
  const out=[];
  function rec(d){
    for(const e of fs.readdirSync(d,{withFileTypes:true})){
      const p=path.join(d,e.name);
      if(e.isDirectory()) rec(p); else out.push(p);
    }
  }
  if(isDir(root)) rec(root);
  return out;
}

function pickGeoJSONUnder(dirPath,tokens){
  if(!isDir(dirPath)) return null;
  const files=walkFiles(dirPath).filter(p=>{
    const b=path.basename(p).toLowerCase();
    if(b.endsWith(".ndjson")) return false;
    return b.endsWith(".geojson")||b.endsWith(".json");
  });
  if(files.length===0) return null;
  if(files.length===1) return files[0];

  const score=(fp)=>{
    const b=path.basename(fp).toLowerCase();
    let s=0;
    for(const t of tokens) if(b.includes(t)) s+=10;
    if(b.endsWith(".geojson")) s+=3;
    const st=fs.statSync(fp);
    return {fp,s,size:st.size,mtime:st.mtimeMs};
  };
  const ranked=files.map(score).sort((a,b)=>(b.s-a.s)||(b.size-a.size)||(b.mtime-a.mtime));
  return ranked[0].fp;
}

// geometry
function geomBbox(geom){
  let minX=Infinity,minY=Infinity,maxX=-Infinity,maxY=-Infinity;
  function scan(coords){
    for(const c of coords){
      if(typeof c[0]==="number" && typeof c[1]==="number"){
        const x=c[0],y=c[1];
        if(x<minX)minX=x; if(y<minY)minY=y; if(x>maxX)maxX=x; if(y>maxY)maxY=y;
      } else scan(c);
    }
  }
  if(!geom) return null;
  if(geom.type==="Polygon"||geom.type==="MultiPolygon") scan(geom.coordinates); else return null;
  if(!isFinite(minX)||!isFinite(minY)||!isFinite(maxX)||!isFinite(maxY)) return null;
  return [minX,minY,maxX,maxY];
}
function pointInRing(pt,ring){
  const x=pt[0],y=pt[1];
  let inside=false;
  for(let i=0,j=ring.length-1;i<ring.length;j=i++){
    const xi=ring[i][0], yi=ring[i][1];
    const xj=ring[j][0], yj=ring[j][1];
    const intersect=((yi>y)!==(yj>y)) && (x < (xj-xi)*(y-yi)/((yj-yi)||1e-12)+xi);
    if(intersect) inside=!inside;
  }
  return inside;
}
function pointInPolygon(pt,poly){
  if(!poly||poly.length===0) return false;
  if(!pointInRing(pt,poly[0])) return false;
  for(let i=1;i<poly.length;i++) if(pointInRing(pt,poly[i])) return false;
  return true;
}
function geomContainsPoint(geom,pt){
  if(!geom) return false;
  if(geom.type==="Polygon") return pointInPolygon(pt,geom.coordinates);
  if(geom.type==="MultiPolygon"){ for(const p of geom.coordinates) if(pointInPolygon(pt,p)) return true; }
  return false;
}
function bboxContainsPoint(b,pt){ return pt[0]>=b[0]&&pt[0]<=b[2]&&pt[1]>=b[1]&&pt[1]<=b[3]; }

function pickProp(props,keys){
  if(!props) return null;
  for(const k of keys){
    if(props[k]!=null) return String(props[k]).trim();
    const lk=k.toLowerCase();
    for(const pk of Object.keys(props)) if(pk.toLowerCase()===lk && props[pk]!=null) return String(props[pk]).trim();
  }
  return null;
}
function normalizeZip(z){
  if(!z) return null;
  const m=String(z).match(/\d{5}/);
  return m?m[0]:String(z).trim();
}
function safeJsonParse(line){ try{ return {ok:true,v:JSON.parse(line)}; }catch{ return {ok:false,v:null}; } }
function toISODateOnly(s){ return (/^\d{4}-\d{2}-\d{2}$/.test(s))?s:null; }
function argsParse(argv){
  const out={};
  for(let i=2;i<argv.length;i++){
    const a=argv[i];
    if(!a.startsWith("--")) continue;
    const k=a.slice(2);
    const v=(i+1<argv.length && !argv[i+1].startsWith("--"))?argv[++i]:true;
    out[k]=v;
  }
  return out;
}

async function main(){
  const args=argsParse(process.argv);

  const asOfDate=toISODateOnly(args.asOfDate);
  if(!asOfDate) die("Missing/invalid --asOfDate (YYYY-MM-DD)");

  const contractIn=args.contractIn;
  if(!contractIn||!isFile(contractIn)) die(`--contractIn must be file: ${contractIn}`);

  const overlayFreezeDir=args.overlayFreezeDir;
  const contractFreezeDir=args.contractFreezeDir;
  if(!overlayFreezeDir) die("Missing --overlayFreezeDir");
  if(!contractFreezeDir) die("Missing --contractFreezeDir");

  ensureDir(overlayFreezeDir); ensureDir(contractFreezeDir);

  const backendRoot=process.cwd();

  const townsRoot=path.join(backendRoot,"publicData","boundaries","_statewide","towns");
  const zipsRoot =path.join(backendRoot,"publicData","boundaries","_statewide","zipcodes","zipcodes");
  const mbtaRoot =path.join(backendRoot,"publicData","boundaries","_statewide","mbta","mbta");
  const schoolsFile=path.join(backendRoot,"publicData","schoolData","ccuv_districts.geojson");
  const rpaZip=path.join(backendRoot,"publicData","boundaries","_statewide","Regional Planning Agencies.zip");

  const layerRes=[];
  const townsFile=pickGeoJSONUnder(townsRoot,["town","towns","muni","municipal"]); if(!townsFile) die("No towns geojson found");
  const zipsFile =pickGeoJSONUnder(zipsRoot, ["zip","zcta"]); if(!zipsFile) die("No zip geojson found");
  const mbtaFile =pickGeoJSONUnder(mbtaRoot, ["mbta","community","service","district"]); if(!mbtaFile) die("No mbta geojson found");
  if(!isFile(schoolsFile)) die("Missing schools file: "+schoolsFile);

  layerRes.push({layer_key:"civic_towns", source_path:townsFile});
  layerRes.push({layer_key:"civic_zipcodes", source_path:zipsFile});
  layerRes.push({layer_key:"civic_mbta", source_path:mbtaFile});
  layerRes.push({layer_key:"civic_school_districts", source_path:schoolsFile});

  // stage raw RPA zip only
  let rpaStaged=null;
  if(isFile(rpaZip)){
    const rawDir=path.join(overlayFreezeDir,"_raw","regional_planning_agencies"); ensureDir(rawDir);
    const dst=path.join(rawDir,path.basename(rpaZip)); fs.copyFileSync(rpaZip,dst);
    rpaStaged=dst;
  }

  const layers=[];
  for(const res of layerRes){
    const dstDir=path.join(overlayFreezeDir,res.layer_key); ensureDir(dstDir);
    const dst=path.join(dstDir,path.basename(res.source_path));
    fs.copyFileSync(res.source_path,dst);
    const datasetHash=await sha256FileStream(dst);

    const geo=JSON.parse(fs.readFileSync(dst,"utf8"));
    if(!geo||geo.type!=="FeatureCollection"||!Array.isArray(geo.features)) die("Bad layer FeatureCollection: "+dst);

    const feats=geo.features.filter(f=>f&&f.type==="Feature"&&f.geometry&&(f.geometry.type==="Polygon"||f.geometry.type==="MultiPolygon"));
    const idx=feats.map((f,i)=>{
      const bbox=geomBbox(f.geometry);
      const props=f.properties||{};
      const name=pickProp(props,["NAME","Name","TOWN","TOWN_NAME","MUNI","MUNICIPALITY","DISTRICT","DIST_NAME","PLAN_NAME"]) ||
                 pickProp(props,["GEOID","GEOID10","GEOID20","ZCTA5CE10","ZIP","ZIPCODE"]) || null;
      const code=pickProp(props,["TOWN_ID","TOWNID","GEOID","GEOID10","GEOID20","ZCTA5CE10","ZIP","ZIPCODE","DIST_ID","DISTRICT_ID"]) || null;
      const seed=`${res.layer_key}|${i}|${name||""}|${code||""}|${bbox?bbox.join(","):""}`;
      const feature_id=crypto.createHash("sha256").update(seed).digest("hex").slice(0,12);
      return {feature_id,bbox,geom:f.geometry,props,name,code};
    });

    fs.writeFileSync(path.join(dstDir,"LAYER_META.json"), JSON.stringify({
      layer_key: res.layer_key,
      as_of_date: asOfDate,
      dataset_hash: datasetHash,
      source_path: res.source_path,
      frozen_path: dst,
      feature_count_total: geo.features.length,
      feature_count_polygon: idx.length
    }, null, 2));

    layers.push({layerKey:res.layer_key, frozen:dst, datasetHash, idx});
  }

  const asOfCompact=asOfDate.replaceAll("-","");
  const contractOut=path.join(contractFreezeDir, `contract_view_phase2_civic_core__${asOfCompact}.ndjson`);
  const attachmentsOut=path.join(overlayFreezeDir, "PHASE2__attachments.ndjson");
  const runReportPath=path.join(overlayFreezeDir, "PHASE2__RUN_REPORT.json");

  console.log("====================================================");
  console.log(" Phase 2 — Statewide Core Civic Boundaries (v1)");
  console.log(` as_of_date: ${asOfDate}`);
  console.log(` contract_in: ${contractIn}`);
  console.log(` contract_out: ${contractOut}`);
  console.log("====================================================");

  const inputHash=crypto.createHash("sha256");
  const outHash=crypto.createHash("sha256");
  const attHash=crypto.createHash("sha256");

  const outStream=fs.createWriteStream(contractOut,{encoding:"utf8"});
  const attStream=fs.createWriteStream(attachmentsOut,{encoding:"utf8"});

  let remainder="";
  let readLines=0,wroteLines=0,badJson=0,anyCivic=0,attachmentsWritten=0;

  function findMatches(layer,pt){
    const hits=[];
    for(const f of layer.idx){
      if(!f.bbox) continue;
      if(!bboxContainsPoint(f.bbox,pt)) continue;
      if(geomContainsPoint(f.geom,pt)) hits.push(f);
    }
    return hits;
  }

  const inStream=fs.createReadStream(contractIn);
  inStream.on("data",(chunk)=>{
    inputHash.update(chunk);
    remainder += chunk.toString("utf8");

    let nl;
    while((nl=remainder.indexOf("\n"))>=0){
      const line=remainder.slice(0,nl).trim();
      remainder=remainder.slice(nl+1);
      if(!line) continue;

      readLines++;
      const parsed=safeJsonParse(line);
      if(!parsed.ok){ badJson++; continue; }
      const rec=parsed.v;

      const property_id=rec.property_id;
      const lon=(typeof rec.parcel_centroid_lon==="number")?rec.parcel_centroid_lon:
                (typeof rec.longitude==="number")?rec.longitude:null;
      const lat=(typeof rec.parcel_centroid_lat==="number")?rec.parcel_centroid_lat:
                (typeof rec.latitude==="number")?rec.latitude:null;

      let civic_count=0;
      const civic_keys=[];
      let town_name=null, zip_code=null, school_name=null;
      const mbta_keys=[];

      if(property_id && lon!=null && lat!=null){
        const pt=[lon,lat];

        for(const layer of layers){
          const hits=findMatches(layer,pt);
          if(hits.length===0) continue;

          if(layer.layerKey==="civic_towns"){
            const h=hits[0];
            town_name=h.name || pickProp(h.props,["TOWN","TOWN_NAME","NAME","MUNI","MUNICIPALITY"]) || town_name;
            civic_keys.push("town"); civic_count+=1;
          } else if(layer.layerKey==="civic_zipcodes"){
            const h=hits[0];
            zip_code=normalizeZip(h.code||h.name||pickProp(h.props,["ZIP","ZIPCODE","ZCTA5CE10","GEOID10","GEOID20"])) || zip_code;
            civic_keys.push("zip"); civic_count+=1;
          } else if(layer.layerKey==="civic_school_districts"){
            const h=hits[0];
            school_name=h.name || pickProp(h.props,["DISTRICT","DIST_NAME","NAME","LEA_NAME","ORG_NAME"]) || school_name;
            civic_keys.push("school"); civic_count+=1;
          } else if(layer.layerKey==="civic_mbta"){
            for(const h of hits){
              const k=h.code||h.name||pickProp(h.props,["NAME","DISTRICT","ZONE","TYPE","MBTA"])||h.feature_id;
              if(k) mbta_keys.push(String(k));
            }
            civic_keys.push("mbta"); civic_count+=1;
          }

          for(const h of hits){
            const att={
              property_id,
              layer_key: layer.layerKey,
              feature_id: h.feature_id,
              feature_name: h.name,
              feature_code: h.code,
              attach_method: "pip_parcel_centroid",
              attach_as_of_date: asOfDate,
              attach_confidence: rec.coord_confidence_grade || null,
              layer_dataset_hash: layer.datasetHash
            };
            const s=JSON.stringify(att);
            attStream.write(s+"\n");
            attHash.update(Buffer.from(s+"\n","utf8"));
            attachmentsWritten++;
          }
        }
      }

      const outRec={
        ...rec,
        civic_as_of_date: asOfDate,
        civic_input_contract_hash: null,
        civic_has_any_core_boundary: civic_count>0,
        civic_core_count: civic_count,
        civic_core_keys: civic_keys,
        civic_town_name: town_name,
        civic_zip_code: zip_code,
        civic_school_district_name: school_name,
        civic_mbta_keys: mbta_keys,
        civic_mbta_count: mbta_keys.length
      };

      if(outRec.civic_has_any_core_boundary) anyCivic++;

      const outLine=JSON.stringify(outRec);
      outStream.write(outLine+"\n");
      outHash.update(Buffer.from(outLine+"\n","utf8"));
      wroteLines++;

      if(readLines % 200000 === 0){
        console.log(`[prog] read=${readLines} wrote=${wroteLines} anyCivic=${anyCivic} bad_json=${badJson} attachments=${attachmentsWritten}`);
      }
    }
  });

  await new Promise((resolve,reject)=>{
    inStream.on("error",reject);
    inStream.on("end",resolve);
  });

  outStream.end(); attStream.end();
  await Promise.all([
    new Promise(res=>outStream.on("finish",res)),
    new Promise(res=>attStream.on("finish",res))
  ]);

  const inputDigest=inputHash.digest("hex").toUpperCase();
  const outDigest=outHash.digest("hex").toUpperCase();
  const attDigest=attHash.digest("hex").toUpperCase();

  fs.writeFileSync(runReportPath, JSON.stringify({
    kind: "phase2_statewide_core_civic",
    as_of_date: asOfDate,
    contract_in: contractIn,
    contract_in_sha256: inputDigest,
    contract_out: contractOut,
    contract_out_sha256: outDigest,
    overlays_freeze_dir: overlayFreezeDir,
    attachments_out: attachmentsOut,
    attachments_sha256: attDigest,
    layers: layers.map(l=>({layer_key:l.layerKey,frozen_path:l.frozen,dataset_hash:l.datasetHash,feature_count_polygon:l.idx.length})),
    raw_rpa_zip_staged: rpaStaged,
    stats: { contract_read: readLines, contract_written: wroteLines, any_civic_lines: anyCivic, bad_json: badJson, attachments_written: attachmentsWritten }
  }, null, 2));

  fs.writeFileSync(path.join(overlayFreezeDir,"MANIFEST.json"), JSON.stringify({
    kind: "MANIFEST",
    phase: "PHASE2_STATEWIDE_CORE_CIVIC",
    as_of_date: asOfDate,
    created_at: new Date().toISOString(),
    contract_in_sha256: inputDigest,
    contract_out_sha256: outDigest,
    attachments_sha256: attDigest,
    raw_rpa_zip_staged: rpaStaged
  }, null, 2));

  console.log(`[done] read=${readLines} wrote=${wroteLines} anyCivic=${anyCivic} bad_json=${badJson}`);
  console.log(`[done] Phase 2 statewide core civic complete.`);
}

main().catch(e=>{ console.error("[fatal] "+(e?.stack||e?.message||String(e))); process.exit(1); });
