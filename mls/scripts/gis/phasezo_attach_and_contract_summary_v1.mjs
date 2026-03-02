import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import readline from "node:readline";

function parseArgs(argv){
  const o={};
  for(let i=2;i<argv.length;i++){
    const a=argv[i];
    if(a.startsWith("--")){
      const k=a.slice(2);
      const v=(argv[i+1] && !argv[i+1].startsWith("--")) ? argv[++i] : "true";
      o[k]=v;
    }
  }
  return o;
}

function sha256Text(s){ return crypto.createHash("sha256").update(s).digest("hex").toUpperCase(); }
function sha256File(fp){
  const h=crypto.createHash("sha256");
  const fd=fs.openSync(fp,"r");
  const buf=Buffer.alloc(1024*1024);
  try{
    while(true){
      const n=fs.readSync(fd,buf,0,buf.length,null);
      if(n<=0) break;
      h.update(buf.subarray(0,n));
    }
  } finally { fs.closeSync(fd); }
  return h.digest("hex").toUpperCase();
}

function normCity(x){
  return String(x||"").trim().toLowerCase()
    .replace(/[\s,]+/g,"_")
    .replace(/[^a-z0-9_]/g,"")
    .replace(/_+/g,"_");
}

function getPath(obj,p){
  if(!obj||!p) return undefined;
  const parts=String(p).split(".");
  let cur=obj;
  for(const part of parts){
    if(cur && Object.prototype.hasOwnProperty.call(cur,part)) cur=cur[part];
    else return undefined;
  }
  return cur;
}
function pick(obj,cands){
  for(const c of cands){
    if(!c) continue;
    const v = c.includes(".") ? getPath(obj,c) : (obj ? obj[c] : undefined);
    if(v!==undefined && v!==null && v!=="") return v;
  }
  return undefined;
}

function toNumberOrNull(v){
  if(v===undefined||v===null||v==="") return null;
  const n=Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * Ray casting point-in-ring. ring is array of [x,y] (lon,lat).
 * Returns true if inside.
 */
function pointInRing(pt, ring){
  const x=pt[0], y=pt[1];
  let inside=false;
  for(let i=0, j=ring.length-1; i<ring.length; j=i++){
    const xi=ring[i][0], yi=ring[i][1];
    const xj=ring[j][0], yj=ring[j][1];
    const intersect = ((yi>y)!==(yj>y)) && (x < (xj-xi)*(y-yi)/((yj-yi)||1e-30) + xi);
    if(intersect) inside=!inside;
  }
  return inside;
}

function pointInPolygonCoords(pt, polyCoords){
  // polyCoords: [ [ring], [hole], ... ]
  if(!polyCoords || polyCoords.length===0) return false;
  const outer = polyCoords[0];
  if(!pointInRing(pt, outer)) return false;
  // holes
  for(let k=1;k<polyCoords.length;k++){
    if(pointInRing(pt, polyCoords[k])) return false;
  }
  return true;
}

function pointInGeometry(pt, geom){
  if(!geom) return false;
  if(geom.type==="Polygon") return pointInPolygonCoords(pt, geom.coordinates);
  if(geom.type==="MultiPolygon"){
    for(const poly of geom.coordinates){
      if(pointInPolygonCoords(pt, poly)) return true;
    }
    return false;
  }
  return false;
}

function bboxOfGeometry(geom){
  // returns [minX,minY,maxX,maxY] for Polygon/MultiPolygon
  let minX=Infinity,minY=Infinity,maxX=-Infinity,maxY=-Infinity;
  const pushCoord=(c)=>{ const x=c[0], y=c[1]; if(x<minX)minX=x; if(y<minY)minY=y; if(x>maxX)maxX=x; if(y>maxY)maxY=y; };
  const walkRing=(ring)=>{ for(const c of ring) pushCoord(c); };
  if(!geom) return null;
  if(geom.type==="Polygon"){
    for(const ring of geom.coordinates) walkRing(ring);
  } else if(geom.type==="MultiPolygon"){
    for(const poly of geom.coordinates) for(const ring of poly) walkRing(ring);
  } else return null;
  if(!Number.isFinite(minX)) return null;
  return [minX,minY,maxX,maxY];
}

function inBbox(pt,b){
  const x=pt[0], y=pt[1];
  return x>=b[0] && x<=b[2] && y>=b[1] && y<=b[3];
}

function safeMkdir(p){ fs.mkdirSync(p,{recursive:true}); }

function guessFeatureCode(props){
  if(!props) return {raw:"", norm:"", label:""};
  const raw = pick(props,[
    "DISTRICT","District","district","DIST","Dist","CODE","Code","code",
    "SUBDIST","SUBDISTRICT","Subdistrict","subdistrict",
    "OVERLAY","Overlay","overlay",
    "NAME","Name","name","LABEL","Label","label","ID","Id","id","OBJECTID","objectid"
  ]) ?? "";
  const label = pick(props,["NAME","Name","name","LABEL","Label","label"]) ?? raw;
  const norm = String(raw||"").trim().toLowerCase().replace(/\s+/g,"_").replace(/[^a-z0-9_]/g,"");
  return {raw:String(raw||""), norm, label:String(label||"")};
}

function loadGeojson(fp){
  const txt = fs.readFileSync(fp,"utf8");
  const gj = JSON.parse(txt);
  if(!gj || !Array.isArray(gj.features)) throw new Error(`Invalid GeoJSON (no features): ${fp}`);
  return gj;
}

function normalizeCrsName(gj){
  // accept CRS84 or EPSG:4326; we do not reproject here
  const crsName = (gj.crs && gj.crs.properties && gj.crs.properties.name) ? String(gj.crs.properties.name) : "";
  return crsName;
}

async function main(){
  const args=parseArgs(process.argv);
  const root = args.root ? path.resolve(args.root) : process.cwd();
  const propertiesPath = args.properties ? path.resolve(args.properties) : null;
  const contractViewIn = args.contractViewIn ? path.resolve(args.contractViewIn) : null;
  const manifestPath = args.manifest ? path.resolve(args.manifest) : null;
  const asOfDate = args.asOfDate || "UNKNOWN";
  const outDir = args.outDir ? path.resolve(args.outDir) : null;

  if(!propertiesPath || !fs.existsSync(propertiesPath)) throw new Error(`--properties missing/not found: ${propertiesPath}`);
  if(!contractViewIn || !fs.existsSync(contractViewIn)) throw new Error(`--contractViewIn missing/not found: ${contractViewIn}`);
  if(!manifestPath || !fs.existsSync(manifestPath)) throw new Error(`--manifest missing/not found: ${manifestPath}`);
  if(!outDir) throw new Error(`--outDir required`);

  safeMkdir(outDir);

  const manifest = JSON.parse(fs.readFileSync(manifestPath,"utf8"));
  const cityEntries = (manifest.cities || []).map(c => ({...c, city: normCity(c.city)}));
  const cityMap = new Map(cityEntries.map(c => [c.city, c]));
  const enabledCities = new Set((args.cities ? String(args.cities).split(",") : (manifest.default_cities||[])).map(normCity).filter(Boolean));

  const propertiesHash = sha256File(propertiesPath);
  const contractHash = sha256File(contractViewIn);

  const runMeta = {
    created_at: new Date().toISOString(),
    phase: "PHASE_ZO_MUNICIPAL_ZONING_OVERLAYS",
    as_of_date: asOfDate,
    root,
    properties: { path: propertiesPath, sha256: propertiesHash },
    contract_view_in: { path: contractViewIn, sha256: contractHash },
    manifest: { path: manifestPath, sha256: sha256Text(JSON.stringify(manifest)) },
    cities: Array.from(enabledCities),
  };
  fs.writeFileSync(path.join(outDir,"RUN_META.json"), JSON.stringify(runMeta,null,2));

  // Load overlay layers per city
  const overlaysByCity = new Map(); // city -> [{layer_key, src, src_hash, features:[{feature_id,bbox,geom,code,label,rawProps}]}]
  for(const city of enabledCities){
    const ce = cityMap.get(city);
    if(!ce){ console.error(`[warn] city not in manifest: ${city} (skip)`); continue; }
    const layers=[];
    for(const ov of (ce.overlays||[])){
      const src = path.resolve(root, ov.rel_path);
      if(!fs.existsSync(src)){
        console.error(`[warn] missing overlay file (skip): ${city} :: ${ov.layer_key} -> ${src}`);
        continue;
      }
      const gj = loadGeojson(src);
      const crsName = normalizeCrsName(gj);
      const srcHash = sha256File(src);
      const feats=[];
      let kept=0, dropped=0;
      for(const f of gj.features){
        const geom=f && f.geometry;
        if(!geom || (geom.type!=="Polygon" && geom.type!=="MultiPolygon")) { dropped++; continue; }
        const bbox = bboxOfGeometry(geom);
        if(!bbox) { dropped++; continue; }
        const props = f.properties || {};
        const code = guessFeatureCode(props);
        // deterministic feature id: layer_key + objectid + bbox + code
        const objid = pick(props,["OBJECTID","objectid","ObjectID","id","ID"]) ?? "";
        const fid = sha256Text(JSON.stringify({layer_key:ov.layer_key, objid, bbox, code_raw:code.raw, srcHash}));
        feats.push({
          feature_id: fid,
          object_id: objid,
          bbox,
          geometry: geom,
          code_raw: code.raw,
          code_norm: code.norm,
          label: code.label,
          properties: props
        });
        kept++;
      }

      const layerOut = path.join(outDir,"overlays",city,ov.layer_key);
      safeMkdir(layerOut);
      // copy geojson into out for freezing
      fs.copyFileSync(src, path.join(layerOut, path.basename(src)));
      fs.writeFileSync(path.join(layerOut,"SOURCE_META.json"), JSON.stringify({
        city, layer_key: ov.layer_key, source_path: src, source_sha256: srcHash, crs: crsName,
        features_total: gj.features.length, features_kept: kept, features_dropped: dropped, as_of_date: asOfDate,
        properties_sha256: propertiesHash
      }, null, 2));

      // write feature catalog
      const catPath = path.join(layerOut, "FEATURE_CATALOG.ndjson");
      const catWs = fs.createWriteStream(catPath,{encoding:"utf8"});
      for(const ft of feats){
        const rec = {
          feature_id: ft.feature_id,
          layer_key: ov.layer_key,
          feature_type: "polygon",
          jurisdiction_type: "city",
          jurisdiction_name: ce.label || city,
          jurisdiction_city: city,
          source_system: "local_geojson",
          source_path: src,
          source_sha256: srcHash,
          as_of_date: asOfDate,
          bbox: ft.bbox,
          code_raw: ft.code_raw,
          code_norm: ft.code_norm,
          label: ft.label
        };
        catWs.write(JSON.stringify(rec)+"\n");
      }
      catWs.end();

      layers.push({ layer_key: ov.layer_key, label: ov.notes || "", source_path: src, source_sha256: srcHash, features: feats });
    }
    overlaysByCity.set(city, layers);
  }

  // Step 1: stream properties, build attachments + aggregation
  console.error(`[run] attach overlays (properties stream)`);
  const aggLayerKeys = new Map(); // property_id -> Set(layer_key)
  const aggCodes = new Map();     // property_id -> Set(code_norm)
  const aggFeatureCount = new Map(); // property_id -> number

  // open attachments writers per layer
  const attachmentWriters = new Map(); // key city|layer -> ws
  for(const [city,layers] of overlaysByCity.entries()){
    for(const layer of layers){
      const layerOut = path.join(outDir,"overlays",city,layer.layer_key);
      const ap = path.join(layerOut,"ATTACHMENTS.ndjson");
      attachmentWriters.set(`${city}|${layer.layer_key}`, fs.createWriteStream(ap,{encoding:"utf8"}));
    }
  }

  const rlProps = readline.createInterface({ input: fs.createReadStream(propertiesPath,{encoding:"utf8"}), crlfDelay: Infinity });
  let read=0, wroteAttach=0, skipped=0;
  for await (const line of rlProps){
    const t=line.trim(); if(!t) continue;
    read++;
    let row; try{ row=JSON.parse(t); } catch { skipped++; continue; }

    const property_id = pick(row,["property_id","propertyId","id"]);
    if(!property_id){ skipped++; continue; }

    const cityRaw = pick(row,["source_city","town","address_city","city"]);
    const city = normCity(cityRaw);
    if(!enabledCities.has(city)) continue;

    const lat = toNumberOrNull(pick(row,["parcel_centroid_lat","centroid_lat","lat","latitude"]));
    const lon = toNumberOrNull(pick(row,["parcel_centroid_lon","centroid_lon","lon","lng","longitude"]));
    if(lat===null || lon===null) { continue; }
    const pt=[lon,lat];

    const layers = overlaysByCity.get(city) || [];
    for(const layer of layers){
      // find matching features
      for(const ft of layer.features){
        if(!inBbox(pt, ft.bbox)) continue;
        if(!pointInGeometry(pt, ft.geometry)) continue;

        // write attachment row
        const ws = attachmentWriters.get(`${city}|${layer.layer_key}`);
        const attach = {
          property_id: String(property_id),
          feature_id: ft.feature_id,
          layer_key: layer.layer_key,
          attach_method: "pip_centroid",
          distance_m: null,
          attach_confidence: "B",
          attach_as_of_date: asOfDate,
          source_city: city,
          evidence: {
            properties_sha256: propertiesHash,
            overlay_sha256: layer.source_sha256,
            overlay_source_path: layer.source_path
          },
          feature_code_raw: ft.code_raw,
          feature_code_norm: ft.code_norm,
          feature_label: ft.label
        };
        ws.write(JSON.stringify(attach)+"\n");
        wroteAttach++;

        // aggregate
        if(!aggLayerKeys.has(property_id)) aggLayerKeys.set(property_id, new Set());
        aggLayerKeys.get(property_id).add(layer.layer_key);

        if(ft.code_norm){
          if(!aggCodes.has(property_id)) aggCodes.set(property_id, new Set());
          aggCodes.get(property_id).add(ft.code_norm);
        }

        aggFeatureCount.set(property_id, (aggFeatureCount.get(property_id)||0)+1);
      }
    }

    if(read % 200000 === 0) console.error(`[prog] properties_read=${read} attachments_written=${wroteAttach} agg_props=${aggLayerKeys.size} skipped_json=${skipped}`);
  }

  for(const ws of attachmentWriters.values()) ws.end();
  console.error(`[done] properties_read=${read} attachments_written=${wroteAttach} agg_props=${aggLayerKeys.size}`);

  // write aggregation map as NDJSON for audit
  const aggOut = path.join(outDir,"AGG_BY_PROPERTY.ndjson");
  const aggWs = fs.createWriteStream(aggOut,{encoding:"utf8"});
  for(const [pid,setKeys] of aggLayerKeys.entries()){
    const codes = aggCodes.get(pid) ? Array.from(aggCodes.get(pid)).sort() : [];
    aggWs.write(JSON.stringify({
      property_id: String(pid),
      zo_overlay_keys: Array.from(setKeys).sort(),
      zo_overlay_codes: codes,
      zo_overlay_feature_count: aggFeatureCount.get(pid)||0,
      has_zo_overlay: true
    })+"\n");
  }
  aggWs.end();

  // Step 2: apply agg to contract view (flags only)
  console.error(`[run] build contract view PhaseZO summary (flags-only)`);
  const contractOutDir = path.join(outDir,"contract_view");
  safeMkdir(contractOutDir);
  const contractOutPath = path.join(contractOutDir, `contract_view_phasezo__${String(asOfDate).replaceAll("-","")}.ndjson`);
  const cvWs = fs.createWriteStream(contractOutPath,{encoding:"utf8"});
  const rlCv = readline.createInterface({ input: fs.createReadStream(contractViewIn,{encoding:"utf8"}), crlfDelay: Infinity });

  // Build a lightweight lookup (pid -> agg record). Store only arrays to reduce memory.
  const aggLookup = new Map();
  for(const [pid,setKeys] of aggLayerKeys.entries()){
    aggLookup.set(pid, {
      zo_overlay_keys: Array.from(setKeys).sort(),
      zo_overlay_codes: aggCodes.get(pid) ? Array.from(aggCodes.get(pid)).sort() : [],
      zo_overlay_feature_count: aggFeatureCount.get(pid)||0
    });
  }

  let cvRead=0, cvWrote=0, cvSkipped=0, anyZo=0;
  for await (const line of rlCv){
    const t=line.trim(); if(!t) continue;
    cvRead++;
    let row; try{ row=JSON.parse(t); } catch { cvSkipped++; continue; }
    const pid = pick(row,["property_id","propertyId","id"]);
    const agg = pid ? aggLookup.get(pid) : null;

    row.has_zo_overlay = !!agg;
    row.zo_overlay_count = agg ? (agg.zo_overlay_keys.length) : 0;
    row.zo_overlay_keys = agg ? agg.zo_overlay_keys : [];
    row.zo_overlay_feature_count = agg ? agg.zo_overlay_feature_count : 0;
    // keep codes small
    const codes = agg ? agg.zo_overlay_codes : [];
    row.zo_overlay_codes = codes.length > 25 ? codes.slice(0,25) : codes;

    cvWs.write(JSON.stringify(row)+"\n");
    cvWrote++;
    if(agg) anyZo++;

    if(cvRead % 200000 === 0) console.error(`[prog] contract_read=${cvRead} wrote=${cvWrote} anyZo=${anyZo} skipped_json=${cvSkipped}`);
  }
  cvWs.end();
  console.error(`[done] contract_read=${cvRead} wrote=${cvWrote} anyZo_lines=${anyZo}`);

  const stats = {
    phase: "PHASE_ZO_MUNICIPAL_ZONING_OVERLAYS",
    as_of_date: asOfDate,
    properties_read: read,
    attachments_written: wroteAttach,
    agg_properties: aggLayerKeys.size,
    contract_view_read: cvRead,
    contract_view_written: cvWrote,
    contract_view_lines_with_any_zo: anyZo,
    cities: Array.from(enabledCities),
  };
  fs.writeFileSync(path.join(outDir,"STATS.json"), JSON.stringify(stats,null,2));
}

main().catch(err=>{
  console.error("[fatal]", err && err.stack ? err.stack : err);
  process.exit(1);
});
