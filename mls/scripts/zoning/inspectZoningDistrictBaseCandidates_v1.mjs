import fs from "fs";
import path from "path";

function arg(name, def=null){
  const i = process.argv.indexOf(name);
  if(i>=0 && i+1<process.argv.length) return process.argv[i+1];
  return def;
}

const zoningRoot = arg("--zoningRoot");
const outCsvAll  = arg("--outCsvAll");
const outCsvPick = arg("--outCsvPick");

if(!zoningRoot) {
  console.error("[ERR] missing --zoningRoot");
  process.exit(2);
}

const MA = { minX:-73.6, maxX:-69.5, minY:41.0, maxY:43.7 }; // generous bounds

const badNameTokens = [
  "water","sewer","storm","drain","utility","utilities",
  "historic","historical","local_historic","landmark",
  "flood","wetland","conservation","open_space","openspace",
  "neighborhood","planning","landuse","land_use","future",
  "overlay","overlays","subdistrict","subdistricts"
];

function escCsv(v){
  const s = (v===null||v===undefined) ? "" : String(v);
  if(/[",\n]/.test(s)) return `"${s.replace(/"/g,'""')}"`;
  return s;
}
function toCsvRow(obj, cols){
  return cols.map(c => escCsv(obj[c])).join(",");
}

function walkCoords(coords, cb){
  if(!coords) return;
  if(typeof coords[0] === "number"){
    const x = coords[0], y = coords[1];
    if(Number.isFinite(x) && Number.isFinite(y)) cb(x,y);
    return;
  }
  for(const c of coords) walkCoords(c, cb);
}

function scanGeom(geom, cb){
  if(!geom) return;
  const t = geom.type;
  if(t === "Point") cb(geom.coordinates[0], geom.coordinates[1]);
  else if(t === "MultiPoint" || t === "LineString") walkCoords(geom.coordinates, cb);
  else if(t === "MultiLineString" || t === "Polygon") walkCoords(geom.coordinates, cb);
  else if(t === "MultiPolygon") walkCoords(geom.coordinates, cb);
  else if(t === "GeometryCollection"){
    for(const g of (geom.geometries||[])) scanGeom(g, cb);
  }
}

function computeBBox(fc){
  let minX=Infinity,minY=Infinity,maxX=-Infinity,maxY=-Infinity;
  let polys=0, lines=0, points=0, other=0;

  const feats = fc?.features || [];
  for(const f of feats){
    const g = f?.geometry;
    if(!g){ other++; continue; }
    if(g.type === "Polygon" || g.type === "MultiPolygon") polys++;
    else if(g.type?.includes("Line")) lines++;
    else if(g.type?.includes("Point")) points++;
    else other++;

    scanGeom(g, (x,y)=>{
      if(x<minX) minX=x;
      if(y<minY) minY=y;
      if(x>maxX) maxX=x;
      if(y>maxY) maxY=y;
    });
  }
  const has = Number.isFinite(minX) && Number.isFinite(minY) && Number.isFinite(maxX) && Number.isFinite(maxY);
  return {
    has,
    minX: has?minX:null, minY: has?minY:null, maxX: has?maxX:null, maxY: has?maxY:null,
    polys, lines, points, other
  };
}

function inMA(bb){
  if(!bb.has) return false;
  // normal case
  const ok = (bb.minX >= MA.minX && bb.maxX <= MA.maxX && bb.minY >= MA.minY && bb.maxY <= MA.maxY);
  return ok;
}

function swappedLooksLikeMA(bb){
  // sometimes lat/lon swapped => X ~ 42, Y ~ -71
  if(!bb.has) return false;
  const xLooksLat = (bb.minX>=MA.minY && bb.maxX<=MA.maxY);
  const yLooksLon = (bb.minY>=MA.minX && bb.maxY<=MA.maxX);
  return xLooksLat && yLooksLon;
}

function scoreFile(town, filePath, fileName, featsCount, geomStats, bb){
  let s = 0;
  const n = fileName.toLowerCase();

  if(n === "zoning_base.geojson") s += 120;
  if(n.includes("zoning_base__")) s += 100;
  if(n.includes("zoning")) s += 40;
  if(n.includes("district")) s += 25;
  if(n.includes("zones")) s += 10;

  // penalize obvious non-base layers
  for(const tok of badNameTokens){
    if(n.includes(tok)) s -= 90;
  }

  // geometry preference
  if(geomStats.polys > 0 && geomStats.lines===0 && geomStats.points===0) s += 25;
  if(geomStats.lines>0 || geomStats.points>0) s -= 60;

  // size/feature heuristics
  if(featsCount >= 50) s += 15;
  if(featsCount >= 500) s += 10;
  if(featsCount <= 5) s -= 30;

  // bounds gating
  if(inMA(bb)) s += 200;
  else if(swappedLooksLikeMA(bb)) s -= 250; // still “bad” because needs fix
  else s -= 1000;

  return s;
}

function listTowns(root){
  return fs.readdirSync(root, { withFileTypes:true })
    .filter(d => d.isDirectory())
    .map(d => d.name)
    .filter(n => !n.startsWith("_"));
}

const towns = listTowns(zoningRoot);

const colsAll = [
  "town","districtsDir","file","fileName","sizeMB","features",
  "polys","lines","points","other",
  "minX","minY","maxX","maxY",
  "inMA","swappedLikeMA","score","note"
];

const rowsAll = [];
const picks = [];

for(const town of towns){
  const districtsDir = path.join(zoningRoot, town, "districts");
  if(!fs.existsSync(districtsDir) || !fs.statSync(districtsDir).isDirectory()){
    continue;
  }
  const files = fs.readdirSync(districtsDir).filter(f => f.toLowerCase().endsWith(".geojson"));
  let best = null;

  for(const fileName of files){
    const filePath = path.join(districtsDir, fileName);
    const sizeMB = +(fs.statSync(filePath).size / (1024*1024)).toFixed(2);

    let fc=null, featsCount=0, geomStats={polys:0,lines:0,points:0,other:0}, bb={has:false};
    let note = "";
    try{
      const txt = fs.readFileSync(filePath, "utf8");
      fc = JSON.parse(txt);
      if(fc?.type !== "FeatureCollection"){
        note = "NOT_FEATURECOLLECTION";
      }
      featsCount = Array.isArray(fc?.features) ? fc.features.length : 0;
      bb = computeBBox(fc);
      geomStats = { polys: bb.polys, lines: bb.lines, points: bb.points, other: bb.other };
    }catch(e){
      note = "PARSE_ERR";
      // keep bb.has=false
    }

    const okMA = inMA(bb);
    const swapped = swappedLooksLikeMA(bb);
    const score = scoreFile(town, filePath, fileName, featsCount, geomStats, bb);

    const row = {
      town,
      districtsDir,
      file: filePath,
      fileName,
      sizeMB,
      features: featsCount,
      polys: geomStats.polys,
      lines: geomStats.lines,
      points: geomStats.points,
      other: geomStats.other,
      minX: bb.has ? +bb.minX.toFixed(6) : "",
      minY: bb.has ? +bb.minY.toFixed(6) : "",
      maxX: bb.has ? +bb.maxX.toFixed(6) : "",
      maxY: bb.has ? +bb.maxY.toFixed(6) : "",
      inMA: okMA ? "true" : "false",
      swappedLikeMA: swapped ? "true" : "false",
      score,
      note: note || (okMA ? "" : (swapped ? "LIKELY_SWAPPED_LATLON" : "BAD_BOUNDS_OUTSIDE_MA"))
    };
    rowsAll.push(row);

    if(!best || row.score > best.score){
      best = row;
    }
  }

  if(best){
    picks.push({
      town: best.town,
      selectedScore: best.score,
      selectedSizeMB: best.sizeMB,
      selectedFile: best.file,
      features: best.features,
      minX: best.minX, minY: best.minY, maxX: best.maxX, maxY: best.maxY,
      note: best.note
    });
  }
}

// write CSVs
if(outCsvAll){
  fs.writeFileSync(outCsvAll, colsAll.join(",") + "\n" + rowsAll.map(r=>toCsvRow(r, colsAll)).join("\n"), "utf8");
}
if(outCsvPick){
  const colsPick = ["town","selectedScore","selectedSizeMB","features","minX","minY","maxX","maxY","selectedFile","note"];
  fs.writeFileSync(outCsvPick, colsPick.join(",") + "\n" + picks.map(r=>toCsvRow(r, colsPick)).join("\n"), "utf8");
}

// console output: picked list
picks.sort((a,b)=> (b.selectedScore - a.selectedScore) || (b.features - a.features));
console.log("town\tselectedScore\tfeatures\tbbox(minX,minY,maxX,maxY)\tselectedFile\tnote");
for(const p of picks){
  console.log(`${p.town}\t${p.selectedScore}\t${p.features}\t(${p.minX},${p.minY},${p.maxX},${p.maxY})\t${p.selectedFile}\t${p.note||""}`);
}

const bad = picks.filter(p => (p.note||"").includes("BAD_BOUNDS") || (p.note||"").includes("PARSE_ERR") || (p.note||"").includes("SWAPPED"));
if(bad.length){
  console.log("\n[WARN] Towns needing attention (bad bounds / parse / swapped):");
  for(const b of bad){
    console.log(` - ${b.town}: ${b.note} :: ${b.selectedFile}`);
  }
}
