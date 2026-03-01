import fs from "node:fs";
import path from "node:path";
import https from "node:https";

function getArg(name, d=null){
  const k=`--${name}`; const i=process.argv.indexOf(k);
  return i>=0 ? process.argv[i+1] : d;
}

const layerUrl = getArg("layerUrl");
const outFile  = getArg("out");
const outSR    = getArg("outSR","4326");
const where    = getArg("where","1=1");
const outFields= getArg("outFields","*");
const pageSize = Number(getArg("pageSize","2000"));
const token    = getArg("token", null);

if(!layerUrl || !outFile){
  console.error("Usage: node arcgisDownloadLayerToGeoJSON_v1.mjs --layerUrl <.../MapServer/ID> --out <file.geojson> [--outSR 4326] [--where 1=1] [--outFields *] [--pageSize 2000] [--token TOKEN]");
  process.exit(1);
}

function stripQuery(u){
  try { const x=new URL(u); x.search=""; x.hash=""; return x.toString(); }
  catch { return u.split("?")[0]; }
}

function fetchJson(url, redirects=0){
  return new Promise((resolve) => {
    const u = new URL(url);
    const req = https.request(u, { method:"GET", headers:{ "User-Agent":"EquityLens-ArcGISDownloader/1.0" } }, (res) => {
      const code = res.statusCode || 0;
      const loc = res.headers.location;
      if([301,302,307,308].includes(code) && loc && redirects < 5){
        const next = new URL(loc, u).toString();
        res.resume();
        return resolve(fetchJson(next, redirects+1));
      }
      let buf=""; res.setEncoding("utf8");
      res.on("data",(d)=>buf+=d);
      res.on("end",()=>{
        try { resolve({ ok: code>=200 && code<300, code, json: JSON.parse(buf) }); }
        catch(e){ resolve({ ok:false, code, err:String(e), raw: buf.slice(0,500) }); }
      });
    });
    req.on("error",(e)=>resolve({ ok:false, code:0, err:String(e) }));
    req.end();
  });
}

function qs(params){
  const u = new URL("https://x/");
  for(const [k,v] of Object.entries(params)){
    if(v===null || v===undefined) continue;
    u.searchParams.set(k,String(v));
  }
  return u.search.slice(1);
}

async function getCount(){
  const base = stripQuery(layerUrl);
  const url = `${base}/query?` + qs({
    where, returnCountOnly: "true", f:"pjson",
    token: token || undefined
  });
  const r = await fetchJson(url);
  if(!r.ok) throw new Error(`count failed (${r.code})`);
  return Number(r.json.count || 0);
}

async function getPage(offset){
  const base = stripQuery(layerUrl);
  const url = `${base}/query?` + qs({
    where,
    outFields,
    returnGeometry: "true",
    f: "geojson",
    outSR,
    resultOffset: offset,
    resultRecordCount: pageSize,
    token: token || undefined
  });
  const r = await fetchJson(url);
  if(!r.ok) throw new Error(`page failed (${r.code})`);
  const feats = (r.json && Array.isArray(r.json.features)) ? r.json.features : [];
  return { fc: r.json, feats };
}

(async () => {
  fs.mkdirSync(path.dirname(outFile), {recursive:true});

  const total = await getCount();
  console.log("[info] count:", total);

  const all = [];
  let offset = 0;

  while(true){
    const { feats } = await getPage(offset);
    if(!feats.length) break;
    all.push(...feats);
    offset += feats.length;
    console.log("[info] fetched:", all.length);
    if(all.length >= total) break;
  }

  const fc = { type:"FeatureCollection", features: all };
  fs.writeFileSync(outFile, JSON.stringify(fc));
  console.log("[done] wrote", outFile, "features:", all.length);
})();
