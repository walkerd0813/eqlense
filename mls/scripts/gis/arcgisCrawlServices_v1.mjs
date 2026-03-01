import fs from "node:fs";
import path from "node:path";
import https from "node:https";

function getArg(name){
  const k = \--\\;
  const i = process.argv.indexOf(k);
  return i >= 0 ? process.argv[i+1] : null;
}

const endpointsFile = getArg("endpointsFile");
const outDir = getArg("outDir");

if(!endpointsFile || !outDir){
  console.error("Usage: node arcgisCrawlServices_v1.mjs --endpointsFile <json> --outDir <dir>");
  process.exit(1);
}

function ensureDir(p){ fs.mkdirSync(p, { recursive:true }); }

function stripQuery(u){
  try { const x = new URL(u); x.search = ""; x.hash = ""; return x.toString(); }
  catch { return u.split("?")[0]; }
}

function addF(u){
  const base = stripQuery(u);
  return base.includes("?") ? base : (base + "?f=pjson");
}

function fetchJson(url, redirects=0){
  return new Promise((resolve) => {
    const u = new URL(url);
    const req = https.request(u, { method:"GET", headers:{ "User-Agent":"EquityLens-GISHarvester/1.0" } }, (res) => {
      const code = res.statusCode || 0;
      const loc = res.headers.location;

      if([301,302,307,308].includes(code) && loc && redirects < 5){
        const next = new URL(loc, u).toString();
        res.resume();
        return resolve(fetchJson(next, redirects+1));
      }

      let buf = "";
      res.setEncoding("utf8");
      res.on("data", (d) => buf += d);
      res.on("end", () => {
        try { resolve({ ok: code>=200 && code<300, code, json: JSON.parse(buf), raw: buf }); }
        catch (e) { resolve({ ok:false, code, err: String(e), raw: buf }); }
      });
    });
    req.on("error", (e)=> resolve({ ok:false, code:0, err:String(e) }));
    req.end();
  });
}

function safeName(s){
  return String(s).replace(/[<>:"/\\\\|?*]+/g, "_").replace(/\\s+/g, "_").slice(0,180);
}

function isServiceUrl(u){
  const b = stripQuery(u).toLowerCase();
  return b.includes("/mapserver") || b.includes("/featureserver") || b.includes("/imageserver");
}

async function crawlDirectory(city, dirUrl, folderPath, out){
  const t0 = Date.now();
  const r = await fetchJson(addF(dirUrl));
  const ms = Date.now() - t0;

  out.endpoints.push({ city, url: dirUrl, ok: r.ok, code: r.code, ms });

  if(!r.ok || !r.json){
    out.errors.push({ city, url: dirUrl, code: r.code, err: r.err || "fetch_failed" });
    return;
  }

  const j = r.json;
  const folders = Array.isArray(j.folders) ? j.folders : [];
  const services = Array.isArray(j.services) ? j.services : [];

  // Save snapshot
  const snapDir = path.join(outDir, "pjson", safeName(city), safeName(folderPath || "_root"));
  ensureDir(snapDir);
  fs.writeFileSync(path.join(snapDir, "_directory.json"), JSON.stringify(j, null, 2));

  // Recurse folders
  for(const f of folders){
    const nextDir = stripQuery(dirUrl) + "/" + encodeURIComponent(f);
    await crawlDirectory(city, nextDir, (folderPath ? (folderPath + "/" + f) : f), out);
  }

  // Services
  for(const s of services){
    const name = s.name;
    const type = s.type;
    const serviceUrl = stripQuery(dirUrl) + "/" + name + "/" + type;
    await crawlService(city, serviceUrl, folderPath, out);
  }
}

async function crawlService(city, serviceUrl, folderPath, out){
  const t0 = Date.now();
  const r = await fetchJson(addF(serviceUrl));
  const ms = Date.now() - t0;

  out.services.push({ city, folder: folderPath || null, serviceUrl, ok: r.ok, code: r.code, ms });

  if(!r.ok || !r.json){
    out.errors.push({ city, url: serviceUrl, code: r.code, err: r.err || "fetch_failed" });
    return;
  }

  const j = r.json;
  const snapDir = path.join(outDir, "pjson", safeName(city), "services");
  ensureDir(snapDir);
  fs.writeFileSync(path.join(snapDir, safeName(serviceUrl) + ".json"), JSON.stringify(j, null, 2));

  const layers = Array.isArray(j.layers) ? j.layers : [];
  for(const L of layers){
    out.layersFlat.push({
      city,
      folder: folderPath || null,
      serviceUrl,
      serviceType: j.type || null,
      mapName: j.mapName || null,
      layerId: L.id,
      layerName: L.name,
      parentLayerId: (L.parentLayerId ?? null),
      subLayerIds: (L.subLayerIds ?? null)
    });
  }
}

const endpoints = JSON.parse(fs.readFileSync(endpointsFile, "utf8"));
const out = { generatedAt: new Date().toISOString(), endpoints: [], services: [], layersFlat: [], errors: [] };

ensureDir(outDir);
fs.writeFileSync(path.join(outDir, "endpoints_used.json"), JSON.stringify(endpoints, null, 2));

for(const e of endpoints){
  if(!e || !e.city) continue;
  if(!e.url){
    out.errors.push({ city: e.city, url: null, code: 0, err: "no_endpoint_url" });
    continue;
  }

  const u = e.url;

  if(isServiceUrl(u)){
    await crawlService(e.city, stripQuery(u), null, out);
  } else {
    await crawlDirectory(e.city, stripQuery(u), null, out);
  }
}

const layersPath = path.join(outDir, "layers_flat.ndjson");
{
  const w = fs.createWriteStream(layersPath, { encoding:"utf8" });
  for(const row of out.layersFlat){
    w.write(JSON.stringify(row) + "\\n");
  }
  w.end();
}

const inv = {
  generatedAt: out.generatedAt,
  endpoints_ok: out.endpoints.filter(x=>x.ok).length,
  endpoints_total: out.endpoints.length,
  services_ok: out.services.filter(x=>x.ok).length,
  services_total: out.services.length,
  layers_total: out.layersFlat.length,
  errors: out.errors.length
};

fs.writeFileSync(path.join(outDir, "inventory.json"), JSON.stringify(inv, null, 2));
fs.writeFileSync(path.join(outDir, "crawl_report.json"), JSON.stringify(out, null, 2));

console.log("✅ ArcGIS crawl complete");
console.log(inv);
console.log("layers_flat:", layersPath);
