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
function loadZones(p){
  const fc = JSON.parse(fs.readFileSync(p,"utf8"));
  const feats = fc.features || [];
  return feats.map(f => {
    const props = f.properties || {};
    const bbox = turf.bbox(f);
    return {
      bbox,
      geom: f,
      zone_code: props.zone_code ?? null,
      zone_name: props.zone_name ?? null,
      zone_label: props.zone_label ?? null,
      el_city: props.el_city ?? props.city ?? null,
      el_kind: props.el_kind ?? null,
      el_source_layer: props.el_source_layer ?? props.source_layer ?? null,
      el_source_url: props.el_source_url ?? props.source_url ?? null,
      el_ingested_at: props.el_ingested_at ?? props.ingested_at ?? null
    };
  });
}
function inBbox(pt, b){
  const x=pt[0], y=pt[1];
  return x>=b[0] && x<=b[2] && y>=b[1] && y<=b[3];
}
function pickLatLon(obj){
  const lat = obj.lat ?? obj.latitude ?? obj.y ?? obj.centroid?.lat ?? obj.centroid?.latitude ?? obj.centroid?.y;
  const lon = obj.lon ?? obj.lng ?? obj.longitude ?? obj.x ?? obj.centroid?.lon ?? obj.centroid?.lng ?? obj.centroid?.longitude ?? obj.centroid?.x;
  if(typeof lat === "number" && typeof lon === "number") return {lat, lon};
  return null;
}
function pickParcelId(obj){
  return obj.parcel_id ?? obj.parcelId ?? obj.LOC_ID ?? obj.loc_id ?? obj.MAP_PAR_ID ?? obj.map_par_id ?? obj.PARCEL_ID ?? obj.par_id ?? obj.id;
}
function matchOne(pt, zones){
  const hits = [];
  for(const z of zones){
    if(!inBbox(pt, z.bbox)) continue;
    if(turf.booleanPointInPolygon(turf.point(pt), z.geom)){
      hits.push(z);
    }
  }
  if(hits.length === 1) return { hit: hits[0], hitsCount: 1 };
  if(hits.length > 1) return { hit: hits[0], hitsCount: hits.length, ambiguous: true };
  return { hit: null, hitsCount: 0 };
}
function matchMany(pt, zones){
  const hits = [];
  for(const z of zones){
    if(!inBbox(pt, z.bbox)) continue;
    if(turf.booleanPointInPolygon(turf.point(pt), z.geom)){
      hits.push(z);
    }
  }
  return hits;
}

async function main(){
  const a=args();
  const centroids=a.centroids, zBase=a.zoningBase, zOv=a.zoningOverlay, zSplit=a.zoningSplit, out=a.out, reportPath=a.report;
  if(!centroids || !zBase || !out || !reportPath) {
    throw new Error("Usage: --centroids <centroids.ndjson> --zoningBase <base_std.geojson> [--zoningOverlay <overlay_std.geojson>] [--zoningSplit <split_std.geojson>] --out <out.ndjson> --report <report.json>");
  }

  const baseZones = loadZones(zBase);
  const ovZones = zOv ? loadZones(zOv) : [];
  const splitZones = zSplit ? loadZones(zSplit) : [];

  const rl = readline.createInterface({ input: fs.createReadStream(centroids,"utf8"), crlfDelay: Infinity });
  const ws = fs.createWriteStream(out,{encoding:"utf8"});

  let total=0, ok=0, missing=0, baseHit=0, baseAmb=0, ovHit=0, splitHit=0;

  for await (const line of rl){
    if(!line.trim()) continue;
    total++;
    let obj;
    try { obj=JSON.parse(line); } catch { missing++; continue; }
    const pid = pickParcelId(obj);
    const ll = pickLatLon(obj);
    if(pid==null || !ll){ missing++; continue; }

    const pt = [ll.lon, ll.lat];

    const b = matchOne(pt, baseZones);
    if(b.hit) baseHit++;
    if(b.ambiguous) baseAmb++;

    const ovs = ovZones.length ? matchMany(pt, ovZones) : [];
    if(ovs.length) ovHit++;

    const splits = splitZones.length ? matchMany(pt, splitZones) : [];
    if(splits.length) splitHit++;

    const confidence =
      b.hit && !b.ambiguous ? 0.95 :
      b.hit && b.ambiguous  ? 0.60 :
      0.00;

    const row = {
      parcel_id: pid,
      centroid: { lat: ll.lat, lon: ll.lon },
      zoning: {
        base: b.hit ? {
          zone_code: b.hit.zone_code,
          zone_name: b.hit.zone_name,
          zone_label: b.hit.zone_label,
          source_layer: b.hit.el_source_layer,
          source_url: b.hit.el_source_url,
          ingested_at: b.hit.el_ingested_at,
          hitsCount: b.hitsCount,
          ambiguous: !!b.ambiguous
        } : null,
        overlays: ovs.map(z => ({
          zone_code: z.zone_code,
          zone_name: z.zone_name,
          zone_label: z.zone_label,
          source_layer: z.el_source_layer,
          source_url: z.el_source_url,
          ingested_at: z.el_ingested_at
        })),
        split: splits.map(z => ({
          zone_code: z.zone_code,
          zone_name: z.zone_name,
          zone_label: z.zone_label,
          source_layer: z.el_source_layer,
          source_url: z.el_source_url,
          ingested_at: z.el_ingested_at
        }))
      },
      audit: {
        join_method: "centroid_point_in_polygon",
        confidence,
        note: "Zoning is informational; evidence-first; not a legal determination."
      }
    };

    ws.write(JSON.stringify(row) + "\n");
    ok++;
  }

  ws.end();

  const report = {
    centroids, centroids_sha256: await sha256File(centroids),
    zoningBase: zBase, zoningBase_sha256: await sha256File(zBase),
    zoningOverlay: zOv || null,
    zoningSplit: zSplit || null,
    out,
    totals: { total, ok, missing_rows: missing },
    stats: { baseHit, baseAmbiguous: baseAmb, overlayAnyHit: ovHit, splitAnyHit: splitHit }
  };

  fs.writeFileSync(reportPath, JSON.stringify(report,null,2), "utf8");
  console.log(`[ok] attach -> ${out}`);
  console.log(`[ok] report -> ${reportPath}`);
}
main().catch(e=>{ console.error("[ERR]", e.message||e); process.exit(1); });
