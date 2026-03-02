import fs from "fs";
import readline from "readline";

function getArg(k){
  const a = process.argv.slice(2);
  const i = a.indexOf(k);
  return i >= 0 ? a[i+1] : null;
}

function bestLatLon(obj){
  const candidates = [
    ["centroid_lat","centroid_lon"],
    ["lat","lon"], ["latitude","longitude"],
    ["lat","lng"], ["latitude","lng"],
    ["y","x"]
  ];
  for (const [a,b] of candidates){
    if (obj?.[a] != null && obj?.[b] != null){
      const lat = Number(obj[a]);
      const lon = Number(obj[b]);
      if (Number.isFinite(lat) && Number.isFinite(lon)) return { lat, lon };
    }
  }
  return null;
}

function isZoned(obj){
  // Try common shapes; we only need a reliable “has base zoning” signal
  const zb = obj?.zoning_base || obj?.zoningBase || obj?.base_zoning;
  if (zb && (zb.district_code_norm || zb.zoning_code || zb.district_code_raw || zb.district_name_norm)) return true;

  const z = obj?.zoning;
  if (z?.base && (z.base.district_code_norm || z.base.zoning_code || z.base.district_code_raw)) return true;

  const attach = obj?.zoning_base_attach || obj?.zoningBaseAttach || obj?.zoning_attach;
  if (attach && (attach.attach_confidence === "A" || attach.attach_confidence === "B")) return true;

  // Fallback: if they stored a “zoning_status”
  const st = (obj?.zoning_status || obj?.zoningStatus || "").toString().toLowerCase();
  if (st && st !== "unknown" && st !== "none") return true;

  return false;
}

async function run(){
  const inPath = getArg("--in");
  const onlyZoned = (getArg("--onlyZoned") || "1") === "1";
  const marginDeg = Number(getArg("--marginDeg") || "0.05");

  if (!inPath) {
    console.error("Usage: node compute_zoned_bbox_from_properties_v1.mjs --in <properties.ndjson> [--onlyZoned 1] [--marginDeg 0.05]");
    process.exit(1);
  }

  let minLat =  999, minLon =  999, maxLat = -999, maxLon = -999;
  let total = 0, used = 0, usedZoned = 0;

  const rl = readline.createInterface({ input: fs.createReadStream(inPath, "utf8"), crlfDelay: Infinity });
  for await (const line of rl){
    if (!line) continue;
    total++;
    let obj;
    try { obj = JSON.parse(line); } catch { continue; }

    if (onlyZoned && !isZoned(obj)) continue;

    const ll = bestLatLon(obj);
    if (!ll) continue;

    const { lat, lon } = ll;
    // MA sanity
    if (lat < 41 || lat > 43.5 || lon > -69.5 || lon < -73.6) continue;

    used++;
    if (onlyZoned) usedZoned++;

    if (lat < minLat) minLat = lat;
    if (lat > maxLat) maxLat = lat;
    if (lon < minLon) minLon = lon;
    if (lon > maxLon) maxLon = lon;
  }

  if (used === 0) {
    console.error("[fatal] bbox tool found 0 usable points (check centroid fields / zoning filter).");
    process.exit(1);
  }

  // Expand bbox slightly
  minLat -= marginDeg; maxLat += marginDeg;
  minLon -= marginDeg; maxLon += marginDeg;

  const out = {
    total_lines_seen: total,
    used_points: used,
    used_zoned_points: usedZoned,
    margin_deg: marginDeg,
    minLat, minLon, maxLat, maxLon
  };

  process.stdout.write(JSON.stringify(out));
}

run().catch(e => { console.error(e); process.exit(1); });
