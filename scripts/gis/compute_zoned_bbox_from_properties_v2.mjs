import fs from "fs";
import readline from "readline";

function getArg(k){
  const a = process.argv.slice(2);
  const i = a.indexOf(k);
  return i >= 0 ? a[i+1] : null;
}

function num(x){
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function coerceLonLat(a, b){
  const x = num(a), y = num(b);
  if (x == null || y == null) return null;

  // try [lon,lat]
  if (y >= 41 && y <= 43.5 && x <= -69.5 && x >= -73.6) return { lon: x, lat: y };
  // try [lat,lon]
  if (x >= 41 && x <= 43.5 && y <= -69.5 && y >= -73.6) return { lon: y, lat: x };

  // generic WGS84 range
  if (Math.abs(x) <= 180 && Math.abs(y) <= 90) {
    // prefer lon=x,lat=y
    return { lon: x, lat: y };
  }
  return null;
}

function findLonLat(obj){
  if (!obj || typeof obj !== "object") return null;

  // direct pairs (top-level)
  const pairs = [
    ["centroid_lon","centroid_lat"],
    ["centroidLon","centroidLat"],
    ["lon","lat"], ["longitude","latitude"],
    ["lng","lat"], ["x","y"]
  ];
  for (const [a,b] of pairs){
    if (obj[a] != null && obj[b] != null){
      const ll = coerceLonLat(obj[a], obj[b]);
      if (ll) return ll;
    }
  }

  // common nested containers
  const nests = ["coord","coords","location","geo","geocode","centroid","parcel_centroid","address_point","point"];
  for (const k of nests){
    if (obj[k] && typeof obj[k] === "object"){
      const ll = findLonLat(obj[k]);
      if (ll) return ll;

      // if it's an array [lon,lat] or [lat,lon]
      if (Array.isArray(obj[k]) && obj[k].length >= 2){
        const ll2 = coerceLonLat(obj[k][0], obj[k][1]);
        if (ll2) return ll2;
      }
    }
  }

  // GeoJSON-ish geometry
  const geom = obj.geometry || obj.geom;
  if (geom && typeof geom === "object"){
    if (geom.type === "Point" && Array.isArray(geom.coordinates) && geom.coordinates.length >= 2){
      const ll = coerceLonLat(geom.coordinates[0], geom.coordinates[1]);
      if (ll) return ll;
    }
  }

  // GeoJSON feature
  if (obj.type === "Feature" && obj.geometry){
    const ll = findLonLat(obj.geometry);
    if (ll) return ll;
  }

  return null;
}

function isProbablyZoned(obj){
  if (!obj || typeof obj !== "object") return false;

  // obvious objects
  if (obj.zoning_base || obj.base_zoning || obj.zoningBase || obj.baseZoning) return true;
  if (obj.zoning_base_attach || obj.zoningBaseAttach || obj.zoning_attach) return true;

  // if zoning is a non-empty string
  if (typeof obj.zoning === "string" && obj.zoning.trim()) return true;

  // if zoning is an object with keys
  if (obj.zoning && typeof obj.zoning === "object" && Object.keys(obj.zoning).length > 0) return true;

  // any top-level keys that look like zoning outputs
  for (const k of Object.keys(obj)){
    const s = k.toLowerCase();
    const v = obj[k];
    if (v == null) continue;
    if (typeof v === "string" && !v.trim()) continue;

    if (s.startsWith("zoning_")) return true;
    if (s.includes("zoning") && s.includes("district")) return true;
    if (s.includes("district_code") && s.includes("norm")) return true;
  }

  return false;
}

async function run(){
  const inPath = getArg("--in");
  const onlyZoned = (getArg("--onlyZoned") || "1") === "1";
  const marginDeg = Number(getArg("--marginDeg") || "0.05");

  if (!inPath) {
    console.error("Usage: node compute_zoned_bbox_from_properties_v2.mjs --in <properties.ndjson> [--onlyZoned 1] [--marginDeg 0.05]");
    process.exit(1);
  }

  let minLat =  999, minLon =  999, maxLat = -999, maxLon = -999;
  let total = 0, parsed = 0, used = 0, usedZoned = 0;
  let firstKeys = null;

  const rl = readline.createInterface({ input: fs.createReadStream(inPath, "utf8"), crlfDelay: Infinity });
  for await (const line of rl){
    if (!line) continue;
    total++;

    let obj;
    try { obj = JSON.parse(line); parsed++; } catch { continue; }
    if (!firstKeys) firstKeys = Object.keys(obj).slice(0, 40);

    if (onlyZoned && !isProbablyZoned(obj)) continue;

    const ll = findLonLat(obj);
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
    console.error("[fatal] bbox tool found 0 usable points.");
    console.error("  parsed lines:", parsed, "total lines:", total);
    console.error("  onlyZoned:", onlyZoned);
    console.error("  sample top-level keys (first record):", firstKeys || []);
    process.exit(1);
  }

  minLat -= marginDeg; maxLat += marginDeg;
  minLon -= marginDeg; maxLon += marginDeg;

  const out = {
    total_lines_seen: total,
    parsed_lines: parsed,
    used_points: used,
    used_zoned_points: usedZoned,
    margin_deg: marginDeg,
    minLat, minLon, maxLat, maxLon
  };

  process.stdout.write(JSON.stringify(out));
}

run().catch(e => { console.error(e); process.exit(1); });
