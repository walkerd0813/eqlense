import fs from "node:fs";
import path from "node:path";
import proj4 from "proj4";

process.on("unhandledRejection", (e) => { console.error(e); process.exit(1); });
process.on("uncaughtException", (e) => { console.error(e); process.exit(1); });

// EPSG:26986 (NAD83 / Massachusetts Mainland) -> WGS84
const EPSG26986 = "+proj=lcc +lat_1=41.71666666666667 +lat_2=42.68333333333333 +lat_0=41 +lon_0=-71.5 +x_0=200000 +y_0=750000 +datum=NAD83 +units=m +no_defs";
proj4.defs("EPSG:26986", EPSG26986);
proj4.defs("EPSG:4326", proj4.WGS84);

const inPath = process.argv[2];
const outPath = process.argv[3];

if (!inPath || !outPath) {
  console.error("Usage: node reprojectTownBoundaries_26986_to_wgs84_v1.mjs <in.geojson> <out.geojson>");
  process.exit(1);
}
if (!fs.existsSync(inPath)) {
  console.error("Input not found:", inPath);
  process.exit(1);
}
fs.mkdirSync(path.dirname(outPath), { recursive: true });

function walkCoords(coords, fn) {
  if (typeof coords?.[0] === "number" && typeof coords?.[1] === "number") {
    return fn(coords);
  }
  return coords.map(c => walkCoords(c, fn));
}

const g = JSON.parse(fs.readFileSync(inPath, "utf8"));

for (const f of (g.features || [])) {
  if (!f.geometry) continue;
  f.geometry.coordinates = walkCoords(f.geometry.coordinates, ([x, y]) => {
    const [lon, lat] = proj4("EPSG:26986", "EPSG:4326", [x, y]);
    return [lon, lat];
  });
}

fs.writeFileSync(outPath, JSON.stringify(g), "utf8");
console.log("DONE:", outPath);
setTimeout(() => process.exit(0), 200).unref();
