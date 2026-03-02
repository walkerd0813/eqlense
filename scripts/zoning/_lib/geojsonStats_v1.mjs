import fs from "fs";

function walkCoords(coords, acc) {
  if (!coords) return;
  // leaf coordinate: [x,y]
  if (typeof coords[0] === "number" && typeof coords[1] === "number") {
    const x = coords[0], y = coords[1];
    if (x < acc.minX) acc.minX = x;
    if (y < acc.minY) acc.minY = y;
    if (x > acc.maxX) acc.maxX = x;
    if (y > acc.maxY) acc.maxY = y;
    return;
  }
  for (const c of coords) walkCoords(c, acc);
}

const p = process.argv[2];
if (!p) {
  console.error("Usage: node geojsonStats_v1.mjs <path-to-geojson>");
  process.exit(2);
}

const txt = fs.readFileSync(p, "utf8");
const gj = JSON.parse(txt);

const features = (gj && Array.isArray(gj.features)) ? gj.features.length : 0;

const acc = { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity };

if (gj && Array.isArray(gj.features)) {
  for (const f of gj.features) {
    const g = f && f.geometry;
    if (!g || !g.coordinates) continue;
    walkCoords(g.coordinates, acc);
  }
}

const bbox = (acc.minX !== Infinity) ? [acc.minX, acc.minY, acc.maxX, acc.maxY] : null;

process.stdout.write(JSON.stringify({ features, bbox }));
