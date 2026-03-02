/**
 * batchGeocodeNeighborhoods.js
 * Loads neighborhood boundaries, computes centroid coordinates,
 * and generates lookup-ready structure.
 */

const fs = require("fs");
const path = require("path");
const centroid = require("@turf/centroid").default;

const INPUT = path.join(__dirname, "../../publicData/boundaries/neighborhoodBoundaries.geojson");
const OUTPUT = path.join(__dirname, "../../publicData/neighborhood/neighborhoodCache.json");

(async () => {
  console.log("📍 Loading neighborhood boundaries…");

  const raw = fs.readFileSync(INPUT, "utf8");
  const geo = JSON.parse(raw);

  const result = [];

  for (const f of geo.features) {
    const center = centroid(f).geometry.coordinates;

    result.push({
      name: f.properties?.NAME || null,
      centroidLng: center[0],
      centroidLat: center[1],
      geometry: f.geometry,
    });
  }

  fs.writeFileSync(OUTPUT, JSON.stringify(result, null, 2));

  console.log("✅ Neighborhood centroid file created.");
})();

