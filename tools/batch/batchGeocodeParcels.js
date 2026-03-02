/**
 * batchGeocodeParcels.js
 * Converts parcel polygons into:
 * - centroid lat/lng
 * - parcel shapeScore
 */

const fs = require("fs");
const path = require("path");
const centroid = require("@turf/centroid").default;
const { geocodeAddress } = require("../../publicData/geocoding/geocodeAddress");

const PARCEL_PATH = path.join(__dirname, "../../publicData/parcels/parcelBoundaryData.geojson");
const OUTPUT = path.join(__dirname, "../../publicData/parcels/parcelCache.json");

(async () => {
  console.log("📦 Loading parcel dataset…");

  const raw = fs.readFileSync(PARCEL_PATH, "utf8");
  const geojson = JSON.parse(raw);

  const results = [];

  for (const f of geojson.features) {
    const center = centroid(f);
    const [lng, lat] = center.geometry.coordinates;

    const parcel = {
      apn: f.properties?.APN || null,
      shape: f.geometry,
      lat,
      lng,
      shapeScore: f.properties?.SHAPE_Area ? 1 / Math.log(f.properties.SHAPE_Area + 1) : 0.5,
    };

    results.push(parcel);
  }

  fs.writeFileSync(OUTPUT, JSON.stringify(results, null, 2));
  console.log("✅ Parcel centroid processing complete.");
})();

