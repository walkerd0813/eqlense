// Example: mls/scripts/civicEnrichment.js (or wherever you enrich listings)

import path from "node:path";
import turf from "@turf/helpers";
import { loadCivicLayers, civicLookup } from "../zoning/modules/civicLookup.js";
import { loadWaterLayers, waterProximityLookup } from "../zoning/modules/waterProximity.js";

const DATA_ROOT = path.resolve(process.cwd(), "backend/publicData");

async function main() {
  // 1) Preload civic + water layers once
  const civicLayers = loadCivicLayers({ DATA_ROOT });
  const waterLayers = loadWaterLayers({ DATA_ROOT });

  // 2) Stream through listingsWithZoning / listingsWithCoords
  //    (pseudo-code; plug into your existing NDJSON loop)
  for await (const listing of eachListingWithCoords()) {
    const { latitude: lat, longitude: lng } = listing;
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      writeOut(listing);
      continue;
    }

    const pt = turf.point([lng, lat]);

    // Civic (neighborhood, ward, etc.)
    const civic = civicLookup(civicLayers, pt);  // neighborhoods, wards, etc.

    // 🌊 Water proximity
    const waterInfo = waterProximityLookup(waterLayers, pt);

    const enriched = {
      ...listing,
      civic: {
        ...(civic || {}),
        water: waterInfo
          ? {
              onWater: !!waterInfo.onWater,
              waterName: waterInfo.waterName || null,
              distanceMeters: waterInfo.distanceMeters ?? null,
              raw: waterInfo.attrs || {},
            }
          : {
              onWater: false,
              waterName: null,
              distanceMeters: null,
            },
      },
    };

    writeOut(enriched);
  }
}
