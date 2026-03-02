// backend/avm/selection/minNeighborhoodFilter.js
// -----------------------------------------------------
// Neighborhood-first filter for CMA/AVM comp selection.
// Uses polygon boundaries (GeoJSON) to ensure we stay
// inside the SAME NEIGHBORHOOD before CMA filtering.
// -----------------------------------------------------

const fs = require("fs");
const path = require("path");
const booleanPointInPolygon = require("@turf/boolean-point-in-polygon").default;
const { point } = require("@turf/helpers");

// ---------------------------------------
// Load neighborhood boundaries (BOSTON ONLY)
// ---------------------------------------
let neighborhoodData = {};

(function loadBostonNeighborhoods() {
  try {
    const bostonPath = path.join(
      __dirname,
      "..",
      "..",
      "publicData",
      "boundaries",
      "neighborhoodBoundaries.geojson"
    );

    const raw = fs.readFileSync(bostonPath, "utf8");
    const parsed = JSON.parse(raw);

    neighborhoodData.boston = parsed;

    console.log(
      "[NeighborhoodFilter] Boston neighborhood file loaded:",
      {
        path: bostonPath,
        featureCount: Array.isArray(parsed.features)
          ? parsed.features.length
          : 0,
      }
    );
  } catch (err) {
    console.warn(
      "[NeighborhoodFilter] ERROR loading Boston dataset:",
      err.message
    );
    neighborhoodData = {};
  }
})();

// ---------------------------------------
// Normalize lookup – figure out which dataset to use
// ---------------------------------------
function normalizeTown(subject) {
  if (!subject) return null;

  const town = subject.town || subject.raw?.town;
  const city = subject.city || subject.raw?.city;

  const text = (town || city || "").toString().toLowerCase();

  if (text.includes("boston")) return "boston";

  return null; // unsupported for now
}

// ---------------------------------------
// Identify subject’s neighborhood
// ---------------------------------------
function findNeighborhoodForSubject(subject, townKey) {
  const dataset = neighborhoodData[townKey];

  if (!dataset || !Array.isArray(dataset.features)) {
    console.warn(
      "[NeighborhoodFilter] Dataset missing or invalid for:",
      townKey
    );
    return null;
  }

  if (subject.lat == null || subject.lng == null) {
    console.warn("[NeighborhoodFilter] Subject missing lat/lng.");
    return null;
  }

  const p = point([subject.lng, subject.lat]);

  for (const feat of dataset.features) {
    if (booleanPointInPolygon(p, feat)) {
      const name =
        feat.properties?.name ||
        feat.properties?.Neighborhood ||
        "Unknown";

      console.log(
        "[NeighborhoodFilter] Subject is inside neighborhood:",
        name
      );

      return {
        name,
        polygon: feat,
      };
    }
  }

  console.log(
    "[NeighborhoodFilter] Subject did NOT fall into ANY neighborhood polygon."
  );
  return null;
}

// ---------------------------------------
// Filter comps within same neighborhood polygon
// WITH ADVANCED DIAGNOSTICS
// ---------------------------------------
function filterCompsByNeighborhood(subject, comps) {
  const townKey = normalizeTown(subject);

  console.log("[NeighborhoodFilter] Town lookup:", townKey);

  if (!townKey) {
    console.log("[NeighborhoodFilter] No neighborhood dataset for this town.");
    return {
      comps,
      neighborhoodName: null,
      wasFiltered: false,
      diagnostics: {
        townKey,
        reason: "Town not supported",
      },
    };
  }

  const subjectArea = findNeighborhoodForSubject(subject, townKey);

  if (!subjectArea) {
    console.log(
      "[NeighborhoodFilter] Subject area undefined — no filtering applied."
    );
    return {
      comps,
      neighborhoodName: null,
      wasFiltered: false,
      diagnostics: {
        townKey,
        reason: "Subject not in neighborhood polygon",
      },
    };
  }

  const polygon = subjectArea.polygon;

  const kept = [];
  const dropped = [];

  for (const c of comps || []) {
    const lat = c.lat ?? c.latitude;
    const lng = c.lng ?? c.longitude ?? c.lon;

    if (lat == null || lng == null) {
      dropped.push({
        address: c.address,
        reason: "Missing lat/lng",
      });
      continue;
    }

    const inside = booleanPointInPolygon(point([lng, lat]), polygon);

    if (inside) {
      kept.push(c);
    } else {
      dropped.push({
        address: c.address,
        reason: "Outside polygon",
      });
    }
  }

  console.log(
    `[NeighborhoodFilter] Kept ${kept.length} comps, Dropped ${dropped.length}`
  );

  return {
    // if neighborhood filter yields nothing, fall back to full list
    comps: kept.length ? kept : comps,
    neighborhoodName: subjectArea.name,
    wasFiltered: kept.length > 0,
    diagnostics: {
      townKey,
      subjectNeighborhood: subjectArea.name,
      keptCount: kept.length,
      droppedCount: dropped.length,
      droppedDetails: dropped,
    },
  };
}

module.exports = {
  filterCompsByNeighborhood,
};