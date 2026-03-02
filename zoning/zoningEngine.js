// backend/zoning/ZoningEngineV4.js
// ESM module. Requires:
//   npm i @turf/boolean-point-in-polygon @turf/distance @turf/helpers
//
// Usage:
//   import { lookupZoningAndCivic } from "./ZoningEngineV4.js";
//   const result = lookupZoningAndCivic({ lat: 42.36, lon: -71.06 });

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

import booleanPointInPolygon from "@turf/boolean-point-in-polygon";
import distance from "@turf/distance";
import { point } from "@turf/helpers";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// -----------------------------------------------------------------------------
// PATH CONFIG – matches your actual folders
// -----------------------------------------------------------------------------

const PROJECT_ROOT = path.resolve(__dirname, ".."); // backend/
const DATA_ROOT = path.resolve(PROJECT_ROOT, "publicData");

// --- ZONING (Boston) ---
const ZONING_BOSTON_ROOT = path.join(DATA_ROOT, "zoning", "boston");
const ZONING_SUBDISTRICTS_ROOT = path.join(ZONING_BOSTON_ROOT, "subdistricts");
const ZONING_DISSOLVED_ROOT = path.join(ZONING_BOSTON_ROOT, "dissolved");

// Already present:
const zoningDistrictsCandidates = [
  path.join(ZONING_BOSTON_ROOT, "zoningDistricts_dissolved.geojson"),
];

// Already present:
const zoningSubdistrictsCandidates = [
  path.join(ZONING_SUBDISTRICTS_ROOT, "zoningSubdistricts_dissolved.geojson"),
];

// To be created via GDAL (you'll generate these into /dissolved):
const gcodCandidates = [path.join(ZONING_DISSOLVED_ROOT, "gcod.geojson")];
const floodCandidates = [path.join(ZONING_DISSOLVED_ROOT, "flood.geojson")];
const mainStreetCandidates = [
  path.join(ZONING_DISSOLVED_ROOT, "mainstreet.geojson"),
];

// --- BOUNDARIES ---
const BOUNDARIES_ROOT = path.join(DATA_ROOT, "boundaries");

const boundaryFiles = {
  // city outline (optional, mainly for safety / masking)
  cityOutline: [
    path.join(
      BOUNDARIES_ROOT,
      "cityOutline",
      "City_of_Boston_Outline_Boundary_Water_Included.geojson"
    ),
  ],

  neighborhoods: [path.join(BOUNDARIES_ROOT, "neighborhoodBoundaries.geojson")],

  wards: [
    path.join(
      BOUNDARIES_ROOT,
      "political",
      "Boston_Ward_Boundaries.geojson"
    ),
  ],

  policeDistricts: [
    path.join(BOUNDARIES_ROOT, "police", "Police_Districts.geojson"),
  ],

  fireDistricts: [
    path.join(BOUNDARIES_ROOT, "fire", "Fire_Districts.geojson"),
  ],

  trashDays: [
    path.join(
      BOUNDARIES_ROOT,
      "trash",
      "Trash_Collection_Days.geojson"
    ),
  ],

  // both snow layers (per your choice C)
  snowParking: [
    path.join(
      BOUNDARIES_ROOT,
      "snowEmergency",
      "Snow_Emergency_Parking.geojson"
    ),
  ],
  snowRoutes: [
    path.join(
      BOUNDARIES_ROOT,
      "snowEmergency",
      "Snow_Emergency_Routes.geojson"
    ),
  ],

  hydroPolygon: [
    path.join(BOUNDARIES_ROOT, "water", "Hydrography_Polygon.geojson"),
  ],

  mbtaEntries: [
    path.join(
      BOUNDARIES_ROOT,
      "mbta",
      "MBTA_Gated_Station_Entries.geojson"
    ),
  ],

  hospitals: [path.join(BOUNDARIES_ROOT, "hospitals", "Hospitals.geojson")],

  communityCenters: [
    path.join(
      BOUNDARIES_ROOT,
      "communityCenters",
      "Community_Centers.geojson"
    ),
  ],

  transportationDistricts: [
    path.join(
      BOUNDARIES_ROOT,
      "transportation",
      "Boston_Transportation_Department_BTD_Districts_.geojson"
    ),
  ],

  zipcodes: [path.join(BOUNDARIES_ROOT, "zipcodes", "ZIP_Codes.geojson")],
};

// -----------------------------------------------------------------------------
// HELPERS – loading + geometry utils
// -----------------------------------------------------------------------------

function loadGeoJSONFromCandidates(candidates, label) {
  for (const p of candidates) {
    if (fs.existsSync(p)) {
      const raw = fs.readFileSync(p, "utf8");
      const json = JSON.parse(raw);
      if (!json || !Array.isArray(json.features)) {
        throw new Error(
          `[ZoningEngineV4] GeoJSON ${label} at ${p} has no features array`
        );
      }
      console.log(
        `[ZoningEngineV4] Loaded ${label} from: ${p} (${json.features.length} features)`
      );
      return json;
    }
  }

  console.warn(
    `[ZoningEngineV4] WARNING: No file found for ${label}. Checked: ${candidates.join(
      ", "
    )}`
  );
  return { type: "FeatureCollection", features: [] };
}

/**
 * Returns first polygon/multipolygon that contains the point.
 */
function firstPolygonHit(pt, collection) {
  for (const feat of collection.features) {
    if (!feat.geometry) continue;
    if (booleanPointInPolygon(pt, feat)) return feat;
  }
  return null;
}

/**
 * Returns all polygons/multipolygons that contain the point.
 */
function allPolygonHits(pt, collection) {
  const hits = [];
  for (const feat of collection.features) {
    if (!feat.geometry) continue;
    if (booleanPointInPolygon(pt, feat)) hits.push(feat);
  }
  return hits;
}

/**
 * Approximate nearest feature (Point / Polygon / MultiPolygon / LineString).
 * Returns { feature, distanceKm } or null.
 */
function nearestFeature(pt, collection) {
  let best = null;
  let bestDist = Infinity;

  for (const feat of collection.features) {
    if (!feat.geometry) continue;

    let targetPoint;

    switch (feat.geometry.type) {
      case "Point":
        targetPoint = feat;
        break;
      case "Polygon":
        if (!feat.geometry.coordinates?.[0]?.[0]) continue;
        targetPoint = point(feat.geometry.coordinates[0][0]);
        break;
      case "MultiPolygon":
        if (!feat.geometry.coordinates?.[0]?.[0]?.[0]) continue;
        targetPoint = point(feat.geometry.coordinates[0][0][0]);
        break;
      case "LineString":
        if (!feat.geometry.coordinates?.[0]) continue;
        targetPoint = point(feat.geometry.coordinates[0]);
        break;
      case "MultiLineString":
        if (!feat.geometry.coordinates?.[0]?.[0]) continue;
        targetPoint = point(feat.geometry.coordinates[0][0]);
        break;
      default:
        continue;
    }

    const d = distance(pt, targetPoint); // km
    if (d < bestDist) {
      bestDist = d;
      best = feat;
    }
  }

  if (!best || bestDist === Infinity) return null;
  return { feature: best, distanceKm: bestDist };
}

function getProp(feat, keys) {
  if (!feat || !feat.properties) return null;
  for (const k of keys) {
    if (feat.properties[k] !== undefined && feat.properties[k] !== null) {
      return feat.properties[k];
    }
  }
  return null;
}

// -----------------------------------------------------------------------------
// LOAD ALL LAYERS ONCE (startup)
// -----------------------------------------------------------------------------

const layers = {
  zoning: {
    districts: loadGeoJSONFromCandidates(
      zoningDistrictsCandidates,
      "zoningDistricts"
    ),
    subdistricts: loadGeoJSONFromCandidates(
      zoningSubdistrictsCandidates,
      "zoningSubdistricts"
    ),
    gcod: loadGeoJSONFromCandidates(gcodCandidates, "gcod"),
    flood: loadGeoJSONFromCandidates(floodCandidates, "flood"),
    mainstreet: loadGeoJSONFromCandidates(
      mainStreetCandidates,
      "mainstreet"
    ),
  },
  boundaries: {
    cityOutline: loadGeoJSONFromCandidates(
      boundaryFiles.cityOutline,
      "cityOutline"
    ),
    neighborhoods: loadGeoJSONFromCandidates(
      boundaryFiles.neighborhoods,
      "neighborhoods"
    ),
    wards: loadGeoJSONFromCandidates(boundaryFiles.wards, "wards"),
    policeDistricts: loadGeoJSONFromCandidates(
      boundaryFiles.policeDistricts,
      "policeDistricts"
    ),
    fireDistricts: loadGeoJSONFromCandidates(
      boundaryFiles.fireDistricts,
      "fireDistricts"
    ),
    trashDays: loadGeoJSONFromCandidates(
      boundaryFiles.trashDays,
      "trashDays"
    ),
    snowParking: loadGeoJSONFromCandidates(
      boundaryFiles.snowParking,
      "snowParking"
    ),
    snowRoutes: loadGeoJSONFromCandidates(
      boundaryFiles.snowRoutes,
      "snowRoutes"
    ),
    hydroPolygon: loadGeoJSONFromCandidates(
      boundaryFiles.hydroPolygon,
      "hydroPolygon"
    ),
    mbtaEntries: loadGeoJSONFromCandidates(
      boundaryFiles.mbtaEntries,
      "mbtaEntries"
    ),
    hospitals: loadGeoJSONFromCandidates(
      boundaryFiles.hospitals,
      "hospitals"
    ),
    communityCenters: loadGeoJSONFromCandidates(
      boundaryFiles.communityCenters,
      "communityCenters"
    ),
    transportationDistricts: loadGeoJSONFromCandidates(
      boundaryFiles.transportationDistricts,
      "transportationDistricts"
    ),
    zipcodes: loadGeoJSONFromCandidates(
      boundaryFiles.zipcodes,
      "zipcodes"
    ),
  },
};

// -----------------------------------------------------------------------------
// MAIN LOOKUP
// -----------------------------------------------------------------------------

/**
 * Zoning + civic + proximity lookup.
 * @param {{ lat: number, lon: number }} param0
 * @returns {object} zoningResult
 */
export function lookupZoningAndCivic({ lat, lon }) {
  if (
    typeof lat !== "number" ||
    Number.isNaN(lat) ||
    typeof lon !== "number" ||
    Number.isNaN(lon)
  ) {
    throw new Error("lookupZoningAndCivic requires numeric lat & lon");
  }

  const pt = point([lon, lat]);

  // --- ZONING ---
  const districtFeat = firstPolygonHit(pt, layers.zoning.districts);
  const subdistrictFeat = firstPolygonHit(pt, layers.zoning.subdistricts);

  const gcodHits = allPolygonHits(pt, layers.zoning.gcod);
  const floodHits = allPolygonHits(pt, layers.zoning.flood);
  const mainStreetHits = allPolygonHits(pt, layers.zoning.mainstreet);

  // --- CIVIC BOUNDARIES ---
  const neighborhoodFeat = firstPolygonHit(
    pt,
    layers.boundaries.neighborhoods
  );
  const wardFeat = firstPolygonHit(pt, layers.boundaries.wards);
  const policeFeat = firstPolygonHit(pt, layers.boundaries.policeDistricts);
  const fireFeat = firstPolygonHit(pt, layers.boundaries.fireDistricts);
  const trashFeat = firstPolygonHit(pt, layers.boundaries.trashDays);
  const hydroFeat = firstPolygonHit(pt, layers.boundaries.hydroPolygon);
  const zipcodeFeat = firstPolygonHit(pt, layers.boundaries.zipcodes);
  const transportFeat = firstPolygonHit(
    pt,
    layers.boundaries.transportationDistricts
  );

  // --- SNOW: PARKING + ROUTES (choice C) ---
  const snowParkingFeat = firstPolygonHit(
    pt,
    layers.boundaries.snowParking
  );

  // Treat routes as "corridor" using nearest distance
  const nearestSnowRoute = nearestFeature(pt, layers.boundaries.snowRoutes);
  const SNOW_ROUTE_BUFFER_KM = 0.05; // ~50m corridor

  const inSnowRouteCorridor =
    nearestSnowRoute && nearestSnowRoute.distanceKm <= SNOW_ROUTE_BUFFER_KM;

  // --- PROXIMITY (HOSPITAL, COMMUNITY CENTER, MBTA) ---
  const nearestHospital = nearestFeature(pt, layers.boundaries.hospitals);
  const nearestCommunityCenter = nearestFeature(
    pt,
    layers.boundaries.communityCenters
  );
  const nearestMbtaEntry = nearestFeature(
    pt,
    layers.boundaries.mbtaEntries
  );

  // ---------------------------------------------------------------------------
  // BUILD RESULT OBJECT
  // ---------------------------------------------------------------------------

  const result = {
    coordinate: { lat, lon },

    zoning: {
      district: districtFeat
        ? {
            code: getProp(districtFeat, [
              "ZONING_",
              "ZONING",
              "DISTRICT",
              "DIST_ID",
            ]),
            name: getProp(districtFeat, [
              "ZONE_NAME",
              "DIST_NAME",
              "LABEL",
              "DESCRIPTIO",
            ]),
            raw: districtFeat.properties,
          }
        : null,

      subdistrict: subdistrictFeat
        ? {
            code: getProp(subdistrictFeat, [
              "SUBDIST",
              "SUBDIST_ID",
              "SUBDISTRIC",
            ]),
            name: getProp(subdistrictFeat, [
              "SUBDIST_N",
              "NAME",
              "LABEL",
              "DESCRIPTIO",
            ]),
            raw: subdistrictFeat.properties,
          }
        : null,

      overlays: {
        gcod: {
          present: gcodHits.length > 0,
          count: gcodHits.length,
          overlays: gcodHits.map((f) => ({
            id: getProp(f, ["GCOD_ID", "OBJECTID", "ID"]),
            name: getProp(f, ["NAME", "LABEL", "DESCRIPTIO"]),
            raw: f.properties,
          })),
        },
        flood: {
          present: floodHits.length > 0,
          count: floodHits.length,
          overlays: floodHits.map((f) => ({
            id: getProp(f, ["FLOOD_ID", "OBJECTID", "ID"]),
            zone: getProp(f, ["FLD_ZONE", "ZONE", "DESCRIPTIO"]),
            raw: f.properties,
          })),
        },
        mainStreet: {
          present: mainStreetHits.length > 0,
          count: mainStreetHits.length,
          overlays: mainStreetHits.map((f) => ({
            id: getProp(f, ["MAIN_ID", "OBJECTID", "ID"]),
            name: getProp(f, ["DIST_NAME", "NAME", "LABEL"]),
            raw: f.properties,
          })),
        },
      },
    },

    civic: {
      neighborhood: neighborhoodFeat
        ? {
            name: getProp(neighborhoodFeat, [
              "Name",
              "NAME",
              "Neighborhood",
              "NBHD_NAME",
            ]),
            raw: neighborhoodFeat.properties,
          }
        : null,

      ward: wardFeat
        ? {
            wardNumber: getProp(wardFeat, ["Ward", "WARD", "WARD_ID"]),
            raw: wardFeat.properties,
          }
        : null,

      policeDistrict: policeFeat
        ? {
          code: getProp(policeFeat, ["DIST", "District", "DISTRICT"]),
          name: getProp(policeFeat, ["DIST_NAME", "NAME", "LABEL"]),
          raw: policeFeat.properties,
        }
        : null,

      fireDistrict: fireFeat
        ? {
          code: getProp(fireFeat, ["DIST", "DISTRICT", "FD_ID"]),
          name: getProp(fireFeat, ["DIST_NAME", "NAME", "LABEL"]),
          raw: fireFeat.properties,
        }
        : null,

      trash: trashFeat
        ? {
            day: getProp(trashFeat, ["TRASHDAY", "DAY", "COLLDAY"]),
            raw: trashFeat.properties,
          }
        : null,

      snow: {
        inParkingZone: !!snowParkingFeat,
        parkingZone: snowParkingFeat
          ? {
              group: getProp(snowParkingFeat, ["GROUP", "SNOW_GRP"]),
              description: getProp(snowParkingFeat, [
                "DESCRIPTIO",
                "DESC",
                "LABEL",
              ]),
              raw: snowParkingFeat.properties,
            }
          : null,
        inSnowRouteCorridor,
        snowRoute: inSnowRouteCorridor && nearestSnowRoute
          ? {
              distanceKm: nearestSnowRoute.distanceKm,
              distanceMeters: nearestSnowRoute.distanceKm * 1000,
              group: getProp(nearestSnowRoute.feature, [
                "GROUP",
                "SNOW_GRP",
                "ROUTE_GRP",
              ]),
              name: getProp(nearestSnowRoute.feature, [
                "NAME",
                "STREET_NAM",
                "LABEL",
              ]),
              raw: nearestSnowRoute.feature.properties,
            }
          : null,
      },

      zipcode: zipcodeFeat
        ? {
            zip: getProp(zipcodeFeat, ["ZIP", "ZIPCODE", "POSTCODE"]),
            raw: zipcodeFeat.properties,
          }
        : null,

      transportationDistrict: transportFeat
        ? {
            code: getProp(transportFeat, ["DISTRICT", "DIST_ID"]),
            name: getProp(transportFeat, ["DIST_NAME", "NAME", "LABEL"]),
            raw: transportFeat.properties,
          }
        : null,

      onWater: !!hydroFeat,
      waterFeature: hydroFeat ? hydroFeat.properties : null,
    },

    proximity: {
      hospital: nearestHospital
        ? {
            distanceKm: nearestHospital.distanceKm,
            distanceMeters: nearestHospital.distanceKm * 1000,
            name: getProp(nearestHospital.feature, [
              "HOSPITAL",
              "NAME",
              "LABEL",
            ]),
            raw: nearestHospital.feature.properties,
          }
        : null,

      communityCenter: nearestCommunityCenter
        ? {
            distanceKm: nearestCommunityCenter.distanceKm,
            distanceMeters: nearestCommunityCenter.distanceKm * 1000,
            name: getProp(nearestCommunityCenter.feature, [
              "NAME",
              "CENTER",
              "LABEL",
            ]),
            raw: nearestCommunityCenter.feature.properties,
          }
        : null,

      mbtaStation: nearestMbtaEntry
        ? {
            distanceKm: nearestMbtaEntry.distanceKm,
            distanceMeters: nearestMbtaEntry.distanceKm * 1000,
            name: getProp(nearestMbtaEntry.feature, [
              "STATION",
              "STOPNAME",
              "STOP_NAME",
              "NAME",
            ]),
            raw: nearestMbtaEntry.feature.properties,
          }
        : null,
    },
  };

  return result;
}

export default {
  lookupZoningAndCivic,
};
