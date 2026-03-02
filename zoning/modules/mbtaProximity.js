// backend/zoning/modules/mbtaProximity.js

import fs from "fs";
import path from "path";
import distance from "@turf/distance";
import { point as turfPoint } from "@turf/helpers";

/**
 * MBTA / TRANSIT PROXIMITY MODULE
 * -------------------------------
 * Multi-city transit support (starting with MBTA for Boston).
 *
 * Folder convention:
 * publicData/boundaries/<city>/mbta/*.geojson
 *
 * Geometry is usually points, but we handle polygons/lines defensively.
 */

const loadGeoJSON = (filepath) => {
  try {
    const raw = fs.readFileSync(filepath, "utf8");
    const json = JSON.parse(raw);
    if (!json.features) return [];
    return json.features.map((f) => ({
      ...f,
      _source: filepath,
    }));
  } catch (err) {
    return [];
  }
};

/**
 * Best-effort classification of a transit stop into:
 *   - type  (Subway, Light Rail, Commuter Rail, Bus, Ferry, etc.)
 *   - lines (["Green Line", "Red Line"], etc.)
 */
function classifyTransitProps(props) {
  if (!props || typeof props !== "object") {
    return { type: null, lines: null };
  }

  // Mode / type
  const modeRaw =
    props.MODE ||
    props.mode ||
    props.MODE_TYPE ||
    props.Vehicle ||
    props.vehicle_type ||
    null;

  let type = null;
  if (modeRaw) {
    const m = String(modeRaw).toLowerCase();
    if (m.includes("subway") || m.includes("rapid") || m.includes("heavy")) {
      type = "Subway";
    } else if (m.includes("light")) {
      type = "Light Rail";
    } else if (m.includes("commuter") || m.includes("rail")) {
      type = "Commuter Rail";
    } else if (m.includes("bus")) {
      type = "Bus";
    } else if (m.includes("ferry")) {
      type = "Ferry";
    }
  }

  // Lines / routes
  const routeRaw =
    props.LINE ||
    props.Line ||
    props.ROUTE ||
    props.route ||
    props.ROUTE_ID ||
    props.Route_ID ||
    props.ROUTES ||
    null;

  let lines = null;
  if (routeRaw) {
    const val = String(routeRaw);
    lines = val
      .split(/[;,/]| and /i)
      .map((s) => s.trim())
      .filter(Boolean);

    lines = lines.map((ln) => {
      const lower = ln.toLowerCase();
      if (lower.startsWith("green")) return "Green Line";
      if (lower.startsWith("orange")) return "Orange Line";
      if (lower.startsWith("red")) return "Red Line";
      if (lower.startsWith("blue")) return "Blue Line";
      if (lower.includes("silver")) return "Silver Line";
      if (
        lower.includes("fairmount") ||
        lower.includes("fitchburg") ||
        lower.includes("providence")
      ) {
        return "Commuter Rail";
      }
      return ln;
    });
  }

  return { type, lines };
}

/**
 * Load transit stops for ALL cities that have /mbta.
 *
 * Returns:
 * [
 *   { city, file, feature }
 * ]
 */
export function loadTransitStops({ DATA_ROOT }) {
  const boundariesRoot = path.join(DATA_ROOT, "boundaries");
  const stops = [];

  if (!fs.existsSync(boundariesRoot)) return stops;

  const cities = fs
    .readdirSync(boundariesRoot)
    .filter((dir) => fs.statSync(path.join(boundariesRoot, dir)).isDirectory());

  for (const city of cities) {
    const mbtaDir = path.join(boundariesRoot, city, "mbta");
    if (!fs.existsSync(mbtaDir)) continue;

    const files = fs
      .readdirSync(mbtaDir)
      .filter((f) => f.endsWith(".geojson"));

    for (const file of files) {
      const full = path.join(mbtaDir, file);
      const features = loadGeoJSON(full);

      for (const feature of features) {
        stops.push({ city, file, feature });
      }
    }
  }

  return stops;
}

/**
 * Helper: nearest transit stop to pt among all cities.
 */
function nearestStop(pt, stops) {
  let best = null;
  let bestDist = Infinity;

  for (const entry of stops) {
    const geom = entry.feature.geometry;
    if (!geom) continue;

    let targetPoint = null;

    if (geom.type === "Point") {
      targetPoint = entry.feature;
    } else if (geom.type === "Polygon" || geom.type === "MultiPolygon") {
      const coords =
        geom.type === "Polygon"
          ? geom.coordinates?.[0]?.[0]
          : geom.coordinates?.[0]?.[0]?.[0];
      if (coords) {
        targetPoint = turfPoint(coords);
      }
    } else if (geom.type === "LineString") {
      if (geom.coordinates && geom.coordinates[0]) {
        targetPoint = turfPoint(geom.coordinates[0]);
      }
    } else if (geom.type === "MultiLineString") {
      if (geom.coordinates && geom.coordinates[0] && geom.coordinates[0][0]) {
        targetPoint = turfPoint(geom.coordinates[0][0]);
      }
    }

    if (!targetPoint) continue;

    const d = distance(pt, targetPoint); // km
    if (d < bestDist) {
      bestDist = d;
      best = entry;
    }
  }

  if (!best || bestDist === Infinity) return null;
  return { entry: best, distanceKm: bestDist };
}

/**
 * Transit proximity lookup.
 *
 * @param {Array} stops      Output of loadTransitStops()
 * @param {Feature<Point>} pt   Turf point([lon, lat])
 *
 * Returns:
 * {
 *   hasStation: boolean,
 *   station: {
 *     city,
 *     distanceKm,
 *     distanceMeters,
 *     name,
 *     type,
 *     lines,
 *     attrs,
 *     feature
 *   } | null
 * }
 */
export function transitProximityLookup(stops, pt) {
  if (!stops || stops.length === 0) {
    return { hasStation: false, station: null };
  }

  const nearest = nearestStop(pt, stops);
  if (!nearest) {
    return { hasStation: false, station: null };
  }

  const meta = classifyTransitProps(nearest.entry.feature.properties || {});

  const name =
    nearest.entry.feature.properties?.STATION ||
    nearest.entry.feature.properties?.STOPNAME ||
    nearest.entry.feature.properties?.STOP_NAME ||
    nearest.entry.feature.properties?.NAME ||
    null;

  return {
    hasStation: true,
    station: {
      city: nearest.entry.city,
      distanceKm: nearest.distanceKm,
      distanceMeters: nearest.distanceKm * 1000,
      name,
      type: meta.type,
      lines: meta.lines,
      attrs: nearest.entry.feature.properties || {},
      feature: nearest.entry.feature,
    },
  };
}
