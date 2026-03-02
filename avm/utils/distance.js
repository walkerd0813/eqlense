// utils/distance.js
// -----------------
// Distance helpers (Haversine + simple scoring)

const EARTH_RADIUS_MILES = 3958.8;

/**
 * Great-circle distance between two lat/lng points (miles)
 */
function haversineMiles(lat1, lng1, lat2, lng2) {
  if (
    typeof lat1 !== "number" ||
    typeof lng1 !== "number" ||
    typeof lat2 !== "number" ||
    typeof lng2 !== "number"
  ) {
    return null;
  }

  const toRad = (deg) => (deg * Math.PI) / 180;

  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);

  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) *
      Math.cos(toRad(lat2)) *
      Math.sin(dLng / 2) *
      Math.sin(dLng / 2);

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return EARTH_RADIUS_MILES * c;
}

/**
 * Convert distance to a 0–1 score where 1 = same location.
 * `maxRadiusMiles` = radius at which score hits 0.
 */
function distanceScore(distanceMiles, maxRadiusMiles = 2) {
  if (distanceMiles == null || !Number.isFinite(distanceMiles)) return 0;
  if (distanceMiles <= 0) return 1;
  if (distanceMiles >= maxRadiusMiles) return 0;

  // Simple smooth drop off
  const ratio = distanceMiles / maxRadiusMiles;
  return 1 - Math.min(1, ratio ** 1.5);
}

module.exports = {
  haversineMiles,
  distanceScore,
};