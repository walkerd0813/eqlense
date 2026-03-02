// utils/score.js
// --------------
// Turn subject + comp into a similarity score using weightConfig.json

const fs = require("fs");
const path = require("path");
const { haversineMiles, distanceScore } = require("./distance");

const ROOT = __dirname;
let WEIGHTS = null;

function loadWeights() {
  if (WEIGHTS) return WEIGHTS;

  const cfgPath = path.join(ROOT, "..", "data", "weightConfig.json");
  try {
    const raw = fs.readFileSync(cfgPath, "utf8");
    WEIGHTS = JSON.parse(raw);
  } catch (err) {
    console.warn(
      "[AVM] Failed to load weightConfig.json – falling back to defaults",
      err.message
    );
    WEIGHTS = {
      distance: 0.3,
      sqft: 0.25,
      beds: 0.15,
      baths: 0.1,
      age: 0.1,
      lot: 0.1,
    };
  }
  return WEIGHTS;
}

function clamp01(x) {
  if (!Number.isFinite(x)) return 0;
  if (x < 0) return 0;
  if (x > 1) return 1;
  return x;
}

/**
 * Ratio-based similarity for numeric attributes.
 * Example: sqft, lot size, etc.
 */
function ratioSimilarity(subVal, compVal, toleranceRatio = 0.3) {
  if (!subVal || !compVal) return 0;
  const bigger = Math.max(subVal, compVal);
  const smaller = Math.min(subVal, compVal);
  const diffRatio = (bigger - smaller) / bigger;
  if (diffRatio <= 0) return 1;
  if (diffRatio >= toleranceRatio) return 0;
  return 1 - diffRatio / toleranceRatio;
}

/**
 * Score a single comp vs subject.
 * Returns { score, breakdown }
 */
function scoreComp(subject, comp, opt = {}) {
  const w = loadWeights();
  const breakdown = {};

  // Distance
  const distMiles = haversineMiles(
    subject.lat,
    subject.lng,
    comp.lat,
    comp.lng
  );
  const distScore = distanceScore(distMiles, opt.maxRadiusMiles || 2);
  breakdown.distance = distScore * (w.distance || 0);

  // Sqft
  const sqftScore = ratioSimilarity(
    subject.sqft,
    comp.sqft,
    opt.sqftTolerance || 0.35
  );
  breakdown.sqft = sqftScore * (w.sqft || 0);

  // Beds
  const bedDiff = Math.abs((subject.beds || 0) - (comp.beds || 0));
  const bedRaw = bedDiff === 0 ? 1 : bedDiff === 1 ? 0.6 : 0.2;
  breakdown.beds = bedRaw * (w.beds || 0);

  // Baths
  const bathDiff = Math.abs((subject.baths || 0) - (comp.baths || 0));
  const bathRaw = bathDiff === 0 ? 1 : bathDiff === 1 ? 0.6 : 0.2;
  breakdown.baths = bathRaw * (w.baths || 0);

  // Age
  const ageSub =
    subject.yearBuilt && Number.isFinite(subject.yearBuilt)
      ? new Date().getFullYear() - subject.yearBuilt
      : null;
  const ageComp =
    comp.yearBuilt && Number.isFinite(comp.yearBuilt)
      ? new Date().getFullYear() - comp.yearBuilt
      : null;

  let ageRaw = 0;
  if (ageSub != null && ageComp != null) {
    const diff = Math.abs(ageSub - ageComp);
    if (diff <= 5) ageRaw = 1;
    else if (diff <= 15) ageRaw = 0.7;
    else if (diff <= 30) ageRaw = 0.4;
    else ageRaw = 0.1;
  }
  breakdown.age = ageRaw * (w.age || 0);

  // Lot
  const lotScore = ratioSimilarity(
    subject.lotSqft,
    comp.lotSqft,
    opt.lotTolerance || 0.5
  );
  breakdown.lot = lotScore * (w.lot || 0);

  const totalWeight =
    (w.distance || 0) +
    (w.sqft || 0) +
    (w.beds || 0) +
    (w.baths || 0) +
    (w.age || 0) +
    (w.lot || 0);

  const rawScore =
    breakdown.distance +
    breakdown.sqft +
    breakdown.beds +
    breakdown.baths +
    breakdown.age +
    breakdown.lot;

  const score = totalWeight > 0 ? clamp01(rawScore / totalWeight) : 0;

  return {
    score,
    breakdown,
    distanceMiles: distMiles,
  };
}

module.exports = {
  scoreComp,
};
