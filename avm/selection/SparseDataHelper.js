// backend/avm/selection/SparseDataHelper.js
// ---------------------------------------------------------
// Sparse-data safety logic for rural, suburban, and city-dense areas.
// Expands radius intelligently ONLY when comp count is low.
// Does NOT override strict CMA logic — it only guides CompSelector
// so AVM never returns empty comps in sparse markets.
// ---------------------------------------------------------

function classifyDensity(subject) {
  const pop = Number(subject.populationDensity || subject.popDensity || 0);

  if (pop >= 15000) return "dense_city"; // e.g., Boston, Cambridge
  if (pop >= 3000) return "suburban"; // e.g., Springfield outskirts
  return "rural"; // rural MA, low-comp areas
}

function computeDynamicRadius(density, baseRadius, compCount) {
  // If you already have enough comps → never expand radius
  if (compCount >= 6) return baseRadius;

  switch (density) {
    case "dense_city":
      // City has many comps, but buildings can be weird:
      // expand only slightly
      return compCount === 0 ? baseRadius + 0.3 : baseRadius + 0.1;

    case "suburban":
      // Suburbs need more wiggle room
      if (compCount === 0) return baseRadius + 1.5;
      if (compCount < 3) return baseRadius + 0.8;
      return baseRadius + 0.4;

    case "rural":
      // Rural towns often need big radius expansions
      if (compCount === 0) return baseRadius + 3.0;
      if (compCount < 3) return baseRadius + 2.0;
      return baseRadius + 1.0;

    default:
      return baseRadius;
  }
}

module.exports = {
  classifyDensity,
  computeDynamicRadius,
};





