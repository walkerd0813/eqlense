// backend/avm/condition/RemarksParse.js
// -------------------------------------
// Turn free-text agent / firm remarks into a rough 0–100 condition score.
// This is deliberately simple + explainable and can be upgraded later.

function normalizeText(text) {
  if (!text) return "";
  return String(text).toLowerCase();
}

/**
 * conditionScoreFromRemarks
 * -------------------------
 * @param {string} remarks
 * @returns {number|null} 0–100 (higher = better), or null if we can't infer
 */
function conditionScoreFromRemarks(remarks) {
  const t = normalizeText(remarks);
  if (!t) return null;

  // Very strong "like new" signals
  const veryHighPatterns = [
    "gut renovation",
    "fully renovated",
    "total renovation",
    "down to the studs",
    "like new",
    "brand new",
    "new construction",
    "fully updated",
    "completely updated",
    "high end finishes",
    "luxury finishes",
    "top to bottom",
  ];

  if (veryHighPatterns.some((p) => t.includes(p))) {
    return 92; // ~A condition
  }

  // Solid "good / updated" signals
  const highPatterns = [
    "recently renovated",
    "recent renovation",
    "recently updated",
    "updated kitchen",
    "updated bath",
    "granite",
    "stone counters",
    "stainless steel",
    "turn key",
    "turn-key",
    "move in ready",
    "move-in ready",
    "pride of ownership",
    "well maintained",
    "meticulously maintained",
  ];

  if (highPatterns.some((p) => t.includes(p))) {
    return 82; // B+ condition
  }

  // Neutral / average – nothing bad, some generic "nice" language
  const neutralPatterns = [
    "cozy",
    "charming",
    "plenty of potential",
    "great bones",
    "solid home",
    "good value",
    "convenient location",
  ];

  if (neutralPatterns.some((p) => t.includes(p))) {
    return 60; // C+ condition
  }

  // Clear "needs work" signals
  const lowPatterns = [
    "needs work",
    "needs updating",
    "bring your ideas",
    "bring your vision",
    "handyman special",
    "investor special",
    "fixer upper",
    "fixer-upper",
    "as is",
    "sold as is",
    "deferred maintenance",
    "tired but livable",
  ];

  if (lowPatterns.some((p) => t.includes(p))) {
    return 40; // D condition
  }

  // Very rough / distressed
  const veryLowPatterns = [
    "shell condition",
    "tear down",
    "teardown",
    "not habitable",
    "uninhabitable",
    "major rehab",
    "significant repairs needed",
    "fire damage",
    "water damage",
  ];

  if (veryLowPatterns.some((p) => t.includes(p))) {
    return 25; // F condition
  }

  // If we only see "fluff" (view, coffee on porch, etc.) – don't guess
  return null;
}

module.exports = {
  conditionScoreFromRemarks,
};