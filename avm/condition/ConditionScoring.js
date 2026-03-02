// backend/avm/condition/ConditionScoring.js
// -----------------------------------------------------------
// Starter condition scoring module
// - Accepts optional subject.photos or subject.conditionNotes
// - Currently uses text-based cues (good/bad/avg keywords)
// - Designed so future ML models can plug in (image CNN)
// -----------------------------------------------------------

function scoreCondition(subject) {
  let base = 70; // neutral = "average"

  // Text-based logic (MLS remarks, uploaded notes, etc.)
  const notes =
    (subject.remarks || subject.conditionNotes || "").toLowerCase();

  if (notes.includes("new") ||
      notes.includes("renovated") ||
      notes.includes("updated") ||
      notes.includes("modern")) {
    base += 15;
  }

  if (notes.includes("needs work") ||
      notes.includes("fixer") ||
      notes.includes("tlc") ||
      notes.includes("deferred") ||
      notes.includes("old")) {
    base -= 15;
  }

  // Placeholder for future ML → photos scoring
  let photoSignal = null;
  if (Array.isArray(subject.photos) && subject.photos.length > 0) {
    // Not implemented yet — but the AVM will not break
    photoSignal = "pending-ml";
  }

  return {
    conditionScore: Math.max(1, Math.min(100, base)),
    method: "text-v1",
    photoSignal,
  };
}

module.exports = {
  scoreCondition,
};
