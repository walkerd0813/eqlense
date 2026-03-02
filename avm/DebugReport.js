// DebugReport.js
// --------------
// Build a human-readable debug payload from the comp selection + pricing.

function buildDebugReport({ subject, selection, pricing }) {
  const lines = [];

  lines.push("=== AVM DEBUG REPORT ===");
  if (subject && subject.address) {
    lines.push(`Subject: ${subject.address} (${subject.propertyClass})`);
  }
  if (pricing && pricing.estimate != null) {
    lines.push(
      `Estimate: $${Math.round(pricing.estimate).toLocaleString()} ` +
        `[${Math.round(pricing.low).toLocaleString()} - ${Math.round(
          pricing.high
        ).toLocaleString()}]`
    );
  }

  if (selection && selection.meta) {
    const m = selection.meta;
    lines.push(
      `Comps used: ${m.usedCount} (scored ${m.totalScored} / ` +
        `${m.totalAvailable} available, radius ${m.maxRadiusMiles}mi, ` +
        `age <= ${m.maxAgeMonths}mo)`
    );
  }

  if (selection && selection.debug && selection.debug.ranked) {
    lines.push("--- Top comps ---");
    selection.debug.ranked.slice(0, 10).forEach((r, idx) => {
      lines.push(
        `#${idx + 1}: score=${r.score.toFixed(3)}, ` +
          `dist=${r.distanceMiles != null ? r.distanceMiles.toFixed(2) : "n/a"}mi, ` +
          `${r.address || "Unknown address"}`
      );
    });
  }

  return {
    summary: lines.join("\n"),
    subject,
    selection,
    pricing,
  };
}

module.exports = {
  buildDebugReport,
};


