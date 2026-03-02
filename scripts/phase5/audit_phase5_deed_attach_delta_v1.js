/**
 * Phase 5 Deeds Attach Delta Audit (Baseline vs Shadow)
 * Institution-grade constraints:
 *  - Zero regressions: no ATTACHED -> UNKNOWN/MISSING
 *  - No parcel flips: attached property_id must not change
 *  - Summarize net gain, method changes, bucket movement
 *
 * Usage:
 * node .\scripts\phase5\audit_phase5_deed_attach_delta_v1.js --baseline <path> --shadow <path> --outDir <dir>
 */
const fs = require("fs");
const path = require("path");
const readline = require("readline");

function arg(name) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : null;
}

const baselinePath = arg("baseline");
const shadowPath = arg("shadow");
const outDir = arg("outDir") || ".";
if (!baselinePath || !shadowPath) {
  console.error("[err] missing args. Need --baseline and --shadow");
  process.exit(1);
}
if (!fs.existsSync(baselinePath)) throw new Error("Baseline file not found: " + baselinePath);
if (!fs.existsSync(shadowPath)) throw new Error("Shadow file not found: " + shadowPath);

fs.mkdirSync(outDir, { recursive: true });

const OUT_SUMMARY = path.join(outDir, "delta_summary.json");
const OUT_REGRESS = path.join(outDir, "regressions.ndjson");
const OUT_FLIPS = path.join(outDir, "parcel_flips.ndjson");
const OUT_PROMOTIONS = path.join(outDir, "promotions_unknown_to_attached.ndjson");
const OUT_METHOD_CHANGES = path.join(outDir, "method_changes.ndjson");

function normStatus(ev) {
  // Try common fields; adjust if your schema uses different names
  return (
    ev.attach_status ||
    ev.attachStatus ||
    ev.status ||
    ev.match_status ||
    "UNKNOWN"
  );
}

function normPropertyId(ev) {
  return (
    ev.property_id ||
    ev.propertyId ||
    ev.spine_property_id ||
    ev.attached_property_id ||
    null
  );
}

function normMethod(ev) {
  return (
    ev.match_method ||
    ev.matchMethod ||
    ev.attach_method ||
    ev.attachMethod ||
    null
  );
}

function getEventId(ev) {
  return ev.event_id || ev.eventId || ev.id || null;
}

async function loadMap(ndjsonPath) {
  const map = new Map();
  const rl = readline.createInterface({
    input: fs.createReadStream(ndjsonPath),
    crlfDelay: Infinity
  });

  let n = 0;
  for await (const line of rl) {
    const s = line.trim();
    if (!s) continue;
    let ev;
    try { ev = JSON.parse(s); } catch { continue; }
    const id = getEventId(ev);
    if (!id) continue;
    map.set(id, {
      raw: ev,
      status: normStatus(ev),
      property_id: normPropertyId(ev),
      method: normMethod(ev)
    });
    n++;
    if (n % 100000 === 0) console.log("[info] loaded", n, "events from", ndjsonPath);
  }
  console.log("[done] loaded", map.size, "events from", ndjsonPath);
  return map;
}

function isAttached(status) {
  const s = String(status || "").toUpperCase();
  return s.startsWith("ATTACHED");
}

(async () => {
  console.log("[info] baseline:", baselinePath);
  console.log("[info] shadow  :", shadowPath);

  const base = await loadMap(baselinePath);
  const shad = await loadMap(shadowPath);

  const regressStream = fs.createWriteStream(OUT_REGRESS, { flags: "w" });
  const flipsStream = fs.createWriteStream(OUT_FLIPS, { flags: "w" });
  const promoStream = fs.createWriteStream(OUT_PROMOTIONS, { flags: "w" });
  const methodStream = fs.createWriteStream(OUT_METHOD_CHANGES, { flags: "w" });

  let baseAttached = 0, shadAttached = 0;
  let regressions = 0, flips = 0, promotions = 0, methodChanges = 0;

  // Count attached
  for (const [, v] of base) if (isAttached(v.status)) baseAttached++;
  for (const [, v] of shad) if (isAttached(v.status)) shadAttached++;

  // Compare baseline -> shadow (institutional rules)
  for (const [id, b] of base) {
    const s = shad.get(id);
    if (!s) continue;

    const bA = isAttached(b.status);
    const sA = isAttached(s.status);

    // Regression: was attached, now not attached
    if (bA && !sA) {
      regressions++;
      regressStream.write(JSON.stringify({
        event_id: id,
        baseline_status: b.status,
        shadow_status: s.status,
        baseline_property_id: b.property_id,
        shadow_property_id: s.property_id,
        baseline_method: b.method,
        shadow_method: s.method
      }) + "\n");
      continue;
    }

    // Parcel flip: attached in both but property_id changed
    if (bA && sA && b.property_id && s.property_id && b.property_id !== s.property_id) {
      flips++;
      flipsStream.write(JSON.stringify({
        event_id: id,
        baseline_property_id: b.property_id,
        shadow_property_id: s.property_id,
        baseline_status: b.status,
        shadow_status: s.status,
        baseline_method: b.method,
        shadow_method: s.method
      }) + "\n");
    }

    // Promotion: unknown -> attached
    if (!bA && sA) {
      promotions++;
      promoStream.write(JSON.stringify({
        event_id: id,
        baseline_status: b.status,
        shadow_status: s.status,
        shadow_property_id: s.property_id,
        shadow_method: s.method
      }) + "\n");
    }

    // Method change (optional visibility)
    if ((b.method || null) !== (s.method || null)) {
      methodChanges++;
      methodStream.write(JSON.stringify({
        event_id: id,
        baseline_method: b.method,
        shadow_method: s.method,
        baseline_status: b.status,
        shadow_status: s.status
      }) + "\n");
    }
  }

  regressStream.end();
  flipsStream.end();
  promoStream.end();
  methodStream.end();

  const summary = {
    baseline_file: baselinePath,
    shadow_file: shadowPath,
    baseline_attached: baseAttached,
    shadow_attached: shadAttached,
    net_gain_attached: shadAttached - baseAttached,
    regressions_attached_to_unattached: regressions,
    parcel_flips: flips,
    promotions_unknown_to_attached: promotions,
    method_changes: methodChanges,
    gate_pass: (regressions === 0 && flips === 0 && (shadAttached >= baseAttached))
  };

  fs.writeFileSync(OUT_SUMMARY, JSON.stringify(summary, null, 2), "utf8");
  console.log("[done] wrote:", OUT_SUMMARY);
  console.log(summary);

  if (!summary.gate_pass) {
    console.error("[FAIL] Gate failed: do NOT promote shadow.");
    process.exit(2);
  } else {
    console.log("[PASS] Gate passed: shadow is eligible for promotion.");
  }
})();
