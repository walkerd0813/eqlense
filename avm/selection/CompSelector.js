// backend/avm/selection/CompSelector.js
// ------------------------------------------------------
// Core comp-selection logic used by the AVM.
// Based on the DiagnosticSelector you ran that produced
// healthy comp counts for 15 Alvarado Ave.
//
// Upgrades:
// - Prefer same ZIP (if enough comps) before radius
// - Remove the subject property itself from the dataset
// - Standard strict CMA filters + relaxed fallback
// - EXTRA: extreme sparse-area fallback for rural / low-data zones
// ------------------------------------------------------

const MS_PER_DAY = 24 * 60 * 60 * 1000;
const DAYS_PER_MONTH = 30.4375;

// -------------------------
// Small helpers
// -------------------------
function toNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function parseBaths(value) {
  if (value == null) return null;
  if (typeof value === "number") return value;
  if (typeof value !== "string") return null;

  const lower = value.toLowerCase();

  let full = 0;
  let half = 0;

  const fullMatch = lower.match(/(\d+)\s*f/);
  if (fullMatch) full = Number(fullMatch[1]);

  const halfMatch = lower.match(/(\d+)\s*h/);
  if (halfMatch) half = Number(halfMatch[1]);

  // If we couldn't match the “1f 1h” style, try just numeric
  if (!fullMatch && !halfMatch) {
    const n = toNumber(value);
    return n;
  }

  return full + half * 0.5;
}

// Haversine distance in miles
function distanceMiles(lat1, lng1, lat2, lng2) {
  const toRad = (v) => (v * Math.PI) / 180;
  const R = 3958.8;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);

  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) *
      Math.cos(toRad(lat2)) *
      Math.sin(dLng / 2) ** 2;

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

// Months between two JS Dates
function monthsBetween(a, b) {
  const diffDays = Math.abs(a - b) / MS_PER_DAY;
  return diffDays / DAYS_PER_MONTH;
}

/**
 * selectComps
 * -----------
 * @param {Object} subject - normalized subject {lat,lng,beds,baths,sqft,...}
 * @param {Array|Object} allComps - array of comp objects OR { data: [...] }
 * @param {Object} options - { maxRadiusMiles, maxAgeMonths, targetCompCount }
 *
 * Returns { comps, debug }
 */
function selectComps(subject, allComps, options = {}) {
  const {
    maxRadiusMiles = 2,
    maxAgeMonths = 18,
    targetCompCount = 12,
  } = options;

  const debugSteps = [];
  const now = new Date();

  // ---------- Safe-load allComps ----------
  let baseComps;
  if (Array.isArray(allComps)) {
    baseComps = allComps;
  } else if (allComps && Array.isArray(allComps.data)) {
    baseComps = allComps.data;
    debugSteps.push({
      step: "allComps_coerced_from_data",
      originalType: typeof allComps,
      originalKeys: Object.keys(allComps || {}),
      initialCount: baseComps.length,
    });
  } else {
    debugSteps.push({
      step: "allComps_invalid",
      message: "allComps was not an array",
      typeofAllComps: typeof allComps,
      allCompsKeys:
        allComps && typeof allComps === "object"
          ? Object.keys(allComps)
          : null,
    });
    return {
      comps: [],
      debug: {
        steps: debugSteps,
        options: { maxRadiusMiles, maxAgeMonths, targetCompCount },
      },
    };
  }

  const subjectLat = toNumber(subject.lat);
  const subjectLng = toNumber(subject.lng);

  if (subjectLat === null || subjectLng === null) {
    debugSteps.push({
      step: "subject_missing_lat_lng",
      message: "Subject is missing lat/lng, cannot select comps.",
      subjectLat: subject.lat,
      subjectLng: subject.lng,
    });
    return {
      comps: [],
      debug: {
        steps: debugSteps,
        options: { maxRadiusMiles, maxAgeMonths, targetCompCount },
      },
    };
  }

  let working = [...baseComps];

  debugSteps.push({
    step: "start",
    totalComps: working.length,
  });

  if (!working.length) {
    debugSteps.push({
      step: "no_comps_available",
      message: "allComps was an empty array.",
    });
    return {
      comps: [],
      debug: {
        steps: debugSteps,
        options: { maxRadiusMiles, maxAgeMonths, targetCompCount },
      },
    };
  }

  // -------------------------------------------------
  // UPGRADE: remove the subject itself from the dataset
  // -------------------------------------------------
  const subjAddressKey =
    subject.address && String(subject.address).toLowerCase().trim();
  const subjZipKey = subject.zip && String(subject.zip).trim();

  if (subjAddressKey || (subjectLat !== null && subjectLng !== null)) {
    const before = working.length;
    working = working.filter((c) => {
      const addr = c.address && String(c.address).toLowerCase().trim();
      const zip = c.zip && String(c.zip).trim();

      const cLat = toNumber(c.lat);
      const cLng = toNumber(c.lng);

      const sameAddress =
        addr && subjAddressKey && addr === subjAddressKey && !!zip && !!subjZipKey && zip === subjZipKey;

      const sameSpot =
        subjectLat !== null &&
        subjectLng !== null &&
        cLat !== null &&
        cLng !== null &&
        distanceMiles(subjectLat, subjectLng, cLat, cLng) < 0.02; // ~100ft

      // Drop if it looks like the subject
      if (sameAddress || sameSpot) return false;

      return true;
    });
    const removed = before - working.length;
    if (removed > 0) {
      debugSteps.push({
        step: "removed_subject_from_dataset",
        removed,
      });
    }
  }



  // =====================================================
  // STRICT CMA FILTERS (human-style)
  // =====================================================

  // STEP 1 – Only comps with lat/lng
  working = working.filter(
    (c) => toNumber(c.lat) !== null && toNumber(c.lng) !== null
  );
  debugSteps.push({
    step: "has_lat_lng",
    remaining: working.length,
  });

  // STEP 1.5 – Prefer same ZIP first (if enough comps)
  const subjZip = subjZipKey;
  if (subjZip) {
    const sameZip = working.filter((c) => {
      const z = c.zip && String(c.zip).trim();
      return z === subjZip;
    });

    if (sameZip.length >= 3) {
      debugSteps.push({
        step: "zip_filter_applied",
        zip: subjZip,
        kept: sameZip.length,
        dropped: working.length - sameZip.length,
      });
      working = sameZip;
    } else {
      debugSteps.push({
        step: "zip_filter_skipped",
        zip: subjZip,
        candidates: sameZip.length,
      });
    }
  }

  // STEP 2 – Within radius
  working = working.filter((c) => {
    const cLat = toNumber(c.lat);
    const cLng = toNumber(c.lng);
    if (cLat === null || cLng === null) return false;

    const d = distanceMiles(subjectLat, subjectLng, cLat, cLng);
    return d <= maxRadiusMiles;
  });
  debugSteps.push({
    step: "within_radius",
    radiusMiles: maxRadiusMiles,
    remaining: working.length,
  });

  // STEP 3 – Beds within ±1
  const subjBeds = toNumber(subject.beds);
  if (subjBeds !== null) {
    working = working.filter((c) => {
      const b = toNumber(c.beds);
      if (b === null) return false;
      return Math.abs(b - subjBeds) <= 1;
    });
    debugSteps.push({
      step: "beds_within_1",
      subjectBeds: subjBeds,
      remaining: working.length,
    });
  }

  // STEP 4 – SQFT within ±25%
  const subjSqft = toNumber(subject.sqft);
  if (subjSqft !== null) {
    const minSqft = subjSqft * 0.75;
    const maxSqft = subjSqft * 1.25;

    working = working.filter((c) => {
      const s = toNumber(c.sqft);
      if (s === null) return false;
      return s >= minSqft && s <= maxSqft;
    });

    debugSteps.push({
      step: "sqft_within_25pct",
      subjectSqft: subjSqft,
      minSqft,
      maxSqft,
      remaining: working.length,
    });
  }

  // STEP 5 – Baths within ±1
  const subjBaths = parseBaths(subject.baths);
  if (subjBaths !== null) {
    working = working.filter((c) => {
      const b = parseBaths(c.baths);
      if (b === null) return false;
      return Math.abs(b - subjBaths) <= 1;
    });
    debugSteps.push({
      step: "baths_within_1",
      subjectBaths: subjBaths,
      remaining: working.length,
    });
  }

  // STEP 6 – Max age (months)
  if (maxAgeMonths && Number.isFinite(maxAgeMonths)) {
    working = working.filter((c) => {
      if (!c.settledDate) return false;
      const d = new Date(c.settledDate);
      if (Number.isNaN(d.getTime())) return false;
      const m = monthsBetween(now, d);
      return m <= maxAgeMonths;
    });
    debugSteps.push({
      step: "age_within_months",
      maxAgeMonths,
      remaining: working.length,
    });
  }

  // STEP 7 – Trim to target count, closest by distance
  if (working.length > targetCompCount) {
    working.sort((a, b) => {
      const aLat = toNumber(a.lat);
      const aLng = toNumber(a.lng);
      const bLat = toNumber(b.lat);
      const bLng = toNumber(b.lng);

      const da = distanceMiles(subjectLat, subjectLng, aLat, aLng);
      const db = distanceMiles(subjectLat, subjectLng, bLat, bLng);
      return da - db;
    });

    working = working.slice(0, targetCompCount);
  }

  debugSteps.push({
    step: "strict_final",
    selected: working.length,
    targetCompCount,
  });

  // =====================================================
  // FALLBACK 1: RELAXED FILTERS IF NOTHING LEFT
  // (prevents the “estimate: null, comps: []” situation)
  // =====================================================
  if (!working.length) {
    let relaxed = [...baseComps];

    // Only comps with lat/lng, still
    relaxed = relaxed.filter(
      (c) => toNumber(c.lat) !== null && toNumber(c.lng) !== null
    );

    // Widen radius to 5 miles (CMA-style “wider neighborhood”)
    const relaxedRadius = Math.max(maxRadiusMiles, 5);
    relaxed = relaxed.filter((c) => {
      const cLat = toNumber(c.lat);
      const cLng = toNumber(c.lng);
      if (cLat === null || cLng === null) return false;

      const d = distanceMiles(subjectLat, subjectLng, cLat, cLng);
      return d <= relaxedRadius;
    });

    // Keep sqft filter but widen to ±35%
    if (subjSqft !== null) {
      const minSqft = subjSqft * 0.65;
      const maxSqft = subjSqft * 1.35;

      relaxed = relaxed.filter((c) => {
        const s = toNumber(c.sqft);
        if (s === null) return false;
        return s >= minSqft && s <= maxSqft;
      });
    }

    // Beds & baths become soft: we don't drop comps on them here

    if (relaxed.length > targetCompCount) {
      relaxed.sort((a, b) => {
        const aLat = toNumber(a.lat);
        const aLng = toNumber(a.lng);
        const bLat = toNumber(b.lat);
        const bLng = toNumber(b.lng);

        const da = distanceMiles(subjectLat, subjectLng, aLat, aLng);
        const db = distanceMiles(subjectLat, subjectLng, bLat, bLng);
        return da - db;
      });

      relaxed = relaxed.slice(0, targetCompCount);
    }

    debugSteps.push({
      step: "fallback_relaxed_filters",
      relaxedRadiusMiles: relaxedRadius,
      selected: relaxed.length,
    });

    working = relaxed;
  }

  // =====================================================
  // FALLBACK 2: EXTREME SPARSE-AREA MODE
  // (rural / weird pockets with very few comps)
  // =====================================================
  const minComfortableComps = Math.max(3, Math.floor(targetCompCount / 3));
  if (working.length < minComfortableComps) {
    let extreme = [...baseComps];

    extreme = extreme.filter(
      (c) => toNumber(c.lat) !== null && toNumber(c.lng) !== null
    );

    const extremeRadius = Math.max(maxRadiusMiles, 20); // up to 20mi search
    extreme = extreme.filter((c) => {
      const cLat = toNumber(c.lat);
      const cLng = toNumber(c.lng);
      if (cLat === null || cLng === null) return false;

      const d = distanceMiles(subjectLat, subjectLng, cLat, cLng);
      return d <= extremeRadius;
    });

    if (subjSqft !== null) {
      const minSqft = subjSqft * 0.5;
      const maxSqft = subjSqft * 1.5;

      extreme = extreme.filter((c) => {
        const s = toNumber(c.sqft);
        if (s === null) return false;
        return s >= minSqft && s <= maxSqft;
      });
    }

    if (extreme.length) {
      // Sort by distance and trim
      if (extreme.length > targetCompCount) {
        extreme.sort((a, b) => {
          const aLat = toNumber(a.lat);
          const aLng = toNumber(a.lng);
          const bLat = toNumber(b.lat);
          const bLng = toNumber(b.lng);

          const da = distanceMiles(subjectLat, subjectLng, aLat, aLng);
          const db = distanceMiles(subjectLat, subjectLng, bLat, bLng);
          return da - db;
        });

        extreme = extreme.slice(0, targetCompCount);
      }

      debugSteps.push({
        step: "sparse_area_extreme_fallback",
        extremeRadiusMiles: extremeRadius,
        selected: extreme.length,
      });

      working = extreme;
    } else {
      debugSteps.push({
        step: "sparse_area_no_comps_even_extreme",
        extremeRadiusMiles: extremeRadius,
      });
    }
  }

  return {
    comps: working,
    debug: {
      steps: debugSteps,
      options: { maxRadiusMiles, maxAgeMonths, targetCompCount },
    },
  };
}

module.exports = {
  selectComps,
};