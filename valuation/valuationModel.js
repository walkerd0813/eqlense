// valuationModel.js — AVM Blueprint Implementation (CQS, Confidence, Trends)

const sf = require("../avm/selection/comps/singleFamily.json");
const mf = require("../avm/selection/comps/multiFamily.json");
const condos = require("../avm/selection/comps/condos.json");

// PUBLIC DATA MODULES
const {
  enrichWithAssessorData,
} = require("../publicData/assessors/assessorEnrichment");

const {
  fetchParcelData,
} = require("../publicData/parcels/parcelLookup");

const {
  fetchHazardData,
} = require("../publicData/hazards/hazardLookup"); // Module D – hazard / flood / risk

// ===============================
// BASIC HELPERS
// ===============================
function parseBaths(bathString) {
  if (!bathString) return 0;
  const match = String(bathString).match(/(\d+)f\s+(\d+)h/i);
  if (!match) return 0;
  const full = Number(match[1]) || 0;
  const half = Number(match[2]) || 0;
  return full + half * 0.5;
}

function safeNumber(n, fallback = null) {
  const num = Number(n);
  return Number.isFinite(num) ? num : fallback;
}

function ppsfOf(comp) {
  if (!comp || !comp.salePrice || !comp.sqft) return null;
  const v = comp.salePrice / comp.sqft;
  return Number.isFinite(v) ? v : null;
}

function parseDate(d) {
  if (!d) return null;
  const dt = new Date(d);
  return isNaN(dt.getTime()) ? null : dt;
}

function monthsAgo(date) {
  const dt = parseDate(date);
  if (!dt) return null;
  const now = new Date();
  const years = now.getFullYear() - dt.getFullYear();
  const months = years * 12 + (now.getMonth() - dt.getMonth());
  return months + (now.getDate() - dt.getDate()) / 30;
}

// ===============================
// DISTANCE — HAVERSINE
// ===============================
function haversineMiles(lat1, lon1, lat2, lon2) {
  if (!lat1 || !lon1 || !lat2 || !lon2) return null;

  const R = 3958.8;
  const dLat = (lat2 - lat1) * (Math.PI / 180);
  const dLon = (lon2 - lon1) * (Math.PI / 180);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * (Math.PI / 180)) *
      Math.cos(lat2 * (Math.PI / 180)) *
      Math.sin(dLon / 2) ** 2;

  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// ===============================
// SUBJECT + DATASET LOADING
// ===============================
function loadDataset(type) {
  if (type === "single_family") return sf;
  if (type === "multi_family" || type === "multi_2_4" || type === "multi_5_plus")
    return mf;
  if (type === "condo") return condos;
  return sf;
}

function normalizeSubject(input) {
  return {
    address: input.address || "",
    zip: (input.zip || "").toString().padStart(5, "0"),
    propertyType: input.propertyType || "single_family",

    beds: safeNumber(input.beds, 3),
    baths: safeNumber(input.baths, 1),
    sqft: Math.max(safeNumber(input.sqft, 1200), 400),

    yearBuilt: safeNumber(input.yearBuilt, null),
    lotSize: safeNumber(input.lotSize, null),
    style: input.style || "",
    garageSpaces: safeNumber(input.garageSpaces, null),

    condition: input.condition || "average",
    renovationStatus: input.renovationStatus || "average",
    roofAge: input.roofAge || null,
    estRepairs: safeNumber(input.estRepairs, 0),

    photoConditionScore: safeNumber(input.photoConditionScore, null),
    photoQualityScore: safeNumber(input.photoQualityScore, null),

    lat: safeNumber(input.lat, null),
    lng: safeNumber(input.lng, null),

    schoolDistrict: input.schoolDistrict || null,
    town: input.town || "",
  };
}

// ===============================
// CONDITION + PHOTO MERGE
// ===============================
function conditionLevel(label) {
  if (!label) return 0.5;
  const c = String(label).toLowerCase();

  if (c.includes("distress") || c.includes("full_gut")) return 0.1;
  if (c.includes("dated") || c.includes("fair")) return 0.3;
  if (c.includes("average") || c.includes("original")) return 0.5;
  if (c.includes("updated") || c.includes("kitchen") || c.includes("baths"))
    return 0.7;
  if (c.includes("like_new") || c.includes("like-new") || c.includes("luxury"))
    return 0.85;
  if (c.includes("new_build") || c.includes("new construction")) return 1.0;

  return 0.5;
}

const MIN_PHOTO_QUALITY_FOR_CONDITION = 0.6;

function effectiveSubjectConditionLevel(subject) {
  const base = conditionLevel(subject.condition);
  const photoScore = safeNumber(subject.photoConditionScore, null);
  const photoQuality = safeNumber(subject.photoQualityScore, null);

  if (
    photoScore === null ||
    photoScore < 0 ||
    photoScore > 1 ||
    photoQuality === null ||
    photoQuality < MIN_PHOTO_QUALITY_FOR_CONDITION
  ) {
    return base;
  }

  return 0.6 * photoScore + 0.4 * base;
}

// ===============================
// COMP CONDITION
// ===============================
function inferCompConditionLevel(comp) {
  const text = (
    (comp.remarks || "") +
    " " +
    (comp.siteCondition || "") +
    " " +
    (comp.heating || "")
  ).toLowerCase();

  if (!text.trim()) return 0.5;
  if (text.includes("fixer") || text.includes("handyman") || text.includes("as-is"))
    return 0.2;
  if (text.includes("distress") || text.includes("short sale"))
    return 0.2;
  if (text.includes("remodeled") || text.includes("updated") || text.includes("new"))
    return 0.75;
  if (text.includes("luxury") || text.includes("custom")) return 0.9;
  if (text.includes("dated") || text.includes("original")) return 0.4;
  return 0.55;
}

// ===============================
// STYLE / LOT / YEAR
// ===============================
function styleGroup(style) {
  if (!style) return "other";
  const s = style.toLowerCase();
  if (s.includes("colonial")) return "colonial";
  if (s.includes("ranch")) return "ranch";
  if (s.includes("cape")) return "cape";
  if (s.includes("split")) return "split";
  if (s.includes("modern") || s.includes("contemporary")) return "contemporary";
  return "other";
}

function styleScore(subject, comp) {
  const a = styleGroup(subject.style);
  const b = styleGroup(comp.style);
  if (!comp.style) return 0.5;
  if (a === b) return 1.0;
  if (a === "other" || b === "other") return 0.7;
  return 0.5;
}

function lotCategory(size) {
  const s = safeNumber(size, null);
  if (!s) return "unknown";
  if (s <= 8000) return "0_8k";
  if (s <= 15000) return "8k_15k";
  if (s <= 25000) return "15k_25k";
  return "25k_plus";
}

function yearBucket(y) {
  const year = safeNumber(y, null);
  if (!year) return "unknown";
  if (year < 1940) return "pre19440";
  if (year < 1970) return "1940_1970";
  if (year < 2000) return "1970_2000";
  return "2000_plus";
}

// similarity scores
function sqftScore(subject, comp) {
  if (!subject.sqft || !comp.sqft) return 0.5;
  const ratio = comp.sqft / subject.sqft;
  const diff = Math.abs(ratio - 1);

  if (diff < 0.05) return 1.0;
  if (diff < 0.1) return 0.85;
  if (diff < 0.2) return 0.65;
  if (diff < 0.35) return 0.45;
  return 0.3;
}

function bedBathScore(subject, comp) {
  const subBeds = safeNumber(subject.beds, 0);
  const compBeds = safeNumber(comp.beds, 0);
  const subBaths = parseBaths(subject.baths);
  const compBaths = parseBaths(comp.baths);

  const bedDiff = Math.abs(compBeds - subBeds);
  const bathDiff = Math.abs(compBaths - subBaths);

  const scoreBeds = bedDiff === 0 ? 1 : bedDiff === 1 ? 0.75 : 0.5;
  const scoreBaths = bathDiff === 0 ? 1 : bathDiff <= 0.5 ? 0.8 : 0.6;

  return 0.6 * scoreBeds + 0.4 * scoreBaths;
}

function yearBuiltScore(subject, comp) {
  const sy = safeNumber(subject.yearBuilt, null);
  const cy = safeNumber(comp.yearBuilt, null);
  if (!sy || !cy) return 0.5;

  const diff = Math.abs(sy - cy);
  if (diff <= 5) return 1;
  if (diff <= 10) return 0.8;
  if (diff <= 20) return 0.6;
  if (diff <= 40) return 0.45;
  return 0.3;
}

function lotScore(subject, comp) {
  const a = lotCategory(subject.lotSize);
  const b = lotCategory(comp.lotSize);
  if (!subject.lotSize || !comp.lotSize) return 0.5;
  if (a === b) return 1;
  if (a === "unknown" || b === "unknown") return 0.6;
  return 0.4;
}

// ===============================
// SCHOOL + NEIGHBORHOOD + LOCATION
// ===============================
function schoolDistrictScore(subject, comp) {
  if (!subject.schoolDistrict || !comp.schoolDistrict) return 0.5;
  if (subject.schoolDistrict === comp.schoolDistrict) return 1.0;

  const sTown = subject.town.toLowerCase();
  const cTown = comp.town ? comp.town.toLowerCase() : "";
  if (sTown && cTown && sTown === cTown) return 0.7;
  return 0.4;
}

function neighborhoodScore(subject, comp, allComps) {
  if (!comp.address) return 0.5;

  const subjectStreet = subject.address.split(" ").slice(1).join(" ").toLowerCase();
  const compStreet = comp.address.split(" ").slice(1).join(" ").toLowerCase();

  if (subjectStreet === compStreet) return 1.0;

  const streetMap = {};
  for (const c of allComps) {
    if (!c.address || !c.ppsf) continue;
    const st = c.address.split(" ").slice(1).join(" ").toLowerCase();
    streetMap[st] = streetMap[st] || [];
    streetMap[st].push(c.ppsf);
  }

  const subjMed = median(streetMap[subjectStreet] || []);
  const compMed = median(streetMap[compStreet] || []);
  if (!subjMed || !compMed) return 0.5;

  const diff = Math.abs(subjMed - compMed) / subjMed;
  if (diff < 0.05) return 0.9;
  if (diff < 0.1) return 0.75;
  if (diff < 0.2) return 0.55;
  return 0.4;
}

function locationScore(subject, comp) {
  if (!subject.lat || !subject.lng || !comp.lat || !comp.lng) {
    const sz = subject.zip;
    const cz = String(comp.zip).padStart(5, "0");
    if (sz === cz) return 1.0;
    if (sz.slice(0, 4) === cz.slice(0, 4)) return 0.75;
    if (sz.slice(0, 3) === cz.slice(0, 3)) return 0.6;
    return 0.4;
  }

  const d = haversineMiles(subject.lat, subject.lng, comp.lat, comp.lng);
  if (d <= 0.25) return 1.0;
  if (d <= 0.5) return 0.9;
  if (d <= 1.0) return 0.75;
  if (d <= 2.0) return 0.55;
  return 0.35;
}

// ===============================
// COMP SCORE AGGREGATION
// ===============================
function computeCompScores(subject, comp, allComps) {
  const loc = locationScore(subject, comp);
  const condSubj = effectiveSubjectConditionLevel(subject);
  const condComp = inferCompConditionLevel(comp);
  const cond = Math.max(0, Math.min(1, 1 - Math.abs(condSubj - condComp)));

  const style = styleScore(subject, comp);
  const sqftS = sqftScore(subject, comp);
  const bedBath = bedBathScore(subject, comp);
  const yearS = yearBuiltScore(subject, comp);
  const lotS = lotScore(subject, comp);
  const school = schoolDistrictScore(subject, comp);
  const hood = neighborhoodScore(subject, comp, allComps);

  const cqs =
    0.22 * loc +
    0.18 * cond +
    0.12 * style +
    0.18 * sqftS +
    0.09 * bedBath +
    0.05 * yearS +
    0.05 * lotS +
    0.11 * school +
    0.08 * hood;

  return {
    cqs,
    subscores: {
      location: loc,
      condition: cond,
      style,
      sqft: sqftS,
      bedBath,
      yearBuilt: yearS,
      lot: lotS,
      schoolDistrict: school,
      neighborhood: hood,
    },
  };
}

// ===============================
// OUTLIERS
// ===============================
function marketTimeBad(mt) {
  const m = safeNumber(mt, null);
  if (!m) return false;
  if (m < 3) return true;
  if (m > 180) return true;
  return false;
}

function median(arr) {
  const nums = arr
    .map((x) => safeNumber(x, null))
    .filter((x) => x !== null)
    .sort((a, b) => a - b);

  if (!nums.length) return null;
  const mid = Math.floor(nums.length / 2);
  return nums.length % 2 ? nums[mid] : (nums[mid - 1] + nums[mid]) / 2;
}

// ===============================
// ADJUSTMENTS (CONDITION + HAZARD + RENOVATION)
// ===============================
function conditionDollarAdjustment(subject, baseValue) {
  const level = effectiveSubjectConditionLevel(subject);
  let pct;

  if (level <= 0.2) pct = -0.25;
  else if (level <= 0.35) pct = -0.15;
  else if (level <= 0.55) pct = 0;
  else if (level <= 0.75) pct = 0.1;
  else if (level <= 0.9) pct = 0.2;
  else pct = 0.25;

  return baseValue * pct;
}

// --- Module D helpers: interpret hazard data into a 0–1 risk score
function deriveHazardScore(hazard) {
  if (!hazard || !hazard.data) return null;
  const data = hazard.data;

  const composite = safeNumber(data.compositeRisk, null);
  if (composite !== null && composite >= 0 && composite <= 1) {
    return composite;
  }

  if (typeof data.floodRisk === "string") {
    const r = data.floodRisk.toLowerCase();
    if (r.includes("very_high") || r.includes("extreme")) return 1.0;
    if (r.includes("high")) return 0.8;
    if (r.includes("moderate") || r.includes("medium")) return 0.5;
    if (r.includes("low")) return 0.2;
  }

  return null;
}

// dollar impact of hazard (downward only, capped at ~8%)
function hazardDollarAdjustment(baseValue, hazard) {
  const score = deriveHazardScore(hazard);
  if (score === null) return 0;

  const maxPct = 0.08; // up to -8% in worst case
  const pct = -maxPct * score;
  return baseValue * pct;
}

// adjust confidence based on hazard
function adjustConfidenceForHazard(confidence, hazard) {
  const score = deriveHazardScore(hazard);
  if (score === null) return confidence;

  const penalty = Math.round(10 * score); // up to -10 points
  const adjusted = confidence - penalty;
  return Math.max(0, Math.min(100, adjusted));
}

function renovationEquityGain(subject) {
  const repairs = safeNumber(subject.estRepairs, 0) || 0;
  let roiPct = 0.6;

  const r = String(subject.renovationStatus || "").toLowerCase();
  if (r.includes("cosmetic")) roiPct = 0.5;
  else if (r.includes("kitchen") || r.includes("bath")) roiPct = 0.65;
  else if (r.includes("full")) roiPct = 0.7;

  return repairs * roiPct;
}

// ===============================
// TRENDS
// ===============================
function computePpsfTrendValue(subject, comps) {
  const dated = comps
    .map((c) => ({ comp: c, months: monthsAgo(c.settledDate) }))
    .filter((x) => x.months !== null);

  let recent = dated.filter((x) => x.months <= 6);
  if (!recent.length) recent = dated;
  if (!recent.length) return null;

  const ppsfArr = recent
    .map((x) => ppsfOf(x.comp))
    .filter((v) => v !== null);

  const med = median(ppsfArr);
  return med ? med * subject.sqft : null;
}

function computeNeighborhoodTrendValue(subject, comps) {
  const prices = comps.filter((c) => c.salePrice).map((c) => c.salePrice);
  const med = median(prices);
  return med || null;
}

// ===============================
// MAIN ENGINE (ASYNC)
// ===============================
async function estimateValue(input) {
  // DEBUG: flag & container
  const debugEnabled = !!input.debug;
  const debug = {
    datasetSize: 0,
    radiusCandidates: {
      r0_25: 0,
      r0_5: 0,
      r1_0: 0,
      fallbackAll: 0,
    },
    scoring: {
      preScoreCount: 0,
      postScoreCount: 0,
    },
    outliers: {
      medianPpsf: null,
      postOutlierCount: 0,
    },
    final: {
      compsUsed: 0,
    },
  };

  const data = loadDataset(input.propertyType);
  debug.datasetSize = Array.isArray(data) ? data.length : 0;

  let subject = normalizeSubject(input);

  // -----------------------------------------
  // PUBLIC DATA MODULE A: ASSESSOR ENRICHMENT
  // -----------------------------------------
  const {
    subject: subjectWithAssessor,
    assessor,
  } = enrichWithAssessorData(subject);
  subject = subjectWithAssessor;

  // -----------------------------------------
  // PUBLIC DATA MODULE B: PARCEL / GEOMETRY
  // -----------------------------------------
  let parcel = await fetchParcelData({ lat: subject.lat, lng: subject.lng });

  if (parcel && parcel.ok) {
    subject.lotSize = subject.lotSize || parcel.data.lotSqft;

    subject.parcelInfo = {
      lotSqft: parcel.data.lotSqft,
      shape: parcel.data.shape,
      frontage: parcel.data.frontage,
      shapeScore: parcel.data.shapeScore,
      parcelScore: parcel.data.parcelScore,
    };
  }

  // -----------------------------------------
  // PUBLIC DATA MODULE D: HAZARD / FLOOD / RISK
  // -----------------------------------------
  let hazard = await fetchHazardData({
    lat: subject.lat,
    lng: subject.lng,
    address: subject.address,
    zip: subject.zip,
  });

  if (!hazard) {
    hazard = { ok: false, data: null, source: null };
  }

  // ============================
  // COMP SELECTION BY RADIUS
  // ============================
  function filterByRadius(r) {
    return data.filter((c) => {
      if (!c.salePrice || !c.sqft || !c.lat || !c.lng) return false;

      const d = haversineMiles(subject.lat, subject.lng, c.lat, c.lng);
      if (d === null || d > r) return false;

      const sizeDiff = Math.abs(c.sqft - subject.sqft) / subject.sqft;
      if (sizeDiff > 0.5) return false;

      return true;
    });
  }

  let comps = filterByRadius(0.25);
  debug.radiusCandidates.r0_25 = comps.length;

  if (comps.length < 5) {
    comps = filterByRadius(0.5);
    debug.radiusCandidates.r0_5 = comps.length;
  }

  if (comps.length < 5) {
    comps = filterByRadius(1.0);
    debug.radiusCandidates.r1_0 = comps.length;
  }

  if (comps.length < 5) {
    comps = data.filter((c) => c.salePrice && c.sqft);
    debug.radiusCandidates.fallbackAll = comps.length;
  }

  if (!comps.length) {
    return {
      estimatedValue: null,
      lowEstimate: null,
      highEstimate: null,
      confidence: 0,
      spreadPercent: null,
      compsUsed: 0,
      sampleComps: [],
      assessor,
      parcel,
      hazard,
      reason: "No valid comps found.",
      debug: debugEnabled ? debug : undefined,
    };
  }

  debug.scoring.preScoreCount = comps.length;

  // ============================
  // SCORING
  // ============================
  let scored = comps
    .map((c) => {
      const { cqs, subscores } = computeCompScores(subject, c, data);
      const ppsf = ppsfOf(c);
      return { ...c, cqs, subscores, ppsf };
    })
    .filter((c) => c.cqs > 0 && c.ppsf !== null);

  debug.scoring.postScoreCount = scored.length;

  if (!scored.length) {
    return {
      estimatedValue: null,
      lowEstimate: null,
      highEstimate: null,
      confidence: 0,
      compsUsed: 0,
      sampleComps: [],
      assessor,
      parcel,
      hazard,
      reason: "All comps filtered out after scoring.",
      debug: debugEnabled ? debug : undefined,
    };
  }

  // ============================
  // OUTLIERS
  // ============================
  const medPpsf = median(scored.map((c) => c.ppsf));
  debug.outliers.medianPpsf = medPpsf;

  scored = scored.filter((c) => {
    if (!medPpsf) return true;
    if (c.ppsf > medPpsf * 2) return false;
    if (c.ppsf < medPpsf * 0.5) return false;
    if (marketTimeBad(c.marketTime)) return false;
    return true;
  });

  debug.outliers.postOutlierCount = scored.length;

  if (!scored.length) {
    return {
      estimatedValue: null,
      lowEstimate: null,
      highEstimate: null,
      confidence: 0,
      compsUsed: 0,
      sampleComps: [],
      assessor,
      parcel,
      hazard,
      reason: "All comps removed as outliers.",
      debug: debugEnabled ? debug : undefined,
    };
  }

  // ============================
  // SORT BY QUALITY
  // ============================
  scored = scored.sort((a, b) => {
    if (b.cqs !== a.cqs) return b.cqs - a.cqs;

    const diffA = Math.abs(a.sqft - subject.sqft);
    const diffB = Math.abs(b.sqft - subject.sqft);
    if (diffA !== diffB) return diffA - diffB;

    const yA = safeNumber(a.yearBuilt, 0);
    const yB = safeNumber(b.yearBuilt, 0);
    const ydA = Math.abs(yA - subject.yearBuilt);
    const ydB = Math.abs(yB - subject.yearBuilt);
    if (ydA !== ydB) return ydA - ydB;

    return a.salePrice - b.salePrice;
  });

  const MAX_COMPS = 12;
  if (scored.length > MAX_COMPS) scored = scored.slice(0, MAX_COMPS);
  const compsUsed = scored.length;
  debug.final.compsUsed = compsUsed;

  // ============================
  // VALUE CALCULATION
  // ============================
  const totalWeight = scored.reduce((s, c) => s + c.cqs, 0) || 1;
  const weightedValue =
    scored.reduce((s, c) => s + c.salePrice * c.cqs, 0) / totalWeight;

  // condition adjustment
  const condAdj = conditionDollarAdjustment(subject, weightedValue);
  let adjustedValue = weightedValue + condAdj;

  // hazard adjustment (Module D)
  const hazardAdj = hazardDollarAdjustment(adjustedValue, hazard);
  adjustedValue += hazardAdj;

  const equityGain = renovationEquityGain(subject);
  const asIsValue = Math.round(adjustedValue);
  const afterRenovationValue = Math.round(adjustedValue + equityGain);

  const ppsfTrendValue =
    computePpsfTrendValue(subject, scored) || adjustedValue;
  const hoodTrendValue =
    computeNeighborhoodTrendValue(subject, scored) || adjustedValue;

  const finalValue =
    0.7 * adjustedValue + 0.2 * ppsfTrendValue + 0.1 * hoodTrendValue;

  const estimatedValue = Math.round(finalValue);

  // ============================
  // CONFIDENCE + SPREAD
  // ============================
  const top = scored.slice(0, Math.min(6, scored.length));
  const avgCqs =
    top.reduce((s, x) => s + x.cqs, 0) / (top.length || 1);

  const rawConfidence = Math.round(
    Math.min(100, Math.max(0, avgCqs * 100))
  );
  const confidence = adjustConfidenceForHazard(rawConfidence, hazard);

  let spreadFactor;
  if (confidence >= 90) spreadFactor = 0.05;
  else if (confidence >= 75) spreadFactor = 0.08;
  else if (confidence >= 50) spreadFactor = 0.12;
  else spreadFactor = 0.18;

  const lowEstimate = Math.round(estimatedValue * (1 - spreadFactor));
  const highEstimate = Math.round(estimatedValue * (1 + spreadFactor));
  const spreadPercent = Math.round(spreadFactor * 100);

  // ============================
  // SAMPLE COMPS
  // ============================
  const sampleComps = scored.slice(0, 10).map((c) => ({
    address: c.address,
    town: c.town,
    zip: String(c.zip).padStart(5, "0"),
    beds: c.beds,
    baths: c.baths,
    sqft: c.sqft,
    salePrice: c.salePrice,
    settledDate: c.settledDate,
    marketTime: c.marketTime,
    lotSize: c.lotSize,
    yearBuilt: c.yearBuilt,
    style: c.style,
    garageSpaces: c.garageSpaces,
    ppsf: c.ppsf,
    cqs: Number(c.cqs.toFixed(3)),
    subscores: c.subscores,
  }));

  // ============================
  // RETURN
  // ============================
  return {
    estimatedValue,
    lowEstimate,
    highEstimate,
    confidence,
    spreadPercent,
    asIsValue,
    afterRenovationValue,
    compsUsed,
    sampleComps,

    // PUBLIC DATA DIAGNOSTICS
    assessor,
    parcel,
    hazard,

    // DEBUG (optional)
    debug: debugEnabled ? debug : undefined,
  };
}

module.exports = { estimateValue };
