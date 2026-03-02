import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";
import proj4 from "proj4";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Paths (override via CLI flags to avoid editing this file)
// Example:
//   node --max-old-space-size=8192 mls/scripts/patchMissingCoordsFromAddressIndex_v14_DROPIN.js --in .\publicData\properties\properties_statewide_geo_zip_district_v13_coords.ndjson --out .\publicData\properties\properties_statewide_geo_zip_district_v14_coords.ndjson
function getArg(flag, fallback = null) {
  const idx = process.argv.indexOf(`--${flag}`);
  if (idx === -1) return fallback;
  const v = process.argv[idx + 1];
  if (!v || v.startsWith("--")) return fallback;
  return v;
}

function resolveUserPath(p) {
  if (!p) return p;
  const s = String(p).trim().replace(/^["']|["']$/g, "");
  // If caller passed an absolute Windows/Posix path, keep it.
  if (path.isAbsolute(s)) return path.normalize(s);
  // Otherwise treat relative paths as relative to where you run the command (backend root).
  return path.resolve(process.cwd(), s);
}

const IN = resolveUserPath(
  getArg("in", "publicData/properties/properties_statewide_geo_zip_district_v22_coords.ndjson")
);
const ADDRESS_INDEX = resolveUserPath(
  getArg("addressIndex", "publicData/addresses/addressIndex.json")
);

const OUT = resolveUserPath(
  getArg("out", "publicData/properties/properties_statewide_geo_zip_district_v23_coords.ndjson")
);

// Safety: never allow IN and OUT to be the same file (would truncate IN to 0 bytes).
if (path.resolve(IN).toLowerCase() === path.resolve(OUT).toLowerCase()) {
  console.error(
    `❌ Refusing to run: --in and --out point to the same path:
  ${IN}
Set --out to a different filename (e.g., v22).`
  );
  process.exit(1);
}

const OUT_META = resolveUserPath(
  getArg("meta", OUT.replace(/\.ndjson$/i, "_meta.json"))
);

// EPSG:26986 = NAD83 / Massachusetts Mainland (meters)
proj4.defs(
  "EPSG:26986",
  "+proj=lcc +lat_1=41.71666666666667 +lat_2=42.68333333333333 +lat_0=41 +lon_0=-71.5 +x_0=200000 +y_0=750000 +datum=NAD83 +units=m +no_defs"
);
const FT_TO_M = 0.3048006096012192;

// NOTE: street suffix expansions (add more as needed)

function stripLeadingNoiseTokens(street) {
  // Some assessor/IDX strings include leading noise like "SS", "ES", "RR S/S", "GAR", etc.
  // We keep the original street too (caller should try both), but this helps recover real names.
  if (!street) return "";
  let s = collapse(street);

  // Normalize slash-ish tokens to stable forms so we can drop them reliably.
  // e.g., "S/S", "S\ S", "S S" all become "S/S" (same for N/E/W)
  s = s
    .replace(/\b([NSEW])\s*[\/\\]\s*S\b/g, "$1/S")
    .replace(/\b([NSEW])\s+S\b/g, "$1/S");

  // Drop leading "unit-ish" tokens often used in condo/complex labels:
  //   "D12 AL PACE DR" -> "AL PACE DR"
  //   "B3 AL PACE DR"  -> "AL PACE DR"
  //   "6T09 TARA DR"   -> "TARA DR" (street no usually comes from parcel_id)
  s = s.replace(/^(?:[A-Z]{1,3}\d{1,6}|\d{1,6}[A-Z]{1,2}\d{1,6})\s+/, "");

  const toks = s.split(" ").filter(Boolean);

  // Leading noise tokens commonly seen in assessor-style strings
  const BAD = new Set(["RR", "SS", "ES", "WS", "NS", "N/S", "S/S", "E/S", "W/S", "E/W", "N/W", "N/E", "S/E", "S/W"]);

  // GAR is sometimes a real word (e.g., "GAR HWY"). We only drop it when it looks like a prefix noise.
  function isDropGar(idx) {
    if (toks[idx] !== "GAR") return false;
    const nxt = toks[idx + 1] || "";
    return nxt && !["HWY", "HIGHWAY"].includes(nxt);
  }

  // Drop 1–2 leading noise tokens (handles "RR S/S ..." by dropping RR then S/S)
  while (toks.length) {
    const t0 = toks[0];
    if (BAD.has(t0)) {
      toks.shift();
      continue;
    }
    if (isDropGar(0)) {
      toks.shift();
      continue;
    }
    break;
  }

  return toks.join(" ");
}


function hasKnownSuffix(street) {
  const parts = collapse(street).split(" ").filter(Boolean);
  if (!parts.length) return false;
  const last = parts[parts.length - 1];
  return SUFFIX_VARIANTS.has(last);
}

const DEFAULT_SUFFIX_APPEND = ["ST", "RD", "LN", "AVE", "DR", "CT", "PL", "TER", "WAY", "CIR", "BLVD"];

const SUFFIX_VARIANTS = new Map([
  // Common street types / abbreviations (we generate *variants*; we do NOT force a single canonical form)
  ["RD", ["RD", "ROAD"]],
  ["ROAD", ["ROAD", "RD"]],
  ["ST", ["ST", "STREET"]],
  ["STREET", ["STREET", "ST"]],
  ["AVE", ["AVE", "AV", "AVENUE"]],
  ["AV", ["AV", "AVE", "AVENUE"]],
  ["AVENUE", ["AVENUE", "AVE", "AV"]],
  ["BLVD", ["BLVD", "BV", "BOULEVARD"]],
  ["BV", ["BV", "BLVD", "BOULEVARD"]],
  ["BOULEVARD", ["BOULEVARD", "BLVD", "BV"]],
  ["DR", ["DR", "DRIVE"]],
  ["DRIVE", ["DRIVE", "DR"]],
  ["LN", ["LN", "LA", "LANE"]],
  ["LA", ["LA", "LN", "LANE"]],
  ["LANE", ["LANE", "LN", "LA"]],
  ["CT", ["CT", "COURT"]],
  ["COURT", ["COURT", "CT"]],
  ["PL", ["PL", "PLACE"]],
  ["PLACE", ["PLACE", "PL"]],
  ["PKWY", ["PKWY", "PARKWAY", "PWY", "PKY", "PKW"]],
  ["PARKWAY", ["PARKWAY", "PKWY", "PWY", "PKY", "PKW"]],
  ["PWY", ["PWY", "PKWY", "PARKWAY"]],
  ["CIR", ["CIR", "CI", "CR", "CIRCLE"]],
  ["CI", ["CI", "CIR", "CR", "CIRCLE"]],
  ["CR", ["CR", "CIR", "CI", "CIRCLE", "CRESCENT"]],
  ["CRESCENT", ["CRESCENT", "CR"]],
  ["CIRCLE", ["CIRCLE", "CIR", "CI", "CR"]],
  ["HWY", ["HWY", "HIGHWAY"]],
  ["HIGHWAY", ["HIGHWAY", "HWY"]],
  ["TER", ["TER", "TERR", "TERRACE"]],
  ["TERR", ["TERR", "TER", "TERRACE"]],
  ["TERRACE", ["TERRACE", "TER", "TERR"]],
  ["TR", ["TR", "TRL", "TRAIL"]],
  ["TRL", ["TRL", "TRAIL"]],
  ["TRAIL", ["TRAIL", "TRL"]],
  ["SQ", ["SQ", "SQUARE"]],
  ["SQUARE", ["SQUARE", "SQ"]],
  ["CTR", ["CTR", "CENTER"]],
  ["CENTER", ["CENTER", "CTR"]],
  ["EXT", ["EXT", "EXTENSION"]],
  ["EXTENSION", ["EXTENSION", "EXT"]],
  ["WY", ["WY", "WAY"]],
  ["WAY", ["WAY", "WY"]],

  // Less common but shows up in MA assessor / parcel labels
  ["RDG", ["RDG", "RIDGE"]],
  ["RIDGE", ["RIDGE", "RDG"]],
  ["HTS", ["HTS", "HEIGHTS"]],
  ["HGTS", ["HGTS", "HEIGHTS", "HTS"]],
  ["HEIGHTS", ["HEIGHTS", "HTS"]],
  ["HOLW", ["HOLW", "HOLLOW"]],
  ["HOLLOW", ["HOLLOW", "HOLW"]],
  ["XING", ["XING", "CROSSING"]],
  ["CROSSING", ["CROSSING", "XING"]],
  ["MT", ["MT", "MOUNT"]],
  ["MOUNT", ["MOUNT", "MT"]],
  ["FT", ["FT", "FORT"]],
  ["FORT", ["FORT", "FT"]],
  ["TNPK", ["TNPK", "TPKE", "TURNPIKE"]],
  ["TPKE", ["TPKE", "TNPK", "TURNPIKE"]],
  ["TURNPIKE", ["TURNPIKE", "TPKE", "TNPK"]],
  ["PKE", ["PKE", "PIKE"]],
  ["PIKE", ["PIKE", "PKE"]],

  // Existing "odd-but-real" abbreviations we've already seen in your buckets
  ["BL", ["BL", "BLUFF"]],
  ["BLUFF", ["BLUFF", "BL"]],
  ["PK", ["PK", "PARK"]],
  ["PARK", ["PARK", "PK"]],
  ["PW", ["PW", "PKWY", "PARKWAY"]],
]);

// spelled-out ordinals you see in Boston / SE Mass
const ORDINAL_MAP = new Map([
  ["FIRST", "1ST"],
  ["SECOND", "2ND"],
  ["THIRD", "3RD"],
  ["FOURTH", "4TH"],
  ["FIFTH", "5TH"],
  ["SIXTH", "6TH"],
  ["SEVENTH", "7TH"],
  ["EIGHTH", "8TH"],
  ["NINTH", "9TH"],
  ["TENTH", "10TH"],
  ["ELEVENTH", "11TH"],
  ["TWELFTH", "12TH"],
  ["THIRTEENTH", "13TH"],
  ["FOURTEENTH", "14TH"],
  ["FIFTEENTH", "15TH"],
  ["SIXTEENTH", "16TH"],
  ["SEVENTEENTH", "17TH"],
  ["EIGHTEENTH", "18TH"],
  ["NINETEENTH", "19TH"],
  ["TWENTIETH", "20TH"],
]);

const STREET_TYPE_WORDS = (() => {
  const s = new Set();
  for (const arr of SUFFIX_VARIANTS.values()) {
    for (const v of arr) s.add(v);
  }
  // also treat these as "type-ish" so we can trim trailing locality tails after them
  for (const v of ["N", "S", "E", "W", "NORTH", "SOUTH", "EAST", "WEST"]) s.add(v);
  return s;
})();

function trimAfterStreetTypeWord(street) {
  if (!street) return null;
  const toks = street.split(/\s+/).filter(Boolean);
  if (toks.length < 3) return null;
  for (let i = toks.length - 1; i >= 0; i--) {
    const t = toks[i];
    if (STREET_TYPE_WORDS.has(t)) {
      // if there are extra tokens after a known street type, keep up to the type (drops village/locality tails)
      if (i < toks.length - 1) return toks.slice(0, i + 1).join(" ");
      return null;
    }
  }
  return null;
}




function collapse(s) {
  if (s == null) return "";
  return String(s)
    .toUpperCase()
    // Remove apostrophes/backticks (common in Cape & historic street names)
    // so "PITCHER'S" -> "PITCHERS", "CAP'N" -> "CAPN"
    .replace(/[`']/g, "")
    .replace(/"/g, "")
    // unify punctuation
    .replace(/[.,]/g, " ")
    .replace(/[\/]/g, " ")
    .replace(/[-]/g, " ")
    .replace(/[&]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function collapseSpaces(s) {
  if (s == null) return "";
  return String(s)
    .replace(/\u00A0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// Strip leading zeros from a street number string.
// Examples:
//  "0145" -> "145"
//  "0007A" -> "7A"
//  "0" -> "0"
//  "117V" -> "117V" (keeps alphanumeric house numbers)
function stripLeadingZeros(streetNo) {
  const raw = String(streetNo ?? "").trim();
  if (!raw) return raw;

  // If it's purely digits, strip leading zeros but keep "0" as "0".
  if (/^\d+$/.test(raw)) {
    const n = raw.replace(/^0+(?=\d)/, "");
    return n === "" ? "0" : n;
  }

  // If it's digits + letters (common: 117V, 0007A), strip zeros from numeric prefix only.
  const m = raw.match(/^0*(\d+)([A-Za-z].*)$/);
  if (m) return `${m[1]}${m[2]}`;

  // Otherwise, leave unchanged (ranges like "12-14" should be handled elsewhere).
  return raw;
}

// Remove unit/suite noise from street fields (keeps only the base street name).
// Examples:
//  "HARRISON AV D-121" -> "HARRISON AV"
//  "LONGWOOD AVE 301 P2 P10" -> "LONGWOOD AVE"
//  "WOODSIDE ROAD, MARSTONS MILLS" -> "WOODSIDE ROAD"
function stripUnitNoise(s) {
  // Normalize & remove common unit/suite noise from a street string.
  // IMPORTANT: This function is intentionally conservative; it should help keys match the addressIndex
  // without destroying legitimate street names.
  let t = collapseSpaces(String(s ?? "")).toUpperCase();

  // 1) Remove trailing unit markers like "#B003", "APT 4", "UNIT 12", "STE 200", "PH"
  t = t
    .replace(/\s*#\s*[A-Z0-9\-]+\s*$/g, "")
    .replace(/\s+(APT|APARTMENT|UNIT|STE|SUITE|FL|FLOOR)\s*[A-Z0-9\-]+\s*$/g, "")
    .replace(/\s+(PH|PENTHOUSE)\s*[A-Z0-9\-]*\s*$/g, "");

  // 2) Remove a leading unit token when the string has no house number.
  // Examples:
  //   "B3 AL PACE DR"     -> "AL PACE DR"
  //   "U-B06 SCOTTY HOLLOW DR" -> "SCOTTY HOLLOW DR"
  //   "U B06 SCOTTY HOLLOW DR" -> "SCOTTY HOLLOW DR"
  t = t.replace(/^(?:UNIT|APT|APARTMENT|STE|SUITE)\s+/g, "");
  t = t.replace(/^U\s*[-\s]\s*[A-Z0-9]+\s+/g, ""); // U-B06 / U B06
  t = t.replace(/^[A-Z]{1,2}\d{1,4}\s+(?=[A-Z].*\b(ST|STREET|RD|ROAD|AVE|AVENUE|DR|DRIVE|LN|LANE|CT|COURT|PL|PLACE|TER|TERR|TRL|TRAIL|WAY|HWY|HIGHWAY|BLVD|BOULEVARD|PKWY|PARKWAY|CIR|CIRCLE)\b)/g, "");

  // 3) Clean odd punctuation
  t = t.replace(/[’`]/g, "'").replace(/\s+/g, " ").trim();

  return collapseSpaces(t);
}


function cleanZip(z) {
  const d = String(z ?? "").replace(/[^0-9]/g, "");
  return d.length >= 5 ? d.slice(0, 5) : (d.length === 4 ? "0" + d : "");
}

function looksWgs84(lng, lat) {
  return Number.isFinite(lng) && Number.isFinite(lat) && Math.abs(lng) <= 180 && Math.abs(lat) <= 90;
}
function looksLikeMA(lng, lat) {
  return Number.isFinite(lng) && Number.isFinite(lat) && lng <= -69 && lng >= -74 && lat >= 41 && lat <= 43.5;
}
function looksLikeProjectedXY(x, y) {
  return Number.isFinite(x) && Number.isFinite(y) && (Math.abs(x) > 180 || Math.abs(y) > 90);
}
function convert26986ToWgs(x, y, scale = 1) {
  try {
    const xx = Number(x) * scale;
    const yy = Number(y) * scale;
    const [lon, lat] = proj4("EPSG:26986", "WGS84", [xx, yy]);
    if (looksLikeMA(lon, lat)) return { lat, lng: lon, epsg: "EPSG:26986", x_sp: xx, y_sp: yy };
  } catch {}
  return null;
}

function parseCoordFromAddressIndex(v) {
  if (!v || typeof v !== "object") return null;

  const town =
    (v.town ?? v.TOWN ?? v.city ?? v.CITY ?? v.municipality ?? v.MUNICIPALITY ?? v.commune ?? null);
  const zip = (v.zip ?? v.ZIP ?? v.postcode ?? v.POSTCODE ?? null);

  const rawLat = Number(v.lat ?? v.latitude ?? v.y ?? v.Y ?? v.northing ?? v.N ?? NaN);
  const rawLon = Number(v.lon ?? v.lng ?? v.longitude ?? v.x ?? v.X ?? v.easting ?? v.E ?? NaN);
  if (!Number.isFinite(rawLat) || !Number.isFinite(rawLon)) return null;

  if (looksWgs84(rawLon, rawLat) && looksLikeMA(rawLon, rawLat)) {
    return { lat: rawLat, lng: rawLon, crs: "WGS84", x_sp: null, y_sp: null, town, zip };
  }
  if (looksLikeProjectedXY(rawLon, rawLat)) {
    const meters = convert26986ToWgs(rawLon, rawLat, 1);
    if (meters) return { ...meters, crs: meters.epsg, town, zip };
    const feet = convert26986ToWgs(rawLon, rawLat, FT_TO_M);
    if (feet) return { ...feet, crs: feet.epsg, town, zip };
  }
  return null;
}


function parseRange(full) {
  const s = collapse(full);
  let m = s.match(/^(\d+)\s+(\d+)\s+(.+)$/);
  if (m) return { lo: m[1], hi: m[2], street: m[3] };
  m = s.match(/^(\d+)\s*\-\s*(\d+)\s+(.+)$/);
  if (m) return { lo: m[1], hi: m[2], street: m[3] };
  return null;
}

function parseAmpersand(full) {
  const s = collapse(full);
  const a = s.match(/^(\d+)\s*(?:&|AND)\s*(\d+)\s+(.+)$/);
  if (a) return { a: a[1], b: a[2], street: a[3] };
  return null;
}

// full_address like "42 -42A-B YALE AVE" or "33 &35 IVANHOE AVE"
// drop the leading number cluster + connectors, keep the street remainder
function deriveStreetFromFullAddress(full) {
  if (!full) return "";
  let s = canonicalizeSpaces(full).toUpperCase();
  const orig = s;

  // Remove obvious tail noise (units, hashes, etc) first.
  s = stripTailNoise(s);

  // Remove leading ranges or numbers: "894-896 ..." -> "..."
  const mRange = s.match(/^\s*\d{1,6}\s*-\s*\d{1,6}\b/);
  if (mRange) {
    s = s.replace(mRange[0], " ");
  } else {
    const mLead = s.match(/^\s*\d{1,6}\b/);
    if (mLead) s = s.replace(mLead[0], " ");
  }

  // If it looked like "17-D THE HOLLOW" or "5-C TOP FLIGHT DR", strip the leftover unit token ("D", "C").
  if (/^\s*\d{1,6}\s*-\s*[A-Z]\b/.test(orig)) {
    s = s.replace(/^\s*[-#]?\s*[A-Z]\b/, " ");
  }

  // If it looked like "6-6T09 TARA DR", strip leftover token like "6T09"
  if (/^\s*\d{1,6}\s*-\s*\d{1,6}[A-Z]{1,2}\d{1,6}\b/.test(orig)) {
    s = s.replace(/^\s*[-#]?\s*\d{1,6}[A-Z]{1,2}\d{1,6}\b/, " ");
  }

  // Strip leading punctuation and commas
  s = s.replace(/^[\s,.-]+/, "").trim();

  // Drop any remaining leading unit-ish tokens (safe: only drops tokens with BOTH letters and digits)
  s = stripLeadingNoiseTokens(s);

  // Normalize directionals at the very start if present (kept minimal)
  s = s.replace(/^\b(N|S|E|W)\b\s+/i, "$1 ");

  // Collapse spaces
  s = collapse(s);

  return s;
}



// v6: strip condo/unit + village tails reliably (works after hyphens become spaces)
function stripTailNoise(street) {
  if (!street) return "";
  let s0 = String(street);

  // Trim locality after comma: "..., HYANNIS"
  s0 = s0.split(",")[0];

  // Remove parenthetical tags but keep surrounding text
  s0 = s0.replace(/\([^)]*\)/g, " ");

  // Normalize some separators
  s0 = s0.replace(/[\/]/g, " ").replace(/-/g, " ");

  // Split common glued patterns: "U212" -> "U 212", "PS9" -> "PS 9"
  s0 = s0.replace(/\b([A-Z]{1,3})(\d{1,5}[A-Z0-9-]*)\b/g, "$1 $2");

  // Normalize leading digit+letter: "267A" -> "267 A"
  s0 = s0.replace(/^(\d+)([A-Z])\b/i, "$1 $2");

  let s = collapse(s0);

  // If the street begins with a single-letter token that is usually a condo/unit marker ("A HIGHLAND STREET"),
  // drop it unless it forms a known street like "A ST"/"A STREET".
  // We only do this when we can see a real street-type word later in the string.
  {
    const toks = s.split(" ").filter(Boolean);
    if (toks.length >= 3) {
      const first = toks[0];
      const second = toks[1];
      const hasStreetType = toks.some((t) => SUFFIX_VARIANTS.has(t) || t === "STREET");
      const isDirection = ["N","S","E","W","NE","NW","SE","SW","NORTH","SOUTH","EAST","WEST","NORTHEAST","NORTHWEST","SOUTHEAST","SOUTHWEST","NO","SO","EA","WE"].includes(first);
      const isAStreet = first === "A" && (second === "ST" || second === "STREET");
      if (first.length === 1 && !isDirection && !isAStreet && hasStreetType) {
        toks.shift();
        s = toks.join(" ");
      }
    }
  }

  // Drop tail tokens that are not street names
  s = s.replace(/\s+\b(?:REAR|OFF|LINE|BEHIND)\b\s*$/i, "");

  // Drop explicit unit/suite tails (often condos)
  s = s.replace(/\s+\b(?:UNIT|APT|APARTMENT|STE|SUITE|FLOOR|FL|PH|PENTHOUSE)\b\s+\b[A-Z0-9-]+\b(?:\s*&\s*\b[A-Z0-9-]+\b)*\s*$/i, "");

  // Drop "#A", "# 12", "U 212"
  s = s.replace(/\s+\#\s*[A-Z0-9-]+\s*$/i, "");
  s = s.replace(/\s+\bU\s+\d+[A-Z]?\b\s*$/i, "");

  // Drop parking / storage codes at end: "PS-9", "PS B23", "P14", "G2"
  s = s.replace(/\s+\b(?:PS|PK|P|GAR|G)\b\s+\b[A-Z0-9-]+\b\s*$/i, "");
  s = s.replace(/\s+\bP\d{1,5}\b\s*$/i, "");
  s = s.replace(/\s+\bG\d{1,5}\b\s*$/i, "");

  // Drop condo/unit building codes at end: "D-121" -> "D 121"
  s = s.replace(/\s+\b[A-Z]{1,3}\s+\d+[A-Z0-9]*\b\s*$/i, "");

  // NEW: drop common assessor/unit tails like "4PH", "10F", "51D"
  s = s.replace(/\s+\d+[A-Z]{1,3}\s*$/i, "");
  s = s.replace(/\s+\d+\s+[A-Z]{1,3}\s*$/i, "");

  // NEW: drop common town/area/trailer codes that show up at the end ("DE", "DP", "WD", ...)
  // (We keep NE/NW/SE/SW because those can be real directionals.)
  s = s.replace(/\s+\b(?:DE|DP|WD|SD|EX|IO)\b\s*$/i, "");

  // Drop trailing two-letter noise tokens that appear after a real street type ("DRIVE DE", "LANE DP")
  {
    const toks = s.split(" ").filter(Boolean);
    if (toks.length >= 3) {
      const last = toks[toks.length - 1];
      const prev = toks[toks.length - 2];
      if (/^[A-Z]{1,2}$/.test(last) && (SUFFIX_VARIANTS.has(prev) || prev === "DRIVE" || prev === "LANE" || prev === "ROAD" || prev === "STREET" || prev === "AVENUE")) {
        toks.pop();
        s = toks.join(" ");
      }
    }
  }

  // Drop trailing single-letter unit marker: "CIR B", "ST A"
  {
    const toks = s.split(" ").filter(Boolean);
    if (toks.length >= 3) {
      const last = toks[toks.length - 1];
      const prev = toks[toks.length - 2];
      if (/^[A-Z]$/.test(last) && (SUFFIX_VARIANTS.has(prev) || prev === "STREET" || prev === "ROAD" || prev === "LANE" || prev === "DRIVE" || prev === "AVENUE")) {
        toks.pop();
        s = toks.join(" ");
      }
    }
  }

  // Drop trailing standalone number token (often a unit)
  s = s.replace(/\s+\d+\s*$/i, "");

  // One more pass: removing one tail can expose another (e.g., "P14 11" -> "P14")
  for (let i = 0; i < 4; i++) {
    const before = s;

    s = s.replace(/\s+\d+[A-Z]{1,3}\s*$/i, "");
    s = s.replace(/\s+\d+\s+[A-Z]{1,3}\s*$/i, "");
    s = s.replace(/\s+\d+\s*$/i, "");
    s = s.replace(/\s+\bP\d{1,5}\b\s*$/i, "");
    s = s.replace(/\s+\bG\d{1,5}\b\s*$/i, "");
    s = s.replace(/\s+\b(?:PS|PK|P|GAR|G)\b\s+\b[A-Z0-9-]+\b\s*$/i, "");
    s = s.replace(/\s+\b[A-Z]{1,3}\s+\d+[A-Z0-9]*\b\s*$/i, "");
    s = s.replace(/\s+\b(?:DE|DP|WD|SD|EX|IO)\b\s*$/i, "");

    if (s === before) break;
  }

  return s.replace(/\s+/g, " ").trim();
}


function normalizeDirections(streetUpper) {
  const s = collapse(streetUpper);
  const parts = s.split(" ").filter(Boolean);
  if (!parts.length) return [s];

  const first = parts[0];
  const out = new Set([s]);

  const addParts = (arr) => out.add(arr.join(" "));
  const replFirst = (rep) => {
    const p = parts.slice();
    p[0] = rep;
    addParts(p);
  };

  // Expand common abbrev directionals (2-letter)
  if (first === "NO") { replFirst("N"); replFirst("NORTH"); }
  if (first === "SO") { replFirst("S"); replFirst("SOUTH"); }
  if (first === "EA") { replFirst("E"); replFirst("EAST"); }
  if (first === "WE") { replFirst("W"); replFirst("WEST"); }

  // Expand single-letter directionals (this was a big miss for "N BEACON ST" style)
  if (first === "N") replFirst("NORTH");
  if (first === "S") replFirst("SOUTH");
  if (first === "E") replFirst("EAST");
  if (first === "W") replFirst("WEST");

  // Diagonal directionals (rare but appears in some towns)
  if (first === "NE") { replFirst("NORTHEAST"); }
  if (first === "NW") { replFirst("NORTHWEST"); }
  if (first === "SE") { replFirst("SOUTHEAST"); }
  if (first === "SW") { replFirst("SOUTHWEST"); }

  // If already long-form, add abbreviations too
  if (first === "NORTH") replFirst("N");
  if (first === "SOUTH") replFirst("S");
  if (first === "EAST") replFirst("E");
  if (first === "WEST") replFirst("W");

  if (first === "NORTHEAST") replFirst("NE");
  if (first === "NORTHWEST") replFirst("NW");
  if (first === "SOUTHEAST") replFirst("SE");
  if (first === "SOUTHWEST") replFirst("SW");

  // Optional: many datasets omit directionals entirely; try dropping them
  if (["N","S","E","W","NE","NW","SE","SW","NORTH","SOUTH","EAST","WEST","NORTHEAST","NORTHWEST","SOUTHEAST","SOUTHWEST","NO","SO","EA","WE"].includes(first) && parts.length >= 2) {
    addParts(parts.slice(1));
  }

  // MT ↔ MOUNT (street-name, not a direction, but common in MA)
  if (first === "MT") replFirst("MOUNT");
  if (first === "MOUNT") replFirst("MT");

  return Array.from(out);
}

// Trailing directionals (e.g., "WORTHEN RD E", "MAIN ST NORTH").
// We generate variants: keep, expand, abbreviate, and drop.
function normalizeTrailingDirections(streetUpper) {
  const s = collapse(streetUpper);
  const parts = s.split(" ").filter(Boolean);
  if (!parts.length) return [s];

  const last = parts[parts.length - 1];
  const out = new Set([s]);

  const replLast = (rep) => {
    const p = parts.slice();
    p[p.length - 1] = rep;
    out.add(p.join(" "));
  };

  const dropLast = () => {
    if (parts.length >= 2) out.add(parts.slice(0, -1).join(" "));
  };

  const map = new Map([
    ["N", "NORTH"],
    ["S", "SOUTH"],
    ["E", "EAST"],
    ["W", "WEST"],
    ["NE", "NORTHEAST"],
    ["NW", "NORTHWEST"],
    ["SE", "SOUTHEAST"],
    ["SW", "SOUTHWEST"],
  ]);

  const rev = new Map([
    ["NORTH", "N"],
    ["SOUTH", "S"],
    ["EAST", "E"],
    ["WEST", "W"],
    ["NORTHEAST", "NE"],
    ["NORTHWEST", "NW"],
    ["SOUTHEAST", "SE"],
    ["SOUTHWEST", "SW"],
  ]);

  if (map.has(last)) { replLast(map.get(last)); dropLast(); }
  if (rev.has(last)) { replLast(rev.get(last)); dropLast(); }

  return Array.from(out);
}


function applyOrdinalVariants(streetUpper) {
  const s = collapse(streetUpper);
  const out = new Set([s]);
  const parts = s.split(" ").filter(Boolean);

  for (let i = 0; i < parts.length; i++) {
    const w = parts[i];
    const ord = ORDINAL_MAP.get(w);
    if (ord) {
      const p = parts.slice();
      p[i] = ord;
      out.add(p.join(" "));
    }
  }
  return Array.from(out);
}

function applyAliasVariants(streetUpper) {
  const s = collapse(streetUpper);
  const out = new Set([s]);

  const add = (v) => {
    const vv = collapse(v);
    if (vv) out.add(vv);
  };

  const swapToken = (from, to) => {
    if (new RegExp(`\\b${from}\\b`).test(s)) {
      add(s.replace(new RegExp(`\\b${from}\\b`, "g"), to));
    }
  };

  // Example: "GAR HWY" <-> "GRAND ARMY OF THE REPUBLIC HWY"
  if (/\bGRAND ARMY OF THE REPUBLIC\b/.test(s)) {
    add(s.replace(/\bGRAND ARMY OF THE REPUBLIC\b/g, "GAR"));
  }
  if (/\bGAR\b/.test(s)) {
    add(s.replace(/\bGAR\b/g, "GRAND ARMY OF THE REPUBLIC"));
  }

  // Common assessor-style mid-token abbreviations
  swapToken("HGTS", "HEIGHTS");
  swapToken("HTS", "HEIGHTS");
  swapToken("HEIGHTS", "HTS");
  swapToken("HEIGHTS", "HGTS");

  swapToken("ISLE", "ISLAND");
  swapToken("ISLAND", "ISLE");

  // "GT" often means "GREAT" in MA parcel/assessor labels
  swapToken("GT", "GREAT");
  swapToken("GREAT", "GT");

  // Saint vs St (only when ST is a leading token; avoids touching "... ST" = STREET suffix)
  if (/^ST\b/.test(s) && !/^STREET\b/.test(s)) add(s.replace(/^ST\b/, "SAINT"));
  if (/^SAINT\b/.test(s)) add(s.replace(/^SAINT\b/, "ST"));

  // Drop JR/SR tokens (they frequently break matching)
  if (/\bJR\b/.test(s)) add(s.replace(/\bJR\b/g, " ").replace(/\s+/g, " ").trim());
  if (/\bSR\b/.test(s)) add(s.replace(/\bSR\b/g, " ").replace(/\s+/g, " ").trim());

  return Array.from(out);
}


// Back-compat name used in some drafts. Also expands a few internal-token aliases.
// NOTE: Keep this conservative to avoid variant explosion.
function applyStreetAliases(streetUpper) {
  const base = applyAliasVariants(streetUpper);
  const out = new Set(base);

  const add = (v) => {
    const vv = collapse(v);
    if (vv) out.add(vv);
  };

  const swaps = [
    ["MT", "MOUNT"],
    ["MOUNT", "MT"],
    ["FT", "FORT"],
    ["FORT", "FT"],
    ["CTR", "CENTER"],
    ["CENTER", "CTR"],
    ["SQ", "SQUARE"],
    ["SQUARE", "SQ"],
    ["PZ", "PLAZA"],
    ["PLAZA", "PZ"],
    ["HWY", "HIGHWAY"],
    ["HIGHWAY", "HWY"],
    ["RTE", "ROUTE"],
    ["ROUTE", "RTE"],
    ["TER", "TERR"],
    ["TERR", "TER"],
    ["WY", "WAY"],
    ["WAY", "WY"],
    ["PL", "PLACE"],
    ["PLACE", "PL"],
    ["AV", "AVE"],
    ["AVE", "AV"],
  ];

  // Apply token swaps to each currently-known variant (single pass).
  for (const s of Array.from(out)) {
    for (const [from, to] of swaps) {
      if (new RegExp(`\\b${from}\\b`).test(s)) {
        add(s.replace(new RegExp(`\\b${from}\\b`, "g"), to));
      }
    }

    // Handle "MT VERNON" style (internal token) without touching suffix "ST"/"RD" etc.
    // Already covered by swaps but kept for clarity.

    // If there is a solitary single-letter token at the end that looks like a unit/section marker,
    // add a version without it (e.g., "RIDGEFIELD CIR B" -> "RIDGEFIELD CIR").
    if (/\b[A-Z]$/.test(s) && !/\b(?:N|S|E|W)$/.test(s)) {
      add(s.replace(/\s+[A-Z]$/, ""));
    }
  }

  return Array.from(out);
}


function expandSuffixVariants(streetUpper) {
  const s = collapse(streetUpper);
  const parts = s.split(" ").filter(Boolean);
  const out = new Set([s]);
  if (!parts.length) return Array.from(out);

  const last = parts[parts.length - 1];
  const variants = SUFFIX_VARIANTS.get(last);
  if (variants) {
    for (const v of variants) {
      const p = parts.slice();
      p[p.length - 1] = v;
      out.add(p.join(" "));
    }
  }
  return Array.from(out);
}

// Aliases: earlier drafts called these names.
function normalizeOrdinals(streetUpper) {
  const s = collapse(streetUpper);
  const out = new Set([s]);
  const parts = s.split(" ").filter(Boolean);
  if (!parts.length) return Array.from(out);

  // Reverse mapping: "8TH" -> "EIGHTH"
  const rev =
    normalizeOrdinals._rev ||
    (normalizeOrdinals._rev = new Map(Array.from(ORDINAL_MAP.entries()).map(([w, n]) => [n, w])));

  for (let i = 0; i < parts.length; i++) {
    const tok = parts[i];

    const toNum = ORDINAL_MAP.get(tok);
    if (toNum) {
      const p = parts.slice();
      p[i] = toNum;
      out.add(p.join(" "));
    }

    const toWord = rev.get(tok);
    if (toWord) {
      const p = parts.slice();
      p[i] = toWord;
      out.add(p.join(" "));
    }
  }

  return Array.from(out);
}

function expandStreetSuffixes(streetUpper) {
  return expandSuffixVariants(streetUpper);
}



function expandStreetVariants(streetRaw) {
  let base = stripUnitNoise(stripTailNoise(deriveStreetFromFullAddress(streetRaw) || streetRaw));
  base = collapse(base);
  if (!base) return [];

  const bases = new Set([base]);
  const noNoise = stripLeadingNoiseTokens(base);
  if (noNoise && noNoise !== base) bases.add(noNoise);

  const out = new Set();

  for (const b of bases) {
    const v1s = normalizeDirections(b);

    for (const v1 of v1s) {
      const vTail = normalizeTrailingDirections(v1);
      const vOrd = normalizeOrdinals(vTail);

      const vAliases = applyStreetAliases(vOrd);
      for (const vAlias of vAliases) {
        // If there's no known suffix (e.g. "BARBERRIES"), try appending common suffixes
        const candidates = [vAlias];
        if (!hasKnownSuffix(vAlias)) {
          for (const suf of DEFAULT_SUFFIX_APPEND) candidates.push(`${vAlias} ${suf}`);
        }

        for (const c of candidates) {
          for (const vSuf of expandStreetSuffixes(c)) out.add(vSuf);
        }
      }
    }

    // Also try trimming after a known street-type word (handles "WASHINGTON ST EXTENSION", etc)
    const trimmed = trimAfterStreetTypeWord(b);
    if (trimmed && trimmed !== b) {
      const t1s = normalizeDirections(trimmed);
      for (const t1 of t1s) {
        const tTail = normalizeTrailingDirections(t1);
        const tOrd = normalizeOrdinals(tTail);
        const tAliases = applyStreetAliases(tOrd);

        for (const tAlias of tAliases) {
          const candidates = [tAlias];
          if (!hasKnownSuffix(tAlias)) {
            for (const suf of DEFAULT_SUFFIX_APPEND) candidates.push(`${tAlias} ${suf}`);
          }

          for (const c of candidates) {
            for (const vSuf of expandStreetSuffixes(c)) out.add(vSuf);
          }
        }
      }
    }
  }

  return Array.from(out);
}
function deriveStreetNumbers(rec) {
  // Return ordered, de-duplicated candidate street numbers to try against addressIndex
  const out = [];
  const seen = new Set();
  const add = (v) => {
    if (v == null) return;
    let s = String(v).trim();
    if (!s) return;
    s = stripLeadingZeros(s);
    if (!s) return;
    if (!seen.has(s)) {
      seen.add(s);
      out.push(s);
    }
  };

  const full = canonicalizeSpaces(rec.full_address || "").toUpperCase();
  const streetNoRaw = rec.street_no != null ? String(rec.street_no).trim() : "";

  // 1) existing street_no, ranges, and letter suffixes
  if (streetNoRaw) {
    // 032 -> 32
    add(streetNoRaw);
    const mRange = streetNoRaw.match(/^(\d{1,6})\s*-\s*(\d{1,6})$/);
    if (mRange) {
      add(mRange[1]);
      add(mRange[2]);
      add(`${stripLeadingZeros(mRange[1])}-${stripLeadingZeros(mRange[2])}`);
    }
    const mLet = streetNoRaw.match(/^(\d{1,6})\s*([A-Z])$/i);
    if (mLet) {
      add(mLet[1]);
      add(`${stripLeadingZeros(mLet[1])}${mLet[2].toUpperCase()}`);
    }
  }

  // 2) pull from full_address if it starts with a number/range
  if (full) {
    const mRange = full.match(/^\s*(\d{1,6})\s*-\s*(\d{1,6})\b/);
    if (mRange) {
      add(mRange[1]);
      add(mRange[2]);
      add(`${stripLeadingZeros(mRange[1])}-${stripLeadingZeros(mRange[2])}`);
    } else {
      const mLead = full.match(/^\s*(\d{1,6})\b/);
      if (mLead) add(mLead[1]);
    }

    // 3) leading unit-letter + number patterns: "D12 AL PACE DR", "B3 ...", "U44 ..."
    // We treat the number as a candidate (this often represents the street number when the letter is a building/unit label)
    const mUnitLead = full.match(/^\s*([A-Z]{1,3})\s*0*(\d{1,6})\b/);
    if (mUnitLead) add(mUnitLead[2]);
    const mUnitLead2 = full.match(/^\s*([A-Z]{1,3})0*(\d{1,6})\b/);
    if (mUnitLead2) add(mUnitLead2[2]);

    // 4) street_no is "0" (or address begins with 0): try 10..90 and 1..9 (user-requested heuristic)
    if ((streetNoRaw === "0" || streetNoRaw === "00" || /^\s*0\b/.test(full)) && !out.length) {
      for (let i = 1; i <= 9; i++) add(String(i));
      for (let i = 1; i <= 9; i++) add(String(i) + "0");
    }
  }

  // 5) condo-style street_no/unit hints embedded in parcel_id or property_id.
  // Examples:
  //   "37-155"  -> 37 (street) + 155 (unit)  => try 37 (and 155 as fallback)
  //   "041-094" -> likely 94 Eastman St #41   => try 94 then 41
  //   "138-411" -> 1384 Sassaquin Ave #11    => try 1384 then 138
  function extractFromId(idRaw) {
    if (!idRaw) return;
    const id = String(idRaw).trim();
    if (!id) return;

    // Take after last colon for property_id like "ma:parcel:41-094"
    const tail = id.includes(":") ? id.split(":").pop() : id;

    // Normalize separators
    const t = tail.replace(/[ _]+/g, "-");

    // Most common: A-B, A-B-C
    const m2 = t.match(/^0*([0-9]{1,6})-0*([0-9]{1,6})(?:$|[^0-9])/);
    const m2a = t.match(/^0*([0-9]{1,6})-0*([0-9]{1,6}[A-Z]{0,2}[0-9]{0,6})$/i);
    const m3 = t.match(/^0*([0-9]{1,6})-0*([0-9]{1,6})-0*([0-9]{1,6})(?:$|[^0-9])/);

    if (m3) {
      // "17-4-3" => 17 is best bet
      add(m3[1]);
      return;
    }

    let a = null, b = null;
    if (m2a) {
      a = stripLeadingZeros(m2a[1]);
      b = String(m2a[2]).toUpperCase();
    } else if (m2) {
      a = stripLeadingZeros(m2[1]);
      b = stripLeadingZeros(m2[2]);
    }
    if (!a || !b) return;

    const bHasLetters = /[A-Z]/.test(b);

    // Heuristic ordering:
    // - If b has letters (e.g., 66D), treat a as street number.
    if (bHasLetters) {
      add(a);
      return;
    }

    // - If a is short (1–2 digits) and b is 3+ digits, often b is the street number and a is the unit/lot.
    if (a.length <= 2 && b.length >= 3) {
      add(b);
      add(a);
      return;
    }

    // - Append-first-digit trick: a(2–3 digits) + first digit of b(3 digits) => street no; last 2 digits => unit
    //   e.g., 138-411 => 1384 + (#11)
    if ((a.length === 2 || a.length === 3) && b.length === 3 && /^[0-9]{3}$/.test(b)) {
      const streetGuess = a + b[0];
      add(streetGuess);
      add(a);
      add(b);
      return;
    }

    // Default: try a first, then b
    add(a);
    add(b);
  }

  if (!out.length) extractFromId(rec.parcel_id);
  if (!out.length) extractFromId(rec.property_id);
  else {
    // Even if we already have candidates, still add alternates from ids (can help when full_address is weird)
    extractFromId(rec.parcel_id);
    extractFromId(rec.property_id);
  }

  // Safety cap (avoid exploding key attempts on messy strings)
  return out.slice(0, 18);
}




function buildKeys(streetNo, streetVariants, zip) {
  const no = collapse(streetNo);
  const z = cleanZip(zip);
  const keys = new Set();

  for (const st of streetVariants) {
    if (!no || !st) continue;
    keys.add(`${no}|${st}`);
    if (z) keys.add(`${no}|${st}|${z}`);
  }
  return Array.from(keys);
}

async function main() {
  console.log("====================================================");
  console.log(" PATCH MISSING COORDS FROM ADDRESS INDEX (v23)");
  console.log("====================================================");
  console.log("IN :", IN);
  console.log("IDX:", ADDRESS_INDEX);
  console.log("OUT:", OUT);
  console.log("META:", OUT_META);
  console.log("----------------------------------------------------");

  console.log("[load] addressIndex.json ...");
  const addressIndex = JSON.parse(fs.readFileSync(ADDRESS_INDEX, "utf8"));

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  const out = fs.createWriteStream(OUT, "utf8");

  const rl = readline.createInterface({ input: fs.createReadStream(IN, "utf8"), crlfDelay: Infinity });

  let total = 0, missing = 0, patched = 0, stillMissing = 0;

  const leftBuckets = {
    noStreetNo: 0,
    streetNoZero: 0,
    lotLike: 0,
    rearOffLine: 0,
    hasCommaOrParen: 0,
    hasUnitCode: 0,
  };

  const samplePatched = [];
  const sampleStillMissing = [];

  for await (const line of rl) {
    if (!line.trim()) continue;
    total++;
    const r = JSON.parse(line);

    const has = Number.isFinite(r.lat) && Number.isFinite(r.lng);

    if (!has) {
      missing++;

      const faU = collapse(r.full_address);
      const snU = collapse(r.street_name);

      // Bucket flags are counted ONLY for records that remain missing AFTER this patch attempt.
      const bucketFlags = {
        noStreetNo: !String(r.street_no ?? "").trim(),
        streetNoZero: String(r.street_no ?? "").trim() === "0",
        lotLike: /^(?:LT|LOT)\b/.test(snU) || /^(?:LT|LOT)\b/.test(faU),
        rearOffLine: /\b(REAR|OFF|LINE)\b/.test(faU) || /\b(REAR|OFF|LINE)\b/.test(snU),
        hasCommaOrParen: /[(),]/.test(String(r.full_address ?? "")) || /[(),]/.test(String(r.street_name ?? "")),
        hasUnitCode: /\b[A-Z]{1,3}[- ]\d+\b/.test(faU) || /\b[A-Z]{1,3}[- ]\d+\b/.test(snU),
      };

      const streetFromFA = deriveStreetFromFullAddress(r.full_address);
      const streetBase = streetFromFA && streetFromFA.length >= 5 ? streetFromFA : (r.street_name || r.full_address || "");

      const numsToTry = deriveStreetNumbers(r);

      let hit = null, hitKey = null;
      if (numsToTry.length) {
        const streetVars = expandStreetVariants(streetBase);

        for (const n of numsToTry) {
          const keys = buildKeys(n, streetVars, r.zip);
          for (const k of keys) {
            const v = addressIndex[k];
            if (!v) continue;
            const ll = parseCoordFromAddressIndex(v);
            if (ll) { hit = ll; hitKey = k; break; }
          }
          if (hit) break;
        }
      }

      if (hit) {
        r.lat = hit.lat;
        r.lng = hit.lng;
        r.coord_crs = hit.crs;
        r.coord_source = "addressIndex:patch:v21";
        r.coord_key_used = hitKey;
        if (Number.isFinite(hit.x_sp) && Number.isFinite(hit.y_sp)) { r.x_sp = hit.x_sp; r.y_sp = hit.y_sp; }
        r.coord_patch = { method: "addressIndex", patchedAt: new Date().toISOString(), version: "v21" };

        patched++;
        if (samplePatched.length < 10) samplePatched.push({ parcel_id: r.parcel_id, full_address: r.full_address, zip: r.zip, key: hitKey, lat: r.lat, lng: r.lng });
      } else {
        stillMissing++;

        if (bucketFlags.noStreetNo) leftBuckets.noStreetNo++;
        if (bucketFlags.streetNoZero) leftBuckets.streetNoZero++;
        if (bucketFlags.lotLike) leftBuckets.lotLike++;
        if (bucketFlags.rearOffLine) leftBuckets.rearOffLine++;
        if (bucketFlags.hasCommaOrParen) leftBuckets.hasCommaOrParen++;
        if (bucketFlags.hasUnitCode) leftBuckets.hasUnitCode++;

        if (sampleStillMissing.length < 10) sampleStillMissing.push({ parcel_id: r.parcel_id, full_address: r.full_address, street_no: r.street_no, street_name: r.street_name, zip: r.zip, town: r.town });
      }
    }

    out.write(JSON.stringify(r) + "\n");

    if (total % 500000 === 0) {
      console.log(`[progress] total=${total.toLocaleString()} missing=${missing.toLocaleString()} patched=${patched.toLocaleString()} stillMissing=${stillMissing.toLocaleString()}`);
    }
  }

  out.end();

  const meta = {
    builtAt: new Date().toISOString(),
    input: path.basename(IN),
    counts: { total, missing, patched, stillMissing },
    leftBuckets,
    samplePatched,
    sampleStillMissing
  };

  fs.mkdirSync(path.dirname(OUT_META), { recursive: true });
  fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2), "utf8");

  console.log("====================================================");
  console.log("[done]", meta.counts);
  console.log("[leftBuckets]", leftBuckets);
  console.log("OUT:", OUT);
  console.log("META:", OUT_META);
  console.log("====================================================");
}

main().catch(e => { console.error("❌ v22 patch failed:", e); process.exit(1); });