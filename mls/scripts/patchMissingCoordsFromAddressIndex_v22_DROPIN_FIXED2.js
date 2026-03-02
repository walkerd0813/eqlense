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
  getArg("in", "publicData/properties/properties_statewide_geo_zip_district_v17_coords.ndjson")
);
const ADDRESS_INDEX = resolveUserPath(
  getArg("addressIndex", "publicData/addresses/addressIndex.json")
);

const OUT = resolveUserPath(
  getArg("out", "publicData/properties/properties_statewide_geo_zip_district_v22_coords.ndjson")
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
  const toks = collapse(street).split(" ").filter(Boolean);
  const BAD = new Set(["RR", "SS", "ES", "WS", "NS", "GAR"]);
  while (toks.length && BAD.has(toks[0])) toks.shift();

  // Collapse variants of "S/S", "N/S", "E/W" after collapse() => "S S", "N S", "E W"
  if (toks.length >= 2) {
    const dir = new Set(["N", "S", "E", "W"]);
    if (dir.has(toks[0]) && dir.has(toks[1])) {
      toks.shift();
      toks.shift();
    }
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
  let s = String(full ?? "").trim();
  if (!s) return "";

  // drop locality after comma
  s = s.split(",")[0];

  // remove parentheses content but keep spacing
  s = s.replace(/\([^)]*\)/g, " ");

  // normalize separators
  s = s.replace(/[\/]/g, " ").replace(/-/g, " ");
  s = collapseSpaces(s);

  // Split weird glued direction: "31REAR" -> "31 REAR"
  s = s.replace(/^(\d+)(REAR|OFF)\b/i, "$1 $2");
  s = s.replace(/^(\d+)([A-Z])\b/i, "$1 $2");

  // Strip leading house number, and also strip a second number if present ("15 17 ST JAMES TER")
  const m = s.match(/^(\d+[A-Z]?)\s+(.*)$/i);
  if (m) {
    let rest = m[2];

    // If ranges / multiple numbers were turned into spaces, drop a second leading number
    rest = rest.replace(/^\d+[A-Z]?\s+/, "");

    // Drop leading REAR/OFF/BEHIND noise
    rest = rest.replace(/^(?:REAR|OFF|BEHIND)\b\s+/i, "");

    // Drop common embedded unit/building codes that appear right after the house number
    rest = rest
      .replace(/^(?:UN|UNIT|APT|APARTMENT|STE|SUITE)\b\s*[A-Z0-9-]+\s+/i, "")
      .replace(/^(?:UF|DP|DE)\s*[A-Z0-9-]*\s+/i, "")
      .replace(/^(?:[A-Z]{1,3}\d{1,6})\s+/i, "")
      .replace(/^(?:[A-Z]{1,3})\s+\d+[A-Z0-9]*\s+/i, "");

    return rest.trim();
  }

  // If no leading house number, but starts with OFF/REAR, strip it
  s = s.replace(/^(?:OFF|REAR|BEHIND)\b\s*/i, "");

  return s.trim();
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
function deriveStreetNumbers(r) {
  const nums = new Set();

  const addNum = (v) => {
    if (v == null) return;
    const s0 = String(v).trim();
    if (!s0) return;

    const s = s0.toUpperCase();
    nums.add(s);

    // Also store leading numeric portion (drops leading zeros)
    const m = s.match(/^(\d+)/);
    if (m) {
      const n = String(parseInt(m[1], 10));
      if (n && n !== "NaN") nums.add(n);
    }
  };

  const addRange = (a, b) => {
    if (!a || !b) return;
    const A = String(a).trim().toUpperCase();
    const B = String(b).trim().toUpperCase();
    if (!A || !B) return;
    // Use both ends + the literal token (some datasets store "12-14")
    addNum(A);
    addNum(B);
    nums.add(`${A}-${B}`);
  };

  // 1) explicit field
  addNum(r.street_no);

  // Street number "0" / leading-zero placeholders (common in bad address feeds)
  const snRaw = String(r.street_no ?? "").trim();
  if (snRaw) {
    if (/^0+$/.test(snRaw)) {
      // Try 10,20,...,90 (user confirmed this fixes many "0 <street>" rows)
      for (let d = 1; d <= 9; d++) addNum(`${d}0`);
    } else if (/^0+\d+$/.test(snRaw)) {
      // e.g. "032" => "32"
      addNum(stripLeadingZeros(snRaw));
      // Also try restoring a missing leading digit: "0xx" => "10xx"..."90xx"
      for (let d = 1; d <= 9; d++) addNum(`${d}${snRaw}`);
    }
  }


  // 2) derive from full_address / street_name (best signal)
  const raw0 = String(r.full_address || r.street_name || "");
  const ru0 = collapse(raw0);

  // Special: patterns like "955 U 109 MAIN ST" where the real house # is after a "U"
  const mU = raw0.match(/^\s*\d+\s+U\s+(\d+[A-Z]?)\s+/i);
  if (mU) addNum(mU[1]);
  const mU2 = raw0.match(/^\s*U\s+(\d+[A-Z]?)\s+/i);
  if (mU2) addNum(mU2[1]);

  // If it's clearly just a route label and does NOT start with a house #
  if (!/^\s*\d/.test(raw0) && /^(ROUTE|RTE|RT)\s+\d+\b/.test(ru0)) {
    // treat as no street number
  } else {
    // Strip leading REAR/OFF/LINE noise for number parsing only
    const rawLeadStripped = raw0.replace(/^\s*(REAR|OFF|LINE|BEHIND)\b\s*/i, "");

    // 2a) Leading range: "12-14 MAIN ST"
    const rng = rawLeadStripped.match(/^\s*(\d+[A-Z]?)\s*-\s*(\d+[A-Z]?)\b/i);
    if (rng) addRange(rng[1], rng[2]);

    // 2b) Leading "&" pairs: "234 &236"
    const amp = rawLeadStripped.match(/^\s*(\d+[A-Z]?)\s*(?:&|AND)\s*(\d+[A-Z]?)\b/i);
    if (amp) { addNum(amp[1]); addNum(amp[2]); }

    // 2c) Leading "234-&236"
    const dashAmp = rawLeadStripped.match(/^\s*(\d+[A-Z]?)\s*-\s*&\s*(\d+[A-Z]?)\b/i);
    if (dashAmp) { addNum(dashAmp[1]); addNum(dashAmp[2]); }

    // 2d) Leading slash patterns: "134/A" or "134/A MAIN ST"
    const slashLetter = rawLeadStripped.match(/^\s*(\d{1,5})\s*\/\s*([A-Z])\b/i);
    if (slashLetter) {
      addNum(`${slashLetter[1]}${slashLetter[2]}`);
      addNum(slashLetter[1]);
    }
    const slashNum = rawLeadStripped.match(/^\s*(\d{1,5})\s*\/\s*(\d{1,5})\b/i);
    if (slashNum) addRange(slashNum[1], slashNum[2]);

    // Normalize separators and parentheses for simple parsing
    let raw = rawLeadStripped.split(",")[0];
    raw = raw.replace(/\([^)]*\)/g, " ");
    raw = raw.replace(/[\/]/g, " ").replace(/&/g, " ");
    // Keep hyphen for literal "12-14" detection, but also build a hyphenless copy
    const rawNoHyphen = collapseSpaces(raw.replace(/-/g, " "));
    raw = collapseSpaces(raw);

    // Split "31REAR" -> "31 REAR", "10A10" -> "10A 10"
    raw = raw.replace(/^(\d+)(REAR|OFF)\b/i, "$1 $2");
    raw = raw.replace(/^(\d+)([A-Z])(\d+)\b/i, "$1$2 $3");

    // Split leading digit+letter: "267A" -> "267 A"
    raw = raw.replace(/^(\d+)([A-Z])\b/i, "$1 $2");

    // If it begins with two numbers (from ranges or dupes): "6 8 WHITTIER PL"
    const m2 = rawNoHyphen.match(/^\s*(\d+[A-Z]?)\s+(\d+[A-Z]?)\s+/i);
    if (m2) { addNum(m2[1]); addNum(m2[2]); }

    // Standard leading house number
    const m = rawNoHyphen.match(/^\s*(\d+[A-Z]?)\s+/i);
    if (m) addNum(m[1]);

    // 2e) Trailing house number cases: "MAIN ST 173"
    // Only consider this when there was NO leading number extracted from text.
    if (nums.size === 0) {
      const tail = ru0.match(/\b(\d{1,5}[A-Z]?)\s*$/i);
      if (tail) addNum(tail[1]);

      // Also handle "... ST 173" (number immediately after street-type token at end)
      const tailAfterType = ru0.match(/\b(?:ST|STREET|RD|ROAD|AVE|AV|AVENUE|BLVD|BOULEVARD|LN|LANE|DR|DRIVE|CT|COURT|WAY|PL|PLACE|TER|TERR|CIR|CIRCLE|PKWY|PARKWAY|HWY|HIGHWAY)\s+(\d{1,5}[A-Z]?)\s*$/i);
      if (tailAfterType) addNum(tailAfterType[1]);
    }
  }

  // 3) If still nothing and parcel_id exists, try to derive a candidate street # from parcel_id tokens.
  // This is LAST RESORT; we only keep it if it matches the addressIndex.
  if (nums.size === 0 && r.parcel_id) {
    const pid = String(r.parcel_id).toUpperCase();
    // Condo-style ids like "31-145", "88-66D", "17-4-3": treat ONLY the leading segment as house number.
    const mCondoLead = pid.match(/^(\d{1,5})\s*-\s*([0-9A-Z][0-9A-Z\-]{0,31})\s*$/);
    if (mCondoLead && mCondoLead[1]) {
      addNum(mCondoLead[1]);
      return Array.from(nums);
    }
    const cleaned = pid.replace(/[^0-9A-Z]+/g, " ").trim();
    const toks = cleaned.split(/\s+/).filter(Boolean);

    const candidates = [];

    // digit + letter split across tokens: "81 9 A" -> "81A" is plausible in some towns
    for (let i = 0; i < toks.length; i++) {
      const t = toks[i];
      if (/^\d{1,5}$/.test(t) && t !== "0") {
        candidates.push(t);
        if (i + 1 < toks.length && /^[A-Z]$/.test(toks[i + 1])) {
          candidates.push(`${t}${toks[i + 1]}`);
        }
      } else if (/^\d{1,5}[A-Z]$/.test(t)) {
        candidates.push(t);
      }
    }

    // Prefer 3–5 digit candidates first, then 1–2 (keeps list small)
    const uniq = Array.from(new Set(candidates));
    uniq.sort((a, b) => {
      const la = String(a).match(/^\d+/)?.[0]?.length ?? 0;
      const lb = String(b).match(/^\d+/)?.[0]?.length ?? 0;
      if (lb !== la) return lb - la;
      return String(a).localeCompare(String(b));
    });

    for (const c of uniq.slice(0, 8)) addNum(c);
  }

  // 4) Special case: many records use "0" as a placeholder. Try 10..90 (user pattern).
  const zeroish =
    String(r.street_no || "").trim() === "0" ||
    /^\s*0[\s-]/.test(String(r.full_address || "")) ||
    /^\s*0REAR\b/i.test(String(r.full_address || "")) ||
    /^\s*0OFF\b/i.test(String(r.full_address || ""));

  if (zeroish) {
    for (let d = 1; d <= 9; d++) nums.add(`${d}0`);
  }

  return Array.from(nums);
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
  console.log(" PATCH MISSING COORDS FROM ADDRESS INDEX (v22)");
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