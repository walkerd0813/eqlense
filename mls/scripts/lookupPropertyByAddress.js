import fs from "fs";
import readline from "readline";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const IN_PROPERTIES = path.resolve(
  __dirname,
  "../../publicData/properties/properties_statewide_geo_zip_district_v2.ndjson"
);

// ---------------- helpers ----------------

// ---------- address normalization ----------
const SUFFIX_EXPAND = new Map([
  // Core
  ["RD", "ROAD"],
  ["ST", "STREET"],
  ["AVE", "AVENUE"],
  ["AV", "AVENUE"],
  ["BLVD", "BOULEVARD"],
  ["DR", "DRIVE"],
  ["LN", "LANE"],
  ["CT", "COURT"],
  ["PL", "PLACE"],
  ["PKWY", "PARKWAY"],
  ["CIR", "CIRCLE"],
  ["HWY", "HIGHWAY"],
  ["TER", "TERRACE"],
  ["TRL", "TRAIL"],
  ["SQ", "SQUARE"],
  ["CTR", "CENTER"],
  ["EXT", "EXTENSION"],

  // Your “missing coords” hitters (from samples)
  ["BV", "BOULEVARD"],      // PULASKI BV
  ["BL", "BOULEVARD"],      // PAGE BL
  ["PW", "PARKWAY"],        // VFW PW
  ["PK", "PARK"],           // SHEPHERD PK / MIACOMET PK
  ["TE", "TERRACE"],        // LAMARTINE TE
  ["TERR", "TERRACE"],      // GORDON TERR
  ["LA", "LANE"],           // ANDERER LA
  ["CI", "CIRCLE"],         // CARLSON CI
  ["CR", "CIRCLE"],         // ORCHARD CR / ELM CR  (alt = CRESCENT, but CIRCLE works as first pass)
  ["TNPK", "TURNPIKE"],     // NEWBURYPORT TNPK
  ["TPKE", "TURNPIKE"],     // TURNPIKE variants
  ["TNPK.", "TURNPIKE"],    // occasional punctuation
  ["TPK", "TURNPIKE"],      // common shorthand
  ["PKE", "PIKE"],          // PIKE

  // Common USPS-ish extras (safe expansions)
  ["DRV", "DRIVE"],
  ["DRIV", "DRIVE"],
  ["CRT", "COURT"],
  ["CTS", "COURTS"],
  ["PLC", "PLACE"],
  ["PLZ", "PLAZA"],
  ["PKY", "PARKWAY"],
  ["PKW", "PARKWAY"],
  ["RDG", "RIDGE"],
  ["RTE", "ROUTE"],         // RT 6A / RTE 6A patterns (still needs separate rule when "RT" is embedded)
  ["RT", "ROUTE"],          // same
  ["EXTN", "EXTENSION"],
  ["EST", "ESTATES"],
  ["ESTS", "ESTATES"],

  // MA-common / high frequency
  ["ALY", "ALLEY"],
  ["ANX", "ANNEX"],
  ["ARC", "ARCADE"],
  ["BCH", "BEACH"],
  ["BND", "BEND"],
  ["BR", "BRIDGE"],
  ["BRG", "BRIDGE"],
  ["BRK", "BROOK"],
  ["BY", "BAY"],
  ["CAPE", "CAPE"],
  ["CMN", "COMMON"],
  ["COR", "CORNER"],
  ["CORS", "CORNERS"],
  ["COVE", "COVE"],
  ["CV", "COVE"],
  ["XING", "CROSSING"],
  ["XRD", "CROSSROAD"],
  ["XWAY", "EXPRESSWAY"],
  ["FLD", "FIELD"],
  ["FRST", "FOREST"],
  ["FLS", "FALLS"],
  ["FRK", "FORK"],
  ["FRKS", "FORKS"],
  ["GDNS", "GARDENS"],
  ["GRDN", "GARDEN"],
  ["GLN", "GLEN"],
  ["GRN", "GREEN"],
  ["GRNS", "GREENS"],
  ["GRV", "GROVE"],
  ["HBR", "HARBOR"],
  ["HL", "HILL"],
  ["HLS", "HILLS"],
  ["HTS", "HEIGHTS"],
  ["JCT", "JUNCTION"],
  ["LNDG", "LANDING"],      // MARTINS LNDG
  ["LK", "LAKE"],
  ["LKS", "LAKES"],
  ["MDW", "MEADOW"],
  ["MDWS", "MEADOWS"],
  ["ML", "MILL"],
  ["MLS", "MILLS"],
  ["MNR", "MANOR"],
  ["MT", "MOUNT"],          // MT VERNON ST / MT HOPE ST
  ["NCK", "NECK"],
  ["OVAL", "OVAL"],
  ["PATH", "PATH"],
  ["PT", "POINT"],
  ["PTS", "POINTS"],
  ["RIV", "RIVER"],
  ["RN", "RUN"],
  ["ROW", "ROW"],
  ["SHR", "SHORE"],
  ["SPUR", "SPUR"],
  ["STA", "STATION"],
  ["STRA", "STRAND"],
  ["TER.", "TERRACE"],
  ["TRCE", "TERRACE"],
  ["TERR.", "TERRACE"],
  ["TPK.", "TURNPIKE"],
  ["VLG", "VILLAGE"],
  ["VLY", "VALLEY"],
  ["VW", "VIEW"],
  ["VWS", "VIEWS"],
  ["WALK", "WALK"],
  ["WALL", "WALL"],
  ["WAY", "WAY"],
  ["WAYS", "WAYS"],
  ["WY", "WAY"],            // APACHE WY
  ["XING.", "CROSSING"],
]);




const BOSTON_NEIGHBORHOODS = new Set([
  "DORCHESTER",
  "WEST ROXBURY",
  "ROXBURY",
  "MATTAPAN",
  "JAMAICA PLAIN",
  "HYDE PARK",
  "CHARLESTOWN",
  "EAST BOSTON",
  "SOUTH BOSTON",
  "BRIGHTON",
  "ALLSTON",
  "BACK BAY",
  "SOUTH END",
  "NORTH END",
  "BEACON HILL",
  "FENWAY",
  "MISSION HILL",
]);

function collapse(s) {
  if (s == null) return "";
  return String(s)
    .toUpperCase()
    .replace(/[.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanZip(z) {
  if (z == null) return null;
  const digits = String(z).replace(/[^0-9]/g, "");
  if (digits.length >= 5) return digits.slice(0, 5);
  if (digits.length === 4) return "0" + digits;
  return null;
}

function stripUnitNoise(s) {
  let t = collapse(s);
  t = t.replace(/\s+\b(?:UNIT|APT|APARTMENT|STE|SUITE)\b\s*[A-Z0-9\-]+/gi, "");
  t = t.replace(/\s+#\s*[A-Z0-9\-]+/gi, "");
  return t.trim();
}

function stripLeadingNumber(s) {
  // e.g. "1461 VFW PW" -> "VFW PW"
  return collapse(s).replace(/^\d+\s+/, "").trim();
}

function expandSuffixOnce(street) {
  const parts = collapse(street).split(" ");
  if (!parts.length) return street;

  const lastRaw = parts[parts.length - 1];
  const last = lastRaw.replace(/[.,;:]+$/g, ""); // NEW
  const rep = SUFFIX_EXPAND.get(last);
  if (!rep) return street;

  parts[parts.length - 1] = rep;
  return parts.join(" ");
}


function normalizeTown(townRaw) {
  let t = collapse(townRaw);
  t = t.replace(/\b(?:TOWN OF|CITY OF)\b/g, "").replace(/\s+/g, " ").trim();
  return t;
}

function isBostonish(town) {
  const t = normalizeTown(town);
  return t === "BOSTON" || BOSTON_NEIGHBORHOODS.has(t);
}

function streetVariants(streetLike) {
  // produce a small set of comparable street-name variants.
  // This makes lookups resilient to:
  // - PW/PKWY/PARKWAY
  // - ST/STREET
  // - address ranges leaking into street_name ("1461 VFW PW")
  // - ADAM <-> ADAMS (limited, conservative)

  const base0 = stripLeadingNumber(stripUnitNoise(streetLike));
  const base = collapse(base0);
  const out = new Set();
  if (!base) return out;

  out.add(base);

  const exp = expandSuffixOnce(base);
  out.add(exp);

  // Conservative plural toggle on a single-word core (ADAM <-> ADAMS)
  // Only when core is alphabetic and short, to avoid creating nonsense variants.
  for (const v of Array.from(out)) {
    const parts = v.split(" ").filter(Boolean);
    if (!parts.length) continue;

    const last = parts[parts.length - 1];
    const hasSuffix = SUFFIX_WORDS.has(last);

    const core = hasSuffix ? parts.slice(0, -1) : parts;
    if (core.length !== 1) continue;

    const token = core[0];
    if (!/^[A-Z]{3,6}$/.test(token)) continue;

    const toggled = token.endsWith("S") ? token.slice(0, -1) : token + "S";
    const rebuilt = hasSuffix ? `${toggled} ${last}` : toggled;
    out.add(rebuilt);
  }

  return out;
}

function parseArgs(argv) {
  const out = { query: null, zip: null, town: null, limit: 10 };
  out.query = argv[0] ?? null;
  for (let i = 1; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--zip") out.zip = cleanZip(argv[++i] ?? "") || null;
    else if (a === "--town") out.town = normalizeTown(argv[++i] ?? "");
    else if (a === "--limit") out.limit = Math.max(1, Number(argv[++i] ?? 10) || 10);
  }
  return out;
}

function parseQuery(q) {
  // Accept:
  //  - "80 Adam Street, Dorchester"
  //  - "1459 VFW Parkway, West Roxbury"
  //  - "1459 VFW Parkway"
  const s = String(q || "").trim();
  const m = s.match(/^\s*(\d+)\s+([^,]+)(?:\s*,\s*(.+))?\s*$/);
  if (!m) return null;

  const streetNo = collapse(m[1]);
  const streetNameRaw = stripUnitNoise(m[2]);
  const streetNameExp = expandSuffixOnce(streetNameRaw);
  const townRaw = m[3] ? normalizeTown(m[3]) : null;

  return { streetNo, streetNameRaw, streetNameExp, townRaw };
}

function pickZoningSummary(r) {
  const d = r?.zoning?.district;
  if (!d) return null;
  return {
    city: d.city ?? null,
    name: d.name ?? null,
    codeNorm: d.codeNorm ?? null,
    stage: d.stage ?? null,
    refs: d.refs ?? null,
  };
}

function streetMatch(queryStreet, rowStreet) {
  const qVars = streetVariants(queryStreet);
  const rVars = streetVariants(rowStreet);
  if (!qVars.size || !rVars.size) return false;
  for (const v of qVars) {
    if (rVars.has(v)) return true;
  }
  return false;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.query) {
    console.error(`Usage:
node .\\mls\\scripts\\lookupPropertyByAddress.js "80 Adam Street, Dorchester" [--zip 02122] [--town DORCHESTER|BOSTON] [--limit 10]
node .\\mls\\scripts\\lookupPropertyByAddress.js "1459 VFW Parkway" --zip 02132`);
    process.exit(1);
  }

  const q = parseQuery(args.query);
  if (!q) throw new Error('Could not parse address. Use: "<number> <street>, <town/neighborhood>"');

  const wantTown = args.town ?? q.townRaw ?? null;
  const wantZip = args.zip ?? null;

  console.log("====================================================");
  console.log(" LOOKUP PROPERTY BY ADDRESS (parcel-first) — v2");
  console.log("====================================================");
  console.log("IN_PROPERTIES:", IN_PROPERTIES);
  console.log("query:", args.query);
  console.log("parsed:", {
    streetNo: q.streetNo,
    street: q.streetNameRaw,
    streetExp: q.streetNameExp,
    town: wantTown,
    zip: wantZip,
  });
  console.log("----------------------------------------------------");

  const rl = readline.createInterface({
    input: fs.createReadStream(IN_PROPERTIES, "utf8"),
    crlfDelay: Infinity,
  });

  const matches = [];
  let scanned = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;
    scanned++;

    const r = JSON.parse(line);

    const no = collapse(r.street_no);
    if (no !== q.streetNo) continue;

    if (wantZip && cleanZip(r.zip) !== wantZip) continue;

    // street match with variants (handles "Adams ST" vs "Adam Street", "PW" vs "PARKWAY",
    // and cases where street_name begins with a second number from an address range)
    if (!streetMatch(q.streetNameRaw, r.street_name)) continue;

    if (wantTown) {
      const rt = normalizeTown(r.town);
      const wt = normalizeTown(wantTown);

      // If user provided Boston or a Boston neighborhood, accept either BOSTON or neighborhood labels.
      if (isBostonish(wt)) {
        if (!isBostonish(rt)) continue;
      } else {
        if (rt !== wt) continue;
      }
    }

    matches.push({
      property_id: r.property_id,
      parcel_id: r.parcel_id,
      full_address: r.full_address,
      town: r.town,
      zip: r.zip,
      lat: r.lat,
      lng: r.lng,
      zoningDistrict: pickZoningSummary(r),
    });

    if (matches.length >= args.limit) break;
  }

  console.log("[done] scanned:", scanned.toLocaleString(), "matches:", matches.length);
  console.log(matches);
}

main().catch((e) => {
  console.error("❌ lookupPropertyByAddress failed:", e);
  process.exit(1);
});
