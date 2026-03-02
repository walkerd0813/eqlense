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

// --- helpers ---
const SUFFIX_EXPAND = new Map([
  // Very common
  ["RD", "ROAD"],
  ["ST", "STREET"],
  ["AVE", "AVENUE"],
  ["AV", "AVENUE"],
  ["BLVD", "BOULEVARD"],
  ["BV", "BOULEVARD"],
  ["DR", "DRIVE"],
  ["LN", "LANE"],
  ["LA", "LANE"],
  ["CT", "COURT"],
  ["PL", "PLACE"],
  ["PKWY", "PARKWAY"],
  ["PWY", "PARKWAY"],
  ["PKY", "PARKWAY"],
  ["PKW", "PARKWAY"],
  ["CIR", "CIRCLE"],
  ["CI", "CIRCLE"],
  ["CR", "CIRCLE"],
  ["HWY", "HIGHWAY"],
  ["TER", "TERRACE"],
  ["TERR", "TERRACE"],
  ["TRL", "TRAIL"],
  ["SQ", "SQUARE"],
  ["CTR", "CENTER"],
  ["EXT", "EXTENSION"],
  ["WY", "WAY"],
  ["PK", "PARK"],

  // Less common but shows up in your remaining-missing buckets
  ["RDG", "RIDGE"],
  ["HTS", "HEIGHTS"],
  ["HOLW", "HOLLOW"],
  ["XING", "CROSSING"],
  ["MT", "MOUNT"],
  ["FT", "FORT"],
  ["TNPK", "TURNPIKE"],
  ["TPKE", "TURNPIKE"],
  ["PKE", "PIKE"],
]);

const BOSTON_NEIGHBORHOODS = new Set([
  "DORCHESTER","WEST ROXBURY","ROXBURY","MATTAPAN","JAMAICA PLAIN","HYDE PARK",
  "CHARLESTOWN","EAST BOSTON","SOUTH BOSTON","BRIGHTON","ALLSTON","BACK BAY",
  "SOUTH END","NORTH END","BEACON HILL","FENWAY","MISSION HILL"
]);

function collapse(s) {
  if (s == null) return "";
  return String(s)
    .toUpperCase()
    .replace(/[.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeStreetNo(v) {
  const s = collapse(v);
  if (!s) return "";
  // only normalize leading zeros for pure-digit house numbers
  if (/^\d+$/.test(s)) return String(parseInt(s, 10));
  return s;
}

function stripUnitNoise(s) {
  let t = collapse(s);
  t = t.replace(/\s+\b(?:UNIT|APT|APARTMENT|STE|SUITE)\b\s*[A-Z0-9\-]+/gi, "");
  t = t.replace(/\s+#\s*[A-Z0-9\-]+/gi, "");
  return t.trim();
}

function expandSuffixOnce(street) {
  const parts = collapse(street).split(" ");
  if (!parts.length) return street;
  const last = parts[parts.length - 1];
  const rep = SUFFIX_EXPAND.get(last);
  if (!rep) return street;
  parts[parts.length - 1] = rep;
  return parts.join(" ");
}

function normalizeTown(townRaw) {
  let t = collapse(townRaw);
  // map Boston neighborhoods -> BOSTON
  if (BOSTON_NEIGHBORHOODS.has(t)) return "BOSTON";
  // strip “TOWN OF / CITY OF”
  t = t.replace(/\b(?:TOWN OF|CITY OF)\b/g, "").replace(/\s+/g, " ").trim();
  return t;
}

function parseArgs(argv) {
  const out = { query: null, zip: null, town: null, limit: 10 };
  out.query = argv[0] ?? null;
  for (let i = 1; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--zip") out.zip = (argv[++i] ?? "").replace(/[^0-9]/g, "").slice(0, 5) || null;
    else if (a === "--town") out.town = normalizeTown(argv[++i] ?? "");
    else if (a === "--limit") out.limit = Math.max(1, Number(argv[++i] ?? 10) || 10);
  }
  return out;
}

function parseQuery(q) {
  // Accept: "80 Adam Street, Dorchester" OR "1459 VFW Parkway, West Roxbury"
  const s = String(q || "").trim();
  const m = s.match(/^\s*(\d+)\s+([^,]+)(?:\s*,\s*(.+))?\s*$/);
  if (!m) return null;
  const streetNo = normalizeStreetNo(m[1]);
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

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.query) {
    console.error(`Usage:
node .\\mls\\scripts\\lookupPropertyByAddress.js "80 Adam Street, Dorchester" [--zip 02124] [--town BOSTON] [--limit 10]`);
    process.exit(1);
  }

  const q = parseQuery(args.query);
  if (!q) throw new Error("Could not parse address. Use: \"<number> <street>, <town/neighborhood>\"");

  const wantTown = args.town ?? q.townRaw ?? null;
  const wantZip = args.zip ?? null;

  console.log("====================================================");
  console.log(" LOOKUP PROPERTY BY ADDRESS (parcel-first)");
  console.log("====================================================");
  console.log("IN_PROPERTIES:", IN_PROPERTIES);
  console.log("query:", args.query);
  console.log("parsed:", { streetNo: q.streetNo, street: q.streetNameRaw, streetExp: q.streetNameExp, town: wantTown, zip: wantZip });
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

    const stRaw = stripUnitNoise(r.street_name);
    const stExp = expandSuffixOnce(stRaw);

    const wantStreetA = q.streetNameRaw;
    const wantStreetB = q.streetNameExp;

    const streetOk =
      stRaw === wantStreetA || stRaw === wantStreetB ||
      stExp === wantStreetA || stExp === wantStreetB;

    if (!streetOk) continue;

    if (wantZip && String(r.zip ?? "").slice(0, 5) !== wantZip) continue;

    if (wantTown) {
      const rt = normalizeTown(r.town);
      if (rt !== wantTown) continue;
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
