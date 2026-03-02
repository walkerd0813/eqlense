/**
 * LINK LISTINGS -> PROPERTIES (vNext v5)
 * -------------------------------------
 * Uses lookup built by buildPropertyLookupForListings.js (v5).
 *
 * Inputs:
 *  - mls/normalized/listings.ndjson
 *  - mls/indexes/propertyLookupForListings.json
 *  - mls/indexes/propertyLookupForListings_props.json
 *  - (optional) mls/raw/reference/towns.txt
 *
 * Outputs:
 *  - mls/enriched/listings_linked.ndjson
 *  - mls/enriched/listings_unmatched.ndjson
 *  - mls/enriched/listings_link_meta.json
 */

import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ROOT = path.resolve(__dirname, "..", "..");
const LISTINGS = path.join(ROOT, "mls", "normalized", "listings.ndjson");

const LOOKUP = path.join(ROOT, "mls", "indexes", "propertyLookupForListings.json");
const LOOKUP_PROPS = path.join(ROOT, "mls", "indexes", "propertyLookupForListings_props.json");

const OUT_DIR = path.join(ROOT, "mls", "enriched");
const OUT_LINK = path.join(OUT_DIR, "listings_linked.ndjson");
const OUT_UNM = path.join(OUT_DIR, "listings_unmatched.ndjson");
const OUT_META = path.join(OUT_DIR, "listings_link_meta.json");

const TOWNS_FILE = path.join(ROOT, "mls", "raw", "reference", "towns.txt");

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function cleanStr(s) {
  if (s == null) return "";
  return String(s).trim();
}

function padZip(z) {
  const d = cleanStr(z).replace(/[^\d]/g, "");
  if (!d) return null;
  return d.padStart(5, "0").slice(0, 5);
}

function normTown(t) {
  const s = cleanStr(t).toUpperCase();
  if (!s) return null;
  return s.replace(/\s+/g, " ").trim();
}

function stripPunct(s) {
  return String(s).replace(/[^A-Za-z0-9\s]/g, " ");
}

const DIR_WORD_MAP = new Map([
  ["NORTH", "N"], ["SOUTH", "S"], ["EAST", "E"], ["WEST", "W"],
  ["NORTHEAST", "NE"], ["NORTHWEST", "NW"], ["SOUTHEAST", "SE"], ["SOUTHWEST", "SW"],
]);

const DIRS = new Set(["N", "S", "E", "W", "NE", "NW", "SE", "SW"]);

const SUFFIX_MAP = new Map([
  ["RD", "ROAD"], ["RD.", "ROAD"], ["ROAD", "ROAD"],
  ["ST", "STREET"], ["ST.", "STREET"], ["STREET", "STREET"],
  ["AVE", "AVENUE"], ["AV", "AVENUE"], ["AVE.", "AVENUE"], ["AV.", "AVENUE"], ["AVENUE", "AVENUE"],
  ["BLVD", "BOULEVARD"], ["BLVD.", "BOULEVARD"], ["BOULEVARD", "BOULEVARD"],
  ["DR", "DRIVE"], ["DR.", "DRIVE"], ["DRIVE", "DRIVE"],
  ["LN", "LANE"], ["LN.", "LANE"], ["LANE", "LANE"],
  ["CT", "COURT"], ["CT.", "COURT"], ["COURT", "COURT"],
  ["PL", "PLACE"], ["PL.", "PLACE"], ["PLACE", "PLACE"],
  ["PKWY", "PARKWAY"], ["PARKWAY", "PARKWAY"],
  ["HWY", "HIGHWAY"], ["HIGHWAY", "HIGHWAY"],
  ["TER", "TERRACE"], ["TERRACE", "TERRACE"],
  ["CIR", "CIRCLE"], ["CIRCLE", "CIRCLE"],
  ["TRL", "TRAIL"], ["TRAIL", "TRAIL"],
  ["SQ", "SQUARE"], ["SQUARE", "SQUARE"],
  ["EXT", "EXTENSION"], ["EXTENSION", "EXTENSION"],
  ["WAY", "WAY"],
]);

function extractUnitFromStreetName(streetNameRaw) {
  const s0 = cleanStr(streetNameRaw);
  if (!s0) return { street_name_clean: null, unit_no: null };

  const s = " " + s0.replace(/\s+/g, " ").trim() + " ";
  const m =
    s.match(/\s#\s*([A-Za-z0-9\-]+)\s/i) ||
    s.match(/\s(?:UNIT|APT|APARTMENT|STE|SUITE)\s+([A-Za-z0-9\-]+)\s/i);

  if (!m) return { street_name_clean: s0, unit_no: null };

  const unit_no = m[1];
  let cleaned = s0
    .replace(new RegExp(`#\\s*${unit_no}\\b`, "i"), "")
    .replace(new RegExp(`\\b(?:UNIT|APT|APARTMENT|STE|SUITE)\\s+${unit_no}\\b`, "i"), "")
    .replace(/\s+/g, " ")
    .trim();

  if (!cleaned) cleaned = s0;
  return { street_name_clean: cleaned, unit_no };
}

function normalizeStreetName(streetNameRaw) {
  const s0 = cleanStr(streetNameRaw);
  if (!s0) return null;

  const { street_name_clean } = extractUnitFromStreetName(s0);
  let s = stripPunct(street_name_clean ?? s0).toUpperCase().replace(/\s+/g, " ").trim();
  if (!s) return null;

  const parts = s.split(" ").filter(Boolean);
  if (!parts.length) return null;

  for (let i = 0; i < parts.length; i++) {
    const repl = DIR_WORD_MAP.get(parts[i]);
    if (repl) parts[i] = repl;
  }

  const last = parts[parts.length - 1];
  const hasDir = DIRS.has(last);
  const idx = hasDir ? parts.length - 2 : parts.length - 1;

  if (idx >= 0) {
    const repl = SUFFIX_MAP.get(parts[idx]);
    if (repl) parts[idx] = repl;
  }

  return parts.join(" ").trim();
}

function parseStreetNoVariants(streetNoRaw) {
  const s0 = cleanStr(streetNoRaw);
  if (!s0) return [];

  const m = s0.match(/^\s*(\d+)\s*[-–]\s*(\d+)\s*$/);
  if (m) {
    const a = m[1];
    const b = m[2];
    const out = [];
    if (a) out.push(a);
    if (b && b !== a) out.push(b);
    return out;
  }

  const m2 = s0.match(/^\s*(\d+)([A-Za-z])?\s*$/);
  if (m2) {
    const base = m2[1];
    const suf = m2[2];
    if (base && suf) return [`${base}${suf.toUpperCase()}`, base];
    if (base) return [base];
  }

  const m3 = s0.match(/^\s*(\d+)/);
  if (m3) return [m3[1]];

  return [];
}

function townFromCity(cityRaw) {
  const s0 = cleanStr(cityRaw);
  if (!s0) return null;
  const first = s0.split(",")[0].trim();
  if (!first) return null;
  return first.toUpperCase().trim() || null;
}

function readTownsMapIfPresent() {
  if (!fs.existsSync(TOWNS_FILE)) return null;
  const txt = fs.readFileSync(TOWNS_FILE, "utf8");
  const lines = txt.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  const map = new Map();

  for (const line of lines) {
    let parts = line.split("|");
    if (parts.length < 2) parts = line.split("\t");
    if (parts.length < 2) parts = line.split(",");
    if (parts.length < 2) parts = line.split(/\s{2,}/);

    if (parts.length >= 2) {
      const a = cleanStr(parts[0]).replace(/[^\d]/g, "");
      const b = cleanStr(parts[1]);
      if (a && b) map.set(a, b);
      continue;
    }

    const m = line.match(/^\s*(\d+)\s+(.+)\s*$/);
    if (m) map.set(m[1], m[2]);
  }

  return map.size ? map : null;
}

function get(obj, pathStr) {
  return pathStr.split(".").reduce((a, k) => (a && a[k] != null ? a[k] : null), obj);
}

function pick(obj, paths) {
  for (const p of paths) {
    const v = get(obj, p);
    if (v != null && cleanStr(v)) return v;
  }
  return null;
}

function buildKeyVariants({ street_no, street_name, zip, town, town_num }) {
  const zip5 = padZip(zip);
  const townName = normTown(town);
  const noVars = parseStreetNoVariants(street_no);
  const st = normalizeStreetName(street_name);

  if (!noVars.length || !st) return [];

  const keys = [];
  for (const no of noVars) {
    if (townName && zip5) keys.push({ key: `${no}|${st}|${townName}|${zip5}`, strength: 3 });
    if (zip5) keys.push({ key: `${no}|${st}|${zip5}`, strength: 2 });
    if (townName) keys.push({ key: `${no}|${st}|${townName}`, strength: 2 });
    if (town_num != null && cleanStr(town_num)) keys.push({ key: `${no}|${st}|TOWNNUM:${cleanStr(town_num)}`, strength: 1 });
    keys.push({ key: `${no}|${st}`, strength: 0 }); // always keep fallback
  }

  const best = new Map();
  for (const k of keys) {
    const prev = best.get(k.key);
    if (!prev || k.strength > prev.strength) best.set(k.key, k);
  }

  const out = [...best.values()];
  out.sort((a, b) => b.strength - a.strength);
  return out;
}

function filterCandidates(cands, propsMini, wantTown, wantZip) {
  if (!propsMini) return cands;

  let out = cands;

  if (wantZip) {
    const f = out.filter((pid) => propsMini[pid]?.zip && propsMini[pid].zip === wantZip);
    if (f.length === 1) return f;
    if (f.length > 0) out = f;
  }

  if (wantTown) {
    const f = out.filter((pid) => propsMini[pid]?.town && propsMini[pid].town === wantTown);
    if (f.length === 1) return f;
    if (f.length > 0) out = f;
  }

  return out;
}

console.log("====================================================");
console.log("      LINK LISTINGS → PROPERTIES (vNext v5)");
console.log("====================================================");
console.log("LISTINGS:", LISTINGS);
console.log("LOOKUP:  ", LOOKUP);
console.log("OUT_LINK:", OUT_LINK);
console.log("OUT_UNM: ", OUT_UNM);
console.log("OUT_META:", OUT_META);
console.log("====================================================");

if (!fs.existsSync(LISTINGS)) throw new Error(`Missing: ${LISTINGS}`);
if (!fs.existsSync(LOOKUP)) throw new Error(`Missing: ${LOOKUP}`);
if (!fs.existsSync(LOOKUP_PROPS)) console.log(`[warn] Missing mini props index: ${LOOKUP_PROPS} (ambiguity filtering will be weaker)`);

ensureDir(OUT_DIR);

const townsMap = readTownsMapIfPresent();
if (townsMap) console.log(`[load] towns map: ${townsMap.size.toLocaleString()} entries (${path.basename(TOWNS_FILE)})`);
else console.log(`[info] no towns map found at ${TOWNS_FILE} (ok)`);

console.log("[load] propertyLookupForListings.json ...");
const lookup = JSON.parse(fs.readFileSync(LOOKUP, "utf8"));
console.log(`[load] lookup keys: ${Object.keys(lookup).length.toLocaleString()}`);

let propsMini = null;
if (fs.existsSync(LOOKUP_PROPS)) {
  console.log("[load] propertyLookupForListings_props.json ...");
  propsMini = JSON.parse(fs.readFileSync(LOOKUP_PROPS, "utf8"));
  console.log(`[load] mini props: ${Object.keys(propsMini).length.toLocaleString()}`);
}

const outLinked = fs.createWriteStream(OUT_LINK, { flags: "w" });
const outUnm = fs.createWriteStream(OUT_UNM, { flags: "w" });

let readCount = 0;
let unique = 0;
let ambig = 0;
let unmatched = 0;
let missingAddr = 0;

const rl = readline.createInterface({ input: fs.createReadStream(LISTINGS) });

for await (const line of rl) {
  if (!line.trim()) continue;
  readCount++;

  let o;
  try {
    o = JSON.parse(line);
  } catch {
    continue;
  }

  // Canonical listing schema: address.streetNumber, address.streetName, address.zip, address.city
  const street_no = pick(o, ["street_no", "streetNo"]) || pick(o.address || {}, ["streetNumber", "street_no", "streetNo"]);
  const street_name = pick(o, ["street_name", "streetName"]) || pick(o.address || {}, ["streetName", "street_name"]);
  const zip = pick(o, ["zip", "zip_code"]) || pick(o.address || {}, ["zip", "zip_code"]);
  const town = pick(o, ["town", "city"]) || townFromCity(o.address?.city);

  const keyVars = buildKeyVariants({ street_no, street_name, zip, town, town_num: null });

  if (!keyVars.length) {
    missingAddr++;
    unmatched++;
    outUnm.write(JSON.stringify(o) + "\n");
    continue;
  }

  const wantZip = padZip(zip);
  const wantTown = normTown(town);

  let match = null;
  let ambigCandidates = null;

  for (const kv of keyVars) {
    const v = lookup[kv.key];
    if (v == null) continue;

    if (typeof v === "string") {
      match = { parcelId: v, key: kv.key, strength: kv.strength, mode: "unique" };
      break;
    }

    if (Array.isArray(v) && v.length) {
      const narrowed = filterCandidates(v.slice(), propsMini, wantTown, wantZip);
      if (narrowed.length === 1) {
        match = { parcelId: narrowed[0], key: kv.key, strength: kv.strength, mode: "unique_narrowed", candidates: v };
        break;
      }

      if (!ambigCandidates || kv.strength > ambigCandidates.strength) {
        ambigCandidates = { candidates: v, key: kv.key, strength: kv.strength };
      }
    }
  }

  if (match) {
    unique++;
    const out = {
      ...o,
      property_id: match.parcelId,
      link: { mode: match.mode, key: match.key, strength: match.strength },
      // make unit explicit (null instead of undefined)
      unit_no: o.unit_no ?? o.address?.unit ?? null,
    };
    if (match.candidates) out.link.candidates = match.candidates;
    outLinked.write(JSON.stringify(out) + "\n");
  } else if (ambigCandidates) {
    ambig++;
    const out = {
      ...o,
      property_id: null,
      link: { mode: "ambiguous", key: ambigCandidates.key, strength: ambigCandidates.strength, candidates: ambigCandidates.candidates },
      unit_no: o.unit_no ?? o.address?.unit ?? null,
    };
    outLinked.write(JSON.stringify(out) + "\n");
  } else {
    unmatched++;
    outUnm.write(JSON.stringify(o) + "\n");
  }

  if (readCount % 200000 === 0) {
    console.log(`[progress] read=${readCount.toLocaleString()} unique=${unique.toLocaleString()} ambig=${ambig.toLocaleString()} unmatched=${unmatched.toLocaleString()}`);
  }
}

outLinked.end();
outUnm.end();

const meta = {
  listingsRead: readCount,
  matchedUnique: unique,
  matchedAmbig: ambig,
  unmatched,
  missingAddr,
  outputs: { linked: OUT_LINK, unmatched: OUT_UNM, meta: OUT_META },
  builtAt: new Date().toISOString(),
};

fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2));

console.log("----------------------------------------------------");
console.log("✅ LISTING LINK COMPLETE");
console.log("Listings read:       ", readCount.toLocaleString());
console.log("Matched (unique):    ", unique.toLocaleString());
console.log("Matched (ambig):     ", ambig.toLocaleString());
console.log("Unmatched:           ", unmatched.toLocaleString());
console.log("Missing addr:        ", missingAddr.toLocaleString());
console.log("OUT_LINKED:          ", OUT_LINK);
console.log("OUT_UNMATCHED:       ", OUT_UNM);
console.log("OUT_META:            ", OUT_META);
console.log("====================================================");
