/**
 * BUILD PROPERTY LOOKUP FOR LISTINGS (vNext v5)
 * --------------------------------------------
 * Builds a key->parcelId lookup for fast linking of MLS events to property_id (parcelId).
 *
 * Inputs:
 *  - mls/normalized/listings.ndjson
 *  - publicData/properties/properties_statewide_geo_zip.ndjson
 *  - (optional) mls/raw/reference/towns.txt
 *
 * Outputs:
 *  - mls/indexes/propertyLookupForListings.json
 *  - mls/indexes/propertyLookupForListings_props.json   (mini props index for ambiguity narrowing)
 *  - mls/indexes/propertyLookupForListings_meta.json
 */

import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ROOT = path.resolve(__dirname, "..", "..");

const LISTINGS = path.join(ROOT, "mls", "normalized", "listings.ndjson");
const PROPS = path.join(ROOT, "publicData", "properties", "properties_statewide_geo_zip.ndjson");

const OUT_DIR = path.join(ROOT, "mls", "indexes");
const OUT_MAP = path.join(OUT_DIR, "propertyLookupForListings.json");
const OUT_PROPS = path.join(OUT_DIR, "propertyLookupForListings_props.json");
const OUT_META = path.join(OUT_DIR, "propertyLookupForListings_meta.json");

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
  ["NORTH", "N"],
  ["SOUTH", "S"],
  ["EAST", "E"],
  ["WEST", "W"],
  ["NORTHEAST", "NE"],
  ["NORTHWEST", "NW"],
  ["SOUTHEAST", "SE"],
  ["SOUTHWEST", "SW"],
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

  // Normalize directional WORDS anywhere (WEST->W, etc.)
  for (let i = 0; i < parts.length; i++) {
    const repl = DIR_WORD_MAP.get(parts[i]);
    if (repl) parts[i] = repl;
  }

  // Normalize suffix token (prefer second-to-last if last is direction)
  const last = parts[parts.length - 1];
  const hasDir = DIRS.has(last);
  const idx = hasDir ? parts.length - 2 : parts.length - 1;

  if (idx >= 0) {
    const cand = parts[idx];
    const repl = SUFFIX_MAP.get(cand);
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

  // Usually "BARNSTABLE, MA"
  const first = s0.split(",")[0].trim();
  if (!first) return null;

  let t = first.toUpperCase().trim();
  t = t.replace(/\s+\bMA\b$/i, "").trim();
  return t || null;
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
    // Strongest
    if (townName && zip5) keys.push({ key: `${no}|${st}|${townName}|${zip5}`, strength: 3 });
    if (zip5) keys.push({ key: `${no}|${st}|${zip5}`, strength: 2 });
    if (townName) keys.push({ key: `${no}|${st}|${townName}`, strength: 2 });

    // town_num variant (rare for listings, but safe)
    if (town_num != null && cleanStr(town_num)) keys.push({ key: `${no}|${st}|TOWNNUM:${cleanStr(town_num)}`, strength: 1 });

    // Always include fallback (CRITICAL: helps when property zip is missing)
    keys.push({ key: `${no}|${st}`, strength: 0 });
  }

  // De-dupe while keeping best strength
  const best = new Map();
  for (const k of keys) {
    const prev = best.get(k.key);
    if (!prev || k.strength > prev.strength) best.set(k.key, k);
  }

  const out = [...best.values()];
  out.sort((a, b) => b.strength - a.strength);
  return out;
}

function pickParcelId(prop) {
  return (
    pick(prop, ["parcel_id", "parcelId", "PARCEL_ID", "pid", "id"]) ||
    null
  );
}

function getPropParts(prop) {
  const street_no = pick(prop, ["street_no", "streetNo", "streetNumber", "addr_no", "address_no"]);
  const street_name = pick(prop, ["street_name", "streetName", "street", "addr_street", "address_street", "full_street"]);
  const zip = pick(prop, ["zip", "zip_code", "ZIP", "ZIP_CODE"]);
  const town = pick(prop, ["town", "city", "municipality", "TOWN"]);
  const town_num = pick(prop, ["town_num", "townNum", "TOWN_NUM"]);
  return { street_no, street_name, zip, town, town_num };
}

function getListingParts(o, townsMap) {
  const addr = o.address || {};

  const street_no =
    pick(o, ["street_no", "streetNo"]) ||
    pick(addr, ["streetNumber", "street_no", "streetNo"]);

  const street_name =
    pick(o, ["street_name", "streetName"]) ||
    pick(addr, ["streetName", "street_name"]);

  const zip =
    pick(o, ["zip", "zip_code", "ZIP_CODE"]) ||
    pick(addr, ["zip", "zip_code"]);

  const town =
    pick(o, ["town", "city", "municipality"]) ||
    townFromCity(addr.city);

  // town_num is usually not in canonical listing, but maybe in raw.row
  let town_num = pick(o, ["town_num", "townNum", "TOWN_NUM"]);
  if (!town_num && o.raw?.row) {
    // try common patterns
    const rr = o.raw.row;
    town_num = rr.TOWN_NUM ?? rr.TOWNNUM ?? rr.TOWNNUMBER ?? null;
  }

  // If still no town but we have town_num and townsMap, use it
  let finalTown = town;
  if (!finalTown && townsMap && town_num) {
    const k = String(town_num).replace(/[^\d]/g, "");
    finalTown = townsMap.get(k) || null;
  }

  return { street_no, street_name, zip, town: finalTown, town_num };
}

function mapWriteJson(wsPath, mapObj) {
  return new Promise((resolve, reject) => {
    const ws = fs.createWriteStream(wsPath, { flags: "w" });
    ws.on("error", reject);
    ws.write("{\n");
    let first = true;

    for (const [k, v] of mapObj.entries()) {
      if (!first) ws.write(",\n");
      first = false;
      ws.write(`${JSON.stringify(k)}:${JSON.stringify(v)}`);
    }

    ws.write("\n}\n");
    ws.end(resolve);
  });
}

console.log("====================================================");
console.log("  BUILD PROPERTY LOOKUP FOR LISTINGS (vNext v5)");
console.log("====================================================");
console.log("LISTINGS:", LISTINGS);
console.log("PROPS:   ", PROPS);
console.log("OUT_MAP: ", OUT_MAP);
console.log("OUT_META:", OUT_META);
console.log("====================================================");

if (!fs.existsSync(LISTINGS)) throw new Error(`Missing: ${LISTINGS}`);
if (!fs.existsSync(PROPS)) throw new Error(`Missing: ${PROPS}`);

ensureDir(OUT_DIR);

const townsMap = readTownsMapIfPresent();
if (townsMap) console.log(`[load] towns map: ${townsMap.size.toLocaleString()} entries (${path.basename(TOWNS_FILE)})`);
else console.log(`[info] no towns map found at ${TOWNS_FILE} (ok)`);

// 1) Collect listing keys
const listingKeys = new Set();
let listingsRead = 0;
let listingsMissingAddr = 0;

{
  const rl = readline.createInterface({ input: fs.createReadStream(LISTINGS) });
  for await (const line of rl) {
    if (!line.trim()) continue;
    listingsRead++;

    let o;
    try {
      o = JSON.parse(line);
    } catch {
      continue;
    }

    const parts = getListingParts(o, townsMap);
    const keys = buildKeyVariants(parts);

    if (!keys.length) {
      listingsMissingAddr++;
      continue;
    }

    for (const kv of keys) listingKeys.add(kv.key);

    if (listingsRead % 200000 === 0) {
      console.log(`[progress:listings] read=${listingsRead.toLocaleString()} uniqueKeys=${listingKeys.size.toLocaleString()}`);
    }
  }
}

console.log("----------------------------------------------------");
console.log("✅ Listing key collection complete");
console.log("Listings read:         ", listingsRead.toLocaleString());
console.log("Unique address keys:   ", listingKeys.size.toLocaleString());
console.log("Listings missing addr: ", listingsMissingAddr.toLocaleString());
console.log("====================================================");

// 2) Walk properties and build lookup
const lookup = new Map();      // key -> parcelId OR [parcelId...]
const propsMini = new Map();   // parcelId -> { zip, town }
let propsRead = 0;
let matchedProps = 0;
let keysWritten = 0;
let collisions = 0;

{
  const rl = readline.createInterface({ input: fs.createReadStream(PROPS) });
  for await (const line of rl) {
    if (!line.trim()) continue;
    propsRead++;

    let p;
    try {
      p = JSON.parse(line);
    } catch {
      continue;
    }

    const parcelId = pickParcelId(p);
    if (!parcelId) continue;

    const parts = getPropParts(p);
    const keys = buildKeyVariants(parts);
    if (!keys.length) continue;

    let wroteAny = false;

    for (const kv of keys) {
      if (!listingKeys.has(kv.key)) continue;

      const existing = lookup.get(kv.key);
      if (existing == null) {
        lookup.set(kv.key, parcelId);
        keysWritten++;
        wroteAny = true;
      } else if (typeof existing === "string") {
        if (existing !== parcelId) {
          lookup.set(kv.key, [existing, parcelId]);
          collisions++;
          wroteAny = true;
        }
      } else if (Array.isArray(existing)) {
        if (!existing.includes(parcelId)) {
          existing.push(parcelId);
          collisions++;
          wroteAny = true;
        }
      }
    }

    if (wroteAny) {
      matchedProps++;
      // store mini props for narrowing (zip/town)
      propsMini.set(parcelId, {
        zip: padZip(parts.zip),
        town: normTown(parts.town),
      });
    }

    if (propsRead % 200000 === 0) {
      console.log(`[progress:props] read=${propsRead.toLocaleString()} matchedProps=${matchedProps.toLocaleString()} keysWritten=${keysWritten.toLocaleString()}`);
    }
  }
}

// 3) Write outputs (streamed JSON)
await mapWriteJson(OUT_MAP, lookup);
await mapWriteJson(OUT_PROPS, propsMini);

const meta = {
  listingsRead,
  listingKeysUnique: listingKeys.size,
  listingsMissingAddr,
  propertiesRead: propsRead,
  matchedProperties: matchedProps,
  keysWritten,
  collisions,
  outputs: { lookup: OUT_MAP, propsMini: OUT_PROPS, meta: OUT_META },
  builtAt: new Date().toISOString(),
};

fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2));

console.log("----------------------------------------------------");
console.log("✅ Property lookup built");
console.log("Listing keys (unique): ", listingKeys.size.toLocaleString());
console.log("Keys written:          ", keysWritten.toLocaleString());
console.log("Collisions:            ", collisions.toLocaleString());
console.log("Properties read:       ", propsRead.toLocaleString());
console.log("Matched properties:    ", matchedProps.toLocaleString());
console.log("OUT_MAP:               ", OUT_MAP);
console.log("OUT_PROPS:             ", OUT_PROPS);
console.log("OUT_META:              ", OUT_META);
console.log("====================================================");
