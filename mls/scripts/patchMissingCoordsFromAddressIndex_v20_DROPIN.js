
/**
 * PATCH MISSING COORDS FROM ADDRESS INDEX (v20) - DROPIN
 * ------------------------------------------------------
 * Adds condo / noStreetNo heuristics based on your examples:
 *  - parcel_id like "55-50" => try BOTH 55 and 50 as possible street_no
 *  - parcel_id with leading zeros like "032-040" => "32" / "40"
 *  - parcel_id like "138-411" => try "1384" (append building digit) and unit hint "11"
 *  - full_address leading token like "D12" => unit "D" + street_no "12"
 *  - leading multi-dash condo codes like "59-U44-4" or "434-U-A2" => street_no "59"/"434", unit hints
 *  - drop 2–3 letter noise prefixes (SS/ES/RR/GAR/etc) when they are standalone tokens
 *  - missing suffix: try STREET/ROAD/LANE/DRIVE/... variants
 *
 * Usage:
 *   node .\mls\scripts\patchMissingCoordsFromAddressIndex_v20_DROPIN.js `
 *     --in  C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v17_coords.ndjson `
 *     --out C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v20_coords.ndjson `
 *     --meta C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v20_coords_meta.json
 */

import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function getArg(name, fallback = null) {
  const key = `--${name}`;
  const i = process.argv.findIndex((a) => a === key);
  if (i >= 0 && i + 1 < process.argv.length) return process.argv[i + 1];
  return fallback;
}

const IN = path.resolve(__dirname, getArg("in", "../../publicData/properties/properties_statewide_geo_zip_district_v17_coords.ndjson"));
const IDX = path.resolve(__dirname, getArg("idx", "../../publicData/addresses/addressIndex.json"));
const OUT = path.resolve(__dirname, getArg("out", "../../publicData/properties/properties_statewide_geo_zip_district_v20_coords.ndjson"));
const OUT_META = path.resolve(__dirname, getArg("meta", OUT.replace(/\.ndjson$/i, "_meta.json")));
const LIMIT_ATTEMPTS = Number(getArg("limitAttemptsPerRow", "140"));

if (path.resolve(IN).toLowerCase() === path.resolve(OUT).toLowerCase()) {
  console.error(`❌ Refusing to run: --in and --out point to the same path:\n  ${IN}`);
  process.exit(1);
}

function collapseSpaces(s) {
  return String(s ?? "").replace(/\s+/g, " ").trim();
}
function normTown(t) {
  return collapseSpaces(t).toUpperCase().replace(/[^A-Z0-9]/g, "");
}
function normHouseNumStr(s) {
  const t = collapseSpaces(s);
  if (!t) return null;
  if (!/^\d+$/.test(t)) return t;
  const n = Number(t);
  if (!Number.isFinite(n)) return t;
  if (n === 0) return "0";
  return String(n); // strips leading zeros
}

function toNum(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}
function hasCoords(o) {
  const lat = toNum(o.lat ?? o.latitude);
  const lon = toNum(o.lon ?? o.lng ?? o.longitude);
  if (lat == null || lon == null) return false;
  if (lat < 40.0 || lat > 46.0) return false;
  if (lon > -66.0 || lon < -76.0) return false;
  return true;
}

function looksWgs84(lon, lat) {
  return Math.abs(lon) <= 180 && Math.abs(lat) <= 90;
}
function looksLikeMA(lon, lat) {
  return lon < -69.5 && lon > -73.6 && lat > 41.0 && lat < 43.6;
}
function looksLikeProjectedXY(x, y) {
  return x > 10000 && y > 10000 && (x > 100000 || y > 100000);
}

function parseCoordFromAddressIndex(v) {
  if (!v || typeof v !== "object") return null;

  const town = v.town ?? v.TOWN ?? v.city ?? v.CITY ?? v.municipality ?? v.MUNICIPALITY ?? null;
  const zip = v.zip ?? v.ZIP ?? v.postcode ?? v.POSTCODE ?? null;

  const rawLat = Number(v.lat ?? v.latitude ?? NaN);
  const rawLon = Number(v.lon ?? v.lng ?? v.longitude ?? NaN);

  if (Number.isFinite(rawLat) && Number.isFinite(rawLon) && looksWgs84(rawLon, rawLat) && looksLikeMA(rawLon, rawLat)) {
    return { lat: rawLat, lng: rawLon, town, zip, coord_crs: "EPSG:4326" };
  }

  const x = Number(v.x_sp ?? v.x ?? v.easting ?? NaN);
  const y = Number(v.y_sp ?? v.y ?? v.northing ?? NaN);
  if (Number.isFinite(x) && Number.isFinite(y) && looksLikeProjectedXY(x, y)) {
    return { x_sp: x, y_sp: y, town, zip, coord_crs: "EPSG:26986" };
  }

  return null;
}

const SUFFIX_MAP = new Map([
  ["ST", "STREET"], ["ST.", "STREET"], ["STREET", "STREET"],
  ["RD", "ROAD"], ["RD.", "ROAD"], ["ROAD", "ROAD"],
  ["AVE", "AVENUE"], ["AV", "AVENUE"], ["AVE.", "AVENUE"], ["AVENUE", "AVENUE"],
  ["BLVD", "BOULEVARD"], ["BLVD.", "BOULEVARD"], ["BOULEVARD", "BOULEVARD"],
  ["DR", "DRIVE"], ["DR.", "DRIVE"], ["DRIVE", "DRIVE"],
  ["LN", "LANE"], ["LN.", "LANE"], ["LANE", "LANE"],
  ["CT", "COURT"], ["CT.", "COURT"], ["COURT", "COURT"],
  ["PL", "PLACE"], ["PL.", "PLACE"], ["PLACE", "PLACE"],
  ["TER", "TERRACE"], ["TERR", "TERRACE"], ["TERR.", "TERRACE"], ["TERRACE", "TERRACE"],
  ["CIR", "CIRCLE"], ["CIR.", "CIRCLE"], ["CIRCLE", "CIRCLE"],
  ["WAY", "WAY"], ["HWY", "HIGHWAY"], ["HWY.", "HIGHWAY"], ["HIGHWAY", "HIGHWAY"],
  ["PKWY", "PARKWAY"], ["PARKWAY", "PARKWAY"],
  ["TRL", "TRAIL"], ["TRAIL", "TRAIL"],
]);

const KNOWN_SUFFIXES = new Set([
  "STREET","ROAD","AVENUE","DRIVE","LANE","COURT","PLACE","TERRACE","CIRCLE",
  "WAY","HIGHWAY","PARKWAY","TRAIL","BOULEVARD"
]);

const TRY_SUFFIXES = [
  "STREET","ROAD","LANE","DRIVE","AVENUE","WAY","COURT","PLACE","TERRACE","CIRCLE","BOULEVARD"
];

const DIR_TOKENS = new Set(["N","S","E","W","NE","NW","SE","SW","NO","SO","NORTH","SOUTH","EAST","WEST"]);
const TRAILING_NOISE = new Set(["DE","DP","EX","WD","IO","SD","PD","OD","AD"]);

const LEADING_NOISE_TOKENS = new Set([
  "RR","SS","ES","NS","EW","WE","S/S","N/S","E/W","W/E","GAR"
]);
const KEEP_SHORT_TOKENS = new Set(["MT","FT","ST","THE","OLD","NEW"]);

function stripParenCommaTails(s) {
  let t = collapseSpaces(String(s ?? ""));
  t = t.replace(/\s*,\s*.*$/g, "");
  t = t.replace(/\s*\(.*?\)\s*/g, " ");
  return collapseSpaces(t);
}

function isShortLetterNoise(tok) {
  if (!/^[A-Z]{1,3}$/.test(tok)) return false;
  if (KEEP_SHORT_TOKENS.has(tok)) return false;
  if (DIR_TOKENS.has(tok)) return false;
  if (KNOWN_SUFFIXES.has(tok)) return false;
  return true;
}

function extractFromCompound(tok) {
  // 17-D, 6-6T09, 6-9, 5-C
  const mDash = tok.match(/^(\d{1,5})-([A-Z0-9]{1,20})$/);
  if (!mDash) return null;

  const a = normHouseNumStr(mDash[1]) ?? mDash[1];
  const b = mDash[2];

  if (/^\d{1,5}$/.test(b)) {
    const bb = normHouseNumStr(b) ?? b;
    return { streetMaybe: [a, bb], unitMaybe: [bb] };
  }

  if (/^[A-Z]{1,3}$/.test(b)) {
    return { streetMaybe: [a], unitMaybe: [b] };
  }

  const mTrail = b.match(/(\d{1,4})$/);
  const trail = mTrail ? String(Number(mTrail[1])) : null;

  const mLead = b.match(/^(\d{1,5})/);
  const lead = mLead ? normHouseNumStr(mLead[1]) : null;

  const streetMaybe = [a];
  if (lead && lead !== a) streetMaybe.push(lead);

  const unitMaybe = [];
  if (trail) unitMaybe.push(trail);

  return { streetMaybe, unitMaybe };
}

function extractFromMultiDash(tok) {
  // 59-U44-4, 434-U-A2, 180-UF131
  const m = tok.match(/^(\d{1,5})-U?([A-Z]{0,2}\d{1,4}|[A-Z]{1,2}\d{1,4})(?:-[A-Z]?\d{1,4}){0,2}$/);
  if (!m) return null;
  const house = normHouseNumStr(m[1]) ?? m[1];
  const unit = m[2];
  return { streetMaybe: [house], unitMaybe: [unit] };
}

function peelLeadingNoise(raw) {
  let t = stripParenCommaTails(raw).toUpperCase();
  t = t.replace(/\s+/g, " ").trim();
  if (!t) return { cleaned: "", streetNos: [], unitHints: [] };

  const toks = t.split(" ").filter(Boolean);
  const streetNos = [];
  const unitHints = [];

  let i = 0;
  while (i < toks.length) {
    const tok = toks[i];

    if (tok.includes("/") && tok.length <= 6) { i++; continue; }
    if (LEADING_NOISE_TOKENS.has(tok)) { i++; continue; }
    if (isShortLetterNoise(tok)) { i++; continue; }

    if (/^[A-Z]{1,2}\d{1,4}$/.test(tok)) {
      unitHints.push(tok);
      const digits = tok.match(/\d{1,4}/)?.[0] ?? null;
      if (digits) streetNos.push(normHouseNumStr(digits) ?? digits);
      i++;
      continue;
    }

    if (tok.includes("-") && tok.split("-").length >= 3) {
      const ex2 = extractFromMultiDash(tok);
      if (ex2) {
        for (const n of ex2.streetMaybe) streetNos.push(n);
        for (const u of ex2.unitMaybe) unitHints.push(u);
        i++;
        continue;
      }
    }

    if (/^\d{1,5}-[A-Z0-9]{1,20}$/.test(tok)) {
      const ex = extractFromCompound(tok);
      if (ex) {
        for (const n of ex.streetMaybe) streetNos.push(n);
        for (const u of ex.unitMaybe) unitHints.push(u);
      }
      i++;
      continue;
    }

    if (/^\d{1,5}[A-Z]\d{1,4}$/.test(tok)) {
      const m = tok.match(/^(\d{1,5})[A-Z](\d{1,4})$/);
      if (m) {
        streetNos.push(normHouseNumStr(m[1]) ?? m[1]);
        unitHints.push(String(Number(m[2])));
      }
      i++;
      continue;
    }

    break;
  }

  const cleaned = toks.slice(i).join(" ");
  return { cleaned: collapseSpaces(cleaned), streetNos: [...new Set(streetNos)], unitHints: [...new Set(unitHints)] };
}

function stripUnitNoise(raw) {
  let t = stripParenCommaTails(raw).toUpperCase();

  t = t.replace(/\s*#\s*[A-Z0-9\-]+\s*$/g, "");
  t = t.replace(/\s+(APT|APARTMENT|UNIT|STE|SUITE|PH|PENTHOUSE|FL|FLOOR)\s*[A-Z0-9\-]+\s*$/g, "");
  t = t.replace(/\s+\bPS-[A-Z0-9]+\b\s*$/g, "");

  t = collapseSpaces(t);
  const toks = t.split(" ").filter(Boolean);
  while (toks.length) {
    const last = toks[toks.length - 1].replace(/[^A-Z0-9]/g, "");
    if (last.length <= 3 && TRAILING_NOISE.has(last)) {
      toks.pop();
      continue;
    }
    break;
  }
  return collapseSpaces(toks.join(" "));
}

function standardizeStreetName(raw) {
  const peeled = peelLeadingNoise(raw);
  let t = stripUnitNoise(peeled.cleaned || raw);

  t = t.replace(/\bN\b/g, "NORTH").replace(/\bS\b/g, "SOUTH").replace(/\bE\b/g, "EAST").replace(/\bW\b/g, "WEST");
  t = t.replace(/\bNO\b/g, "NORTH").replace(/\bSO\b/g, "SOUTH");

  const toks = t.split(" ").filter(Boolean);
  const out = [];
  for (const tok of toks) {
    const clean = tok.replace(/[^A-Z0-9]/g, "");
    if (!clean) continue;
    out.push(SUFFIX_MAP.get(clean) ?? clean);
  }
  return collapseSpaces(out.join(" "));
}

function endsWithKnownSuffix(streetStd) {
  const toks = streetStd.split(" ").filter(Boolean);
  if (!toks.length) return false;
  return KNOWN_SUFFIXES.has(toks[toks.length - 1]);
}
function expandSuffixVariants(streetStd) {
  if (!streetStd) return [];
  if (endsWithKnownSuffix(streetStd)) return [streetStd];
  const variants = [streetStd];
  for (const suf of TRY_SUFFIXES) variants.push(`${streetStd} ${suf}`);
  return variants;
}

function deriveStreetNosFromText(addrLike) {
  const raw = collapseSpaces(addrLike).toUpperCase();
  if (!raw) return [];

  const peeled = peelLeadingNoise(raw);
  const streetNos = [...peeled.streetNos];

  const cleaned = peeled.cleaned || raw;
  const m = cleaned.match(/^(\d{1,5})(?:\s*[-\/&]\s*(\d{1,5}))?\b/);
  if (m) {
    streetNos.push(normHouseNumStr(m[1]) ?? m[1]);
    if (m[2] && m[2] !== m[1]) streetNos.push(normHouseNumStr(m[2]) ?? m[2]);
  }

  return [...new Set(streetNos)].filter(Boolean).slice(0, 8);
}

function deriveStreetNosFromIdLike(idStr) {
  const s0 = collapseSpaces(idStr).toUpperCase();
  if (!s0) return [];

  const nums = [];
  const parts = s0.split(/[^A-Z0-9]+/g).filter(Boolean);
  for (const p of parts) {
    if (/^\d{1,5}$/.test(p)) {
      const n = normHouseNumStr(p);
      if (n && n !== "0") nums.push(n);
    } else {
      const m = p.match(/^(\d{1,5})/);
      if (m) {
        const n = normHouseNumStr(m[1]);
        if (n && n !== "0") nums.push(n);
      }
    }
  }

  const mLeadDash = s0.match(/^(\d{1,5})-([A-Z0-9]{1,20})$/);
  if (mLeadDash) {
    const n = normHouseNumStr(mLeadDash[1]);
    if (n && n !== "0") nums.unshift(n);
  }

  // Condo encoding: 138-411 => 1384 (street), 11 (unit hint)
  const mCondo = s0.match(/^0*(\d{1,3})-0*(\d{3})$/);
  if (mCondo) {
    const a = String(Number(mCondo[1]));
    const b = mCondo[2];
    const buildingDigit = b[0];
    const unit2 = String(Number(b.slice(1)));
    if (buildingDigit !== "0") nums.unshift(`${a}${buildingDigit}`);
    if (unit2 !== "0") nums.push(unit2);
  }

  return [...new Set(nums)].slice(0, 12);
}

function tryMatch(addressIndex, town, streetNoCandidates, streetNameRaw) {
  const townNorm = normTown(town);
  const streetStd = standardizeStreetName(streetNameRaw);
  const variants = expandSuffixVariants(streetStd);

  let attempts = 0;
  for (const no of streetNoCandidates) {
    for (const v of variants) {
      attempts++;
      if (attempts > LIMIT_ATTEMPTS) return null;

      const key = `${no}|${v}`;
      const hit = addressIndex[key];
      if (!hit) continue;

      const ll = parseCoordFromAddressIndex(hit);
      if (!ll) continue;

      const idxTown = normTown(ll.town ?? hit.town ?? hit.city ?? hit.CITY ?? "");
      if (idxTown && townNorm && idxTown !== townNorm) continue;

      return { key, ll, street_no_used: no, street_std_used: v };
    }
  }
  return null;
}

async function main() {
  console.log("====================================================");
  console.log(" PATCH MISSING COORDS FROM ADDRESS INDEX (v20)");
  console.log("====================================================");
  console.log("IN :", IN);
  console.log("IDX:", IDX);
  console.log("OUT:", OUT);
  console.log("META:", OUT_META);
  console.log("----------------------------------------------------");

  if (!fs.existsSync(IN)) throw new Error(`input not found: ${IN}`);
  if (!fs.existsSync(IDX)) throw new Error(`addressIndex not found: ${IDX}`);

  console.log("[load] addressIndex.json ...");
  const addressIndex = JSON.parse(fs.readFileSync(IDX, "utf8"));

  const rl = readline.createInterface({
    input: fs.createReadStream(IN, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });
  const out = fs.createWriteStream(OUT, { encoding: "utf8" });

  let total = 0, missing = 0, patched = 0;
  let hitsFromText = 0, hitsFromIds = 0, hitsFromCondoAppend = 0;

  const leftBuckets = {
    noStreetNo: 0,
    streetNoZero: 0,
    lotLike: 0,
    rearOffLine: 0,
    hasCommaOrParen: 0,
    hasUnitCode: 0,
  };

  for await (const line of rl) {
    if (!line) continue;
    total++;

    let r;
    try { r = JSON.parse(line); } catch { continue; }

    const had = hasCoords(r);
    if (!had) missing++;

    if (!had) {
      const streetNo = collapseSpaces(r.street_no);
      const full = collapseSpaces(r.full_address).toUpperCase();
      const name = collapseSpaces(r.street_name).toUpperCase();
      const unit = collapseSpaces(r.unit).toUpperCase();

      if (!streetNo) leftBuckets.noStreetNo++;
      if (streetNo === "0") leftBuckets.streetNoZero++;
      if (/\b(LOT|LT)\b/.test(full) || /\b(LOT|LT)\b/.test(name)) leftBuckets.lotLike++;
      if (/\b(REAR|OFF)\b/.test(full) || /\b(REAR|OFF)\b/.test(name)) leftBuckets.rearOffLine++;
      if (/[(),]/.test(r.full_address ?? "")) leftBuckets.hasCommaOrParen++;
      if (unit || /\b(UNIT|APT|APARTMENT|STE|SUITE|#)\b/.test(full) || /\b(UNIT|APT|APARTMENT|STE|SUITE|#)\b/.test(name)) {
        leftBuckets.hasUnitCode++;
      }
    }

    // Only patch rows missing coords AND missing street_no
    if (!had && !collapseSpaces(r.street_no)) {
      const streetNameRaw = r.street_name ?? r.full_address ?? "";
      if (streetNameRaw) {
        const fromText = deriveStreetNosFromText(r.full_address ?? r.street_name ?? "");
        const fromParcel = deriveStreetNosFromIdLike(r.parcel_id ?? "");
        const fromProp = deriveStreetNosFromIdLike(String(r.property_id ?? ""));

        const streetNoCandidates = [...new Set([...fromText, ...fromParcel, ...fromProp])].filter(Boolean).slice(0, 12);

        if (streetNoCandidates.length) {
          const match = tryMatch(addressIndex, r.town, streetNoCandidates, streetNameRaw);
          if (match) {
            const ll = match.ll;

            if (ll.lat != null && ll.lng != null) {
              r.lat = ll.lat;
              r.lng = ll.lng;
              r.lon = ll.lng;
            }
            if (ll.x_sp != null && ll.y_sp != null) {
              r.x_sp = ll.x_sp;
              r.y_sp = ll.y_sp;
            }

            r.coord_crs = r.coord_crs ?? ll.coord_crs;
            r.coord_source = "addressIndex:noStreetNo->derivedNo(v20)";
            r.coord_key_used = match.key;
            r.street_no = match.street_no_used;

            if (fromText.includes(match.street_no_used)) hitsFromText++;
            else hitsFromIds++;

            const pid = collapseSpaces(r.parcel_id);
            if (/^\d{1,3}-\d{3}$/.test(pid) && String(match.street_no_used).length === 4) hitsFromCondoAppend++;

            patched++;
          }
        }
      }
    }

    out.write(JSON.stringify(r) + "\n");
    if (total % 500000 === 0) console.log(`[progress] ${total.toLocaleString()} lines...`);
  }

  out.end();

  const meta = {
    total, missing, patched, stillMissing: Math.max(0, missing - patched),
    hits: { hitsFromText, hitsFromIds, hitsFromCondoAppend },
    leftBuckets,
    asOf: new Date().toISOString(),
    in: IN, out: OUT, idx: IDX,
    limitAttemptsPerRow: LIMIT_ATTEMPTS
  };
  fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2), "utf8");

  console.log("====================================================");
  console.log("[done]", { total, missing, patched, stillMissing: Math.max(0, missing - patched) });
  console.log("[hits]", meta.hits);
  console.log("[leftBuckets]", leftBuckets);
  console.log("OUT:", OUT);
  console.log("META:", OUT_META);
  console.log("====================================================");
}

main().catch((e) => {
  console.error("❌ v20 patch failed:", e);
  process.exit(1);
});
