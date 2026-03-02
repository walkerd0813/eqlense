// backend/mls/scripts/FuzzyPass2.js
// ============================================================
// FUZZY PASS 2 — internal fuzzy street match on unmatched_PASS1
// Strategy:
//  - bucket candidates by (houseNumber, zip)
//  - compare street strings to candidate streets in that bucket
// Output:
//  - mls/normalized/pass2_newMatches.ndjson
//  - mls/normalized/unmatched_PASS2.ndjson
//  - mls/normalized/listingsWithCoords_PASS2.ndjson (FAST matched + PASS1 new + PASS2 new)
// ============================================================

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const NORM_DIR = path.resolve(__dirname, "../normalized");

const FAST_MATCHED = path.join(NORM_DIR, "listingsWithCoords_FAST.ndjson");

const PASS1_UNMATCHED = path.join(NORM_DIR, "unmatched_PASS1.ndjson");
const PASS1_NEW = path.join(NORM_DIR, "pass1_newMatches.ndjson");

const PASS2_MATCHED = path.join(NORM_DIR, "listingsWithCoords_PASS2.ndjson");
const PASS2_UNMATCHED = path.join(NORM_DIR, "unmatched_PASS2.ndjson");
const PASS2_NEW = path.join(NORM_DIR, "pass2_newMatches.ndjson");

const PARCEL_INDEX_PATH = path.resolve(
  __dirname,
  "../../publicData/parcels/parcelCentroidIndex.json"
);
const ADDRESS_INDEX_PATH = path.resolve(
  __dirname,
  "../../publicData/addresses/addressIndex.json"
);

function die(msg) {
  console.error(`❌ ${msg}`);
  process.exit(1);
}
function fileExists(p) {
  return fs.existsSync(p);
}

function normalizeZip5(z) {
  if (!z) return "";
  const digits = String(z).replace(/\D/g, "");
  if (!digits) return "";
  if (digits.length >= 5) return digits.slice(0, 5);
  return digits.padStart(5, "0");
}

function stripUnit(s) {
  return String(s || "")
    .replace(/\bU:\s*[A-Z0-9\-]+\b/gi, "")
    .replace(/\b(APT|UNIT|STE|SUITE)\s*[A-Z0-9\-]+\b/gi, "")
    .replace(/\s+#\s*[A-Z0-9\-]+\b/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

function extractFromRawAddress(rawAddress) {
  const a = String(rawAddress || "").trim();
  const m = a.match(/^(\d+)\s+(.*)$/);
  if (!m) return { number: "", street: "" };
  const number = m[1];
  let street = m[2] || "";
  street = street.replace(/\s+U:.*$/i, "").trim();
  street = stripUnit(street);
  return { number, street };
}

function getListingNumberStreetZip(listing) {
  const addr = listing?.address || {};
  const raw = listing?.raw?.row || {};

  let number =
    addr.streetNumber ??
    raw.STREET_NUM ??
    raw.STREET_NO ??
    raw.ADDR_NUM ??
    "";

  let street =
    addr.streetName ??
    raw.STREET_NAME ??
    raw.FULL_STR ??
    raw.SITE_ADDR ??
    "";

  if (!number || !street) {
    const rawAddr = listing?.raw?.combinedAddress ?? raw.ADDRESS ?? "";
    const parsed = extractFromRawAddress(rawAddr);
    if (!number) number = parsed.number;
    if (!street) street = parsed.street;
  }

  const zip =
    addr.zip ??
    raw.ZIP_CODE ??
    raw.ZIP ??
    "";

  number = String(number || "").trim();
  street = String(street || "").trim();
  return { number, street, zip: normalizeZip5(zip) };
}

function extractLatLon(v) {
  if (!v) return null;

  if (typeof v === "object" && !Array.isArray(v)) {
    const lat = v.lat ?? v.latitude ?? v.y ?? v.Y;
    const lon = v.lon ?? v.lng ?? v.longitude ?? v.x ?? v.X;
    if (Number.isFinite(+lat) && Number.isFinite(+lon)) {
      return { latitude: +lat, longitude: +lon };
    }
  }

  if (Array.isArray(v) && v.length >= 2) {
    const a = +v[0];
    const b = +v[1];
    if (Number.isFinite(a) && Number.isFinite(b)) {
      const aIsLon = Math.abs(a) <= 180 && Math.abs(a) > 90;
      const bIsLat = Math.abs(b) <= 90;
      if (aIsLon && bIsLat) return { latitude: b, longitude: a };
      const bIsLon = Math.abs(b) <= 180 && Math.abs(b) > 90;
      const aIsLat = Math.abs(a) <= 90;
      if (bIsLon && aIsLat) return { latitude: a, longitude: b };
      return { latitude: b, longitude: a };
    }
  }

  return null;
}

function canonStreet(s) {
  // aggressive canonical form for similarity
  return stripUnit(String(s || ""))
    .toUpperCase()
    .replace(/\./g, "")
    .replace(/[^A-Z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// fast Levenshtein for short strings
function levenshtein(a, b) {
  if (a === b) return 0;
  const al = a.length;
  const bl = b.length;
  if (al === 0) return bl;
  if (bl === 0) return al;

  const v0 = new Array(bl + 1);
  const v1 = new Array(bl + 1);

  for (let i = 0; i <= bl; i++) v0[i] = i;

  for (let i = 0; i < al; i++) {
    v1[0] = i + 1;
    const ca = a.charCodeAt(i);

    for (let j = 0; j < bl; j++) {
      const cost = ca === b.charCodeAt(j) ? 0 : 1;
      const m = Math.min(
        v1[j] + 1,
        v0[j + 1] + 1,
        v0[j] + cost
      );
      v1[j + 1] = m;
    }
    for (let j = 0; j <= bl; j++) v0[j] = v1[j];
  }
  return v1[bl];
}

function similarity(a, b) {
  const A = canonStreet(a);
  const B = canonStreet(b);
  if (!A || !B) return 0;
  const dist = levenshtein(A, B);
  const denom = Math.max(A.length, B.length) || 1;
  return 1 - dist / denom;
}

function confidenceFromSim(sim) {
  // map similarity to confidence
  const c = Math.max(0.60, Math.min(0.99, 0.70 + sim * 0.29));
  return Math.round(c * 100) / 100;
}

async function appendFileToStream(src, out) {
  if (!fileExists(src)) return 0;
  const rl = readline.createInterface({
    input: fs.createReadStream(src),
    crlfDelay: Infinity,
  });
  let count = 0;
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    out.write(t + "\n");
    count++;
  }
  return count;
}

async function collectNeededNumZipCombos(unmatchedPath) {
  const needed = new Set();
  const rl = readline.createInterface({
    input: fs.createReadStream(unmatchedPath),
    crlfDelay: Infinity,
  });
  let rows = 0;
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    rows++;
    let listing;
    try { listing = JSON.parse(t); } catch { continue; }
    const { number, zip } = getListingNumberStreetZip(listing);
    if (number && zip) needed.add(`${number}|${zip}`);
  }
  return { needed, rows };
}

function parseParcelKey(k) {
  // "800 BEARSES WAY 02601" => num=800, street="BEARSES WAY", zip=02601
  const parts = String(k).split(" ").filter(Boolean);
  if (parts.length < 3) return null;
  const zip = parts[parts.length - 1];
  if (!/^\d{5}$/.test(zip)) return null;
  const number = parts[0];
  const street = parts.slice(1, -1).join(" ");
  return { number, street, zip };
}

function parseAddressKey(k) {
  // "800|BEARSES WAY|02601"
  const parts = String(k).split("|");
  if (parts.length !== 3) return null;
  const [number, street, zip] = parts;
  if (!number || !street || !zip) return null;
  if (!/^\d{5}$/.test(zip)) return null;
  return { number, street, zip };
}

async function main() {
  console.log("====================================================");
  console.log(" FUZZY PASS 2 — FUZZY STREET (INTERNAL)");
  console.log("====================================================");

  if (!fileExists(PASS1_UNMATCHED)) die(`Missing input: ${PASS1_UNMATCHED}`);
  if (!fileExists(FAST_MATCHED)) die(`Missing input: ${FAST_MATCHED}`);
  if (!fileExists(PARCEL_INDEX_PATH)) die(`Missing: ${PARCEL_INDEX_PATH}`);
  if (!fileExists(ADDRESS_INDEX_PATH)) die(`Missing: ${ADDRESS_INDEX_PATH}`);
  if (!fileExists(PASS1_NEW)) {
    console.log(`⚠️ Missing PASS1 new matches (still OK): ${PASS1_NEW}`);
  }

  console.log("[pass2] Reading PASS1 unmatched to build needed (num|zip) set...");
  const { needed, rows } = await collectNeededNumZipCombos(PASS1_UNMATCHED);
  console.log(`[pass2] PASS1 unmatched rows: ${rows.toLocaleString()}`);
  console.log(`[pass2] unique num|zip combos: ${needed.size.toLocaleString()}`);

  console.log("[pass2] Loading parcel centroid index...");
  const parcelIndex = JSON.parse(await fsp.readFile(PARCEL_INDEX_PATH, "utf8"));
  console.log("[pass2] Loading address-point index...");
  const addressIndex = JSON.parse(await fsp.readFile(ADDRESS_INDEX_PATH, "utf8"));

  // Build candidate buckets ONLY for needed combos
  // bucket: "num|zip" -> [{street, ll, source}]
  const buckets = new Map();

  function addCandidate(num, zip, street, ll, source) {
    const key = `${num}|${zip}`;
    if (!needed.has(key)) return;
    if (!ll) return;
    const arr = buckets.get(key) || [];
    arr.push({ street, ll, source });
    buckets.set(key, arr);
  }

  console.log("[pass2] Scanning parcel keys for candidates (filtered)...");
  {
    const keys = Object.keys(parcelIndex);
    for (let i = 0; i < keys.length; i++) {
      const parsed = parseParcelKey(keys[i]);
      if (!parsed) continue;
      const ll = extractLatLon(parcelIndex[keys[i]]);
      addCandidate(parsed.number, parsed.zip, parsed.street, ll, "parcel_centroid");
      if ((i + 1) % 500000 === 0) {
        console.log(`[pass2] parcel scanned: ${(i + 1).toLocaleString()}/${keys.length.toLocaleString()} | buckets=${buckets.size.toLocaleString()}`);
      }
    }
  }

  console.log("[pass2] Scanning address keys for candidates (filtered)...");
  {
    const keys = Object.keys(addressIndex);
    for (let i = 0; i < keys.length; i++) {
      const parsed = parseAddressKey(keys[i]);
      if (!parsed) continue;
      const ll = extractLatLon(addressIndex[keys[i]]);
      addCandidate(parsed.number, parsed.zip, parsed.street, ll, "address_point");
      if ((i + 1) % 500000 === 0) {
        console.log(`[pass2] address scanned: ${(i + 1).toLocaleString()}/${keys.length.toLocaleString()} | buckets=${buckets.size.toLocaleString()}`);
      }
    }
  }

  console.log(`[pass2] Candidate buckets built: ${buckets.size.toLocaleString()}`);

  const outNew = fs.createWriteStream(PASS2_NEW, { flags: "w" });
  const outStill = fs.createWriteStream(PASS2_UNMATCHED, { flags: "w" });

  const rl = readline.createInterface({
    input: fs.createReadStream(PASS1_UNMATCHED),
    crlfDelay: Infinity,
  });

  let scanned = 0;
  let newMatches = 0;
  let still = 0;

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    scanned++;

    let listing;
    try { listing = JSON.parse(t); } catch {
      still++;
      outStill.write(t + "\n");
      continue;
    }

    const { number, street, zip } = getListingNumberStreetZip(listing);
    if (!number || !street || !zip) {
      still++;
      outStill.write(JSON.stringify(listing) + "\n");
      continue;
    }

    const bucketKey = `${number}|${zip}`;
    const candidates = buckets.get(bucketKey) || [];

    let best = null;
    let bestScore = 0;
    let second = 0;

    for (const c of candidates) {
      const s = similarity(street, c.street);
      if (s > bestScore) {
        second = bestScore;
        bestScore = s;
        best = c;
      } else if (s > second) {
        second = s;
      }
    }

    // threshold + separation guard
    const ok = best && bestScore >= 0.88 && (bestScore - second) >= 0.03;

    if (ok) {
      listing.latitude = best.ll.latitude;
      listing.longitude = best.ll.longitude;
      listing.coordTier = "T3_FUZZY";
      listing.coordSource = best.source;
      listing.coordTaxonomy = "fuzzy_numZip_street";
      listing.coordConfidence = confidenceFromSim(bestScore);
      listing.coordFuzzy = { score: Math.round(bestScore * 1000) / 1000 };

      outNew.write(JSON.stringify(listing) + "\n");
      newMatches++;
    } else {
      outStill.write(JSON.stringify(listing) + "\n");
      still++;
    }

    if (scanned % 5000 === 0) {
      console.log(
        `[pass2] scanned=${scanned.toLocaleString()} | newMatches=${newMatches.toLocaleString()} | still=${still.toLocaleString()}`
      );
    }
  }

  outNew.end();
  outStill.end();

  // Build PASS2 matched file (single “coords-ready” file for zoning)
  const outPass2Matched = fs.createWriteStream(PASS2_MATCHED, { flags: "w" });

  console.log("[pass2] Building listingsWithCoords_PASS2 (FAST + PASS1new + PASS2new)...");
  const a = await appendFileToStream(FAST_MATCHED, outPass2Matched);
  const b = await appendFileToStream(PASS1_NEW, outPass2Matched);
  const c = await appendFileToStream(PASS2_NEW, outPass2Matched);
  outPass2Matched.end();

  console.log("====================================================");
  console.log("PASS 2 SUMMARY");
  console.log("----------------------------------------------------");
  console.log("PASS1 unmatched scanned:  ", scanned.toLocaleString());
  console.log("NEW matches (PASS2):      ", newMatches.toLocaleString());
  console.log("STILL unmatched (PASS2):  ", still.toLocaleString());
  console.log("PASS2 matched build lines:");
  console.log("  FAST matched:           ", a.toLocaleString());
  console.log("  PASS1 new:              ", b.toLocaleString());
  console.log("  PASS2 new:              ", c.toLocaleString());
  console.log("Outputs:");
  console.log("  ", PASS2_MATCHED);
  console.log("  ", PASS2_NEW);
  console.log("  ", PASS2_UNMATCHED);
  console.log("====================================================");
}

main().catch((e) => die(e?.stack || String(e)));
