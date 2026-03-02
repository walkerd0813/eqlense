import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";
import fetch from "node-fetch";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ================= CONFIG =================
const TIMEOUT_MS = 6000;
// OSM/Nominatim is sensitive to rate. Keep this conservative unless you're using a paid provider.
const RATE_DELAY_MS = 1100;

// ================= ARGS =================
const argv = process.argv.slice(2);
const RESUME = argv.includes("--resume");
const args = argv.filter((a) => a !== "--resume");

const INPUT = args[0];
const OUT_GOOD = args[1] || path.join(__dirname, "../../mls/normalized/external_geocoded.ndjson");
const OUT_BAD  = args[2] || path.join(__dirname, "../../mls/normalized/external_unmatched.ndjson");

if (!INPUT) {
  console.error("Usage:");
  console.error("  node mls/scripts/externalGeocode.js [--resume] <input.ndjson> <outputGood.ndjson> <outputBad.ndjson>");
  process.exit(1);
}

if (!fs.existsSync(INPUT)) {
  console.error("❌ Missing input:", INPUT);
  process.exit(1);
}

// ================= HELPERS =================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchWithTimeout(url, options = {}, timeout = TIMEOUT_MS) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    return res;
  } finally {
    clearTimeout(id);
  }
}

function extractListingIdFromLine(line) {
  if (!line || !line.trim()) return null;
  try {
    const j = JSON.parse(line);
    return j?.listingId || null;
  } catch {
    const m = line.match(/"listingId"\s*:\s*"([^"]+)"/);
    return m ? m[1] : null;
  }
}

async function loadProcessedIds(paths) {
  const ids = new Set();
  for (const p of paths) {
    if (!p || !fs.existsSync(p)) continue;
    const rl = readline.createInterface({
      input: fs.createReadStream(p, { encoding: "utf8" }),
      crlfDelay: Infinity,
    });
    for await (const line of rl) {
      const id = extractListingIdFromLine(line);
      if (id) ids.add(id);
    }
  }
  return ids;
}

function buildQuery(listing) {
  const addr = listing?.address || {};
  const row = listing?.raw?.row || {};

  const streetNumber =
    addr.streetNumber ||
    row.STREET_NUM ||
    row.STREET_NO ||
    "";

  const streetName =
    addr.streetName ||
    row.STREET_NAME ||
    "";

  // If you already enriched unit into address.unit, use it.
  // Otherwise fall back to raw ADDRESS patterns like "... U:1NE"
  let unit = addr.unit || "";
  if (!unit) {
    const rawAddr = row.ADDRESS || "";
    const um = rawAddr.match(/\bU:\s*([^\s,]+.*)$/i);
    if (um && um[1]) unit = um[1].trim();
  }

  const cityRaw =
    addr.city ||
    row.TOWN_DESC ||
    row.TOWN ||
    "";

  const state = addr.state || "MA";
  const zip = addr.zip || row.ZIP_CODE || "";

  const parts = [];
  if (streetNumber) parts.push(String(streetNumber).trim());
  if (streetName) parts.push(String(streetName).trim());
  if (unit) parts.push(`Unit ${String(unit).trim()}`);
  if (cityRaw) parts.push(String(cityRaw).trim());
  parts.push(state);
  if (zip) parts.push(String(zip).trim());

  return parts.filter(Boolean).join(" ");
}

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

// ================= GEOCODERS =================
async function tryOSM(query) {
  const url = `https://nominatim.openstreetmap.org/search?format=json&limit=1&q=${encodeURIComponent(query)}`;
  const res = await fetchWithTimeout(url, {
    headers: {
      "User-Agent": "EquityLens/1.0 (contact: dev@equitylens.local)",
      "Accept": "application/json",
    },
  });

  if (!res || !res.ok) return null;
  const j = await res.json();
  if (!Array.isArray(j) || j.length === 0) return null;

  const hit = j[0];
  const lat = Number(hit.lat);
  const lon = Number(hit.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;

  // A simple confidence heuristic:
  const importance = Number(hit.importance ?? 0.5);
  const confidence = clamp(0.60 + importance * 0.35, 0.60, 0.93);

  return { lat, lon, confidence, raw: hit };
}

async function tryCensus(query) {
  const url =
    `https://geocoding.geo.census.gov/geocoder/locations/onelineaddress` +
    `?benchmark=2020&format=json&address=${encodeURIComponent(query)}`;

  const res = await fetchWithTimeout(url, {
    headers: { "Accept": "application/json" },
  });

  if (!res || !res.ok) return null;
  const j = await res.json();
  const matches = j?.result?.addressMatches;
  if (!Array.isArray(matches) || matches.length === 0) return null;

  const hit = matches[0];
  const lon = Number(hit?.coordinates?.x);
  const lat = Number(hit?.coordinates?.y);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;

  // Census tends to be fairly reliable when it returns a match
  const confidence = 0.90;

  return { lat, lon, confidence, raw: hit };
}

async function geocodeOne(listing) {
  const query = buildQuery(listing);
  if (!query || query.length < 8) return null;

  // OSM first
  try {
    const osm = await tryOSM(query);
    if (osm) return { ...osm, source: "external_osm", query };
  } catch {}

  // Census fallback
  try {
    const cen = await tryCensus(query);
    if (cen) return { ...cen, source: "external_census", query };
  } catch {}

  return null;
}

// ================= MAIN =================
async function main() {
  console.log("====================================================");
  console.log(" EXTERNAL GEOCODE (OSM + Census)");
  console.log("====================================================");
  console.log("Input: ", path.resolve(INPUT));
  console.log("Good:  ", path.resolve(OUT_GOOD));
  console.log("Bad:   ", path.resolve(OUT_BAD));
  console.log("Resume:", RESUME ? "YES" : "NO");
  console.log("----------------------------------------------------");

  const processed = RESUME ? await loadProcessedIds([OUT_GOOD, OUT_BAD]) : new Set();
  if (RESUME) console.log(`Loaded processed IDs: ${processed.size}`);

  const outGood = fs.createWriteStream(OUT_GOOD, { flags: RESUME ? "a" : "w" });
  const outBad  = fs.createWriteStream(OUT_BAD,  { flags: RESUME ? "a" : "w" });

  const rl = readline.createInterface({
    input: fs.createReadStream(INPUT, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let scanned = 0, matched = 0, unmatched = 0, skipped = 0, badJson = 0;

  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    scanned++;

    let listing;
    try {
      listing = JSON.parse(line);
    } catch {
      badJson++;
      continue;
    }

    const id = listing?.listingId || null;

    if (RESUME && id && processed.has(id)) {
      skipped++;
      continue;
    }

    // If coords already exist, keep as-is (treat as matched)
    if (Number.isFinite(listing?.latitude) && Number.isFinite(listing?.longitude)) {
      listing.coordSource = listing.coordSource || "preexisting";
      listing.coordConfidence = Number.isFinite(listing.coordConfidence) ? listing.coordConfidence : 0.99;
      outGood.write(JSON.stringify(listing) + "\n");
      matched++;
    } else {
      const geo = await geocodeOne(listing);
      if (geo) {
        listing.latitude = geo.lat;
        listing.longitude = geo.lon;
        listing.coordSource = geo.source;
        listing.coordConfidence = geo.confidence;
        listing.externalGeocode = { query: geo.query };
        outGood.write(JSON.stringify(listing) + "\n");
        matched++;
      } else {
        outBad.write(JSON.stringify(listing) + "\n");
        unmatched++;
      }
      await sleep(RATE_DELAY_MS);
    }

    if (scanned % 1000 === 0) {
      console.log(`[geo] scanned=${scanned} matched=${matched} unmatched=${unmatched} skipped=${skipped} badJson=${badJson}`);
    }
  }

  outGood.end();
  outBad.end();

  console.log("====================================================");
  console.log("✅ External geocode complete");
  console.log(`scanned=${scanned} matched=${matched} unmatched=${unmatched} skipped=${skipped} badJson=${badJson}`);
  console.log("====================================================");
}

main().catch((e) => {
  console.error("❌ External geocode failed:", e);
  process.exit(1);
});
