// backend/mls/scripts/attachCoordinatesParcelPolygon.js
// ---------------------------------------------------
// Tier 4 — Parcel polygon verification / attachment (tile-based).
//
// INPUT  (default): backend/mls/normalized/unmatched_FAST_geocoded.ndjson
// OUTPUT (matched): backend/mls/normalized/unmatched_TIER4_matched.ndjson
// OUTPUT (still):   backend/mls/normalized/unmatched_TIER4_stillUnmatched.ndjson
//
// Run (from C:\seller-app\backend):
//   node mls/scripts/attachCoordinatesParcelPolygon.js

import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

import booleanPointInPolygon from "@turf/boolean-point-in-polygon";
import { point as turfPoint } from "@turf/helpers";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function getArg(name, fallback = null) {
  const idx = process.argv.indexOf(name);
  if (idx === -1) return fallback;
  return process.argv[idx + 1] ?? fallback;
}
function numArg(name, fallback) {
  const v = getArg(name, null);
  if (v == null) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

const INPUT_PATH = path.resolve(__dirname, getArg("--in", "../../mls/normalized/unmatched_FAST_geocoded.ndjson"));
const OUT_MATCHED = path.resolve(__dirname, getArg("--matchedOut", "../../mls/normalized/unmatched_TIER4_matched.ndjson"));
const OUT_UNMATCHED = path.resolve(__dirname, getArg("--unmatchedOut", "../../mls/normalized/unmatched_TIER4_stillUnmatched.ndjson"));

const TILE_DEG = numArg("--tileDeg", 0.02);
const TILES_DIR = path.resolve(__dirname, getArg("--tilesDir", "../../publicData/parcels/tiles_0p02"));

function tileIndex(v) {
  return Math.floor(v / TILE_DEG);
}
function tileKeyFromLatLon(lat, lon) {
  return `${tileIndex(lat)}_${tileIndex(lon)}`;
}
function tileFilePath(tileKey) {
  return path.join(TILES_DIR, `tile_${tileKey}.ndjson`);
}

function normCity(s) {
  return (s || "").toString().trim().toUpperCase();
}
function normZip(s) {
  return (s || "").toString().trim().slice(0, 5);
}

function computeTier4Score({ zipMatch, cityMatch }) {
  // Tier taxonomy scale (tune later):
  // Tier1 ~0.98, Tier2 ~0.92, Tier3 ~0.88, Tier4 ~0.74–0.87
  let score = 0.74;
  if (zipMatch) score += 0.08;
  if (cityMatch) score += 0.05;
  return Math.min(0.87, score);
}

async function loadListingsGroupedByTile() {
  if (!fs.existsSync(INPUT_PATH)) {
    console.error("❌ Missing input:", INPUT_PATH);
    process.exit(1);
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(INPUT_PATH),
    crlfDelay: Infinity,
  });

  const groups = new Map();
  const noCoords = [];
  let total = 0;

  for await (const line of rl) {
    if (!line) continue;
    total++;

    let listing;
    try {
      listing = JSON.parse(line);
    } catch (e) {
      console.error("❌ Bad JSON in listings at line", total);
      throw e;
    }

    const lat = Number(listing.latitude);
    const lon = Number(listing.longitude);

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      noCoords.push(listing);
      continue;
    }

    const key = tileKeyFromLatLon(lat, lon);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(listing);

    if (total % 10_000 === 0) {
      console.log(`[Tier4] Grouped ${total.toLocaleString()} listings... tiles=${groups.size.toLocaleString()}`);
    }
  }

  return { groups, noCoords, total };
}

async function processTile(tileKey, listings) {
  const parcelFile = tileFilePath(tileKey);
  if (!fs.existsSync(parcelFile)) {
    return { matched: [], unmatched: listings };
  }

  const pts = listings.map((l) => ({
    listing: l,
    pt: turfPoint([Number(l.longitude), Number(l.latitude)]),
    done: false,
  }));

  const rl = readline.createInterface({
    input: fs.createReadStream(parcelFile),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if (!line) continue;

    let parcelFeat;
    try {
      parcelFeat = JSON.parse(line);
    } catch {
      continue;
    }

    const props = parcelFeat?.properties || {};
    const parcelZip = normZip(props.ZIP);
    const parcelCity = normCity(props.CITY);

    for (const item of pts) {
      if (item.done) continue;

      const l = item.listing;
      const lZip = normZip(l?.address?.zip || l?.raw?.row?.ZIP_CODE || l?.ZIP_CODE);
      const lCity = normCity(l?.address?.city || l?.raw?.row?.TOWN_DESC || l?.TOWN_DESC);

      // cheap filter
      if (parcelZip && lZip && parcelZip !== lZip) continue;

      let inside = false;
      try {
        inside = booleanPointInPolygon(item.pt, parcelFeat);
      } catch {
        inside = false;
      }
      if (!inside) continue;

      const cityMatch = !!(parcelCity && lCity && lCity.includes(parcelCity));
      const zipMatch = !!(parcelZip && lZip && parcelZip === lZip);
      const score = computeTier4Score({ zipMatch, cityMatch });

      const enriched = {
        ...l,
        coordSource: "parcel_polygon",
        coordTier: 4,
        coordConfidence: score,
        coordTaxonomy: {
          tier: 4,
          method: "point_in_parcel_polygon",
          score,
          zipMatch,
          cityMatch,
        },
        parcelMatch: {
          MAP_PAR_ID: props.MAP_PAR_ID ?? null,
          LOC_ID: props.LOC_ID ?? null,
          PROP_ID: props.PROP_ID ?? null,
          SITE_ADDR: props.SITE_ADDR ?? null,
          CITY: props.CITY ?? null,
          ZIP: props.ZIP ?? null,
        },
      };

      item.done = true;
      item.listing = enriched;
    }

    if (pts.every((x) => x.done)) break;
  }

  const matched = [];
  const unmatched = [];
  for (const it of pts) (it.done ? matched : unmatched).push(it.listing);
  return { matched, unmatched };
}

async function main() {
  console.log("====================================================");
  console.log(" TIER 4 — PARCEL POLYGON ATTACH (TILE-BASED)");
  console.log("====================================================");
  console.log("Input:       ", INPUT_PATH);
  console.log("TilesDir:    ", TILES_DIR);
  console.log("Tile°:       ", TILE_DEG);
  console.log("MatchedOut:  ", OUT_MATCHED);
  console.log("UnmatchedOut:", OUT_UNMATCHED);
  console.log("----------------------------------------------------");

  const outMatched = fs.createWriteStream(OUT_MATCHED, { flags: "w" });
  const outUnmatched = fs.createWriteStream(OUT_UNMATCHED, { flags: "w" });

  const { groups, noCoords, total } = await loadListingsGroupedByTile();

  let matchedCount = 0;
  let stillCount = 0;
  let processedTiles = 0;

  for (const l of noCoords) {
    stillCount++;
    outUnmatched.write(JSON.stringify(l) + "\n");
  }

  for (const [tileKey, listings] of groups.entries()) {
    processedTiles++;
    const { matched, unmatched } = await processTile(tileKey, listings);

    for (const m of matched) {
      matchedCount++;
      outMatched.write(JSON.stringify(m) + "\n");
    }
    for (const u of unmatched) {
      stillCount++;
      outUnmatched.write(JSON.stringify(u) + "\n");
    }

    if (processedTiles % 250 === 0) {
      console.log(`[Tier4] tiles=${processedTiles.toLocaleString()} | matched=${matchedCount.toLocaleString()} | still=${stillCount.toLocaleString()}`);
    }
  }

  outMatched.end();
  outUnmatched.end();

  console.log("====================================================");
  console.log("✅ Tier 4 complete");
  console.log("Total listings read:", total.toLocaleString());
  console.log("Matched Tier 4:", matchedCount.toLocaleString());
  console.log("Still unmatched:", stillCount.toLocaleString());
  console.log("====================================================");
}

main().catch((err) => {
  console.error("❌ Fatal Tier 4 error:", err);
  process.exit(1);
});
