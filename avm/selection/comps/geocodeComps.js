/**
 * geocodeComps.js
 * Automatically geocodes all comps (singleFamily, multiFamily, condos)
 * Adds { lat, lng } to each comp
 * Saves processed files into ./comps_geocoded/
 */

const fs = require("fs");
const path = require("path");

// ------------------------------
// UNIVERSAL FETCH SHIM FOR NODE
// ------------------------------
const fetch = (...args) =>
  import("node-fetch").then(({ default: fetch }) => fetch(...args));

// Input files
const INPUT_DIR = path.join(__dirname, "..", "comps");
const FILES = ["singleFamily.json", "multiFamily.json", "condos.json"];

// Output directory
const OUTPUT_DIR = path.join(__dirname, "..", "comps_geocoded");
if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR);

// Nominatim (OpenStreetMap)
const NOMINATIM_URL = "https://nominatim.openstreetmap.org/search";

async function geocodeAddress(addr) {
  const url =
    NOMINATIM_URL +
    `?q=${encodeURIComponent(addr)}&format=json&addressdetails=0&limit=1`;

  console.log("🌐 Geocoding:", addr);

  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": "EquityLens AVM (contact: support@equitylens.ai)",
      },
      timeout: 10000,
    });

    if (!res.ok) {
      console.error("❌ HTTP error:", res.status, res.statusText);
      return null;
    }

    const json = await res.json();
    if (!json || json.length === 0) return null;

    return {
      lat: Number(json[0].lat),
      lng: Number(json[0].lon),
    };
  } catch (err) {
    console.error("❌ Geocode error:", err.message || err);
    return null;
  }
}

async function processFile(filename) {
  const filePath = path.join(INPUT_DIR, filename);
  const raw = fs.readFileSync(filePath, "utf8");
  const comps = JSON.parse(raw);

  const updated = [];

  for (const comp of comps) {
    const fullAddress = `${comp.address}, ${comp.town} ${comp.zip}`;

    // Already geocoded? Keep it.
    if (comp.lat && comp.lng) {
      updated.push(comp);
      continue;
    }

    const geo = await geocodeAddress(fullAddress);

    if (geo) {
      comp.lat = geo.lat;
      comp.lng = geo.lng;
      console.log(` → Geocoded OK: ${geo.lat}, ${geo.lng}`);
    } else {
      comp.lat = null;
      comp.lng = null;
      console.log(" → Geocode FAILED (null inserted)");
    }

    // Respect Nominatim rate limit — MINIMUM 1 request/sec
    await new Promise((resolve) => setTimeout(resolve, 1200));

    updated.push(comp);
  }

  // Save updated file
  const outPath = path.join(OUTPUT_DIR, filename);
  fs.writeFileSync(outPath, JSON.stringify(updated, null, 2));
  console.log(`💾 Saved geocoded → ${outPath}`);
}

(async () => {
  console.log("🚀 Starting Geocoding of Comp Datasets…");

  for (const file of FILES) {
    console.log(`\n📘 Processing ${file}…`);
    await processFile(file);
  }

  console.log("\n✅ All comp datasets geocoded!");
})();
