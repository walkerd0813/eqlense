/**
 * DiagnosticSelector.js
 * 
 * Tests why the AVM is returning no comps.
 * Loads subject + comp dataset, runs filters step by step.
 */

const path = require("path");
const fs = require("fs");

function loadJSON(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

// Paths to your data
const compsPath = path.join(__dirname, "comps", "singleFamily.json");

// Load comps
const comps = loadJSON(compsPath);
console.log("Loaded comps:", comps.length);

// -------- Subject (same one from ThunderClient) --------
const subject = {
  address: "15 Alvarado Ave",
  town: "Worcester, MA",
  zip: "01604",
  sqft: 1540,
  beds: 3,
  baths: "1f 1h",
  lat: 42.2727461,
  lng: -71.7608709
};

// Convert baths like “1f 1h”
function bathsToNumber(b) {
  const parts = b.split(" ");
  let full = 0, half = 0;

  parts.forEach(p => {
    if (p.toLowerCase().includes("f")) full += 1;
    if (p.toLowerCase().includes("h")) half += 0.5;
  });

  return full + half;
}

const subjectBaths = bathsToNumber(subject.baths);

// ----------- Step-by-step filtering -----------
console.log("\nSTEP 1 — Only comps with lat/lng");
const step1 = comps.filter(c => c.lat && c.lng);
console.log("Remaining:", step1.length);

console.log("\nSTEP 2 — Within 2 miles");
function haversine(lat1, lon1, lat2, lon2) {
  const R = 3958.8;
  const dLat = (lat2 - lat1) * Math.PI/180;
  const dLon = (lon2 - lon1) * Math.PI/180;
  const a =
    Math.sin(dLat/2)**2 +
    Math.cos(lat1 * Math.PI/180) *
    Math.cos(lat2 * Math.PI/180) *
    Math.sin(dLon/2)**2;

  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

const step2 = step1.filter(c => {
  const dist = haversine(subject.lat, subject.lng, c.lat, c.lng);
  return dist <= 2;
});
console.log("Remaining:", step2.length);

console.log("\nSTEP 3 — Beds within ±1");
const step3 = step2.filter(c => Math.abs(c.beds - subject.beds) <= 1);
console.log("Remaining:", step3.length);

console.log("\nSTEP 4 — SQFT within ±25%");
const step4 = step3.filter(c => {
  const ratio = c.sqft / subject.sqft;
  return ratio >= 0.75 && ratio <= 1.25;
});
console.log("Remaining:", step4.length);

console.log("\nSTEP 5 — Baths within ±1");
const step5 = step4.filter(c => Math.abs(bathsToNumber(c.baths) - subjectBaths) <= 1);
console.log("Remaining:", step5.length);

console.log("\n======= FINAL RESULTS =======");
console.log("Selected comps:", step5.length);
console.log(step5.slice(0, 5));