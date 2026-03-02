// backend/avm/lookupSubject.js
// ------------------------------------------------------
// Find a "subject" row in the AVM dataset by address + zip
// Used by /api/avm/estimate so we can get sqft / beds / baths, etc.
// ------------------------------------------------------

const fs = require("fs");
const path = require("path");

let singleFamilyRows = null;

// Load and cache the single-family comps dataset once
function loadSingleFamilyDataset() {
  if (singleFamilyRows) return singleFamilyRows;

  const filePath = path.join(
    __dirname,
    "selection",
    "comps",
    "singleFamily.json"
  );

  const raw = fs.readFileSync(filePath, "utf8");
  singleFamilyRows = JSON.parse(raw);

  console.log(
    "[lookupSubject] Loaded singleFamily dataset:",
    Array.isArray(singleFamilyRows) ? singleFamilyRows.length : "N/A",
    "rows"
  );

  return singleFamilyRows;
}

// Normalize address so "15 Alvarado Ave" vs "15 ALVARADO AVE" still match
function normalizeAddress(str) {
  return String(str || "")
    .toLowerCase()
    .replace(/[\s.,#]/g, ""); // remove spaces, dots, commas, #
}

// Find the row that matches the incoming address + zip
function findSubjectByAddress({ address, zip }) {
  if (!address || !zip) return null;

  const rows = loadSingleFamilyDataset();
  const targetAddr = normalizeAddress(address);
  const targetZip = String(zip).slice(0, 5); // normalize 01604 vs "01604-1234"

  const match = rows.find((row) => {
    const rowAddr = normalizeAddress(row.address);
    const rowZip = String(row.zip).slice(0, 5);
    return rowAddr === targetAddr && rowZip === targetZip;
  });

  if (!match) {
    console.warn(
      "[lookupSubject] No subject row found for",
      address,
      "ZIP",
      targetZip
    );
  } else {
    console.log(
      "[lookupSubject] Found subject row for",
      address,
      "ZIP",
      targetZip
    );
  }

  return match || null;
}

module.exports = {
  findSubjectByAddress,
};






