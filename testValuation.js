// testValuation.js
const { estimateValue } = require("./valuationModel");

const input = {
  address: "48 Santa Barbara St",
  zip: "01104",
  propertyType: "single_family",
  beds: 4,
  baths: 2,
  sqft: 1755,
  yearBuilt: 1950,
  lotSize: 6000,
  style: "colonial",
  condition: "average",
  renovationStatus: "average"
};

console.log("---- RUNNING LOCAL AVM TEST ----");
const result = estimateValue(input);
console.log(JSON.stringify(result, null, 2));