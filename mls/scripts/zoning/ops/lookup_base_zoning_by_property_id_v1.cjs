const { lookupBaseZoningByPropertyId } = require("../../../services/baseZoningIndexLookup_v1.cjs");

function arg(name, defVal = null) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : defVal;
}
function has(name) {
  return process.argv.includes(name);
}

const propertyId = arg("--propertyId") || arg("--property_id");
const out = arg("--out", "");
const includeMeta = has("--includeMeta");

if (!propertyId) {
  console.error("Usage: node lookup_base_zoning_by_property_id_v1.cjs --propertyId <ma:parcel:...> [--includeMeta] [--out <json>]");
  process.exit(1);
}

const res = lookupBaseZoningByPropertyId(propertyId, { includeMeta });

if (out) {
  require("fs").writeFileSync(out, JSON.stringify(res, null, 2));
  console.log("[OK ] wrote:", out);
} else {
  console.log(JSON.stringify(res, null, 2));
}
