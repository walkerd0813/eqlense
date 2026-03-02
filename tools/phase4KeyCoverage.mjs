import fs from "node:fs";
import readline from "node:readline";

const file = process.argv[2];
if (!file) { console.log("usage: node tools/phase4KeyCoverage.mjs <properties.ndjson>"); process.exit(2); }

const rl = readline.createInterface({ input: fs.createReadStream(file, {encoding:"utf8"}), crlfDelay: Infinity });

let n=0, hasParcel=0, hasPropId=0;
let space=0, hyphen=0, digitsOnly=0, hasZeroLead=0;
let parsedLooksLikeMapLot=0;
let hasAnyAssessor=0, hasMass=0, hasMuni=0;

function isDigits(s){ return typeof s==="string" && /^[0-9]+$/.test(s); }
function looksMapLot(s){
  // crude heuristic: contains space or dash and has digits on both sides
  return typeof s==="string" && /[0-9].*[\s-].*[0-9]/.test(s);
}

rl.on("line", (l) => {
  n++;
  let o; try { o = JSON.parse(l); } catch { return; }

  const pid = o.property_id ?? null;
  const parcel = o.parcel_id ?? null;

  if (pid) hasPropId++;
  if (parcel) {
    hasParcel++;
    if (parcel.includes(" ")) space++;
    if (parcel.includes("-")) hyphen++;
    if (isDigits(parcel)) digitsOnly++;
    if (/^0+/.test(parcel)) hasZeroLead++;
    if (looksMapLot(parcel)) parsedLooksLikeMapLot++;
  }

  const asrc = o.assessor_by_source ?? null;
  if (asrc) {
    if (asrc.massgis_statewide_raw) hasMass++;
    if (asrc.city_assessor_raw) hasMuni++;
    if (asrc.massgis_statewide_raw || asrc.city_assessor_raw) hasAnyAssessor++;
  }

  if (n % 500000 === 0) console.log("[progress]", n);
});

rl.on("close", () => {
  console.log(JSON.stringify({
    rows: n,
    has_property_id: hasPropId,
    has_parcel_id: hasParcel,
    parcel_patterns: {
      contains_space: space,
      contains_hyphen: hyphen,
      digits_only: digitsOnly,
      leading_zeros: hasZeroLead,
      looks_maplot: parsedLooksLikeMapLot
    },
    assessor_presence: {
      any: hasAnyAssessor,
      massgis: hasMass,
      muni: hasMuni
    }
  }, null, 2));
});
