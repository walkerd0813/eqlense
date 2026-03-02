#!/usr/bin/env node
/**
 * Address Authority — Analyze Missing Essentials (v1 DROP-IN)
 * Streams a properties NDJSON and produces:
 *  - counts + buckets explaining WHY rows are missing street_no/street_name/zip
 *  - optional NDJSON of parcel_ids needing authority upgrade
 *
 * Usage:
 *  node .\mls\scripts\addressAuthority_analyzeMissing_v1_DROPIN.js `
 *    --in  "C:\seller-app\backend\publicData\properties\properties_statewide_geo_zip_district_v27_CANONICAL.ndjson" `
 *    --meta "C:\seller-app\backend\publicData\properties\v27_missingAddress_meta.json" `
 *    --outParcelIds "C:\seller-app\backend\publicData\properties\v27_missingAddress_parcelIds.ndjson"
 */

import fs from "fs";
import readline from "readline";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1];
    if (v && !v.startsWith("--")) {
      out[k] = v;
      i++;
    } else {
      out[k] = true;
    }
  }
  return out;
}

function firstField(obj, keys) {
  for (const k of keys) {
    if (obj && Object.prototype.hasOwnProperty.call(obj, k)) return obj[k];
  }
  return undefined;
}

function normStr(v) {
  if (v === null || v === undefined) return "";
  return String(v).trim();
}

function isMissing(v) {
  const s = normStr(v);
  return s === "" || s === "null" || s === "undefined";
}

function looksLikeZip5(v) {
  const s = normStr(v);
  return /^\d{5}$/.test(s);
}

function startsWithNumber(siteAddr) {
  const s = normStr(siteAddr);
  return /^\d/.test(s);
}

function parseLeadingNumber(siteAddr) {
  const s = normStr(siteAddr);
  // Accept: 105.1, 270-6, 105 1/2 (as "105 1/2"), 105A
  const m = s.match(/^(\d+(?:\.\d+)?(?:-[\dA-Za-z]+)?(?:\s+1\/2)?[A-Za-z]?)\s+(.+)$/);
  if (!m) return null;
  const no = m[1].trim();
  const rest = m[2].trim();
  if (no === "0") return null;
  return { no, rest };
}

async function main() {
  const args = parseArgs(process.argv);
  const inFile = args.in;
  const metaFile = args.meta;
  const outParcelIds = args.outParcelIds;

  if (!inFile || !fs.existsSync(inFile)) throw new Error(`--in not found: ${inFile}`);
  if (!metaFile) throw new Error(`--meta is required`);

  const rl = readline.createInterface({
    input: fs.createReadStream(inFile, "utf8"),
    crlfDelay: Infinity,
  });

  const parcelIdWriter = outParcelIds
    ? fs.createWriteStream(outParcelIds, { encoding: "utf8" })
    : null;

  const buckets = {};
  const counts = {
    total: 0,
    zipMissing: 0,
    streetNameMissing: 0,
    streetNoMissing: 0,
    essentialsMissingAny: 0,
    wroteParcelIds: 0,
    badJsonLines: 0,
  };

  const FIELD = {
    parcelId: ["parcel_id", "parcelId", "LOC_ID", "loc_id", "MAP_PAR_ID", "map_par_id"],
    streetNo: ["street_no", "streetNo", "street_number", "streetNumber", "ADDR_NUM", "addr_num"],
    streetName: ["street_name", "streetName", "street", "STREET", "FULL_STR", "full_str"],
    fullAddr: ["full_address", "fullAddress", "SITE_ADDR", "site_addr", "siteAddress"],
    zip: ["zip", "ZIP", "zip5", "ZIP5", "POSTCODE", "postcode"],
    city: ["city_town", "cityTown", "CITY", "city", "TOWN", "town"],
    addrNum: ["ADDR_NUM", "addr_num"],
    siteAddr: ["SITE_ADDR", "site_addr", "full_address", "fullAddress"],
    fullStr: ["FULL_STR", "full_str"],
  };

  function bump(name) {
    buckets[name] = (buckets[name] || 0) + 1;
  }

  for await (const line of rl) {
    if (!line.trim()) continue;
    counts.total++;

    let row;
    try {
      row = JSON.parse(line);
    } catch {
      counts.badJsonLines++;
      continue;
    }

    const parcelId = normStr(firstField(row, FIELD.parcelId));
    const streetNo = firstField(row, FIELD.streetNo);
    const streetName = firstField(row, FIELD.streetName);
    const zip = firstField(row, FIELD.zip);

    const addrNum = firstField(row, FIELD.addrNum);
    const siteAddr = firstField(row, FIELD.siteAddr);
    const fullStr = firstField(row, FIELD.fullStr);

    const missNo = isMissing(streetNo) || normStr(streetNo) === "0";
    const missName = isMissing(streetName);
    const missZip = !looksLikeZip5(zip);

    if (missNo) counts.streetNoMissing++;
    if (missName) counts.streetNameMissing++;
    if (missZip) counts.zipMissing++;

    const any = missNo || missName || missZip;
    if (!any) continue;

    counts.essentialsMissingAny++;

    // Bucket diagnostics for street_no issues
    if (missNo) {
      const an = normStr(addrNum);
      const sa = normStr(siteAddr);
      if (!isMissing(addrNum)) {
        if (an === "0") bump("streetNo.addrNum_zero");
        else if (/^\d+$/.test(an)) bump("streetNo.addrNum_int_present_but_not_used");
        else bump("streetNo.addrNum_nonint_present");
      } else if (!isMissing(siteAddr)) {
        const parsed = parseLeadingNumber(sa);
        if (parsed) bump("streetNo.siteAddr_has_parseable_number");
        else if (startsWithNumber(sa)) bump("streetNo.siteAddr_startsWithNumber_but_unparseable");
        else bump("streetNo.siteAddr_streetOnly_or_noNumber");
        if (sa.includes(",")) bump("streetNo.siteAddr_contains_comma_subcommunity");
        if (/\bREAR\b|\b\(OFF\)\b|\bOFF\b/i.test(sa)) bump("streetNo.siteAddr_rear_or_off");
      } else {
        bump("streetNo.no_addr_fields");
      }
    }

    // Bucket diagnostics for zip issues
    if (missZip) {
      const ct = normStr(firstField(row, FIELD.city));
      if (ct) bump("zip.missing_has_city");
      else bump("zip.missing_no_city");
    }

    // Output parcel ids needing authority upgrade (usually street_no missing)
    if (parcelIdWriter && parcelId && (missNo || missName)) {
      parcelIdWriter.write(JSON.stringify({ parcel_id: parcelId }) + "\n");
      counts.wroteParcelIds++;
    }
  }

  parcelIdWriter?.end();

  const meta = {
    run: {
      started_at: new Date().toISOString(),
      node: process.version,
      in: inFile,
      outParcelIds: outParcelIds || null,
    },
    counts,
    buckets,
  };

  fs.writeFileSync(metaFile, JSON.stringify(meta, null, 2), "utf8");
  console.log("[done]", counts);
  console.log("[meta]", metaFile);
  if (outParcelIds) console.log("[parcelIds]", outParcelIds);
}

main().catch((e) => {
  console.error("❌ analyzeMissing failed:", e);
  process.exit(1);
});
