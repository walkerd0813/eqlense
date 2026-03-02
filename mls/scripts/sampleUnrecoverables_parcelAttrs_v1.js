import fs from "fs";
import readline from "readline";

const unrecoverablePath = process.argv[2];
const parcelsPath = process.argv[3];
const N = Number(process.argv[4] ?? 50);

if (!unrecoverablePath || !parcelsPath) {
  console.log("Usage: node sampleUnrecoverables_parcelAttrs_v1.js <unrecoverable.ndjson> <parcels.ndjson> [N=50]");
  process.exit(1);
}

function norm(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s.length ? s : null;
}
function pidFromRow(r) {
  return norm(r.parcel_id ?? r.parcelId ?? r.MAP_PAR_ID ?? r.LOC_ID);
}
function pick(props, keys) {
  for (const k of keys) {
    const v = norm(props?.[k]);
    if (v) return { k, v };
  }
  return { k: null, v: null };
}

async function main() {
  // 1) take first N unrecoverables and collect parcel_ids
  const ids = new Set();
  const sampleRows = [];

  const rl1 = readline.createInterface({
    input: fs.createReadStream(unrecoverablePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl1) {
    if (sampleRows.length >= N) break;
    const t = line.trim();
    if (!t) continue;
    const row = JSON.parse(t);
    const pid = pidFromRow(row);
    if (!pid) continue;
    ids.add(pid);
    sampleRows.push({ pid, reasons: row.address_backfill_reasons ?? row.address_backfill_reasons ?? [] });
  }

  console.log(`\n[sample] grabbed ${sampleRows.length} rows, ${ids.size} unique parcel_ids\n`);

  // 2) scan parcels.ndjson and print the address-ish fields for those parcel_ids
  const hits = new Map();

  const rl2 = readline.createInterface({
    input: fs.createReadStream(parcelsPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl2) {
    const t = line.trim();
    if (!t) continue;
    const obj = JSON.parse(t);
    const props = obj.properties ?? obj;

    const mapPar = pick(props, ["MAP_PAR_ID", "map_par_id", "PARCEL_ID", "parcel_id"]).v;
    const locId = pick(props, ["LOC_ID", "loc_id"]).v;

    const candidates = [mapPar, locId].filter(Boolean);
    let matched = null;
    for (const c of candidates) {
      if (ids.has(c)) matched = c;
    }
    if (!matched) continue;

    const addrNum = pick(props, ["ADDR_NUM","addr_num","ADDRNUM","ST_NUM","HOUSE_NUM","ADDRESS_NUM"]);
    const siteAddr = pick(props, ["SITE_ADDR","site_addr","SITEADDR","ADDRESS","SITEADDRESS"]);
    const fullStr = pick(props, ["FULL_STR","full_str","FULLADDR","FULL_ADDRESS","FULLADDRS"]);
    const street = pick(props, ["STREET","street","ST_NAME","st_name","STREETNAME","streetname","ROAD_NAME","road_name"]);
    const city = pick(props, ["CITY","city","TOWN","town","MUNICIPALITY"]);
    const zip = pick(props, ["ZIP","zip","ZIPCODE","ZipCode","POSTCODE"]);

    hits.set(matched, {
      matched,
      addrNum,
      siteAddr,
      fullStr,
      street,
      city,
      zip,
    });

    if (hits.size >= ids.size) break;
  }

  console.log("[parcel attrs sample]");
  for (const s of sampleRows) {
    const h = hits.get(s.pid);
    console.log("----------------------------------------------------");
    console.log(`parcel_id: ${s.pid}`);
    console.log(`reasons:   ${JSON.stringify(s.reasons)}`);
    if (!h) {
      console.log("parcel record: NOT FOUND in parcels.ndjson (unexpected)");
      continue;
    }
    console.log(`ADDR_NUM:  ${h.addrNum.k ?? "-"} = ${h.addrNum.v ?? "-"}`);
    console.log(`SITE_ADDR: ${h.siteAddr.k ?? "-"} = ${h.siteAddr.v ?? "-"}`);
    console.log(`FULL_STR:  ${h.fullStr.k ?? "-"} = ${h.fullStr.v ?? "-"}`);
    console.log(`STREET:    ${h.street.k ?? "-"} = ${h.street.v ?? "-"}`);
    console.log(`CITY/TOWN: ${h.city.k ?? "-"} = ${h.city.v ?? "-"}`);
    console.log(`ZIP:       ${h.zip.k ?? "-"} = ${h.zip.v ?? "-"}`);
  }
  console.log("----------------------------------------------------\nDONE\n");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
