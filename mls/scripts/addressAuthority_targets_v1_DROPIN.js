// addressAuthority_targets_v1_DROPIN.js
// ESM, streaming. Reads properties NDJSON and outputs parcel_id targets for missing address essentials.

import fs from "fs";
import readline from "readline";
import path from "path";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
      out[k] = v;
    }
  }
  return out;
}

function pick(obj, keys) {
  for (const k of keys) {
    if (obj && obj[k] !== undefined && obj[k] !== null) return obj[k];
  }
  return undefined;
}

function isBlank(v) {
  if (v === undefined || v === null) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  return false;
}

function normZip(z) {
  if (isBlank(z)) return "";
  const s = String(z).trim();
  const m = s.match(/\b(\d{5})\b/);
  return m ? m[1] : "";
}

function normStreetNo(n) {
  if (isBlank(n)) return "";
  const s = String(n).trim();
  if (s === "0" || s === "0.0") return ""; // treat 0 as missing (institutional)
  return s;
}

async function main() {
  const args = parseArgs(process.argv);

  const IN = args.in;
  const OUT = args.outTargets || args.out || "";
  const META = args.meta || "";

  if (!IN) throw new Error("Missing --in <properties.ndjson>");

  const outTargets = OUT || path.join(path.dirname(IN), path.basename(IN).replace(/\.ndjson$/i, "") + "_addrTargets.json");
  const outMeta = META || outTargets.replace(/\.json$/i, "_meta.json");

  const rl = readline.createInterface({
    input: fs.createReadStream(IN, "utf8"),
    crlfDelay: Infinity,
  });

  const missingStreetNo = [];
  const missingStreetName = [];
  const missingZip = [];
  const missingFullAddr = [];
  const missingAnySet = new Set();

  let total = 0;
  let noParcelId = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;
    total++;
    let row;
    try {
      row = JSON.parse(line);
    } catch {
      continue;
    }

    const parcelId = pick(row, ["parcel_id", "parcelId", "PARCEL_ID", "LOC_ID", "MAP_PAR_ID"]);
    if (isBlank(parcelId)) {
      noParcelId++;
      continue;
    }
    const pid = String(parcelId);

    const streetNo = normStreetNo(pick(row, ["street_no", "streetNo", "street_number", "ADDR_NUM"]));
    const streetName = pick(row, ["street_name", "streetName", "street", "FULL_STR", "STREET"]);
    const fullAddr = pick(row, ["full_address", "fullAddress", "SITE_ADDR", "address"]);
    const zip = normZip(pick(row, ["zip", "ZIP", "zip5", "ZIP5", "POSTCODE"]));

    const missNo = streetNo === "";
    const missName = isBlank(streetName);
    const missFull = isBlank(fullAddr);
    const missZip = zip === "";

    if (missNo) missingStreetNo.push(pid);
    if (missName) missingStreetName.push(pid);
    if (missFull) missingFullAddr.push(pid);
    if (missZip) missingZip.push(pid);

    if (missNo || missName || missFull || missZip) missingAnySet.add(pid);
  }

  const targets = {
    created_at: new Date().toISOString(),
    in: IN,
    totals: {
      total_rows: total,
      no_parcel_id_rows: noParcelId,
      candidate_parcels: missingAnySet.size,
    },
    parcel_ids: {
      missing_any: Array.from(missingAnySet),
      missing_street_no: missingStreetNo,
      missing_street_name: missingStreetName,
      missing_full_address: missingFullAddr,
      missing_zip: missingZip,
    },
  };

  fs.writeFileSync(outTargets, JSON.stringify(targets), "utf8");

  const meta = {
    created_at: targets.created_at,
    in: IN,
    outTargets: outTargets,
    counts: {
      total_rows: total,
      candidate_parcels: missingAnySet.size,
      missing_street_no: missingStreetNo.length,
      missing_street_name: missingStreetName.length,
      missing_full_address: missingFullAddr.length,
      missing_zip: missingZip.length,
      no_parcel_id_rows: noParcelId,
    },
  };
  fs.writeFileSync(outMeta, JSON.stringify(meta, null, 2), "utf8");

  console.log("[done]", meta);
}

main().catch((e) => {
  console.error("❌ addressAuthority_targets failed:", e);
  process.exit(1);
});
