// backend/mls/scripts/addressAuthority_buildTargets_v1_DROPIN.js
import fs from "fs";
import path from "path";
import readline from "readline";
import crypto from "crypto";

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const v = argv[i + 1];
      if (!v || v.startsWith("--")) out[a] = true;
      else out[a] = v, i++;
    }
  }
  return out;
}

function first(obj, keys) {
  for (const k of keys) {
    if (obj && obj[k] !== undefined && obj[k] !== null && obj[k] !== "") return obj[k];
  }
  return null;
}

function normId(v) {
  if (v === null || v === undefined) return null;
  return String(v).trim();
}

function isMissingStreetNo(v) {
  if (v === null || v === undefined) return true;
  const s = String(v).trim();
  if (!s) return true;
  if (s === "0") return true;
  return false;
}

function isMissingText(v) {
  return v === null || v === undefined || String(v).trim() === "";
}

function sha256File(filePath) {
  const h = crypto.createHash("sha256");
  const fd = fs.openSync(filePath, "r");
  const buf = Buffer.allocUnsafe(1024 * 1024);
  let bytes = 0;
  try {
    while ((bytes = fs.readSync(fd, buf, 0, buf.length, null)) > 0) h.update(buf.subarray(0, bytes));
  } finally {
    fs.closeSync(fd);
  }
  return h.digest("hex");
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  const inNdjson = args["--in"];
  const outTargets = args["--outTargets"];
  const outMeta = args["--meta"];
  const hashInputs = String(args["--hashInputs"] || "false").toLowerCase() === "true";

  if (!inNdjson || !outTargets || !outMeta) {
    console.log(`
Usage:
  node mls/scripts/addressAuthority_buildTargets_v1_DROPIN.js \\
    --in "C:\\seller-app\\backend\\publicData\\properties\\v27_properties.ndjson" \\
    --outTargets "C:\\seller-app\\backend\\publicData\\properties\\v27_addrTargets.json" \\
    --meta "C:\\seller-app\\backend\\publicData\\properties\\v27_addrTargets_meta.json" \\
    [--hashInputs true]
`);
    process.exit(1);
  }

  console.log("====================================================");
  console.log("   ADDRESS AUTHORITY V1 — BUILD TARGET PARCEL LIST");
  console.log("====================================================");
  console.log("in:", inNdjson);
  console.log("outTargets:", outTargets);
  console.log("meta:", outMeta);

  const rl = readline.createInterface({
    input: fs.createReadStream(inNdjson, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  const parcelIds = new Set();

  let total = 0;
  let candidates = 0;

  let missStreetNo = 0;
  let missStreetName = 0;
  let missFullAddress = 0;
  let missZip = 0;

  let mStyle = 0;
  let nonMStyle = 0;

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    total++;

    let row;
    try {
      row = JSON.parse(t);
    } catch {
      continue;
    }

    const parcelId = normId(first(row, ["parcel_id", "PARCEL_ID", "LOC_ID", "loc_id", "id"]));
    if (!parcelId) continue;

    const streetNo = first(row, ["street_no", "addr_num", "ADDRESS_NUMBER", "site_addr_num"]);
    const streetName = first(row, ["street_name", "st_name", "STREET_NAME", "site_street"]);
    const fullAddr = first(row, ["full_address", "site_addr", "SITE_ADDR", "address"]);
    const zip = first(row, ["zip", "zipcode", "POSTCODE", "postal_code"]);

    const needs =
      isMissingStreetNo(streetNo) ||
      isMissingText(streetName) ||
      isMissingText(fullAddr) ||
      isMissingText(zip);

    if (!needs) continue;

    candidates++;

    if (isMissingStreetNo(streetNo)) missStreetNo++;
    if (isMissingText(streetName)) missStreetName++;
    if (isMissingText(fullAddr)) missFullAddress++;
    if (isMissingText(zip)) missZip++;

    parcelIds.add(parcelId);

    if (/^M_\d+_\d+$/.test(parcelId)) mStyle++;
    else nonMStyle++;
    if (total % 250000 === 0) console.log(`[progress] scanned ${total.toLocaleString()} rows...`);
  }

  const payload = {
    created_at: new Date().toISOString(),
    inputs: {
      inNdjson,
      sha256: hashInputs ? sha256File(inNdjson) : null,
    },
    counts: {
      total_rows_scanned: total,
      candidate_rows: candidates,
      unique_parcels: parcelIds.size,
      missing_street_no_rows: missStreetNo,
      missing_street_name_rows: missStreetName,
      missing_full_address_rows: missFullAddress,
      missing_zip_rows: missZip,
    },
    key_stats: {
      m_style_like_M_123_456: mStyle,
      non_m_style: nonMStyle,
      note:
        "V1 MAD join is deterministic on CENTROID_ID. If most parcels are non M_* format, V1 coverage will be limited and we move to V2 (point-in-parcel) later.",
    },
    parcel_ids: Array.from(parcelIds),
  };

  fs.mkdirSync(path.dirname(outTargets), { recursive: true });
  fs.writeFileSync(outTargets, JSON.stringify(payload, null, 2), "utf8");
  fs.writeFileSync(outMeta, JSON.stringify({ compiler: "addressAuthority_buildTargets_v1", ...payload }, null, 2), "utf8");

  console.log("----------------------------------------------------");
  console.log("[done]", {
    scanned: total,
    candidate_rows: candidates,
    unique_parcels: parcelIds.size,
    m_style: mStyle,
    non_m_style: nonMStyle,
  });
  console.log("----------------------------------------------------");
}

main().catch((e) => {
  console.error("[fatal]", e);
  process.exit(1);
});
