// backend/mls/scripts/addressAuthority_applyMadAuthority_v1_DROPIN.js
import fs from "fs";
import path from "path";
import readline from "readline";

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

function buildFullAddress(num, street, unit, unitType) {
  const n = String(num || "").trim();
  const s = String(street || "").trim();
  if (!n || !s) return null;
  let out = `${n} ${s}`;
  const u = String(unit || "").trim();
  const ut = String(unitType || "").trim();
  if (u) out += ut ? `, ${ut} ${u}` : `, Unit ${u}`;
  return out;
}

async function loadCandidatesNdjson(filePath) {
  console.log(`[info] loading MAD candidates into memory: ${filePath}`);
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  const byId = new Map();
  let n = 0;

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    let row;
    try {
      row = JSON.parse(t);
    } catch {
      continue;
    }
    const id = normId(row.CENTROID_ID);
    if (!id) continue;
    byId.set(id, row);
    n++;
    if (n % 200000 === 0) console.log(`[progress] loaded ${n.toLocaleString()} candidates...`);
  }

  console.log(`[info] candidates loaded: ${byId.size.toLocaleString()}`);
  return byId;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  const inNdjson = args["--in"];
  const candidatesNdjson = args["--candidates"];
  const outNdjson = args["--out"];
  const outMeta = args["--meta"];

  if (!inNdjson || !candidatesNdjson || !outNdjson || !outMeta) {
    console.log(`
Usage:
  node mls/scripts/addressAuthority_applyMadAuthority_v1_DROPIN.js \\
    --in "C:\\seller-app\\backend\\publicData\\properties\\v27_properties.ndjson" \\
    --candidates "C:\\seller-app\\backend\\publicData\\properties\\v27_madCandidates.ndjson" \\
    --out "C:\\seller-app\\backend\\publicData\\properties\\v28_properties_addrAuthority.ndjson" \\
    --meta "C:\\seller-app\\backend\\publicData\\properties\\v28_properties_addrAuthority_meta.json"
`);
    process.exit(1);
  }

  console.log("====================================================");
  console.log("   ADDRESS AUTHORITY V1 — APPLY MAD BACKFILL");
  console.log("====================================================");
  console.log("in:", inNdjson);
  console.log("candidates:", candidatesNdjson);
  console.log("out:", outNdjson);
  console.log("meta:", outMeta);

  const cand = await loadCandidatesNdjson(candidatesNdjson);

  fs.mkdirSync(path.dirname(outNdjson), { recursive: true });
  const out = fs.createWriteStream(outNdjson, { encoding: "utf8" });

  const rl = readline.createInterface({
    input: fs.createReadStream(inNdjson, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let scanned = 0;
  let madMatched = 0;

  let filledStreetNo = 0;
  let filledStreetName = 0;
  let filledZip = 0;
  let filledFullAddress = 0;

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    scanned++;

    let row;
    try {
      row = JSON.parse(t);
    } catch {
      continue;
    }

    const parcelId = normId(first(row, ["parcel_id", "PARCEL_ID", "LOC_ID", "loc_id", "id"]));
    const m = parcelId ? cand.get(parcelId) : null;

    if (m) {
      madMatched++;

      const curStreetNo = first(row, ["street_no", "addr_num"]);
      const curStreetName = first(row, ["street_name", "st_name"]);
      const curZip = first(row, ["zip", "zipcode"]);
      const curFull = first(row, ["full_address", "site_addr", "address"]);

      const madNum = m.ADDRESS_NUMBER || m.FULL_NUMBER_STANDARDIZED;
      const madStreet = m.STREET_NAME;
      const madZip = m.POSTCODE;
      const madUnit = m.UNIT;
      const madUnitType = m.UNIT_TYPE;

      // Only fill if missing (do not overwrite good data)
      if (isMissingStreetNo(curStreetNo) && !isMissingText(madNum)) {
        row.street_no = String(madNum).trim();
        filledStreetNo++;
      }
      if (isMissingText(curStreetName) && !isMissingText(madStreet)) {
        row.street_name = String(madStreet).trim();
        filledStreetName++;
      }
      if (isMissingText(curZip) && !isMissingText(madZip)) {
        row.zip = String(madZip).trim();
        filledZip++;
      }

      // Build full address if missing and we now have enough
      const finalStreetNo = first(row, ["street_no", "addr_num"]);
      const finalStreetName = first(row, ["street_name", "st_name"]);
      if (isMissingText(curFull)) {
        const fa = buildFullAddress(finalStreetNo, finalStreetName, madUnit, madUnitType);
        if (fa) {
          row.full_address = fa;
          filledFullAddress++;
        }
      }

      // Attach an audit breadcrumb (small + explicit)
      row.addr_authority = {
        source: "MassGIS_MAD",
        method: "CENTROID_ID",
        centroid_id: m.CENTROID_ID,
        master_address_id: m.MASTER_ADDRESS_ID || null,
        point_type: m.POINT_TYPE || null,
        updated_at: new Date().toISOString(),
      };
    }

    out.write(JSON.stringify(row) + "\n");

    if (scanned % 250000 === 0) {
      console.log(`[progress] scanned ${scanned.toLocaleString()} | MAD matched ${madMatched.toLocaleString()}`);
    }
  }

  out.end();

  const meta = {
    created_at: new Date().toISOString(),
    inputs: {
      inNdjson,
      candidatesNdjson,
    },
    counts: {
      rows_scanned: scanned,
      parcels_with_mad_match: madMatched,
      filled_street_no: filledStreetNo,
      filled_street_name: filledStreetName,
      filled_zip: filledZip,
      filled_full_address: filledFullAddress,
    },
    notes: [
      "V1 is deterministic: only fills when parcel_id == MAD.CENTROID_ID match exists.",
      "If coverage is low, next step is V2: point-in-parcel join using parcel polygons (still audit-safe).",
    ],
  };

  fs.writeFileSync(outMeta, JSON.stringify(meta, null, 2), "utf8");

  console.log("----------------------------------------------------");
  console.log("[done]", meta);
  console.log("----------------------------------------------------");
}

main().catch((e) => {
  console.error("[fatal]", e);
  process.exit(1);
});
