// addressAuthority_applyCandidates_v1_DROPIN.js
// Applies candidates.ndjson to properties.ndjson by parcel_id.
// Fills only blanks; stamps addr_source/addr_method/addr_key_used.

import fs from "fs";
import readline from "readline";

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
  if (s === "0" || s === "0.0") return "";
  return s;
}

async function loadCandidates(fp) {
  const map = new Map();
  const rl = readline.createInterface({ input: fs.createReadStream(fp, "utf8"), crlfDelay: Infinity });
  let n = 0;
  for await (const line of rl) {
    if (!line.trim()) continue;
    let row;
    try {
      row = JSON.parse(line);
    } catch {
      continue;
    }
    if (!row.parcel_id) continue;
    map.set(String(row.parcel_id), row);
    n++;
  }
  return { map, count: n };
}

async function main() {
  const args = parseArgs(process.argv);
  const IN = args.in;
  const CANDS = args.candidates;
  const OUT = args.out;
  const META = args.meta;

  if (!IN) throw new Error("Missing --in <properties.ndjson>");
  if (!CANDS) throw new Error("Missing --candidates <candidates.ndjson>");
  if (!OUT) throw new Error("Missing --out <out.ndjson>");

  const { map: candMap, count: candCount } = await loadCandidates(CANDS);

  const rl = readline.createInterface({ input: fs.createReadStream(IN, "utf8"), crlfDelay: Infinity });
  const outStream = fs.createWriteStream(OUT, { encoding: "utf8" });

  let total = 0;
  let appliedRows = 0;
  let filledStreetNo = 0;
  let filledStreetName = 0;
  let filledZip = 0;
  let filledFullAddr = 0;

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
    const pid = !isBlank(parcelId) ? String(parcelId) : "";
    const cand = pid ? candMap.get(pid) : null;

    if (cand) {
      const before = {
        street_no: normStreetNo(pick(row, ["street_no", "streetNo", "street_number", "ADDR_NUM"])),
        street_name: pick(row, ["street_name", "streetName", "street", "FULL_STR", "STREET"]),
        full_address: pick(row, ["full_address", "fullAddress", "SITE_ADDR", "address"]),
        zip: normZip(pick(row, ["zip", "ZIP", "zip5", "ZIP5", "POSTCODE"])),
      };

      let changed = false;

      // Fill only blanks
      if (before.street_no === "" && cand.street_no) {
        row.street_no = cand.street_no;
        filledStreetNo++;
        changed = true;
      }
      if (isBlank(before.street_name) && cand.street_name) {
        row.street_name = cand.street_name;
        filledStreetName++;
        changed = true;
      }
      if (before.zip === "" && cand.zip) {
        row.zip = cand.zip;
        filledZip++;
        changed = true;
      }
      if (isBlank(before.full_address) && cand.site_addr) {
        row.full_address = cand.site_addr;
        filledFullAddr++;
        changed = true;
      }

      if (changed) {
        appliedRows++;
        row.addr_source = "MAD:StatewideAddressPointsForGeocoding";
        row.addr_method = "parcel_id_join";
        row.addr_key_used = `${cand?.evidence?.parcel_field || "parcel_id"}:${pid}`;
        row.addr_evidence = cand.evidence || null;
      }
    }

    outStream.write(JSON.stringify(row) + "\n");
  }

  outStream.end();

  const meta = {
    created_at: new Date().toISOString(),
    in: IN,
    candidates: CANDS,
    out: OUT,
    counts: {
      total_rows: total,
      candidates_loaded: candCount,
      applied_rows: appliedRows,
      filled_street_no: filledStreetNo,
      filled_street_name: filledStreetName,
      filled_zip: filledZip,
      filled_full_address: filledFullAddr,
    },
  };

  if (META) fs.writeFileSync(META, JSON.stringify(meta, null, 2), "utf8");
  console.log("[done]", meta);
}

main().catch((e) => {
  console.error("❌ addressAuthority_applyCandidates failed:", e);
  process.exit(1);
});
