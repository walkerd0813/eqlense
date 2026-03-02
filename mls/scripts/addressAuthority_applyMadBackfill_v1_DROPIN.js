// ADDRESS AUTHORITY V1 — APPLY MAD BACKFILL (key-join candidates)
// ESM drop-in: node .\mls\scripts\addressAuthority_applyMadBackfill_v1_DROPIN.js --in ... --candidates ... --out ... --meta ...

import fs from "fs";
import path from "path";
import readline from "readline";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : "true";
    out[k] = v;
  }
  return out;
}

function isBlank(v) {
  if (v === null || v === undefined) return true;
  const s = String(v).trim();
  return s === "" || s === "-" || s.toLowerCase() === "null" || s.toLowerCase() === "undefined";
}

function normStreetNo(v) {
  if (isBlank(v)) return "";
  const s = String(v).trim();
  if (s === "0") return "";
  return s;
}

function normZip(v) {
  if (isBlank(v)) return "";
  const s = String(v).replace(/[^\d]/g, "").slice(0, 5);
  return s.length === 5 ? s : "";
}

function buildFullAddress(streetNo, streetName) {
  const no = normStreetNo(streetNo);
  const nm = isBlank(streetName) ? "" : String(streetName).trim();
  if (!no || !nm) return "";
  return `${no} ${nm}`.replace(/\s+/g, " ").trim();
}

async function loadCandidatesMap(candidatesPath) {
  const map = new Map(); // parcel_id -> candidate
  const rl = readline.createInterface({
    input: fs.createReadStream(candidatesPath, "utf8"),
    crlfDelay: Infinity,
  });

  let n = 0;
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    const c = JSON.parse(t);
    const pid = c?.parcel_id ? String(c.parcel_id).trim() : "";
    if (!pid) continue;
    map.set(pid, c);
    n++;
  }

  return { map, loaded: n };
}

async function main() {
  const args = parseArgs(process.argv);

  const IN = args.in;
  const CAND = args.candidates;
  const OUT = args.out;
  const META = args.meta;

  if (!IN || !CAND || !OUT) {
    console.log("USAGE:");
    console.log("  node .\\mls\\scripts\\addressAuthority_applyMadBackfill_v1_DROPIN.js --in <properties.ndjson> --candidates <candidates.ndjson> --out <out.ndjson> [--meta <meta.json>]");
    process.exit(1);
  }

  if (!fs.existsSync(IN)) throw new Error(`IN not found: ${IN}`);
  if (!fs.existsSync(CAND)) throw new Error(`candidates not found: ${CAND}`);

  console.log("ADDRESS AUTHORITY V1 — APPLY MAD BACKFILL");
  console.log("====================================================");
  console.log("in:", IN);
  console.log("candidates:", CAND);
  console.log("out:", OUT);
  if (META) console.log("meta:", META);
  console.log("----------------------------------------------------");

  console.log("[info] loading MAD candidates into memory:", CAND);
  const { map: candByPid, loaded } = await loadCandidatesMap(CAND);
  console.log("[info] candidates loaded:", loaded);

  const tmpOut = OUT + ".tmp";
  const inRL = readline.createInterface({
    input: fs.createReadStream(IN, "utf8"),
    crlfDelay: Infinity,
  });
  const outStream = fs.createWriteStream(tmpOut, { encoding: "utf8" });

  let total = 0;
  let matched = 0;
  let patchedAny = 0;
  let patchedStreetNo = 0;
  let patchedStreetName = 0;
  let patchedZip = 0;
  let patchedCity = 0;
  let patchedFullAddr = 0;

  for await (const line of inRL) {
    const t = line.trim();
    if (!t) continue;
    total++;

    const obj = JSON.parse(t);
    const pid = obj?.parcel_id ? String(obj.parcel_id).trim() : "";

    const cand = pid ? candByPid.get(pid) : null;
    if (!cand) {
      outStream.write(JSON.stringify(obj) + "\n");
      continue;
    }

    matched++;

    let changed = false;

    // candidate fields
    const cStreetNo = normStreetNo(cand.street_no);
    const cStreetName = isBlank(cand.street_name) ? "" : String(cand.street_name).trim();
    const cZip = normZip(cand.zip);
    const cCity = isBlank(cand.city) ? "" : String(cand.city).trim();
    const cSite = isBlank(cand.site_addr) ? "" : String(cand.site_addr).trim();

    // property fields (use your canonical names)
    const pStreetNo = normStreetNo(obj.street_no);
    const pStreetName = isBlank(obj.street_name) ? "" : String(obj.street_name).trim();
    const pZip = normZip(obj.zip);
    const pCity = isBlank(obj.city_town) ? (isBlank(obj.city) ? "" : String(obj.city).trim()) : String(obj.city_town).trim();
    const pFull = isBlank(obj.full_address) ? "" : String(obj.full_address).trim();
    const pSite = isBlank(obj.site_addr) ? "" : String(obj.site_addr).trim();

    // Fill only if missing
    if (!pStreetNo && cStreetNo) {
      obj.street_no = cStreetNo;
      patchedStreetNo++;
      changed = true;
    }

    if (!pStreetName && cStreetName) {
      obj.street_name = cStreetName;
      patchedStreetName++;
      changed = true;
    }

    if (!pZip && cZip) {
      obj.zip = cZip;
      patchedZip++;
      changed = true;
    }

    // Only fill city if property city missing (you already have city on almost all rows)
    if (!pCity && cCity) {
      obj.city_town = cCity;
      patchedCity++;
      changed = true;
    }

    // If full_address missing, generate from street_no+street_name, else fall back to cand.site_addr
    if (!pFull) {
      const gen = buildFullAddress(obj.street_no, obj.street_name);
      if (gen) {
        obj.full_address = gen;
        patchedFullAddr++;
        changed = true;
      } else if (cSite) {
        obj.full_address = cSite;
        patchedFullAddr++;
        changed = true;
      }
    }

    // If site_addr missing, keep candidate site_addr (optional)
    if (!pSite && cSite) {
      obj.site_addr = cSite;
      changed = true;
    }

    if (changed) {
      patchedAny++;
      obj.addr_authority = {
        ...(obj.addr_authority || {}),
        mad_key_join: {
          source: cand?.evidence?.source || "MAD",
          method: "KEY_JOIN",
          parcel_field: cand?.evidence?.parcel_field || "CENTROID_ID",
          input: cand?.evidence?.input || CAND,
          applied_at: new Date().toISOString(),
        },
      };
    }

    outStream.write(JSON.stringify(obj) + "\n");
  }

  outStream.end();
  await new Promise((res) => outStream.on("finish", res));

  fs.renameSync(tmpOut, OUT);

  const meta = {
    created_at: new Date().toISOString(),
    in: IN,
    candidates: CAND,
    out: OUT,
    counts: {
      total_rows: total,
      candidate_rows_matched: matched,
      patched_rows_any: patchedAny,
      patched_street_no: patchedStreetNo,
      patched_street_name: patchedStreetName,
      patched_zip: patchedZip,
      patched_city: patchedCity,
      patched_full_address: patchedFullAddr,
    },
    note: "This is a key-join apply pass. It will only patch parcels whose parcel_id matches candidates.parcel_id (e.g., MAD CENTROID_ID style IDs).",
  };

  if (META) fs.writeFileSync(META, JSON.stringify(meta, null, 2), "utf8");

  console.log("[done]", meta);
}

main().catch((e) => {
  console.error("❌ addressAuthority_applyMadBackfill failed:", e);
  process.exit(1);
});
