// addressAuthority_buildMadCandidates_fromCSV_v1_DROPIN.js
// Streaming CSV → best candidate per parcel_id. Outputs NDJSON candidates for Part 3 apply script.
// No geometry required.

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

// Robust-ish CSV line parser (handles quoted commas).
function parseCSVLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (ch === '"' && line[i + 1] === '"') {
      // Escaped quote
      cur += '"';
      i++;
      continue;
    }

    if (ch === '"') {
      inQ = !inQ;
      continue;
    }

    if (ch === "," && !inQ) {
      out.push(cur);
      cur = "";
      continue;
    }

    cur += ch;
  }
  out.push(cur);
  return out;
}

function getField(row, keyMap, candidates) {
  for (const k of candidates) {
    const idx = keyMap.get(k);
    if (idx !== undefined) return row[idx];
  }
  return "";
}

function parseSiteAddr(siteAddr) {
  if (isBlank(siteAddr)) return { street_no: "", street_name: "" };
  let s = String(siteAddr).trim();
  s = s.replace(/\s+/g, " ");
  s = s.replace(/\s*\(.*?\)\s*$/g, "").trim();     // trailing (...) like (OFF)
  s = s.replace(/\b(REAR|OFF)\b\s*$/i, "").trim(); // trailing REAR/OFF

  const m = s.match(/^([0-9]+(?:\.[0-9]+)?(?:-[0-9]+)?[A-Za-z]?)\s+(.*)$/);
  if (!m) return { street_no: "", street_name: s };

  return { street_no: normStreetNo(m[1]), street_name: (m[2] || "").trim() };
}

function scoreCandidate(c) {
  let score = 0;
  if (c.street_no) score += 10;
  if (c.street_name) score += 4;
  if (c.zip) score += 5;
  const addr = (c.site_addr || "").toUpperCase();
  const unitish = /\b(UNIT|APT|APARTMENT|SUITE|STE|FL|FLOOR|#)\b/.test(addr);
  if (!unitish) score += 2;
  return score;
}

async function main() {
  const args = parseArgs(process.argv);

  const TARGETS = args.targets;
  const CSV = args.csv;
  const OUT = args.outCandidates || args.out;
  const META = args.meta;

  if (!TARGETS) throw new Error("Missing --targets <targets.json>");
  if (!CSV) throw new Error("Missing --csv <mad_points.csv>");
  if (!OUT) throw new Error("Missing --outCandidates <candidates.ndjson>");

  const targets = JSON.parse(fs.readFileSync(TARGETS, "utf8"));
  const targetSet = new Set(targets?.parcel_ids?.missing_any || []);
  if (targetSet.size === 0) {
    fs.writeFileSync(OUT, "", "utf8");
    if (META) fs.writeFileSync(META, JSON.stringify({ total_targets: 0, matched: 0 }, null, 2), "utf8");
    console.log("[done] no targets");
    return;
  }

  // Likely field names in MAD exports
const parcelFieldCandidates = ["CENTROID_ID", "LOC_ID", "PARCEL_ID", "MAP_PAR_ID", "PID", "parcel_id"];
  const siteAddrCandidates = ["SITE_ADDR", "FULL_ADDR", "ADDRESS", "ADDR", "SITEADDRESS", "SITEADDRESS1", "STREETADDR"];
  const zipFieldCandidates = ["ZIP5", "ZIP", "POSTCODE", "POST_CODE"];
  const cityFieldCandidates = ["CITY", "TOWN", "GEOTOWN", "POSTTOWN", "MSAGCOMM", "MUNI", "MUNICIPALITY"];
  const streetNoCandidates = ["ADDR_NUM", "ADDRNUM", "HOUSE_NUM", "HOUSE_NUMB", "NUM"];
  const streetNameCandidates = ["FULL_STR", "STREET", "STREETNAME", "STREET_NAME", "ROADNAME"];

  const rl = readline.createInterface({ input: fs.createReadStream(CSV, "utf8"), crlfDelay: Infinity });

  let header = null;
  let keyMap = null;

  // Auto-detect best parcel field by counting target hits in first N rows
  const sampleN = Number(args.sampleN || 50000);
  const hitCounts = new Map(parcelFieldCandidates.map((k) => [k, 0]));

  const buffered = [];
  let lineNo = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;
    lineNo++;

    if (!header) {
      header = parseCSVLine(line).map((s) => s.trim());
      keyMap = new Map(header.map((k, i) => [k, i]));
      continue;
    }

    buffered.push(line);
    const row = parseCSVLine(line);

    for (const k of parcelFieldCandidates) {
      const idx = keyMap.get(k);
      if (idx === undefined) continue;
      const pid = row[idx] ? String(row[idx]).trim() : "";
      if (pid && targetSet.has(pid)) hitCounts.set(k, (hitCounts.get(k) || 0) + 1);
    }

    if (buffered.length >= sampleN) break;
  }

  // Pick best parcel field (or user override)
let parcelField = args.parcelField || "";
if (!parcelField) {
  let best = { k: "", v: -1 };
  for (const [k, v] of hitCounts.entries()) {
    if (v > best.v) best = { k, v };
  }
    parcelField = best.k || "";
  if (!parcelField) {
    // If nothing matched targets in the sample, fall back to “field exists in CSV header”
    if (keyMap?.has("CENTROID_ID")) parcelField = "CENTROID_ID";
    else if (keyMap?.has("LOC_ID")) parcelField = "LOC_ID";
    else throw new Error("No usable parcel id field found in CSV header (need CENTROID_ID or LOC_ID).");
  }

}


  // Re-open full stream for real pass
  const rl2 = readline.createInterface({ input: fs.createReadStream(CSV, "utf8"), crlfDelay: Infinity });
  header = null;
  keyMap = null;

  const bestByPid = new Map();
  let totalRows = 0;
  let matchedRows = 0;

  for await (const line of rl2) {
    if (!line.trim()) continue;

    if (!header) {
      header = parseCSVLine(line).map((s) => s.trim());
      keyMap = new Map(header.map((k, i) => [k, i]));
      continue;
    }

    totalRows++;
    const row = parseCSVLine(line);

    const pidRaw = getField(row, keyMap, [parcelField]);
    const pid = pidRaw ? String(pidRaw).trim() : "";
    if (!pid || !targetSet.has(pid)) continue;

    matchedRows++;

    const siteAddr = getField(row, keyMap, siteAddrCandidates).trim();
    const zip = normZip(getField(row, keyMap, zipFieldCandidates));
    const city = getField(row, keyMap, cityFieldCandidates).trim();

    // prefer explicit fields if present, else parse site address
    const explicitNo = normStreetNo(getField(row, keyMap, streetNoCandidates));
    const explicitName = getField(row, keyMap, streetNameCandidates).trim();

    const parsed = parseSiteAddr(siteAddr);

    const cand = {
      parcel_id: pid,
      site_addr: siteAddr || "",
      street_no: explicitNo || parsed.street_no,
      street_name: explicitName || parsed.street_name,
      zip,
      city,
      evidence: {
        source: "MAD:StatewideAddressPointsForGeocoding",
        input: CSV,
        parcel_field: parcelField,
        fields_used: {
          site_addr: siteAddrCandidates.find((k) => keyMap.has(k)) || null,
          zip: zipFieldCandidates.find((k) => keyMap.has(k)) || null,
          city: cityFieldCandidates.find((k) => keyMap.has(k)) || null,
          street_no: streetNoCandidates.find((k) => keyMap.has(k)) || null,
          street_name: streetNameCandidates.find((k) => keyMap.has(k)) || null,
        },
      },
    };

    const score = scoreCandidate(cand);
    const prev = bestByPid.get(pid);
    if (!prev || score > prev._score) bestByPid.set(pid, { ...cand, _score: score });
  }

  const outStream = fs.createWriteStream(OUT, { encoding: "utf8" });
  for (const v of bestByPid.values()) {
    const { _score, ...clean } = v;
    outStream.write(JSON.stringify(clean) + "\n");
  }
  outStream.end();

  const meta = {
    created_at: new Date().toISOString(),
    targets: TARGETS,
    csv: CSV,
    outCandidates: OUT,
    counts: {
      target_parcels: targetSet.size,
      csv_rows_scanned: totalRows,
      csv_rows_matched: matchedRows,
      candidates_built: bestByPid.size,
    },
    fields_used: { parcelField },
  };

  if (META) fs.writeFileSync(META, JSON.stringify(meta, null, 2), "utf8");
  console.log("[done]", meta);
}

main().catch((e) => {
  console.error("❌ buildMadCandidates_fromCSV failed:", e);
  process.exit(1);
});
