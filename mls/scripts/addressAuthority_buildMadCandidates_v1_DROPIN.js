// addressAuthority_buildMadCandidates_v1_DROPIN.js
// Reads MAD points GeoJSONSeq (.geojsonl/.ndjson) and a target parcel_id list,
// picks the best candidate per parcel_id, outputs candidates.ndjson (one line per parcel).

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

function parseSiteAddr(siteAddr) {
  if (isBlank(siteAddr)) return { street_no: "", street_name: "" };
  let s = String(siteAddr).trim();

  // normalize noise that breaks parsing
  s = s.replace(/\s+/g, " ");
  s = s.replace(/\s*\(.*?\)\s*$/g, "").trim();     // trailing (...) like (OFF)
  s = s.replace(/\b(REAR|OFF)\b\s*$/i, "").trim(); // trailing REAR/OFF

  // If it starts with a number-ish token, treat that as street_no
  // Supports "105.1", "270-6", "12A", "007" (kept), etc.
  const m = s.match(/^([0-9]+(?:\.[0-9]+)?(?:-[0-9]+)?[A-Za-z]?)\s+(.*)$/);
  if (!m) return { street_no: "", street_name: s };

  const streetNo = normStreetNo(m[1]);
  const streetName = m[2]?.trim() || "";

  return { street_no: streetNo, street_name: streetName };
}

function scoreCandidate(c) {
  let score = 0;
  if (c.street_no) score += 10;
  if (c.street_name) score += 4;
  if (c.zip) score += 5;
  // prefer non-unit addresses if detectable
  const addr = (c.site_addr || "").toUpperCase();
  const unitish = /\b(UNIT|APT|APARTMENT|SUITE|STE|FL|FLOOR|#)\b/.test(addr);
  if (!unitish) score += 2;
  return score;
}

async function main() {
  const args = parseArgs(process.argv);

  const TARGETS = args.targets;
  const MAD = args.madPoints;
  const OUT = args.outCandidates || args.out;
  const META = args.meta;

  if (!TARGETS) throw new Error("Missing --targets <targets.json>");
  if (!MAD) throw new Error("Missing --madPoints <madPoints.geojsonl>");
  if (!OUT) throw new Error("Missing --outCandidates <candidates.ndjson>");

  const targets = JSON.parse(fs.readFileSync(TARGETS, "utf8"));
  const targetSet = new Set(targets?.parcel_ids?.missing_any || []);
  if (targetSet.size === 0) {
    console.log("[done] no targets; nothing to do.");
    fs.writeFileSync(OUT, "", "utf8");
    if (META) fs.writeFileSync(META, JSON.stringify({ total_targets: 0, matched: 0 }, null, 2), "utf8");
    return;
  }

  // Known field candidates (MAD exports vary)
  const parcelFieldCandidates = ["LOC_ID", "MAP_PAR_ID", "PARCEL_ID", "parcel_id", "PID", "pid"];
  const zipFieldCandidates = ["ZIP5", "ZIP", "POSTCODE", "POST_CODE"];
  const cityFieldCandidates = ["CITY", "TOWN", "MUNI", "MUNICIPALITY"];
  const siteAddrCandidates = ["SITE_ADDR", "FULL_ADDR", "ADDRESS", "ADDR", "SITEADDRESS"];

  const ext = path.extname(MAD).toLowerCase();
  if (ext === ".geojson") {
    throw new Error("MAD points is a FeatureCollection .geojson. Export to GeoJSONSeq (.geojsonl) first (QGIS: Save Features As → GeoJSONSeq).");
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(MAD, "utf8"),
    crlfDelay: Infinity,
  });

  let totalFeat = 0;
  let matchedFeat = 0;

  // Auto-detect parcel field using first N features (intersection with targets)
  const sampleN = Number(args.sampleN || 5000);
  const sampleScores = new Map(parcelFieldCandidates.map((k) => [k, 0]));
  const sampleSeen = new Set();

  const bufferedLines = [];
  for await (const line of rl) {
    if (!line.trim()) continue;
    bufferedLines.push(line);
    totalFeat++;

    let feat;
    try {
      feat = JSON.parse(line);
    } catch {
      continue;
    }
    const props = feat.properties || feat;

    for (const k of parcelFieldCandidates) {
      if (props[k] !== undefined && props[k] !== null) {
        const pid = String(props[k]);
        if (targetSet.has(pid)) sampleScores.set(k, (sampleScores.get(k) || 0) + 1);
      }
    }

    if (bufferedLines.length >= sampleN) break;
  }

  // pick best parcel field
  let parcelField = args.parcelField || "";
  if (!parcelField) {
    let best = { k: "", v: -1 };
    for (const [k, v] of sampleScores.entries()) {
      if (v > best.v) best = { k, v };
    }
    parcelField = best.k || "LOC_ID";
  }

  // reopen stream (we consumed some)
  const rl2 = readline.createInterface({
    input: fs.createReadStream(MAD, "utf8"),
    crlfDelay: Infinity,
  });

  // Pick other fields by “first hit”
  let zipField = args.zipField || "";
  let cityField = args.cityField || "";
  let siteAddrField = args.siteAddrField || "";
  let featureIdField = args.featureIdField || "";

  const bestByPid = new Map();

  for await (const line of rl2) {
    if (!line.trim()) continue;

    let feat;
    try {
      feat = JSON.parse(line);
    } catch {
      continue;
    }
    const props = feat.properties || feat;

    if (!zipField) zipField = zipFieldCandidates.find((k) => props[k] !== undefined) || "";
    if (!cityField) cityField = cityFieldCandidates.find((k) => props[k] !== undefined) || "";
    if (!siteAddrField) siteAddrField = siteAddrCandidates.find((k) => props[k] !== undefined) || "";
    if (!featureIdField) featureIdField = ["OBJECTID", "OBJECTID_1", "ID", "MASTER_ADD_ID"].find((k) => props[k] !== undefined) || "";

    const rawPid = props[parcelField];
    if (rawPid === undefined || rawPid === null) continue;
    const pid = String(rawPid);
    if (!targetSet.has(pid)) continue;

    matchedFeat++;

    const siteAddr = siteAddrField ? props[siteAddrField] : "";
    const parsed = parseSiteAddr(siteAddr);
    const candidate = {
      parcel_id: pid,
      site_addr: isBlank(siteAddr) ? "" : String(siteAddr),
      street_no: parsed.street_no,
      street_name: parsed.street_name,
      zip: zipField ? normZip(props[zipField]) : "",
      city: cityField && !isBlank(props[cityField]) ? String(props[cityField]).trim() : "",
      evidence: {
        source: "MAD:StatewideAddressPointsForGeocoding",
        parcel_field: parcelField,
        zip_field: zipField || null,
        site_addr_field: siteAddrField || null,
        feature_id_field: featureIdField || null,
        feature_id: featureIdField ? props[featureIdField] : null,
      },
    };

    const score = scoreCandidate(candidate);
    const prev = bestByPid.get(pid);
    if (!prev || score > prev._score) {
      bestByPid.set(pid, { ...candidate, _score: score });
    }
  }

  // write candidates NDJSON
  const outStream = fs.createWriteStream(OUT, { encoding: "utf8" });
  for (const v of bestByPid.values()) {
    const { _score, ...clean } = v;
    outStream.write(JSON.stringify(clean) + "\n");
  }
  outStream.end();

  const meta = {
    created_at: new Date().toISOString(),
    targets: TARGETS,
    madPoints: MAD,
    outCandidates: OUT,
    counts: {
      target_parcels: targetSet.size,
      candidates_built: bestByPid.size,
      mad_features_total_scanned_estimate: totalFeat, // sample only; true scan is full file
      mad_features_matched: matchedFeat,
    },
    fields_used: { parcelField, zipField: zipField || null, cityField: cityField || null, siteAddrField: siteAddrField || null },
  };

  if (META) fs.writeFileSync(META, JSON.stringify(meta, null, 2), "utf8");
  console.log("[done]", meta);
}

main().catch((e) => {
  console.error("❌ addressAuthority_buildMadCandidates failed:", e);
  process.exit(1);
});