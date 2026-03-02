#!/usr/bin/env node
/**
 * backfillAddressFieldsFromParcelsNDJSON_v1_DROPIN.js
 *
 * Institutional address cleanup (NO guessing):
 * - Reads properties NDJSON
 * - Finds rows missing address essentials (street_no/street_name/zip/full_address/city)
 * - Collects their parcel_ids
 * - Scans parcels.ndjson ONCE and builds a small lookup ONLY for needed parcel_ids
 * - Second pass over properties: fills blanks only, logs lineage fields
 * - Writes:
 *    1) out NDJSON
 *    2) meta JSON (counts + hashes optional)
 *    3) unrecoverable NDJSON (still missing essentials after backfill)
 *
 * ESM-only. Streaming-safe.
 */

import fs from "fs";
import path from "path";
import crypto from "crypto";
import readline from "readline";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1];
      if (!v || v.startsWith("--")) out[k] = true;
      else {
        out[k] = v;
        i++;
      }
    }
  }
  return out;
}

function nowISO() {
  return new Date().toISOString();
}

function die(msg) {
  console.error(`\n❌ ${msg}\n`);
  process.exit(1);
}

function exists(fp) {
  return fs.existsSync(fp);
}

function normStr(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s.length ? s : null;
}

function isBlank(v) {
  const s = normStr(v);
  return !s;
}

function isStreetNoMissing(v) {
  const s = normStr(v);
  if (!s) return true;
  if (s === "0" || s === "00" || s === "000") return true;
  return false;
}

function isZipMissing(v) {
  const s = normStr(v);
  if (!s) return true;
  const digits = s.replace(/\D/g, "");
  return digits.length < 5;
}

function sha256File(fp) {
  return new Promise((resolve, reject) => {
    const h = crypto.createHash("sha256");
    const rs = fs.createReadStream(fp);
    rs.on("error", reject);
    rs.on("data", (buf) => h.update(buf));
    rs.on("end", () => resolve(h.digest("hex")));
  });
}

function getRowPid(row) {
  return normStr(row.parcel_id ?? row.parcelId ?? row.MAP_PAR_ID ?? row.LOC_ID);
}

// Pull parcel attributes robustly (field name variations)
function pickFirst(props, keys) {
  for (const k of keys) {
    const v = props?.[k];
    const s = normStr(v);
    if (s) return { value: s, key: k };
  }
  return { value: null, key: null };
}

function extractParcelAttrs(parcelObj) {
  const props = parcelObj?.properties ?? parcelObj ?? {};
  const idMap = pickFirst(props, ["MAP_PAR_ID", "map_par_id", "PARCEL_ID", "parcel_id"]);
  const idLoc = pickFirst(props, ["LOC_ID", "loc_id"]);
  const addrNum = pickFirst(props, ["ADDR_NUM", "addr_num", "ADDRNUM", "ST_NUM", "HOUSE_NUM", "ADDRESS_NUM"]);
  const siteAddr = pickFirst(props, ["SITE_ADDR", "site_addr", "SITEADDR", "ADDRESS", "SITEADDRESS"]);
  const fullStr = pickFirst(props, ["FULL_STR", "full_str", "FULLADDR", "FULL_ADDRESS", "FULLADDRS"]);
  const city = pickFirst(props, ["CITY", "city", "TOWN", "town", "MUNICIPALITY"]);
  const zip = pickFirst(props, ["ZIP", "zip", "ZIPCODE", "ZipCode", "POSTCODE"]);

  return {
    MAP_PAR_ID: idMap.value,
    LOC_ID: idLoc.value,
    fields: {
      addrNum,
      siteAddr,
      fullStr,
      city,
      zip,
    },
  };
}

// Deterministic parse of "123 MAIN ST ..." (no guessing)
// Returns { streetNo, streetName, unitMaybe } with best-effort stripping.
function parseSiteAddr(line) {
  const raw = normStr(line);
  if (!raw) return null;

  // Remove comma tail parts if present
  const head = raw.split(",")[0].trim();

  // Match leading number
  const m = head.match(/^(\d+)\s+(.*)$/);
  if (!m) return null;

  const streetNo = m[1].trim();
  let rest = m[2].trim().replace(/\s+/g, " ");

  // If rest contains unit markers, keep street name portion before them (deterministic cut)
  // (We do NOT invent unit; we only avoid polluting street_name.)
  const unitMarkers = [" APT ", " UNIT ", " #", " FL ", " FLOOR ", " STE ", " SUITE "];
  for (const marker of unitMarkers) {
    const idx = rest.toUpperCase().indexOf(marker.trim());
    if (idx > 0 && marker.trim() !== "#") {
      rest = rest.slice(0, idx).trim();
      break;
    }
  }

  // Remove trailing parentheses noise deterministically
  rest = rest.replace(/\s*\(.*?\)\s*$/g, "").trim();

  return {
    streetNo,
    streetName: rest.length ? rest : null,
  };
}

function needsBackfill(row) {
  const reasons = [];
  const streetNo = row.street_no ?? row.streetNo ?? row.addr_num ?? row.ADDR_NUM;
  const streetName = row.street_name ?? row.streetName ?? row.street ?? row.STREET;
  const fullAddr = row.full_address ?? row.fullAddress ?? row.address ?? row.SITE_ADDR;
  const zip = row.zip ?? row.ZIP ?? row.zip_code ?? row.zipCode;
  const city = row.city ?? row.town ?? row.CITY ?? row.TOWN;

  if (isStreetNoMissing(streetNo)) reasons.push("MISSING_STREET_NO");
  if (isBlank(streetName)) reasons.push("MISSING_STREET_NAME");
  if (isBlank(fullAddr)) reasons.push("MISSING_FULL_ADDRESS");
  if (isZipMissing(zip)) reasons.push("MISSING_ZIP");
  if (isBlank(city)) reasons.push("MISSING_CITY_TOWN");

  return { needs: reasons.length > 0, reasons };
}

async function collectNeededParcelIds(inPropertiesPath) {
  const needed = new Set();
  const counts = {
    total_rows: 0,
    candidate_rows: 0,
    already_complete_rows: 0,
    reasonBuckets: {},
    missingParcelId_on_candidate: 0,
  };

  const rl = readline.createInterface({
    input: fs.createReadStream(inPropertiesPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    counts.total_rows++;

    let row;
    try {
      row = JSON.parse(t);
    } catch {
      continue;
    }

    const chk = needsBackfill(row);
    if (!chk.needs) {
      counts.already_complete_rows++;
      continue;
    }

    counts.candidate_rows++;
    for (const r of chk.reasons) {
      counts.reasonBuckets[r] = (counts.reasonBuckets[r] ?? 0) + 1;
    }

    const pid = getRowPid(row);
    if (!pid) {
      counts.missingParcelId_on_candidate++;
      continue;
    }
    needed.add(pid);
  }

  return { needed, counts };
}

async function buildParcelLookup(parcelsNdjsonPath, neededSet, pidMode = "any") {
  const byId = new Map();
  const stats = {
    parcels_seen: 0,
    parcels_indexed: 0,
    hits_on_needed: 0,
  };

  const rl = readline.createInterface({
    input: fs.createReadStream(parcelsNdjsonPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    stats.parcels_seen++;

    let obj;
    try {
      obj = JSON.parse(t);
    } catch {
      continue;
    }

    const ex = extractParcelAttrs(obj);
    const idMap = ex.MAP_PAR_ID;
    const idLoc = ex.LOC_ID;

    const candidateIds = [];
    if (pidMode === "map_par_id") {
      if (idMap) candidateIds.push({ id: idMap, idField: "MAP_PAR_ID" });
    } else if (pidMode === "loc_id") {
      if (idLoc) candidateIds.push({ id: idLoc, idField: "LOC_ID" });
    } else {
      if (idMap) candidateIds.push({ id: idMap, idField: "MAP_PAR_ID" });
      if (idLoc) candidateIds.push({ id: idLoc, idField: "LOC_ID" });
    }

    let matched = false;
    for (const c of candidateIds) {
      if (neededSet.has(c.id)) {
        matched = true;
        byId.set(c.id, {
          idField: c.idField,
          idValue: c.id,
          fields: ex.fields,
        });
      }
    }

    if (matched) {
      stats.hits_on_needed++;
      stats.parcels_indexed = byId.size;
    }
  }

  return { byId, stats };
}

function fillIfBlank(row, key, newVal) {
  const current = row[key];
  if (isBlank(current) && !isBlank(newVal)) {
    row[key] = newVal;
    return true;
  }
  return false;
}

function fillStreetNoIfMissing(row, newVal) {
  const current = row.street_no ?? row.streetNo;
  if (isStreetNoMissing(current) && !isBlank(newVal)) {
    row.street_no = newVal;
    return true;
  }
  return false;
}

function fillZipIfMissing(row, newVal) {
  const current = row.zip ?? row.ZIP ?? row.zip_code ?? row.zipCode;
  if (isZipMissing(current) && !isBlank(newVal)) {
    row.zip = newVal;
    return true;
  }
  return false;
}

async function main() {
  const args = parseArgs(process.argv);

  const inPath = args.in;
  const parcelsPath = args.parcels;
  const outPath = args.out;
  const metaPath = args.meta;
  const unrecoverablePath = args.unrecoverable;
  const pidMode = args.pidMode ?? "any";
  const skipHashes = !!args.skipHashes;

  if (!inPath || !parcelsPath || !outPath || !metaPath || !unrecoverablePath) {
    console.log(`
Usage:
  node mls/scripts/backfillAddressFieldsFromParcelsNDJSON_v1_DROPIN.js \\
    --in <properties.ndjson> \\
    --parcels <parcels.ndjson> \\
    --out <properties_out.ndjson> \\
    --meta <meta.json> \\
    --unrecoverable <unrecoverable.ndjson> \\
    [--pidMode any|map_par_id|loc_id] \\
    [--skipHashes]
`);
    process.exit(1);
  }

  if (!exists(inPath)) die(`--in not found: ${inPath}`);
  if (!exists(parcelsPath)) die(`--parcels not found: ${parcelsPath}`);

  const inAbs = path.resolve(inPath);
  const outAbs = path.resolve(outPath);
  if (inAbs === outAbs) die("--out must differ from --in");

  console.log("====================================================");
  console.log(" ADDRESS BACKFILL FROM PARCEL ATTRS (P3) — v1 DROPIN");
  console.log("====================================================");
  console.log(`[run] started_at: ${nowISO()}`);
  console.log(`[run] node:       ${process.version}`);
  console.log(`[run] pidMode:    ${pidMode}`);
  console.log(`[run] in:         ${inPath}`);
  console.log(`[run] parcels:    ${parcelsPath}`);
  console.log(`[run] out:        ${outPath}`);
  console.log(`[run] meta:       ${metaPath}`);
  console.log(`[run] unrecover:  ${unrecoverablePath}`);
  console.log("----------------------------------------------------");

  const hashes = skipHashes
    ? { in_sha256: null, parcels_sha256: null }
    : {
        in_sha256: await sha256File(inPath),
        parcels_sha256: await sha256File(parcelsPath),
      };

  // Pass 1: find candidates + needed parcel_ids
  console.log("[pass1] scanning properties to collect needed parcel_ids...");
  const { needed, counts: pass1 } = await collectNeededParcelIds(inPath);
  console.log(`[pass1] total_rows:       ${pass1.total_rows.toLocaleString()}`);
  console.log(`[pass1] candidate_rows:   ${pass1.candidate_rows.toLocaleString()}`);
  console.log(`[pass1] needed parcel_ids:${needed.size.toLocaleString()}`);
  console.log(`[pass1] missingParcelId on candidates: ${pass1.missingParcelId_on_candidate.toLocaleString()}`);

  // Pass 2: build parcel lookup only for needed IDs
  console.log("[pass2] scanning parcels.ndjson to build minimal lookup...");
  const { byId, stats: pass2 } = await buildParcelLookup(parcelsPath, needed, pidMode);
  console.log(`[pass2] parcels_seen:     ${pass2.parcels_seen.toLocaleString()}`);
  console.log(`[pass2] hits_on_needed:   ${pass2.hits_on_needed.toLocaleString()}`);
  console.log(`[pass2] lookup_size:      ${byId.size.toLocaleString()}`);

  // Pass 3: write output + unrecoverables
  const meta = {
    script: "backfillAddressFieldsFromParcelsNDJSON_v1_DROPIN.js",
    started_at: nowISO(),
    args: { in: inPath, parcels: parcelsPath, out: outPath, meta: metaPath, unrecoverable: unrecoverablePath, pidMode, skipHashes },
    hashes,
    pass1,
    pass2,
    counts: {
      total_rows: 0,
      candidate_rows: 0,
      parcel_match_found: 0,
      patched_rows: 0,
      filled: {
        street_no: 0,
        street_name: 0,
        full_address: 0,
        zip: 0,
        city_town: 0,
      },
      still_missing_essentials: 0,
      unrecoverable_written: 0,
    },
  };

  const outWS = fs.createWriteStream(outPath, { encoding: "utf8" });
  const unrWS = fs.createWriteStream(unrecoverablePath, { encoding: "utf8" });

  const rl = readline.createInterface({
    input: fs.createReadStream(inPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  console.log("[pass3] writing output + unrecoverables...");

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    meta.counts.total_rows++;

    let row;
    try {
      row = JSON.parse(t);
    } catch {
      continue;
    }

    const chkBefore = needsBackfill(row);
    if (!chkBefore.needs) {
      outWS.write(JSON.stringify(row) + "\n");
      continue;
    }

    meta.counts.candidate_rows++;

    const pid = getRowPid(row);
    if (!pid) {
      // No parcel_id to backfill from → unrecoverable candidate (for address fields)
      row.address_backfill_status = "NO_PARCEL_ID";
      row.address_backfill_at = nowISO();
      row.address_backfill_reasons = chkBefore.reasons;
      meta.counts.still_missing_essentials++;
      meta.counts.unrecoverable_written++;
      unrWS.write(JSON.stringify(row) + "\n");
      outWS.write(JSON.stringify(row) + "\n");
      continue;
    }

    const hit = byId.get(pid);
    if (!hit) {
      row.address_backfill_status = "PARCEL_NOT_FOUND";
      row.address_backfill_at = nowISO();
      row.address_backfill_reasons = chkBefore.reasons;
      meta.counts.still_missing_essentials++;
      meta.counts.unrecoverable_written++;
      unrWS.write(JSON.stringify(row) + "\n");
      outWS.write(JSON.stringify(row) + "\n");
      continue;
    }

    meta.counts.parcel_match_found++;

    const usedFields = [];
    const fillMap = {};

    const addrNum = hit.fields.addrNum.value;
    const siteAddr = hit.fields.siteAddr.value;
    const fullStr = hit.fields.fullStr.value;
    const city = hit.fields.city.value;
    const zip = hit.fields.zip.value;

    if (hit.fields.addrNum.key) usedFields.push(hit.fields.addrNum.key);
    if (hit.fields.siteAddr.key) usedFields.push(hit.fields.siteAddr.key);
    if (hit.fields.fullStr.key) usedFields.push(hit.fields.fullStr.key);
    if (hit.fields.city.key) usedFields.push(hit.fields.city.key);
    if (hit.fields.zip.key) usedFields.push(hit.fields.zip.key);

    // Parse SITE_ADDR if needed
    const parsed = parseSiteAddr(siteAddr);

    let didPatch = false;

    // street_no
    const streetNoCandidate = addrNum ?? parsed?.streetNo ?? null;
    if (fillStreetNoIfMissing(row, streetNoCandidate)) {
      meta.counts.filled.street_no++;
      fillMap.street_no = addrNum ? hit.fields.addrNum.key : "SITE_ADDR_PARSE";
      didPatch = true;
    }

    // street_name
    const streetNameCandidate = parsed?.streetName ?? null;
    const currentStreetName = row.street_name ?? row.streetName ?? row.street ?? null;
    if (isBlank(currentStreetName) && !isBlank(streetNameCandidate)) {
      row.street_name = streetNameCandidate;
      meta.counts.filled.street_name++;
      fillMap.street_name = "SITE_ADDR_PARSE";
      didPatch = true;
    }

    // city/town (fill only if missing)
    const currentCity = row.city ?? row.town ?? row.CITY ?? row.TOWN ?? null;
    if (isBlank(currentCity) && !isBlank(city)) {
      row.city = city;
      meta.counts.filled.city_town++;
      fillMap.city = hit.fields.city.key;
      didPatch = true;
    }

    // zip
    if (fillZipIfMissing(row, zip)) {
      meta.counts.filled.zip++;
      fillMap.zip = hit.fields.zip.key;
      didPatch = true;
    }

    // full_address
    const currentFull = row.full_address ?? row.fullAddress ?? row.address ?? null;
    const fullCandidate = fullStr ?? siteAddr ?? null;
    if (isBlank(currentFull) && !isBlank(fullCandidate)) {
      row.full_address = fullCandidate;
      meta.counts.filled.full_address++;
      fillMap.full_address = fullStr ? hit.fields.fullStr.key : hit.fields.siteAddr.key;
      didPatch = true;
    }

    if (didPatch) {
      meta.counts.patched_rows++;
      row.address_source = "parcels.ndjson";
      row.address_method = "parcel_id_join_fill_blanks_only";
      row.address_method_version = "v1_DROPIN";
      row.address_pid_used = pid;
      row.address_pid_field_used = hit.idField;
      row.address_fields_used = Array.from(new Set(usedFields)).filter(Boolean);
      row.address_fill_map = fillMap;
      row.address_patched_at = nowISO();
    }

    // Check after fill
    const chkAfter = needsBackfill(row);
    if (chkAfter.needs) {
      row.address_backfill_status = row.address_backfill_status ?? "STILL_MISSING_AFTER_BACKFILL";
      row.address_backfill_reasons = chkAfter.reasons;
      meta.counts.still_missing_essentials++;
      meta.counts.unrecoverable_written++;
      unrWS.write(JSON.stringify(row) + "\n");
    }

    outWS.write(JSON.stringify(row) + "\n");
  }

  outWS.end();
  unrWS.end();

  meta.finished_at = nowISO();

  fs.writeFileSync(metaPath, JSON.stringify(meta, null, 2), "utf8");

  console.log("====================================================");
  console.log("DONE — ADDRESS BACKFILL (P3)");
  console.log("----------------------------------------------------");
  console.log(`total_rows:               ${meta.counts.total_rows.toLocaleString()}`);
  console.log(`candidate_rows:           ${meta.counts.candidate_rows.toLocaleString()}`);
  console.log(`parcel_match_found:       ${meta.counts.parcel_match_found.toLocaleString()}`);
  console.log(`patched_rows:             ${meta.counts.patched_rows.toLocaleString()}`);
  console.log(`filled.street_no:         ${meta.counts.filled.street_no.toLocaleString()}`);
  console.log(`filled.street_name:       ${meta.counts.filled.street_name.toLocaleString()}`);
  console.log(`filled.full_address:      ${meta.counts.filled.full_address.toLocaleString()}`);
  console.log(`filled.zip:               ${meta.counts.filled.zip.toLocaleString()}`);
  console.log(`filled.city_town:         ${meta.counts.filled.city_town.toLocaleString()}`);
  console.log(`still_missing_essentials: ${meta.counts.still_missing_essentials.toLocaleString()}`);
  console.log(`unrecoverable_written:    ${meta.counts.unrecoverable_written.toLocaleString()}`);
  console.log("----------------------------------------------------");
  console.log(`out:          ${outPath}`);
  console.log(`meta:         ${metaPath}`);
  console.log(`unrecoverable:${unrecoverablePath}`);
  console.log("====================================================");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
