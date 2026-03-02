import fs from "fs";
import path from "path";
import readline from "readline";
import crypto from "crypto";

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
    args[k] = v;
  }
  return args;
}

function stripBom(s) {
  if (!s) return s;
  return String(s).replace(/^\uFEFF/, "");
}

function safeJsonParse(line) {
  try { return JSON.parse(line); } catch { return null; }
}

function sha256hex(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}

function normJurisdictionKey(s) {
  if (!s) return "";
  return String(s)
    .trim()
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function getDeep(o, pathArr) {
  let cur = o;
  for (const k of pathArr) {
    if (!cur || typeof cur !== "object") return undefined;
    cur = cur[k];
  }
  return cur;
}

function firstNonEmpty(...vals) {
  for (const v of vals) {
    if (v === undefined || v === null) continue;
    const s = String(v).trim();
    if (s.length) return s;
  }
  return "";
}

function parsePropertyUidMaybe(val) {
  const s = String(val || "").trim();
  if (!s.includes("|")) return "";
  const [left, right] = s.split("|");
  const j = normJurisdictionKey(left);
  const pid = String(right || "").trim();
  if (!j || !pid) return "";
  return `${j}|${pid}`;
}

function extractListingPropertyUid(listing) {
  // direct fields
  const direct =
    firstNonEmpty(
      listing.property_uid,
      listing.propertyUid,
      listing.propertyUID,
      getDeep(listing, ["link", "property_uid"]),
      getDeep(listing, ["link", "propertyUid"]),
      getDeep(listing, ["link", "propertyUID"])
    );
  const parsed = parsePropertyUidMaybe(direct);
  if (parsed) return parsed;

  // sometimes property_id is actually property_uid (rare)
  const pid = firstNonEmpty(listing.property_id, getDeep(listing, ["link", "property_id"]));
  const parsed2 = parsePropertyUidMaybe(pid);
  if (parsed2) return parsed2;

  return "";
}

function deriveJurisdictionName(listing) {
  // best-effort only (used for disambiguation if property_id collides)
  return firstNonEmpty(
    listing.jurisdiction_name,
    listing.jurisdiction,
    getDeep(listing, ["link", "jurisdiction_key"]),
    getDeep(listing, ["link", "jurisdiction_name"]),
    getDeep(listing, ["link", "jurisdiction"]),
    getDeep(listing, ["address", "town"]),
    getDeep(listing, ["address", "city"]),
    getDeep(listing, ["physical", "town"]),
    getDeep(listing, ["physical", "city"]),
    getDeep(listing, ["raw", "TOWN"]),
    getDeep(listing, ["raw", "town"]),
    getDeep(listing, ["raw", "CITY"]),
    getDeep(listing, ["raw", "City"])
  );
}

function betterPick(a, b) {
  const aConf = Number(a?.base_zone_confidence || 0);
  const bConf = Number(b?.base_zone_confidence || 0);
  if (bConf !== aConf) return bConf > aConf ? b : a;

  const aHasCode = !!a?.base_district_code;
  const bHasCode = !!b?.base_district_code;
  if (bHasCode !== aHasCode) return bHasCode ? b : a;

  const aHasName = !!a?.base_district_name;
  const bHasName = !!b?.base_district_name;
  if (bHasName !== aHasName) return bHasName ? b : a;

  const aMeth = String(a?.base_zone_attach_method || "");
  const bMeth = String(b?.base_zone_attach_method || "");
  if (aMeth !== bMeth) {
    if (bMeth === "point_in_poly") return b;
    if (aMeth === "point_in_poly") return a;
  }
  return a;
}

async function loadUidIndex(indexDir) {
  const bucketsDir = path.join(indexDir, "buckets");
  const files = fs.readdirSync(bucketsDir).filter(f => f.toLowerCase().endsWith(".ndjson"));

  const byUid = new Map();          // property_uid -> record
  const byPropertyId = new Map();   // property_id  -> record OR array of records

  let parseErr = 0, lines = 0, dupUid = 0, dupPid = 0, pidMulti = 0;

  for (const f of files) {
    const full = path.join(bucketsDir, f);
    const rl = readline.createInterface({
      input: fs.createReadStream(full, { encoding: "utf8" }),
      crlfDelay: Infinity
    });

    for await (const line of rl) {
      if (!line) continue;
      lines++;
      const o = safeJsonParse(line);
      if (!o) { parseErr++; continue; }

      const uid = o.property_uid;
      const pid = o.property_id;

      // byUid (merge duplicates safely)
      if (uid) {
        if (byUid.has(uid)) {
          dupUid++;
          byUid.set(uid, betterPick(byUid.get(uid), o));
        } else {
          byUid.set(uid, o);
        }
      }

      // byPropertyId (may collide across towns)
      if (pid) {
        const cur = byPropertyId.get(pid);
        if (!cur) {
          byPropertyId.set(pid, o);
        } else {
          dupPid++;
          if (Array.isArray(cur)) {
            cur.push(o);
            pidMulti++;
          } else {
            byPropertyId.set(pid, [cur, o]);
            pidMulti++;
          }
        }
      }
    }
  }

  return { byUid, byPropertyId, files: files.length, lines, parseErr, dupUid, dupPid, pidMulti };
}

function attachDefaults(o, asOf) {
  // Stable schema for downstream consumers (UNKNOWN is explicit)
  if (!("property_uid" in o)) o.property_uid = "";
  if (!("base_district_code" in o)) o.base_district_code = "";
  if (!("base_district_name" in o)) o.base_district_name = "";
  if (!("base_zone_attach_method" in o)) o.base_zone_attach_method = "unknown";
  if (!("base_zone_confidence" in o)) o.base_zone_confidence = 0;
  if (!("base_zone_evidence" in o)) o.base_zone_evidence = null;
  if (!("as_of" in o)) o.as_of = asOf || null;
  if (!("base_zone_join_key" in o)) o.base_zone_join_key = "none"; // uid | property_id | derived | ambiguous | none
}

function applyZoning(o, z, joinKey, asOf) {
  o.property_uid = z.property_uid || o.property_uid || "";
  o.base_district_code = z.base_district_code || "";
  o.base_district_name = z.base_district_name || "";
  o.base_zone_attach_method = z.base_zone_attach_method || "unknown";
  o.base_zone_confidence = Number(z.base_zone_confidence || 0);
  o.base_zone_evidence = z.base_zone_evidence || null;
  o.as_of = z.as_of || asOf || null;
  o.base_zone_join_key = joinKey;
}

async function main() {
  const args = parseArgs(process.argv);
  const backendRoot = process.cwd();

  const infile = args.in;
  const outfile = args.out;
  const reportPath = args.report || null;
  const logEvery = Number(args.logEvery || 100000);

  if (!infile || !outfile) {
    throw new Error("Usage: --in <listings.ndjson> --out <out.ndjson> [--report <report.json>] [--logEvery N]");
  }

  let indexDir = args.indexDir;
  if (!indexDir) {
    const ptr = path.join(backendRoot, "publicData", "properties", "_frozen", "CURRENT_BASE_ZONING_INDEX_UID.txt");
    if (!fs.existsSync(ptr)) throw new Error("Missing UID index pointer: " + ptr);
    indexDir = stripBom(fs.readFileSync(ptr, "utf8")).trim();
  }

  const manifestPath = path.join(indexDir, "INDEX_MANIFEST.json");
  const indexManifest = fs.existsSync(manifestPath) ? JSON.parse(stripBom(fs.readFileSync(manifestPath, "utf8"))) : null;
  const asOf = indexManifest?.as_of || indexManifest?.asOf || null;

  console.log("====================================================");
  console.log("[START] Join MLS listings with Base Zoning (UID index + property_id fallback) v4");
  console.log("[INFO ] listings in : " + path.resolve(infile));
  console.log("[INFO ] listings out: " + path.resolve(outfile));
  console.log("[INFO ] uidIndexDir  : " + path.resolve(indexDir));
  console.log("[INFO ] as_of        : " + (asOf || "(null)"));
  console.log("====================================================");

  const idx = await loadUidIndex(indexDir);
  console.log(`[INDEX] byUid=${idx.byUid.size.toLocaleString()} byPropertyIdKeys=${idx.byPropertyId.size.toLocaleString()} bucketFiles=${idx.files} linesRead=${idx.lines.toLocaleString()} parseErr=${idx.parseErr} dupUidMerged=${idx.dupUid.toLocaleString()} dupPid=${idx.dupPid.toLocaleString()} pidMulti=${idx.pidMulti.toLocaleString()}`);

  fs.mkdirSync(path.dirname(outfile), { recursive: true });
  const out = fs.createWriteStream(outfile, { flags: "w", encoding: "utf8" });

  const rl = readline.createInterface({
    input: fs.createReadStream(infile, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  let lines = 0, found = 0, notFound = 0, missingPropId = 0, parseErr = 0, ambiguous = 0;
  let joinByUid = 0, joinByPropertyId = 0, joinAmbiguousResolved = 0;

  for await (const line of rl) {
    if (!line) continue;
    lines++;

    const o = safeJsonParse(line);
    if (!o) { parseErr++; continue; }

    attachDefaults(o, asOf);

    // 1) If listing already has property_uid, use it
    const uidDirect = extractListingPropertyUid(o);
    if (uidDirect) o.property_uid = uidDirect;

    let z = null;

    if (o.property_uid) {
      z = idx.byUid.get(o.property_uid) || null;
      if (z) {
        applyZoning(o, z, "uid", asOf);
        found++; joinByUid++;
        out.write(JSON.stringify(o) + "\n");
        if (logEvery && lines % logEvery === 0) {
          console.log(`[PROG] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingPropId=${missingPropId.toLocaleString()} ambiguous=${ambiguous.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
        }
        continue;
      }
    }

    // 2) Otherwise, use listing.property_id (best available)
    const propertyId = firstNonEmpty(o.property_id, getDeep(o, ["link", "property_id"]));
    if (!propertyId) {
      missingPropId++;
      notFound++;
      out.write(JSON.stringify(o) + "\n");
      if (logEvery && lines % logEvery === 0) {
        console.log(`[PROG] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingPropId=${missingPropId.toLocaleString()} ambiguous=${ambiguous.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
      }
      continue;
    }

    const cand = idx.byPropertyId.get(propertyId) || idx.byPropertyId.get(String(propertyId).trim().toLowerCase()) || null;

    if (!cand) {
      notFound++;
      out.write(JSON.stringify(o) + "\n");
      if (logEvery && lines % logEvery === 0) {
        console.log(`[PROG] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingPropId=${missingPropId.toLocaleString()} ambiguous=${ambiguous.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
      }
      continue;
    }

    if (!Array.isArray(cand)) {
      // single mapping: safe
      applyZoning(o, cand, "property_id", asOf);
      found++; joinByPropertyId++;
      out.write(JSON.stringify(o) + "\n");
      if (logEvery && lines % logEvery === 0) {
        console.log(`[PROG] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingPropId=${missingPropId.toLocaleString()} ambiguous=${ambiguous.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
      }
      continue;
    }

    // 3) property_id collision: try disambiguate using jurisdiction if available
    ambiguous++;
    const jName = deriveJurisdictionName(o);
    const jKey = normJurisdictionKey(jName);
    let pick = null;

    if (jKey) {
      pick = cand.find(r => String(r.property_uid || "").startsWith(jKey + "|")) || null;
    }

    if (pick) {
      applyZoning(o, pick, "property_id", asOf);
      found++; joinByPropertyId++; joinAmbiguousResolved++;
    } else {
      // unresolved collision -> do not guess
      o.base_zone_join_key = "ambiguous";
      notFound++;
    }

    out.write(JSON.stringify(o) + "\n");

    if (logEvery && lines % logEvery === 0) {
      console.log(`[PROG] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingPropId=${missingPropId.toLocaleString()} ambiguous=${ambiguous.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
    }
  }

  out.end();

  const report = {
    created_at: new Date().toISOString(),
    infile: path.resolve(infile),
    outfile: path.resolve(outfile),
    uid_index_dir: path.resolve(indexDir),
    as_of: asOf,
    index: {
      byUid: idx.byUid.size,
      byPropertyIdKeys: idx.byPropertyId.size,
      bucketFiles: idx.files,
      linesRead: idx.lines,
      parseErr: idx.parseErr,
      dupUidMerged: idx.dupUid,
      dupPid: idx.dupPid,
      pidMulti: idx.pidMulti
    },
    join: {
      lines,
      found,
      notFound,
      missingPropId,
      ambiguousPid: ambiguous,
      ambiguousResolved: joinAmbiguousResolved,
      joinByUid,
      joinByPropertyId,
      parseErr
    }
  };

  if (reportPath) {
    fs.mkdirSync(path.dirname(reportPath), { recursive: true });
    fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
  }

  console.log("----------------------------------------------------");
  console.log("[DONE] Join complete.");
  console.log(`[DONE] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingPropId=${missingPropId.toLocaleString()} ambiguousPid=${ambiguous.toLocaleString()} ambiguousResolved=${joinAmbiguousResolved.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
  if (reportPath) console.log("[DONE] report -> " + path.resolve(reportPath));
  console.log("====================================================");
}

main().catch(err => {
  console.error("[FAIL]", err?.stack || err);
  process.exit(1);
});
