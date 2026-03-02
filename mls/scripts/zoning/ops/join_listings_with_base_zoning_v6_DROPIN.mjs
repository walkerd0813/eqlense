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

function firstNonEmptyRaw(...vals) {
  for (const v of vals) {
    if (v === undefined || v === null) continue;
    if (typeof v === "string" && v.trim()) return v;
  }
  return null;
}

function extractStringFromMaybeObject(v) {
  if (v === undefined || v === null) return "";
  if (typeof v === "string") return v.trim();
  if (Array.isArray(v)) {
    for (const item of v) {
      const got = extractStringFromMaybeObject(item);
      if (got) return got;
    }
    return "";
  }
  if (typeof v === "object") {
    const keys = ["property_uid","propertyUid","property_id","propertyId","id","value","pid","parcel_id","parcelId","map_par_id","MAP_PAR_ID"];
    for (const k of keys) {
      if (typeof v[k] === "string" && v[k].trim()) return v[k].trim();
    }
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
  const direct = firstNonEmptyRaw(
    listing.property_uid,
    listing.propertyUid,
    listing.propertyUID,
    getDeep(listing, ["link", "property_uid"]),
    getDeep(listing, ["link", "propertyUid"]),
    getDeep(listing, ["link", "propertyUID"])
  );
  const parsed = parsePropertyUidMaybe(direct);
  if (parsed) return parsed;
  return "";
}

function deriveJurisdictionName(listing) {
  return (
    extractStringFromMaybeObject(firstNonEmptyRaw(
      listing.jurisdiction_name,
      listing.jurisdiction,
      getDeep(listing, ["link","jurisdiction_key"]),
      getDeep(listing, ["link","jurisdiction_name"]),
      getDeep(listing, ["address","town"]),
      getDeep(listing, ["address","city"]),
      getDeep(listing, ["physical","town"]),
      getDeep(listing, ["physical","city"]),
      getDeep(listing, ["raw","TOWN"]),
      getDeep(listing, ["raw","town"]),
      getDeep(listing, ["raw","CITY"]),
      getDeep(listing, ["raw","City"])
    )) || ""
  );
}

function normalizePropertyId(s) {
  const raw = String(s || "").trim();
  if (!raw) return "";
  if (/^ma:parcel:/i.test(raw)) return raw.toLowerCase().startsWith("ma:parcel:") ? raw : raw.replace(/^MA:/, "ma:");
  if (/^[0-9A-Za-z\-]+$/.test(raw)) return `ma:parcel:${raw}`;
  return raw;
}

function extractParcelIdFromPropertyId(pid) {
  const s = String(pid || "").trim();
  if (!s) return "";
  const m = s.match(/^ma:parcel:(.+)$/i);
  if (m && m[1]) return String(m[1]).trim();
  return "";
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

  const byUid = new Map();         // property_uid -> best record
  const byPropertyId = new Map();  // property_id -> array (collisions expected)

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
      const pid = normalizePropertyId(o.property_id);

      if (uid) {
        if (byUid.has(uid)) {
          dupUid++;
          byUid.set(uid, betterPick(byUid.get(uid), o));
        } else {
          byUid.set(uid, o);
        }
      }

      if (pid) {
        const cur = byPropertyId.get(pid);
        if (!cur) byPropertyId.set(pid, [o]);
        else { cur.push(o); dupPid++; pidMulti++; }
      }
    }
  }

  return { byUid, byPropertyId, files: files.length, lines, parseErr, dupUid, dupPid, pidMulti };
}

function attachDefaults(o, asOf) {
  if (!("property_uid" in o)) o.property_uid = "";
  if (!("base_district_code" in o)) o.base_district_code = "";
  if (!("base_district_name" in o)) o.base_district_name = "";
  if (!("base_zone_attach_method" in o)) o.base_zone_attach_method = "unknown";
  if (!("base_zone_confidence" in o)) o.base_zone_confidence = 0;
  if (!("base_zone_evidence" in o)) o.base_zone_evidence = null;
  if (!("as_of" in o)) o.as_of = asOf || null;
  if (!("base_zone_join_key" in o)) o.base_zone_join_key = "none"; // uid | computed_uid | property_id | ambiguous | none
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

  if (!infile || !outfile) throw new Error("Usage: --in <listings.ndjson> --out <out.ndjson> [--report <report.json>] [--logEvery N]");

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
  console.log("[START] Join MLS listings with Base Zoning (UID computed) v6");
  console.log("[INFO ] listings in : " + path.resolve(infile));
  console.log("[INFO ] listings out: " + path.resolve(outfile));
  console.log("[INFO ] uidIndexDir  : " + path.resolve(indexDir));
  console.log("[INFO ] as_of        : " + (asOf || "(null)"));
  console.log("====================================================");

  const idx = await loadUidIndex(indexDir);
  console.log(`[INDEX] byUid=${idx.byUid.size.toLocaleString()} propertyIdKeys=${idx.byPropertyId.size.toLocaleString()} bucketFiles=${idx.files} linesRead=${idx.lines.toLocaleString()} parseErr=${idx.parseErr} dupUidMerged=${idx.dupUid.toLocaleString()} pidMultiLines=${idx.pidMulti.toLocaleString()}`);

  fs.mkdirSync(path.dirname(outfile), { recursive: true });
  const out = fs.createWriteStream(outfile, { flags: "w", encoding: "utf8" });

  const rl = readline.createInterface({
    input: fs.createReadStream(infile, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  let lines = 0, found = 0, notFound = 0, missingKey = 0, parseErr = 0, ambiguous = 0;
  let joinByUid = 0, joinByComputedUid = 0, joinByPropertyId = 0;

  for await (const line of rl) {
    if (!line) continue;
    lines++;

    const o = safeJsonParse(line);
    if (!o) { parseErr++; continue; }

    attachDefaults(o, asOf);

    // A) direct UID if present
    const uidDirect = extractListingPropertyUid(o);
    if (uidDirect) o.property_uid = uidDirect;

    if (o.property_uid) {
      const z = idx.byUid.get(o.property_uid);
      if (z) {
        applyZoning(o, z, "uid", asOf);
        found++; joinByUid++;
        out.write(JSON.stringify(o) + "\n");
        if (logEvery && lines % logEvery === 0) console.log(`[PROG] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingKey=${missingKey.toLocaleString()} ambiguous=${ambiguous.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
        continue;
      }
    }

    // B) compute UID = <jurisdiction>|<parcel_id> (SAFE resolution)
    const pidRaw = extractStringFromMaybeObject(firstNonEmptyRaw(o.property_id, getDeep(o, ["link","property_id"])));
    const pidNorm = normalizePropertyId(pidRaw);

    const parcelId =
      extractStringFromMaybeObject(firstNonEmptyRaw(getDeep(o, ["link","parcel_id"]), getDeep(o, ["link","parcelId"]))) ||
      extractParcelIdFromPropertyId(pidNorm);

    const jName = deriveJurisdictionName(o);
    const jKey = normJurisdictionKey(jName);

    if (parcelId && jKey) {
      const computedUid = `${jKey}|${parcelId}`;
      o.property_uid = o.property_uid || computedUid;

      const z2 = idx.byUid.get(computedUid);
      if (z2) {
        applyZoning(o, z2, "computed_uid", asOf);
        found++; joinByComputedUid++;
        out.write(JSON.stringify(o) + "\n");
        if (logEvery && lines % logEvery === 0) console.log(`[PROG] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingKey=${missingKey.toLocaleString()} ambiguous=${ambiguous.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
        continue;
      }
    }

    // C) property_id fallback (ambiguous allowed, but no guessing)
    if (pidNorm) {
      const arr = idx.byPropertyId.get(pidNorm) || null;
      if (!arr || arr.length === 0) {
        notFound++;
      } else if (arr.length === 1) {
        applyZoning(o, arr[0], "property_id", asOf);
        found++; joinByPropertyId++;
      } else {
        // still ambiguous — do NOT guess
        ambiguous++;
        o.base_zone_join_key = "ambiguous";
        notFound++;
      }
      out.write(JSON.stringify(o) + "\n");
      if (logEvery && lines % logEvery === 0) console.log(`[PROG] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingKey=${missingKey.toLocaleString()} ambiguous=${ambiguous.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
      continue;
    }

    // D) no usable keys
    missingKey++;
    notFound++;
    out.write(JSON.stringify(o) + "\n");
    if (logEvery && lines % logEvery === 0) console.log(`[PROG] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingKey=${missingKey.toLocaleString()} ambiguous=${ambiguous.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
  }

  out.end();

  const report = {
    created_at: new Date().toISOString(),
    infile: path.resolve(infile),
    outfile: path.resolve(outfile),
    uid_index_dir: path.resolve(indexDir),
    as_of: asOf,
    join: {
      lines,
      found,
      notFound,
      missingKey,
      ambiguous,
      joinByUid,
      joinByComputedUid,
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
  console.log(`[DONE] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingKey=${missingKey.toLocaleString()} ambiguous=${ambiguous.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
  if (reportPath) console.log("[DONE] report -> " + path.resolve(reportPath));
  console.log("====================================================");
}

main().catch(err => {
  console.error("[FAIL]", err?.stack || err);
  process.exit(1);
});
