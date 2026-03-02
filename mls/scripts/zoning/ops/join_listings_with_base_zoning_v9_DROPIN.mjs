import fs from "fs";
import path from "path";
import readline from "readline";

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
function safeParse(line) { try { return JSON.parse(line); } catch { return null; } }

function cleanTownRaw(t) {
  if (!t) return "";
  let s = String(t).trim();

  // If it's "Barnstable, MA" or "Somerville, MA", keep only the city part
  if (s.includes(",")) s = s.split(",")[0].trim();

  // If it ends with " MA" (some feeds do), strip it
  s = s.replace(/\bMA\b$/i, "").trim();

  return s;
}

function normTown(t) {
  const raw = cleanTownRaw(t);
  if (!raw) return "";
  return raw
    .toUpperCase()
    .replace(/\s+/g, "_")
    .replace(/[^A-Z0-9_]/g, "");
}

function normalizePropertyId(pid) {
  if (!pid) return "";
  let s = String(pid).trim();
  s = s.replace(/^ma:parcel:/i, "ma:parcel:");
  if (!s.includes(":") && s.length > 0) s = `ma:parcel:${s}`;
  return s;
}

function parcelIdFromPropertyId(pidNorm) {
  if (!pidNorm) return "";
  const prefix = "ma:parcel:";
  if (pidNorm.startsWith(prefix)) return pidNorm.slice(prefix.length);
  return "";
}

function normalizeUid(uid) {
  if (!uid) return "";
  const s = String(uid).trim();
  const parts = s.split("|");
  if (parts.length !== 2) return "";
  return `${normTown(parts[0])}|${String(parts[1]).trim()}`;
}

function extractTown(o) {
  return (
    o?.address?.town ||
    o?.address?.city ||
    o?.address?.municipality ||
    o?.physical?.town ||
    o?.physical?.city ||
    o?.link?.town ||
    o?.link?.city ||
    o?.link?.jurisdiction_name ||
    o?.raw?.Town ||
    o?.raw?.TOWN ||
    o?.raw?.CITY ||
    ""
  );
}

function readPointer(pointerPath) {
  const p = fs.readFileSync(pointerPath, "utf8").trim();
  if (!p) throw new Error(`Empty pointer file: ${pointerPath}`);
  return p;
}

function loadUidIndex(uidIndexDir) {
  const manifestPath = path.join(uidIndexDir, "INDEX_MANIFEST.json");
  const manifestRaw = fs.readFileSync(manifestPath, "utf8").replace(/^\uFEFF/, "");
  const manifest = JSON.parse(manifestRaw);

  const bucketsDir = path.join(uidIndexDir, "buckets");
  const byUid = new Map();
  const byPid = new Map();

  let linesRead = 0, parseErr = 0, dupUidMerged = 0, pidMultiLines = 0;

  const files = fs.readdirSync(bucketsDir).filter(f => f.endsWith(".ndjson")).sort();
  for (const f of files) {
    const full = path.join(bucketsDir, f);
    const content = fs.readFileSync(full, "utf8");
    const lines = content.split(/\r?\n/);

    for (const line of lines) {
      if (!line) continue;
      linesRead++;

      const o = safeParse(line);
      if (!o) { parseErr++; continue; }

      const uid = normalizeUid(o.property_uid);
      if (uid) {
        if (byUid.has(uid)) dupUidMerged++;
        else byUid.set(uid, o);
      }

      const pid = normalizePropertyId(o.property_id);
      if (pid) {
        if (!byPid.has(pid)) byPid.set(pid, []);
        byPid.get(pid).push(o);
        if (byPid.get(pid).length > 1) pidMultiLines++;
      }
    }
  }

  return { manifest, byUid, byPid, stats: { linesRead, parseErr, dupUidMerged, pidMultiLines, bucketFiles: files.length } };
}

function attachZoning(target, z, asOf) {
  target.base_district_code = z.base_district_code ?? "";
  target.base_district_name = z.base_district_name ?? "";
  target.base_zone_attach_method = z.base_zone_attach_method ?? "";
  target.base_zone_confidence = z.base_zone_confidence ?? 0;
  target.base_zone_evidence = z.base_zone_evidence ?? null;
  target.as_of = asOf || (z.as_of ?? "");
}

async function main() {
  const args = parseArgs(process.argv);

  const infile = args.in;
  const outfile = args.out;
  const reportPath = args.report;
  const logEvery = Number(args.logEvery || 100000);

  if (!infile || !outfile || !reportPath) {
    throw new Error("Usage: --in <listings.ndjson> --out <out.ndjson> --report <report.json> [--logEvery N]");
  }

  const uidIndexDir = readPointer(path.join("publicData", "properties", "_frozen", "CURRENT_BASE_ZONING_INDEX_UID.txt"));

  console.log("====================================================");
  console.log("[START] Join MLS listings with Base Zoning v9 (fix city 'Town, MA')");
  console.log("[INFO ] listings in : " + path.resolve(infile));
  console.log("[INFO ] listings out: " + path.resolve(outfile));
  console.log("[INFO ] uidIndexDir  : " + path.resolve(uidIndexDir));
  console.log("====================================================");

  const { manifest, byUid, byPid, stats } = loadUidIndex(uidIndexDir);
  const asOf = manifest.as_of || "";

  console.log(`[INDEX] byUid=${byUid.size.toLocaleString()} byPropertyIdKeys=${byPid.size.toLocaleString()} bucketFiles=${stats.bucketFiles} linesRead=${stats.linesRead.toLocaleString()} dupUidMerged=${stats.dupUidMerged.toLocaleString()} pidMultiLines=${stats.pidMultiLines.toLocaleString()} parseErr=${stats.parseErr.toLocaleString()}`);

  fs.mkdirSync(path.dirname(outfile), { recursive: true });
  fs.mkdirSync(path.dirname(reportPath), { recursive: true });

  const outStream = fs.createWriteStream(outfile, { encoding: "utf8" });

  let lines = 0, parseErr = 0;
  let found = 0, notFound = 0, missingKey = 0, ambiguous = 0, ambiguousResolvedTown = 0;
  let resolvedUid = 0, resolvedPidUnique = 0, resolvedPidTown = 0, resolvedPidMultiSame = 0;

  const rl = readline.createInterface({
    input: fs.createReadStream(infile, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    if (!line) continue;
    lines++;

    const o = safeParse(line);
    if (!o) { parseErr++; continue; }

    // pull property_id from multiple places
    const pidRaw = o.property_id || o?.link?.property_id || o?.link?.propertyId || "";
    let pid = normalizePropertyId(pidRaw);

    // if property_id missing but link has parcel id, promote it
    const linkParcel = o?.link?.parcel_id || o?.link?.parcelId || "";
    if (!pid && linkParcel) pid = normalizePropertyId(linkParcel);

    const uidExisting = normalizeUid(o.property_uid || o?.link?.property_uid || "");
    const townRaw = extractTown(o);
    const townNorm = normTown(townRaw);

    const parcelId = parcelIdFromPropertyId(pid);
    const uidComputed = (townNorm && parcelId) ? `${townNorm}|${parcelId}` : "";
    const uid = uidExisting || uidComputed;

    // ensure fields exist even for misses
    o.base_district_code = o.base_district_code ?? "";
    o.base_district_name = o.base_district_name ?? "";
    o.base_zone_attach_method = o.base_zone_attach_method ?? "";
    o.base_zone_confidence = o.base_zone_confidence ?? 0;
    o.base_zone_evidence = o.base_zone_evidence ?? null;
    o.as_of = o.as_of ?? "";
    o.base_zone_join_key = o.base_zone_join_key ?? "";

    let z = null;

    if (uid && byUid.has(uid)) {
      z = byUid.get(uid);
      attachZoning(o, z, asOf);
      o.base_zone_join_key = uidExisting ? "property_uid_existing" : "property_uid_computed";
      found++; resolvedUid++;
    } else if (pid) {
      const cands = byPid.get(pid) || [];

      if (cands.length === 0) {
        notFound++;
        o.base_zone_join_key = "property_id_not_found";
      } else if (cands.length === 1) {
        z = cands[0];
        attachZoning(o, z, asOf);
        o.base_zone_join_key = "property_id_unique";
        found++; resolvedPidUnique++;
      } else {
        let townMatches = [];
        if (townNorm) townMatches = cands.filter(x => normTown(x.town) === townNorm);

        if (townMatches.length === 1) {
          z = townMatches[0];
          attachZoning(o, z, asOf);
          o.base_zone_join_key = "property_id+town_disambiguated";
          found++; ambiguousResolvedTown++; resolvedPidTown++;
        } else {
          const sig = (x) => `${x.base_district_code || ""}||${x.base_district_name || ""}||${x.base_zone_attach_method || ""}||${x.as_of || ""}`;
          const sig0 = sig(cands[0]);
          const allSame = cands.every(x => sig(x) === sig0);

          if (allSame) {
            z = cands[0];
            attachZoning(o, z, asOf);
            o.base_zone_join_key = "property_id_multi_samezone";
            found++; resolvedPidMultiSame++;
          } else {
            ambiguous++;
            o.base_zone_join_key = townNorm ? "property_id_ambiguous_town_mismatch" : "property_id_ambiguous_no_town";
          }
        }
      }
    } else {
      missingKey++;
      o.base_zone_join_key = "missing_property_id";
    }

    outStream.write(JSON.stringify(o) + "\n");

    if (logEvery && lines % logEvery === 0) {
      console.log(`[PROG] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingKey=${missingKey.toLocaleString()} ambiguous=${ambiguous.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
    }
  }

  outStream.end();

  const report = {
    created_at: new Date().toISOString(),
    as_of: asOf,
    infile: path.resolve(infile),
    outfile: path.resolve(outfile),
    uidIndexDir: path.resolve(uidIndexDir),
    counts: {
      lines, found, notFound, missingKey, ambiguous, ambiguousResolvedTown,
      resolvedUid, resolvedPidUnique, resolvedPidTown, resolvedPidMultiSame,
      parseErr,
      hit_rate_pct: lines ? +(100 * found / lines).toFixed(3) : 0
    }
  };

  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");

  console.log("----------------------------------------------------");
  console.log("[DONE] Join complete.");
  console.log(`[DONE] lines=${lines.toLocaleString()} found=${found.toLocaleString()} notFound=${notFound.toLocaleString()} missingKey=${missingKey.toLocaleString()} ambiguous=${ambiguous.toLocaleString()} ambiguousResolvedTown=${ambiguousResolvedTown.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
  console.log("[DONE] report -> " + path.resolve(reportPath));
  console.log("====================================================");
}

main().catch(err => {
  console.error("[FAIL]", err?.stack || err);
  process.exit(1);
});
