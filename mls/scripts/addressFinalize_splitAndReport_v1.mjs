// addressFinalize_splitAndReport_v1.mjs
import fs from "fs";
import path from "path";
import readline from "readline";
import crypto from "crypto";

function arg(name, def = null) {
  const ix = process.argv.indexOf(name);
  if (ix === -1) return def;
  const v = process.argv[ix + 1];
  if (!v || v.startsWith("--")) return def;
  return v;
}

function normStr(x) {
  if (x === null || x === undefined) return null;
  return String(x).trim();
}

function looksLikeMA(lat, lng) {
  if (typeof lat !== "number" || typeof lng !== "number") return false;
  // generous MA bounds
  return lat >= 41.0 && lat <= 43.7 && lng >= -73.7 && lng <= -69.4;
}

function isUnknownParcel(o) {
  const pid = normStr(o?.property_id);
  const parcel = normStr(o?.parcel_id);
  if (!pid && !parcel) return true;
  if (parcel === "UNKNOWN") return true;
  if (pid && pid.includes(":UNKNOWN")) return true;
  return false;
}

function getVerifiedTown(o) {
  return (
    o?.address_verified?.town_pip_stateplane?.verifiedTown ||
    o?.address_verified?.town_pip?.verifiedTown ||
    null
  );
}

function ciEq(a, b) {
  if (a == null || b == null) return false;
  return String(a).trim().toUpperCase() === String(b).trim().toUpperCase();
}

function sha256File(fp) {
  const h = crypto.createHash("sha256");
  const fd = fs.createReadStream(fp);
  return new Promise((resolve, reject) => {
    fd.on("data", (d) => h.update(d));
    fd.on("error", reject);
    fd.on("end", () => resolve(h.digest("hex")));
  });
}

async function main() {
  const inPath = arg("--in");
  const outDir = arg("--outDir");
  const reportPath = arg("--report");

  if (!inPath || !outDir || !reportPath) {
    console.error("Missing args. Required: --in --outDir --report");
    process.exit(1);
  }
  if (!fs.existsSync(inPath)) {
    console.error("Input not found:", inPath);
    process.exit(1);
  }

  fs.mkdirSync(outDir, { recursive: true });

  const outCanonical = path.join(outDir, "v44_CANONICAL_FOR_ZONING.ndjson");
  const outQuarantine = path.join(outDir, "v44_ADDRESS_QUARANTINE.ndjson");
  const outExclude = path.join(outDir, "v44_EXCLUDE.ndjson");
  const outMailingReady = path.join(outDir, "v44_MAILING_READY_TIERA.ndjson");

  const wCanonical = fs.createWriteStream(outCanonical);
  const wQuar = fs.createWriteStream(outQuarantine);
  const wEx = fs.createWriteStream(outExclude);
  const wMail = fs.createWriteStream(outMailingReady);

  const rl = readline.createInterface({ input: fs.createReadStream(inPath) });

  const counts = {
    total: 0,
    parseErr: 0,
    for_zoning: 0,
    mailing_ready_tierA: 0,
    quarantine_address: 0,
    exclude: 0,

    exclude_unknown_parcel: 0,
    exclude_no_coords: 0,
    exclude_bad_coords: 0,

    tierA: 0,
    tierB: 0,
    tierC: 0,

    town_verified_ok: 0,
    town_verified_mismatch: 0,
    town_verified_missing: 0,
  };

  const samples = {
    exclude: [],
    quarantine: [],
    verified_mismatch: [],
  };

  const VERSION = "finalize_split_v1";
  const AT = new Date().toISOString();

  rl.on("line", (line) => {
    if (!line) return;
    let o;
    try {
      o = JSON.parse(line);
    } catch {
      counts.parseErr++;
      return;
    }
    counts.total++;

    const tier = normStr(o.address_tier);
    if (tier === "A") counts.tierA++;
    else if (tier === "B") counts.tierB++;
    else counts.tierC++;

    const lat = typeof o.lat === "number" ? o.lat : (typeof o.latitude === "number" ? o.latitude : null);
    const lng = typeof o.lng === "number" ? o.lng : (typeof o.lon === "number" ? o.lon : null);

    const unknownParcel = isUnknownParcel(o);
    const hasCoords = (lat !== null && lng !== null);
    const goodCoords = hasCoords && looksLikeMA(lat, lng);

    const verifiedTown = getVerifiedTown(o);
    if (!verifiedTown) counts.town_verified_missing++;
    else if (ciEq(o.town, verifiedTown)) counts.town_verified_ok++;
    else {
      counts.town_verified_mismatch++;
      if (samples.verified_mismatch.length < 25) {
        samples.verified_mismatch.push({
          row_uid: o.row_uid || null,
          parcel_id: o.parcel_id || null,
          town: o.town || null,
          verifiedTown,
          address_label: o.address_label || null,
        });
      }
    }

    let bucket = null;

    if (unknownParcel) {
      bucket = "EXCLUDE_UNKNOWN_PARCEL";
      counts.exclude++;
      counts.exclude_unknown_parcel++;
      if (samples.exclude.length < 25) samples.exclude.push({ row_uid: o.row_uid || null, reason: bucket, address_label: o.address_label || null });
      wEx.write(line + "\n");
      return;
    }

    if (!hasCoords) {
      bucket = "EXCLUDE_NO_COORDS";
      counts.exclude++;
      counts.exclude_no_coords++;
      if (samples.exclude.length < 25) samples.exclude.push({ row_uid: o.row_uid || null, reason: bucket, address_label: o.address_label || null });
      wEx.write(line + "\n");
      return;
    }

    if (!goodCoords) {
      bucket = "EXCLUDE_BAD_COORDS";
      counts.exclude++;
      counts.exclude_bad_coords++;
      if (samples.exclude.length < 25) samples.exclude.push({ row_uid: o.row_uid || null, reason: bucket, lat, lng, address_label: o.address_label || null });
      wEx.write(line + "\n");
      return;
    }

    // Attach zoning to ALL rows with valid coords + non-UNKNOWN parcel.
    const forZoning = true;
    counts.for_zoning++;

    const mailingReady = (tier === "A");
    const quarantineAddr = !mailingReady;

    // add finalize marker
    o.finalize = {
      version: VERSION,
      at: AT,
      for_zoning: forZoning,
      mailing_ready: mailingReady,
      bucket: mailingReady ? "MAILING_READY_TIERA" : "ADDRESS_QUARANTINE_TIERB_OR_TIERC",
    };

    const outLine = JSON.stringify(o);

    wCanonical.write(outLine + "\n");

    if (mailingReady) {
      counts.mailing_ready_tierA++;
      wMail.write(outLine + "\n");
    } else {
      counts.quarantine_address++;
      if (samples.quarantine.length < 25) samples.quarantine.push({ row_uid: o.row_uid || null, tier, address_label: o.address_label || null });
      wQuar.write(outLine + "\n");
    }
  });

  await new Promise((res) => rl.on("close", res));
  wCanonical.end(); wQuar.end(); wEx.end(); wMail.end();

  const report = {
    created_at: AT,
    in: inPath,
    outDir,
    outputs: { outCanonical, outQuarantine, outExclude, outMailingReady },
    counts,
    samples,
    notes: {
      zoning_rule: "Keep all rows with valid MA-like lat/lng AND non-UNKNOWN parcel_id/property_id.",
      address_rule: "Tier A = mailing-ready; Tier B/C = quarantine (still zoning attachable).",
    },
  };

  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
  console.log("DONE.");
  console.log(reportPath);
}

main().catch((e) => {
  console.error("FATAL:", e);
  process.exit(1);
});
