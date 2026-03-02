// backend/mls/scripts/addressAuthority_buildMadCandidates_fromWgs84CSV_v1_DROPIN.js
import fs from "fs";
import path from "path";
import readline from "readline";
import crypto from "crypto";

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

function sha256File(filePath) {
  const h = crypto.createHash("sha256");
  const fd = fs.openSync(filePath, "r");
  const buf = Buffer.allocUnsafe(1024 * 1024);
  let bytes = 0;
  try {
    while ((bytes = fs.readSync(fd, buf, 0, buf.length, null)) > 0) h.update(buf.subarray(0, bytes));
  } finally {
    fs.closeSync(fd);
  }
  return h.digest("hex");
}

// Minimal CSV parser that supports quoted fields + commas.
function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        const next = line[i + 1];
        if (next === '"') { // escaped quote
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
    } else {
      if (ch === '"') inQuotes = true;
      else if (ch === ",") {
        out.push(cur);
        cur = "";
      } else {
        cur += ch;
      }
    }
  }
  out.push(cur);
  return out;
}

function normId(v) {
  if (v === null || v === undefined) return null;
  return String(v).trim();
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  const targetsJson = args["--targets"];
  const csvPath = args["--csv"];
  const outCandidates = args["--outCandidates"];
  const outMeta = args["--meta"];
  const idField = (args["--idField"] || "CENTROID_ID").trim();
  const hashInputs = String(args["--hashInputs"] || "false").toLowerCase() === "true";

  if (!targetsJson || !csvPath || !outCandidates || !outMeta) {
    console.log(`
Usage:
  node mls/scripts/addressAuthority_buildMadCandidates_fromWgs84CSV_v1_DROPIN.js \\
    --targets "C:\\seller-app\\backend\\publicData\\properties\\v27_addrTargets.json" \\
    --csv "C:\\seller-app\\backend\\publicData\\addresses\\mad_statewide_points_wgs84.csv" \\
    --outCandidates "C:\\seller-app\\backend\\publicData\\properties\\v27_madCandidates.ndjson" \\
    --meta "C:\\seller-app\\backend\\publicData\\properties\\v27_madCandidates_meta.json" \\
    [--idField CENTROID_ID] [--hashInputs true]
`);
    process.exit(1);
  }

  console.log("====================================================");
  console.log("  ADDRESS AUTHORITY V1 — BUILD MAD CANDIDATES (ID)");
  console.log("====================================================");
  console.log("targets:", targetsJson);
  console.log("csv:", csvPath);
  console.log("outCandidates:", outCandidates);
  console.log("meta:", outMeta);
  console.log("idField:", idField);

  const targets = JSON.parse(fs.readFileSync(targetsJson, "utf8"));
  const parcelIds = new Set((targets.parcel_ids || []).map(normId).filter(Boolean));

  console.log(`[info] target parcels loaded: ${parcelIds.size.toLocaleString()}`);

  fs.mkdirSync(path.dirname(outCandidates), { recursive: true });
  const out = fs.createWriteStream(outCandidates, { encoding: "utf8" });

  const rl = readline.createInterface({
    input: fs.createReadStream(csvPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let header = null;
  let idx = {};
  let scanned = 0;
  let matched = 0;

  for await (const line of rl) {
    const t = line.trimEnd();
    if (!t) continue;

    if (!header) {
      header = parseCsvLine(t);
      header.forEach((h, i) => (idx[h.trim()] = i));
      if (idx[idField] === undefined) {
        console.error(`[fatal] CSV missing idField "${idField}". Found headers:`, header.slice(0, 20), "...");
        process.exit(2);
      }
      continue;
    }

    scanned++;
    const cols = parseCsvLine(t);
    const key = normId(cols[idx[idField]]);
    if (!key) continue;

    if (parcelIds.has(key)) {
      matched++;
      // Keep a tight payload to reduce file size
      const rec = {
        CENTROID_ID: key,
        X: cols[idx["X"]],
        Y: cols[idx["Y"]],
        ADDRESS_NUMBER: cols[idx["ADDRESS_NUMBER"]],
        FULL_NUMBER_STANDARDIZED: cols[idx["FULL_NUMBER_STANDARDIZED"]],
        STREET_NAME: cols[idx["STREET_NAME"]],
        UNIT: cols[idx["UNIT"]],
        UNIT_TYPE: cols[idx["UNIT_TYPE"]],
        POSTCODE: cols[idx["POSTCODE"]],
        GEOGRAPHIC_TOWN: cols[idx["GEOGRAPHIC_TOWN"]],
        COMMUNITY_NAME: cols[idx["COMMUNITY_NAME"]],
        MASTER_ADDRESS_ID: cols[idx["MASTER_ADDRESS_ID"]],
        POINT_TYPE: cols[idx["POINT_TYPE"]],
      };
      out.write(JSON.stringify(rec) + "\n");
    }

    if (scanned % 500000 === 0) {
      console.log(`[progress] scanned ${scanned.toLocaleString()} MAD rows | matched ${matched.toLocaleString()}`);
    }
  }

  out.end();

  const meta = {
    created_at: new Date().toISOString(),
    inputs: {
      targetsJson,
      targets_sha256: hashInputs ? sha256File(targetsJson) : null,
      csvPath,
      csv_sha256: hashInputs ? sha256File(csvPath) : null,
      idField,
    },
    counts: {
      target_parcels: parcelIds.size,
      csv_rows_scanned: scanned,
      csv_rows_matched: matched,
      candidates_built: matched,
    },
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
