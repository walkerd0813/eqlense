import fs from "fs";
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
  return v === null || v === undefined || String(v).trim() === "";
}

function normZip(v) {
  const s = isBlank(v) ? "" : String(v).trim();
  const m = s.match(/\b(\d{5})(?:-\d{4})?\b/);
  return m ? m[1] : "";
}

function isValidStreetNo(v) {
  const s = isBlank(v) ? "" : String(v).trim();
  if (!s) return false;
  if (s.includes("-")) return false; // ambiguous ranges like 270-6
  const m = s.match(/^0*(\d+)(\.\d+)?([A-Za-z])?$/);
  if (!m) return false;
  const n = Number(m[1]);
  return Number.isFinite(n) && n > 0;
}

function normStreetNo(v) {
  const s = isBlank(v) ? "" : String(v).trim();
  if (!s) return "";
  if (s.includes("-")) return ""; // keep strict: don't guess ranges
  const m = s.match(/^0*(\d+)(\.\d+)?([A-Za-z])?$/);
  if (!m) return "";
  const n = Number(m[1]);
  if (!Number.isFinite(n) || n <= 0) return "";
  const dec = m[2] || "";
  const suf = m[3] || "";
  return `${n}${dec}${suf}`;
}

function normStreetName(v) {
  const s = isBlank(v) ? "" : String(v).trim();
  if (!s) return "";
  return s
    .replace(/[,\t]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

function buildFullAddress(no, name, unit) {
  const n = isBlank(no) ? "" : String(no).trim();
  const s = isBlank(name) ? "" : String(name).trim();
  if (!n || !s) return "";
  const u = isBlank(unit) ? "" : String(unit).trim();
  return u ? `${n} ${s} #${u}`.replace(/\s+/g, " ").trim() : `${n} ${s}`.replace(/\s+/g, " ").trim();
}

function parseFromLabel(label, townUpper) {
  if (isBlank(label)) return { street_no: "", street_name: "" };

  let t = String(label).trim().toUpperCase();

  // kill trailing zip / state / town if appended
  t = t.replace(/\bMA\b\s*\d{5}(?:-\d{4})?$/, "").trim();
  t = t.replace(/\s+\d{5}(?:-\d{4})?$/, "").trim();
  if (townUpper && t.endsWith(" " + townUpper)) t = t.slice(0, -(" " + townUpper).length).trim();

  // remove comma chunks after street
  t = t.split(",")[0].trim();

  // strip common unit markers at end (we keep existing p.unit; we won’t override)
  t = t.replace(/\s+(APT|APARTMENT|UNIT|STE|SUITE|FL|FLOOR|#)\s*.*$/, "").trim();

  const m = t.match(/^(\d+[A-Z]?|\d+\.\d+)\s+(.+)$/);
  if (!m) return { street_no: "", street_name: "" };

  const no = normStreetNo(m[1]);
  const name = normStreetName(m[2]);

  // reject garbage
  if (!no || !name || name.length < 2) return { street_no: "", street_name: "" };

  return { street_no: no, street_name: name };
}

async function main() {
  const args = parseArgs(process.argv);

  const IN = args.in;
  const OUT = args.outPatches;
  const META = args.meta;

  if (!IN || !OUT) {
    console.log("USAGE: node addressNormalize_buildAddrFixPatches_v1_DROPIN.js --in <v28.ndjson> --outPatches <patches.ndjson> [--meta <meta.json>]");
    process.exit(1);
  }
  if (!fs.existsSync(IN)) throw new Error(`IN not found: ${IN}`);

  const rl = readline.createInterface({ input: fs.createReadStream(IN, "utf8"), crlfDelay: Infinity });
  const out = fs.createWriteStream(OUT, { encoding: "utf8" });

  let total = 0;
  let candidates = 0;
  let patchesWritten = 0;
  let patchNo = 0, patchName = 0, patchFull = 0, patchZip = 0;

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    total++;

    let p;
    try { p = JSON.parse(t); } catch { continue; }

    const pid = p.parcel_id ? String(p.parcel_id).trim() : "";
    if (!pid) continue;

    const streetNoRaw = isBlank(p.street_no) ? "" : String(p.street_no).trim();
    const streetNameRaw = isBlank(p.street_name) ? "" : String(p.street_name).trim();
    const fullRaw = isBlank(p.full_address) ? "" : String(p.full_address).trim();
    const zipRaw = isBlank(p.zip) ? "" : String(p.zip).trim();

    const needsNo = !isValidStreetNo(streetNoRaw);
    const needsName = isBlank(streetNameRaw);
    const needsFull = isBlank(fullRaw);
    const needsZip = isBlank(normZip(zipRaw));

    if (!(needsNo || needsName || needsFull || needsZip)) continue;
    candidates++;

    const townUpper = p.town ? String(p.town).trim().toUpperCase() : "";
    const label = fullRaw || p.address_label || p.site_key || "";
    const parsed = parseFromLabel(label, townUpper);

    const nextNo = needsNo ? parsed.street_no : normStreetNo(streetNoRaw);
    const nextName = needsName ? parsed.street_name : normStreetName(streetNameRaw);
    const nextZip = needsZip ? normZip(label) : normZip(zipRaw);

    const nextFull = buildFullAddress(nextNo, nextName, p.unit);

    const patch = { parcel_id: pid, evidence: { source: "AddressNormalize:v1", rule: "STRICT_PARSE_FROM_LABEL", input: IN } };
    let changed = false;

    if (needsNo && nextNo) { patch.street_no = nextNo; patchNo++; changed = true; }
    if (needsName && nextName) { patch.street_name = nextName; patchName++; changed = true; }
    if (needsZip && nextZip) { patch.zip = nextZip; patchZip++; changed = true; }
    if (needsFull && nextFull) { patch.full_address = nextFull; patchFull++; changed = true; }

    if (changed) {
      out.write(JSON.stringify(patch) + "\n");
      patchesWritten++;
    }

    if (total % 200000 === 0) {
      console.log(`[progress] rows=${total.toLocaleString()} candidates=${candidates.toLocaleString()} patches=${patchesWritten.toLocaleString()}`);
    }
  }

  out.end();

  const meta = {
    created_at: new Date().toISOString(),
    in: IN,
    outPatches: OUT,
    counts: {
      total_rows: total,
      candidate_rows: candidates,
      patches_written: patchesWritten,
      patched_street_no: patchNo,
      patched_street_name: patchName,
      patched_zip: patchZip,
      patched_full_address: patchFull
    }
  };

  if (META) fs.writeFileSync(META, JSON.stringify(meta, null, 2), "utf8");
  console.log("[done]", meta);
}

main().catch((e) => {
  console.error("❌ buildAddrFixPatches failed:", e);
  process.exit(1);
});
