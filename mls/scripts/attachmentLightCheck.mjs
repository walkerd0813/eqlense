// mls/scripts/lights/attachmentLightCheck.mjs
import fs from "node:fs";
import readline from "node:readline";

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

function getPath(obj, path) {
  const parts = path.split(".").map(s => s.trim()).filter(Boolean);
  let cur = obj;
  for (const p of parts) {
    if (cur == null) return undefined;
    cur = cur[p];
  }
  return cur;
}

function isFilled(v) {
  if (v == null) return false;
  if (typeof v === "string") return v.trim().length > 0;
  if (Array.isArray(v)) return v.length > 0;
  return true; // numbers/booleans/objects count as present
}

const args = parseArgs(process.argv);
const inPath = args.in;
const fields = String(args.fields || "zoning_primary,zoning_confidence")
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);
const sampleN = Number(args.sample || 3);

if (!inPath) {
  console.error("Missing --in <path-to-ndjson>");
  process.exit(1);
}
if (!fs.existsSync(inPath)) {
  console.error(`File not found: ${inPath}`);
  process.exit(1);
}

const rs = fs.createReadStream(inPath, { encoding: "utf8" });
const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });

let total = 0;
const okCounts = Object.fromEntries(fields.map(f => [f, 0]));
const missingSamples = [];
const okSamples = [];

for await (const line of rl) {
  const s = line.trim();
  if (!s) continue;

  let row;
  try {
    row = JSON.parse(s);
  } catch {
    continue; // skip bad lines
  }

  total++;

  let rowOk = true;
  for (const f of fields) {
    const v = f.includes(".") ? getPath(row, f) : row[f];
    const ok = isFilled(v);
    if (ok) okCounts[f]++;
    else rowOk = false;
  }

  // Keep a few samples to show quick truth
  const mini = {
    parcel_id: row.parcel_id ?? row.PARCEL_ID ?? row.parcelId,
    address: row.address ?? row.full_address ?? row.site_address,
    city: row.city ?? row.town,
    zip: row.zip,
    zoning_primary: row.zoning_primary ?? getPath(row, "zoning.primary"),
    zoning_confidence: row.zoning_confidence ?? getPath(row, "zoning.confidence"),
  };

  if (!rowOk && missingSamples.length < sampleN) missingSamples.push(mini);
  if (rowOk && okSamples.length < sampleN) okSamples.push(mini);
}

console.log("====================================================");
console.log("        ATTACHMENT LIGHT CHECK (NDJSON)");
console.log("====================================================");
console.log(`File:   ${inPath}`);
console.log(`Rows:   ${total.toLocaleString()}`);
console.log("");

for (const f of fields) {
  const ok = okCounts[f] || 0;
  const pct = total ? (ok / total) * 100 : 0;
  console.log(`Field '${f}':  OK ${ok.toLocaleString()} / ${total.toLocaleString()}  (${pct.toFixed(2)}%)`);
}

console.log("\n--- Samples (MISSING) ---");
if (missingSamples.length === 0) console.log("(none)");
else console.log(JSON.stringify(missingSamples, null, 2));

console.log("\n--- Samples (ATTACHED) ---");
if (okSamples.length === 0) console.log("(none)");
else console.log(JSON.stringify(okSamples, null, 2));
