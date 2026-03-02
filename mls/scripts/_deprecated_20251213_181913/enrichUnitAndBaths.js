import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

function parseArgs(argv) {
  const out = { input: null, output: null };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--input") out.input = argv[++i];
    else if (a === "--output") out.output = argv[++i];
  }
  if (!out.input || !out.output) {
    console.error("❌ Usage: node mls/scripts/enrichUnitAndBaths.js --input <in.ndjson> --output <out.ndjson>");
    process.exit(1);
  }
  return out;
}

function isPresent(v) {
  if (v === null || v === undefined) return false;
  if (typeof v === "string") return v.trim().length > 0;
  return true;
}

function parseUnitFromAddress(rawAddress) {
  const s = String(rawAddress || "");
  const m = s.match(/\bU:\s*(.+)\s*$/i); // capture everything after U: to end
  if (!m) return null;

  let unit = (m[1] || "").trim();

  // treat placeholder units as null
  if (!unit || unit === "." || unit === "-" || unit.toLowerCase() === "na") return null;

  // remove leading # (with optional space): "#12", "# 5G"
  unit = unit.replace(/^#\s*/i, "");

  // unwrap leading parentheses: "(3) PH" -> "3 PH"
  unit = unit.replace(/^\(([^)]+)\)\s*/i, "$1 ");

  // common leading words (just in case)
  unit = unit.replace(/^(unit|apt|apartment|ph)\s+/i, (w) => w.toUpperCase() + " ");
  // ^ harmless if not present; keeps PH if you ever get "PH 3" formats

  // collapse whitespace and strip trailing punctuation
  unit = unit.replace(/\s+/g, " ").replace(/[,\.;]+$/g, "").trim();

  if (!unit || unit === ".") return null;
  return unit;
}


function parseBathDesc(bth) {
  const s = String(bth || "").toLowerCase(); // "3f 0h"
  const mF = s.match(/(\d+)\s*f/);
  const mH = s.match(/(\d+)\s*h/);
  const full = mF ? Number(mF[1]) : null;
  const half = mH ? Number(mH[1]) : null;
  const total = (full ?? 0) + (half ? half * 0.5 : 0);
  return { fullBaths: full, halfBaths: half, totalBaths: total };
}

async function main() {
  const { input, output } = parseArgs(process.argv);

  const inPath = path.resolve(process.cwd(), input);
  const outPath = path.resolve(process.cwd(), output);

  if (!fs.existsSync(inPath)) {
    console.error("❌ Missing input:", inPath);
    process.exit(1);
  }

  console.log("====================================================");
  console.log(" ENRICH address.unit + physical.*Baths (STREAMING)");
  console.log("====================================================");
  console.log("Input: ", inPath);
  console.log("Output:", outPath);
  console.log("----------------------------------------------------");

  const rl = readline.createInterface({
    input: fs.createReadStream(inPath),
    crlfDelay: Infinity,
  });

  const out = fs.createWriteStream(outPath, { flags: "w" });

  let rows = 0;
  let unitFilled = 0;
  let bathsFilled = 0;

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;

    let obj;
    try { obj = JSON.parse(t); } catch { continue; }

    rows++;

    obj.address = obj.address || {};
    obj.physical = obj.physical || {};

    const raw = obj?.raw?.row || {};

    // Fill unit if missing
    if (!isPresent(obj.address.unit)) {
      const unit = parseUnitFromAddress(raw.ADDRESS);
      if (isPresent(unit)) {
        obj.address.unit = unit;
        unitFilled++;
      }
    }

    // Fill baths if missing
    const needsBaths =
      !isPresent(obj.physical.fullBaths) ||
      !isPresent(obj.physical.halfBaths) ||
      !isPresent(obj.physical.totalBaths);

    if (needsBaths) {
      const b = parseBathDesc(raw.BTH_DESC);
      if (isPresent(b.fullBaths) || isPresent(b.halfBaths)) {
        if (!isPresent(obj.physical.fullBaths)) obj.physical.fullBaths = b.fullBaths;
        if (!isPresent(obj.physical.halfBaths)) obj.physical.halfBaths = b.halfBaths;
        if (!isPresent(obj.physical.totalBaths)) obj.physical.totalBaths = b.totalBaths;
        bathsFilled++;
      }
    }

    out.write(JSON.stringify(obj) + "\n");

    if (rows % 50000 === 0) {
      console.log(`[enrich] rows=${rows.toLocaleString()} unitFilled=${unitFilled.toLocaleString()} bathsFilled=${bathsFilled.toLocaleString()}`);
    }
  }

  out.end();

  console.log("====================================================");
  console.log("✅ Done");
  console.log("Rows:", rows.toLocaleString());
  console.log("Unit filled:", unitFilled.toLocaleString());
  console.log("Baths filled:", bathsFilled.toLocaleString());
  console.log("====================================================");
}

main().catch((e) => {
  console.error("❌ enrich failed:", e?.stack || String(e));
  process.exit(1);
});
