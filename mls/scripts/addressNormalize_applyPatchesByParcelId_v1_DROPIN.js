import fs from "fs";
import readline from "readline";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
    out[k] = v;
  }
  return out;
}

const isBlank = (v) => v === null || v === undefined || String(v).trim() === "";

async function loadPatches(patchesPath) {
  const map = new Map();
  const rl = readline.createInterface({ input: fs.createReadStream(patchesPath, "utf8"), crlfDelay: Infinity });
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    const p = JSON.parse(t);
    const pid = isBlank(p.parcel_id) ? "" : String(p.parcel_id).trim();
    if (!pid) continue;
    map.set(pid, p);
  }
  return map;
}

async function main() {
  const args = parseArgs(process.argv);
  const IN = args.in;
  const PATCHES = args.patches;
  const OUT = args.out;
  const META = args.meta;

  if (!IN || !PATCHES || !OUT) {
    console.log("USAGE: node addressNormalize_applyPatchesByParcelId_v1_DROPIN.js --in <v28.ndjson> --patches <patches.ndjson> --out <v29.ndjson> [--meta <meta.json>]");
    process.exit(1);
  }
  if (!fs.existsSync(IN)) throw new Error(`IN not found: ${IN}`);
  if (!fs.existsSync(PATCHES)) throw new Error(`PATCHES not found: ${PATCHES}`);

  console.log(`[info] loading patches: ${PATCHES}`);
  const patchMap = await loadPatches(PATCHES);
  console.log(`[info] patches loaded: ${patchMap.size}`);

  const rl = readline.createInterface({ input: fs.createReadStream(IN, "utf8"), crlfDelay: Infinity });
  const out = fs.createWriteStream(OUT, { encoding: "utf8" });

  let total = 0;
  let applied = 0;

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    total++;
    const o = JSON.parse(t);

    const pid = isBlank(o.parcel_id) ? "" : String(o.parcel_id).trim();
    const p = pid ? patchMap.get(pid) : null;

    if (p) {
      // only overwrite if patch has a value
      if (p.street_no !== undefined) o.street_no = p.street_no;
      if (p.street_name !== undefined) o.street_name = p.street_name;
      if (p.zip !== undefined) o.zip = p.zip;
      if (p.full_address !== undefined) o.full_address = p.full_address;

      // keep evidence without breaking existing schema
      o.addr_fix_v1 = p.evidence || { method: "DETERMINISTIC_NORMALIZE_V1" };
      applied++;
    }

    out.write(JSON.stringify(o) + "\n");
  }

  out.end();

  const meta = { created_at: new Date().toISOString(), in: IN, patches: PATCHES, out: OUT, counts: { total_rows: total, applied } };
  if (META) fs.writeFileSync(META, JSON.stringify(meta, null, 2), "utf8");
  console.log("[done]", meta);
}

main().catch((e) => {
  console.error("❌ addressNormalize_applyPatches failed:", e);
  process.exit(1);
});
