import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const IN = path.resolve(__dirname, "../../publicData/properties/properties_statewide_geo_zip_district.ndjson");
const OUT = path.resolve(__dirname, "../../publicData/properties/properties_statewide_geo_zip_district_v2.ndjson");
const OUT_META = path.resolve(__dirname, "../../publicData/properties/properties_statewide_geo_zip_district_v2_meta.json");

function normalizeDistrictCode(s) {
  if (s == null) return null;
  let t = String(s).trim().toUpperCase();
  if (!t) return null;

  // strip accents
  t = t.normalize("NFKD").replace(/[\u0300-\u036f]/g, "");

  // make stable join token
  t = t.replace(/[^A-Z0-9]+/g, "_").replace(/^_+|_+$/g, "");

  return t || null;
}

function safeJson(line) {
  try { return JSON.parse(line); } catch { return null; }
}

async function main() {
  console.log("====================================================");
  console.log(" PATCH DISTRICT codeNorm (string, stable join key)");
  console.log("====================================================");
  console.log("IN :", IN);
  console.log("OUT:", OUT);
  console.log("META:", OUT_META);
  console.log("----------------------------------------------------");

  if (!fs.existsSync(IN)) throw new Error(`Missing input: ${IN}`);

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  const out = fs.createWriteStream(OUT, "utf8");

  const rl = readline.createInterface({
    input: fs.createReadStream(IN, "utf8"),
    crlfDelay: Infinity,
  });

  const builtAt = new Date().toISOString();

  let total = 0;
  let hasDistrict = 0;
  let patched = 0;
  let preservedNormMeta = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;
    total++;

    const r = safeJson(line);
    if (!r) continue;

    const d = r.zoning?.district;
    if (d) {
      hasDistrict++;

      // Preserve the old object norm (if present) into attach for audit, but DO NOT treat it as codeNorm.
      if (d.codeNorm && typeof d.codeNorm === "object") {
        r.zoning.attach = r.zoning.attach ?? {};
        r.zoning.attach.zoningFeatureNormMeta = d.codeNorm; // audit only
        preservedNormMeta++;
      }

      // Ensure we have codeRaw (fallback to name if missing)
      d.codeRaw = d.codeRaw ?? d.code ?? d.name ?? null;

      // Patch codeNorm to a stable string
      const next = normalizeDistrictCode(d.codeRaw ?? d.name);
      if (next && d.codeNorm !== next) {
        d.codeNorm = next;
        patched++;
      }
    }

    out.write(JSON.stringify(r) + "\n");

    if (total % 500000 === 0) {
      console.log(`[progress] total=${total.toLocaleString()} hasDistrict=${hasDistrict.toLocaleString()} patched=${patched.toLocaleString()}`);
    }
  }

  out.end();

  const meta = {
    builtAt,
    script: "patchDistrictCodeNorm.js",
    in: IN,
    out: OUT,
    counts: { total, hasDistrict, patched, preservedNormMeta },
    rule: "district.codeNorm = normalize(DISTRICT/codeRaw/name) as stable string; old object moved to zoning.attach.zoningFeatureNormMeta",
  };

  fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2), "utf8");

  console.log("====================================================");
  console.log("[done]", meta.counts);
  console.log("OUT:", OUT);
  console.log("META:", OUT_META);
  console.log("====================================================");
}

main().catch((e) => {
  console.error("❌ patchDistrictCodeNorm failed:", e);
  process.exit(1);
});
