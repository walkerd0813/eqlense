import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ✅ Use the patched properties artifact
const IN_PROPERTIES = path.resolve(
  __dirname,
  "../../publicData/properties/properties_statewide_geo_zip_district_v2.ndjson"
);

const IN_PROPERTIES_META = path.resolve(
  __dirname,
  "../../publicData/properties/properties_statewide_geo_zip_district_v2_meta.json"
);

// ⚠️ Confirm this path exists in your repo; adjust if your filename differs
const IN_LISTINGS = path.resolve(
  __dirname,
  "../../mls/enriched/listings_linked.ndjson"
);

const OUT_LISTINGS = path.resolve(
  __dirname,
  "../../mls/enriched/listings_events_enriched.ndjson"
);

const OUT_META = path.resolve(
  __dirname,
  "../../mls/enriched/listings_events_enriched_meta.json"
);

function safeJson(x) {
  try { return JSON.parse(x); } catch { return null; }
}

async function buildZoningIndex() {
  console.log("[index] loading property zoning (only where district exists)...");
  const rl = readline.createInterface({
    input: fs.createReadStream(IN_PROPERTIES, "utf8"),
    crlfDelay: Infinity,
  });

  const map = new Map();
  let scanned = 0;
  let kept = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;
    scanned++;
    const r = safeJson(line);
    if (!r) continue;

    const pid = r.property_id;
    const d = r.zoning?.district;
    if (pid && d) {
      // keep minimal, stable payload
      map.set(pid, {
        district: {
          city: d.city ?? null,
          layer: d.layer ?? "district",
          name: d.name ?? null,
          codeRaw: d.codeRaw ?? null,
          codeNorm: d.codeNorm ?? null,
          stage: d.stage ?? null,
          refs: d.refs ?? null,
          source: d.source ?? null,
        },
        // optional civics inheritance
        zip: r.zip ?? null,
        town: r.town ?? null,
      });
      kept++;
    }

    if (scanned % 500000 === 0) {
      console.log(`[progress] scannedProps=${scanned.toLocaleString()} kept=${kept.toLocaleString()}`);
    }
  }

  console.log(`[index] done. scannedProps=${scanned.toLocaleString()} kept=${kept.toLocaleString()} mapSize=${map.size.toLocaleString()}`);
  return { map, scanned, kept };
}

async function main() {
  console.log("====================================================");
  console.log(" ENRICH LISTINGS EVENTS WITH PROPERTY ZONING (v2)");
  console.log("====================================================");
  console.log("IN_PROPERTIES:", IN_PROPERTIES);
  console.log("IN_LISTINGS:", IN_LISTINGS);
  console.log("OUT_LISTINGS:", OUT_LISTINGS);
  console.log("OUT_META:", OUT_META);
  console.log("----------------------------------------------------");

  if (!fs.existsSync(IN_PROPERTIES)) throw new Error(`Missing IN_PROPERTIES: ${IN_PROPERTIES}`);
  if (!fs.existsSync(IN_LISTINGS)) throw new Error(`Missing IN_LISTINGS: ${IN_LISTINGS}`);

  const builtAt = new Date().toISOString();
  const propsMeta = fs.existsSync(IN_PROPERTIES_META)
    ? JSON.parse(fs.readFileSync(IN_PROPERTIES_META, "utf8"))
    : null;

  const { map: zoningByPid, scanned, kept } = await buildZoningIndex();

  fs.mkdirSync(path.dirname(OUT_LISTINGS), { recursive: true });
  const out = fs.createWriteStream(OUT_LISTINGS, "utf8");

  const rl = readline.createInterface({
    input: fs.createReadStream(IN_LISTINGS, "utf8"),
    crlfDelay: Infinity,
  });

  let total = 0;
  let enriched = 0;
  let missingPid = 0;
  let noZoning = 0;

  for await (const line of rl) {
    if (!line.trim()) continue;
    total++;

    const r = safeJson(line);
    if (!r) continue;

    const pid = r.property_id ?? r.propertyId ?? null;
    if (!pid) {
      missingPid++;
      out.write(JSON.stringify(r) + "\n");
      continue;
    }

    const hit = zoningByPid.get(pid);
    if (hit?.district) {
      r.zoning = r.zoning ?? {};
      r.zoning.district = hit.district;

      // optional: civics context from property
      r.property_context = r.property_context ?? {};
      if (hit.zip) r.property_context.zip = hit.zip;
      if (hit.town) r.property_context.town = hit.town;

      enriched++;
    } else {
      noZoning++;
    }

    out.write(JSON.stringify(r) + "\n");

    if (total % 250000 === 0) {
      console.log(`[progress] listings=${total.toLocaleString()} enriched=${enriched.toLocaleString()} noZoning=${noZoning.toLocaleString()} missingPid=${missingPid.toLocaleString()}`);
    }
  }

  out.end();

  const meta = {
    builtAt,
    script: "enrichListingsEventsWithPropertyZoning.js",
    inputs: {
      properties: IN_PROPERTIES,
      propertiesMeta: IN_PROPERTIES_META,
      listings: IN_LISTINGS,
      zoningSha256: propsMeta?.inputs?.zoningSha256 ?? null,
      zoningAsOf: propsMeta?.builtAt ?? null,
    },
    propertyIndex: { scannedProps: scanned, keptZoned: kept, mapSize: zoningByPid.size },
    counts: { listingsTotal: total, enriched, noZoning, missingPid },
  };

  fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2), "utf8");

  console.log("====================================================");
  console.log("[done]", meta.counts);
  console.log("OUT_LISTINGS:", OUT_LISTINGS);
  console.log("OUT_META:", OUT_META);
  console.log("====================================================");
}

main().catch((e) => {
  console.error("❌ enrichListingsEventsWithPropertyZoning failed:", e);
  process.exit(1);
});
