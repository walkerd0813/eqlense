import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

function parseArgs(argv) {
  const o = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : "true";
      o[k] = v;
    }
  }
  return o;
}

function safeJsonParse(line) {
  try { return JSON.parse(line); } catch { return null; }
}

function normStr(x) {
  if (x === undefined || x === null) return "";
  return String(x).trim();
}

function getFeatureProps(fcRow) {
  if (!fcRow || typeof fcRow !== "object") return {};
  if (fcRow.properties && typeof fcRow.properties === "object") return fcRow.properties;
  if (fcRow.attrs && typeof fcRow.attrs === "object") return fcRow.attrs;
  if (fcRow.attributes && typeof fcRow.attributes === "object") return fcRow.attributes;
  if (fcRow.feature && fcRow.feature.properties && typeof fcRow.feature.properties === "object") return fcRow.feature.properties;
  return {};
}

function pickAny(obj, keys) {
  if (!obj) return undefined;
  for (const k of keys) {
    if (!k) continue;
    if (Object.prototype.hasOwnProperty.call(obj, k)) {
      const v = obj[k];
      if (v !== undefined && v !== null && v !== "") return v;
    }
    const lk = String(k).toLowerCase();
    for (const kk of Object.keys(obj)) {
      if (String(kk).toLowerCase() === lk) {
        const v = obj[kk];
        if (v !== undefined && v !== null && v !== "") return v;
      }
    }
  }
  return undefined;
}

function readPointerDir(overlaysFrozenDir, pointerFile) {
  const p = path.join(overlaysFrozenDir, pointerFile);
  const raw = fs.readFileSync(p, "utf8").trim();
  if (!raw) return null;
  // Allow .\... paths
  const cleaned = raw.replace(/^[.][\\/]/, "");
  // Resolve relative to cwd
  return path.resolve(process.cwd(), cleaned);
}

function assertGreen(dir) {
  const m = path.join(dir, "MANIFEST.json");
  const s = path.join(dir, "SKIPPED.txt");
  const fc = path.join(dir, "feature_catalog.ndjson");
  const att = path.join(dir, "attachments.ndjson");
  if (!fs.existsSync(m)) throw new Error(`NOT GREEN (no MANIFEST): ${dir}`);
  if (fs.existsSync(s)) throw new Error(`NOT GREEN (has SKIPPED): ${dir}`);
  if (!fs.existsSync(fc)) throw new Error(`Missing feature_catalog.ndjson: ${dir}`);
  if (!fs.existsSync(att)) throw new Error(`Missing attachments.ndjson: ${dir}`);
  return { manifest: m, featureCatalog: fc, attachments: att };
}

function floodRank(zoneRaw) {
  const z = normStr(zoneRaw).toUpperCase().replace(/\s+/g, "");
  if (!z) return 0;
  // Heuristic severity ranking (worst -> best)
  if (z.includes("VE")) return 10;
  if (z.includes("V")) return 9;
  if (z.includes("AE")) return 8;
  if (z.includes("AO")) return 7;
  if (z.includes("AH")) return 6;
  if (z.startsWith("A")) return 5;
  if (z.includes("0.2")) return 2;
  if (z.includes("X")) return 1;
  if (z.includes("D")) return 1;
  return 1;
}

function aquiferRank(clsRaw) {
  const v = normStr(clsRaw).toUpperCase();
  if (!v) return 0;
  if (v.includes("HIGH")) return 3;
  if (v.includes("MED")) return 2;
  if (v.includes("LOW")) return 1;
  // numeric fallback
  const n = Number(v);
  if (!Number.isNaN(n)) {
    if (n >= 3) return 3;
    if (n === 2) return 2;
    if (n === 1) return 1;
  }
  return 1; // unknown but present
}

function swspRank(zRaw) {
  const v = normStr(zRaw).toUpperCase();
  if (!v) return 0;
  // Prefer A over B over C (more restrictive)
  if (/\bA\b/.test(v) || v === "A" || v.includes("ZONEA")) return 3;
  if (/\bB\b/.test(v) || v === "B" || v.includes("ZONEB")) return 2;
  if (/\bC\b/.test(v) || v === "C" || v.includes("ZONEC")) return 1;
  return 1;
}

function swspNorm(zRaw) {
  const v = normStr(zRaw).toUpperCase();
  if (!v) return "none";
  if (/\bA\b/.test(v) || v === "A" || v.includes("ZONEA")) return "A";
  if (/\bB\b/.test(v) || v === "B" || v.includes("ZONEB")) return "B";
  if (/\bC\b/.test(v) || v === "C" || v.includes("ZONEC")) return "C";
  return "unknown";
}

function aquiferNorm(clsRaw) {
  const v = normStr(clsRaw).toUpperCase();
  if (!v) return "none";
  if (v.includes("HIGH")) return "high";
  if (v.includes("MED")) return "medium";
  if (v.includes("LOW")) return "low";
  const n = Number(v);
  if (!Number.isNaN(n)) {
    if (n >= 3) return "high";
    if (n === 2) return "medium";
    if (n === 1) return "low";
  }
  return "unknown";
}

async function buildFeatureValueMap(featureCatalogPath, kind) {
  // kind: "nfhl_zone" | "aquifer_class" | "swsp_zone"
  const m = new Map();
  const rl = readline.createInterface({ input: fs.createReadStream(featureCatalogPath, { encoding: "utf8" }), crlfDelay: Infinity });
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    const row = safeJsonParse(t);
    if (!row) continue;
    const fid = row.feature_id || row.featureId || row.id;
    if (!fid) continue;
    const props = getFeatureProps(row);
    if (kind === "nfhl_zone") {
      const zone = pickAny(props, ["FLD_ZONE", "fld_zone", "ZONE", "Zone", "FLOODZONE", "fldzn"]);
      if (zone !== undefined) m.set(String(fid), normStr(zone));
    } else if (kind === "aquifer_class") {
      const cls = pickAny(props, ["AQ_CLASS", "AQUIFER_CLASS", "AQUIFERCLS", "CLASS", "RISK", "VULNERABILITY", "AQUIFER", "ZONE"]);
      if (cls !== undefined) m.set(String(fid), normStr(cls));
    } else if (kind === "swsp_zone") {
      const z = pickAny(props, ["ZONE", "SWP_ZONE", "SWSP_ZONE", "ZONETYPE", "ZONE_ABC", "SWPZ", "TYPE"]);
      if (z !== undefined) m.set(String(fid), normStr(z));
    }
  }
  return m;
}

async function consumeAttachments(attachmentsPath, aggMap, layerKey, featureValueMap) {
  const rl = readline.createInterface({ input: fs.createReadStream(attachmentsPath, { encoding: "utf8" }), crlfDelay: Infinity });
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    const row = safeJsonParse(t);
    if (!row) continue;
    const pid = row.property_id || row.propertyId || row.pid;
    if (!pid) continue;

    let o = aggMap.get(pid);
    if (!o) { o = {}; aggMap.set(pid, o); }

    if (layerKey === "env_nfhl_flood_hazard__ma__v1") {
      o.env_nfhl_has_flood_hazard = true;
      o.env_nfhl_attach_count = (o.env_nfhl_attach_count || 0) + 1;
      const fid = row.feature_id || row.featureId || row.fid;
      if (fid && featureValueMap && featureValueMap.size > 0) {
        const z = featureValueMap.get(String(fid));
        if (z) {
          const zr = floodRank(z);
          const cur = o.env_nfhl_zone || "";
          if (!cur || floodRank(cur) < zr) o.env_nfhl_zone = z;
        }
      }
    } else if (layerKey === "env_wetlands__ma__v1") {
      o.env_wetlands_on_parcel = true;
      o.env_wetlands_attach_count = (o.env_wetlands_attach_count || 0) + 1;
    } else if (layerKey === "env_wetlands_buffer_100ft__ma__v1") {
      o.env_wetlands_buffer_100ft = true;
      o.env_wetlands_buffer_attach_count = (o.env_wetlands_buffer_attach_count || 0) + 1;
    } else if (layerKey === "env_pros__ma__v1") {
      o.env_in_protected_open_space = true;
      o.env_pros_attach_count = (o.env_pros_attach_count || 0) + 1;
    } else if (layerKey === "env_aquifers__ma__v1") {
      o.env_has_aquifer = true;
      o.env_aquifers_attach_count = (o.env_aquifers_attach_count || 0) + 1;
      const fid = row.feature_id || row.featureId || row.fid;
      if (fid && featureValueMap && featureValueMap.size > 0) {
        const cls = featureValueMap.get(String(fid));
        if (cls) {
          const r = aquiferRank(cls);
          const cur = o.env_aquifer_class || "";
          if (!cur || aquiferRank(cur) < r) o.env_aquifer_class = cls;
        }
      }
    } else if (layerKey === "env_zoneii_iwpa__ma__v1") {
      o.env_has_zoneii_iwpa = true;
      o.env_zoneii_attach_count = (o.env_zoneii_attach_count || 0) + 1;
    } else if (layerKey === "env_swsp_zones_abc__ma__v1") {
      o.env_has_swsp = true;
      o.env_swsp_attach_count = (o.env_swsp_attach_count || 0) + 1;
      const fid = row.feature_id || row.featureId || row.fid;
      if (fid && featureValueMap && featureValueMap.size > 0) {
        const z = featureValueMap.get(String(fid));
        if (z) {
          const r = swspRank(z);
          const cur = o.env_swsp_zone_abc || "";
          if (!cur || swspRank(cur) < r) o.env_swsp_zone_abc = z;
        }
      }
    }
  }
}

async function main() {
  const args = parseArgs(process.argv);
  const inContract = args.inContract;
  const outNdjson = args.outNdjson;
  const overlaysFrozenDir = args.overlaysFrozenDir || "./publicData/overlays/_frozen";
  const asOfDate = args.asOfDate || "UNKNOWN";
  const outStats = args.outStats || "";

  if (!inContract || !outNdjson) {
    console.error("Usage: node phase1a_build_env_summary_v1.mjs --inContract <contract.ndjson> --outNdjson <out.ndjson> --overlaysFrozenDir <dir> --asOfDate <YYYY-MM-DD> [--outStats <stats.json>]");
    process.exit(2);
  }

  const required = [
    { pointer: "CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt", layerKey: "env_nfhl_flood_hazard__ma__v1", kind: "nfhl_zone" },
    { pointer: "CURRENT_ENV_WETLANDS_MA.txt", layerKey: "env_wetlands__ma__v1", kind: null },
    { pointer: "CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt", layerKey: "env_wetlands_buffer_100ft__ma__v1", kind: null },
    { pointer: "CURRENT_ENV_PROS_MA.txt", layerKey: "env_pros__ma__v1", kind: null },
    { pointer: "CURRENT_ENV_AQUIFERS_MA.txt", layerKey: "env_aquifers__ma__v1", kind: "aquifer_class" },
    { pointer: "CURRENT_ENV_ZONEII_IWPA_MA.txt", layerKey: "env_zoneii_iwpa__ma__v1", kind: null },
    { pointer: "CURRENT_ENV_SWSP_ZONES_ABC_MA.txt", layerKey: "env_swsp_zones_abc__ma__v1", kind: "swsp_zone" }
  ];

  // Build aggregation map from overlay attachments (only properties with any env overlay)
  const agg = new Map();
  const overlayStats = {};

  for (const r of required) {
    const dir = readPointerDir(overlaysFrozenDir, r.pointer);
    if (!dir) throw new Error(`Missing/empty pointer: ${r.pointer}`);
    const { featureCatalog, attachments } = assertGreen(dir);

    let featureValueMap = null;
    if (r.kind === "nfhl_zone") featureValueMap = await buildFeatureValueMap(featureCatalog, "nfhl_zone");
    if (r.kind === "aquifer_class") featureValueMap = await buildFeatureValueMap(featureCatalog, "aquifer_class");
    if (r.kind === "swsp_zone") featureValueMap = await buildFeatureValueMap(featureCatalog, "swsp_zone");

    await consumeAttachments(attachments, agg, r.layerKey, featureValueMap);

    overlayStats[r.layerKey] = {
      pointer: r.pointer,
      dir,
      feature_catalog: featureCatalog,
      attachments,
      feature_value_map_size: featureValueMap ? featureValueMap.size : 0
    };
  }

  fs.mkdirSync(path.dirname(outNdjson), { recursive: true });
  const rl = readline.createInterface({ input: fs.createReadStream(inContract, { encoding: "utf8" }), crlfDelay: Infinity });
  const ws = fs.createWriteStream(outNdjson, { encoding: "utf8" });

  let read = 0, wrote = 0, skipped = 0;
  let anyConstraintProps = 0;

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    const row = safeJsonParse(t);
    if (!row) { skipped++; continue; }
    read++;

    const pid = row.property_id || row.propertyId || row.id;
    if (!pid) { skipped++; continue; }

    const o = agg.get(String(pid)) || {};

    const nfhlHas = !!o.env_nfhl_has_flood_hazard;
    const wetlandsOn = !!o.env_wetlands_on_parcel;
    const wetlandsBuf = !!o.env_wetlands_buffer_100ft;
    const pros = !!o.env_in_protected_open_space;
    const aqu = !!o.env_has_aquifer;
    const zoneii = !!o.env_has_zoneii_iwpa;
    const swspHas = !!o.env_has_swsp;

    const any = nfhlHas || wetlandsOn || wetlandsBuf || pros || aqu || zoneii || swspHas;
    if (any) anyConstraintProps++;

    // Normalize classifications
    const out = {
      ...row,

      // Phase1A ENV SUMMARY (NO GEOMETRY)
      env_has_any_constraint: any,
      env_constraints_as_of_date: asOfDate,

      env_nfhl_has_flood_hazard: nfhlHas,
      env_nfhl_zone: nfhlHas ? normStr(o.env_nfhl_zone) || "UNKNOWN" : "none",
      env_nfhl_attach_count: o.env_nfhl_attach_count || 0,

      env_wetlands_on_parcel: wetlandsOn,
      env_wetlands_attach_count: o.env_wetlands_attach_count || 0,

      env_wetlands_buffer_100ft: wetlandsBuf,
      env_wetlands_buffer_attach_count: o.env_wetlands_buffer_attach_count || 0,

      env_in_protected_open_space: pros,
      env_pros_attach_count: o.env_pros_attach_count || 0,

      env_has_aquifer: aqu,
      env_aquifer_class: aqu ? aquiferNorm(o.env_aquifer_class) : "none",
      env_aquifers_attach_count: o.env_aquifers_attach_count || 0,

      env_has_zoneii_iwpa: zoneii,
      env_zoneii_attach_count: o.env_zoneii_attach_count || 0,

      env_has_swsp: swspHas,
      env_swsp_zone_abc: swspHas ? swspNorm(o.env_swsp_zone_abc) : "none",
      env_swsp_attach_count: o.env_swsp_attach_count || 0
    };

    ws.write(JSON.stringify(out) + "\n");
    wrote++;

    if (read % 200000 === 0) {
      console.error(`[prog] read=${read} wrote=${wrote} skipped=${skipped} any_constraint_props=${anyConstraintProps} agg_keys=${agg.size}`);
    }
  }

  ws.end();

  const stats = {
    created_at: new Date().toISOString(),
    as_of_date: asOfDate,
    in_contract: inContract,
    out_ndjson: outNdjson,
    rows_read: read,
    rows_written: wrote,
    rows_skipped: skipped,
    properties_with_any_env_constraint: anyConstraintProps,
    overlay_sources: overlayStats,
    agg_properties_tracked: agg.size
  };

  if (outStats) {
    fs.writeFileSync(outStats, JSON.stringify(stats, null, 2), "utf8");
  }

  console.error(`[done] read=${read} wrote=${wrote} skipped=${skipped} any_constraint_props=${anyConstraintProps} agg_keys=${agg.size}`);
}

main().catch((e) => {
  console.error("[fatal]", e && e.stack ? e.stack : String(e));
  process.exit(1);
});
