import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

import { ensureDirSync, safeReadJsonSync, readGeoJSON, formatDateStamp, inferFeatureType, listContracts, findPointerCandidates, writePointer, nowIso } from "./lib/utils.mjs";

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
      args[k] = v;
    }
  }
  return args;
}

function toTitle(s) {
  return String(s||"").split(/[_\-\s]+/g).filter(Boolean).map(w => w[0]?.toUpperCase() + w.slice(1)).join(" ");
}

function bestNameField(stats) {
  const prefer = [
    /^(name|NAME)$/i,
    /neigh/i,
    /ward/i,
    /precinct/i,
    /district/i,
    /village/i,
    /plan/i,
    /label/i,
    /title/i
  ];

  let best = null;
  let bestScore = -1;

  for (const [k, v] of Object.entries(stats)) {
    const sCount = v.nonEmptyString || 0;
    const nCount = v.nonNull || 0;
    if (sCount === 0) continue;

    let score = (nCount > 0) ? (sCount / nCount) : 0;

    for (let i = 0; i < prefer.length; i++) {
      if (prefer[i].test(k)) {
        score += (prefer.length - i) * 0.05;
        break;
      }
    }

    const avgLen = v.avgLen || 0;
    if (avgLen > 80) score -= 0.10;
    if (avgLen > 200) score -= 0.20;

    if (score > bestScore) { bestScore = score; best = k; }
  }
  return best;
}

function bestIdField(stats, sampleN) {
  const prefer = [
    /^(objectid|OBJECTID)$/i,
    /globalid/i,
    /^(fid|FID)$/i,
    /^(gid|GID)$/i,
    /^id$/i
  ];

  let best = null;
  let bestScore = -1;

  for (const [k, v] of Object.entries(stats)) {
    const nCount = v.nonNull || 0;
    if (nCount === 0) continue;

    let score = (sampleN > 0) ? (nCount / sampleN) : 0;

    for (let i = 0; i < prefer.length; i++) {
      if (prefer[i].test(k)) { score += (prefer.length - i) * 0.08; break; }
    }
    if (score > bestScore) { bestScore = score; best = k; }
  }
  return best;
}

async function canonicalizeFile({ frozenPath, outPath, nameField, idField, city, layerKey }) {
  let turf;
  try {
    turf = await import("@turf/turf");
  } catch {
    throw new Error("Missing dependency @turf/turf (needed only for --writeCanon). Install it or run without --writeCanon.");
  }

  const gj = await readGeoJSON(frozenPath);

  const out = {
    type: "FeatureCollection",
    features: gj.features.map((f) => {
      const props = { ...(f.properties || {}) };
      const ft = props.__el_feature_type || inferFeatureType(f.geometry);
      const fid = props.__el_feature_id || null;

      const nameVal = (nameField && props[nameField] != null) ? String(props[nameField]).trim() : null;
      const idVal = (idField && props[idField] != null) ? String(props[idField]).trim() : null;

      let bbox = null;
      let centroid = null;
      try { bbox = turf.bbox(f); } catch {}
      try {
        const c = turf.centroid(f);
        centroid = c?.geometry?.coordinates ? { lon: c.geometry.coordinates[0], lat: c.geometry.coordinates[1] } : null;
      } catch {}

      props.el_feature_id = fid;
      props.el_layer_key = layerKey;
      props.el_feature_type = ft;
      props.el_name = nameVal;
      props.el_source_object_id = idVal;
      props.el_city = city;
      props.el_confidence_grade = (ft === "polygon") ? "B" : "C";
      if (bbox) props.el_bbox = bbox;
      if (centroid) { props.el_centroid_lat = centroid.lat; props.el_centroid_lon = centroid.lon; }

      return { ...f, properties: props };
    })
  };

  await fsp.mkdir(path.dirname(outPath), { recursive: true });
  await fsp.writeFile(outPath, JSON.stringify(out), "utf-8");
  return { outPath, features: out.features.length };
}

async function main() {
  console.log("====================================================");
  console.log("PHASE 2 â€” CITY CIVICS HEADER AUDIT v1");
  console.log("====================================================");

  const args = parseArgs(process.argv);
  const root = args.root ? path.resolve(String(args.root)) : process.cwd();
  const writeCanon = String(args.writeCanon || "false").toLowerCase() === "true";
  const writePtr = String(args.writePointer || "false").toLowerCase() === "true";

  console.log(`[info] root: ${root}`);
  console.log(`[info] writeCanon: ${writeCanon}`);
  console.log(`[info] writePointer: ${writePtr}`);

  const contracts = listContracts(root);
  if (!contracts.length) throw new Error("No contract_view_ma__phase2_city_civics__v1__*.json found under publicData/_contracts.");
  const contractPath = contracts[0];
  const contractObj = safeReadJsonSync(contractPath);
  console.log(`[info] using contract: ${contractPath}`);

  const phase = contractObj.phase2_city_civics;
  if (!phase || !Array.isArray(phase.layers)) throw new Error("Contract missing phase2_city_civics.layers.");

  const stamp = formatDateStamp();
  const auditDir = path.join(root, "publicData", "_audit", "phase2_city_civics");
  const dictDir = path.join(root, "publicData", "overlays", "_frozen", "_dict");
  ensureDirSync(auditDir);
  ensureDirSync(dictDir);

  const dictionary = { created_at: nowIso(), contract_used: contractPath, layers: [] };
  const audit = { created_at: nowIso(), contract_used: contractPath, pointer_candidates: [], layers: [], canon_written: [] };

  audit.pointer_candidates = findPointerCandidates(root);

  for (const l of phase.layers) {
    const frozenPath = l.frozen;
    if (!frozenPath || !fs.existsSync(frozenPath)) {
      audit.layers.push({ layer_key: l.layer_key, city: l.city, status: "missing_frozen", frozen: frozenPath });
      continue;
    }

    const gj = await readGeoJSON(frozenPath);
    const sampleN = Math.min(1000, gj.features.length);
    const stats = {};
    const featureTypes = {};

    for (let i = 0; i < sampleN; i++) {
      const f = gj.features[i];
      const ft = inferFeatureType(f.geometry);
      featureTypes[ft] = (featureTypes[ft] || 0) + 1;

      const props = f.properties || {};
      for (const [k, v] of Object.entries(props)) {
        if (!stats[k]) stats[k] = { nonNull: 0, nonEmptyString: 0, samples: [], lenSum: 0, lenCount: 0 };
        if (v != null && v !== "") stats[k].nonNull += 1;
        if (typeof v === "string" && v.trim()) {
          stats[k].nonEmptyString += 1;
          const s = v.trim();
          stats[k].lenSum += s.length;
          stats[k].lenCount += 1;
          if (stats[k].samples.length < 6) stats[k].samples.push(s.slice(0, 120));
        }
      }
    }

    for (const v of Object.values(stats)) {
      v.avgLen = v.lenCount ? (v.lenSum / v.lenCount) : 0;
    }

    const nameField = bestNameField(stats);
    const idField = bestIdField(stats, sampleN);

    const city = l.city || "Unknown";
    const layerKey = l.layer_key;
    const display = `${toTitle(city)} â€” ${toTitle(layerKey.replace(/^\w+_/, ""))}`;

    const entry = {
      layer_key: layerKey,
      city,
      display_name: display,
      frozen_file: frozenPath,
      feature_count: l.feature_count ?? gj.features.length,
      geom_summary: l.geomSummary ?? featureTypes,
      name_field: nameField,
      id_field: idField,
      name_examples: nameField ? (stats[nameField]?.samples || []) : [],
      recommended_ui: { label: display, primary_text: "el_name" }
    };

    dictionary.layers.push(entry);
    audit.layers.push({ ...entry, status: "ok", sampleN });

    if (writeCanon) {
      const outPath = frozenPath.replace(/\.geojson$/i, "__canon.geojson");
      const res = await canonicalizeFile({ frozenPath, outPath, nameField, idField, city, layerKey });
      audit.canon_written.push(res);
      console.log(`[canon] wrote ${res.outPath} (features=${res.features})`);
    }
  }

  const auditPath = path.join(auditDir, `phase2_city_civics_header_audit__v1__${stamp}.json`);
  const dictPath = path.join(dictDir, `phase2_city_civics_dictionary__v1__${stamp}.json`);

  await fsp.writeFile(auditPath, JSON.stringify(audit, null, 2), "utf-8");
  await fsp.writeFile(dictPath, JSON.stringify(dictionary, null, 2), "utf-8");

  console.log(`[audit] ${auditPath}`);
  console.log(`[dict]  ${dictPath}`);

  if (writePtr) {
    const ptr = await writePointer(root, contractPath);
    console.log(`[pointer] wrote ${ptr} -> ${contractPath}`);
  }

  console.log("[done] Header audit finished.");
}

main().catch((e) => { console.error("[fatal]", e.message); process.exit(1); });

