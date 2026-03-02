import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

function stamp() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
      out[k] = v;
    }
  }
  return out;
}

function toTitle(s) {
  return String(s || "")
    .split(/[_\-\s]+/g)
    .filter(Boolean)
    .map(w => w[0]?.toUpperCase() + w.slice(1))
    .join(" ");
}

function listLatestDict(dictDir) {
  if (!fs.existsSync(dictDir)) return null;
  const files = fs.readdirSync(dictDir)
    .filter(f => /^phase2_city_civics_dictionary__v1__\d+_\d+\.json$/i.test(f))
    .map(f => path.join(dictDir, f))
    .sort((a,b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
  return files[0] || null;
}

async function readJson(p) {
  return JSON.parse(await fsp.readFile(p, "utf-8"));
}

async function readGeoJSON(p) {
  const gj = JSON.parse(await fsp.readFile(p, "utf-8"));
  if (!gj || gj.type !== "FeatureCollection" || !Array.isArray(gj.features)) {
    throw new Error(`Invalid GeoJSON FeatureCollection: ${p}`);
  }
  return gj;
}

function inferFeatureType(geom) {
  if (!geom || !geom.type) return "unknown";
  if (geom.type === "Polygon" || geom.type === "MultiPolygon") return "polygon";
  if (geom.type === "Point" || geom.type === "MultiPoint") return "point";
  if (geom.type === "LineString" || geom.type === "MultiLineString") return "line";
  return "unknown";
}

function buildStats(features, sampleN=500) {
  const n = Math.min(sampleN, features.length);
  const stats = {}; // key -> { nonNull, nonEmptyString, lenSum, lenCount }
  for (let i = 0; i < n; i++) {
    const props = features[i]?.properties || {};
    for (const [k, v] of Object.entries(props)) {
      if (!stats[k]) stats[k] = { nonNull: 0, nonEmptyString: 0, lenSum: 0, lenCount: 0 };
      if (v != null && v !== "") stats[k].nonNull += 1;
      if (typeof v === "string" && v.trim()) {
        stats[k].nonEmptyString += 1;
        const s = v.trim();
        stats[k].lenSum += s.length;
        stats[k].lenCount += 1;
      }
    }
  }
  for (const v of Object.values(stats)) {
    v.avgLen = v.lenCount ? (v.lenSum / v.lenCount) : 0;
  }
  return { stats, n };
}

function chooseNameField(stats, sampleN) {
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
    // never allow system fields as UI label
    if (/^__el_/i.test(k)) continue;

    const sCount = v.nonEmptyString || 0;
    if (sCount === 0) continue;

    let score = (sampleN > 0) ? (sCount / sampleN) : 0;

    for (let i = 0; i < prefer.length; i++) {
      if (prefer[i].test(k)) { score += (prefer.length - i) * 0.05; break; }
    }

    // avoid huge description blobs
    if ((v.avgLen || 0) > 80) score -= 0.10;
    if ((v.avgLen || 0) > 200) score -= 0.20;

    if (score > bestScore) { bestScore = score; best = k; }
  }

  return best;
}

function chooseIdField(stats, sampleN, nameField) {
  const prefer = [
    /^(objectid|OBJECTID)$/i,
    /globalid/i,
    /^(row_id|ROW_ID)$/i,
    /^(fid|FID)$/i,
    /^id$/i
  ];

  let best = null;
  let bestScore = -1;

  for (const [k, v] of Object.entries(stats)) {
    if (/^__el_/i.test(k)) continue;
    if (nameField && k === nameField) continue;

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

async function main() {
  console.log("====================================================");
  console.log("PHASE 2 — CITY CIVICS DICTIONARY REFINE v1");
  console.log("====================================================");

  const args = parseArgs(process.argv);
  const root = args.root ? path.resolve(String(args.root)) : process.cwd();

  const dictDir = path.join(root, "publicData", "overlays", "_frozen", "_dict");
  const auditDir = path.join(root, "publicData", "_audit", "phase2_city_civics");
  const pointerPath = path.join(dictDir, "CURRENT_PHASE2_CITY_CIVICS_DICT.json");

  const dictPath = listLatestDict(dictDir);
  if (!dictPath) throw new Error(`No dictionary found in ${dictDir}`);

  console.log(`[info] root: ${root}`);
  console.log(`[info] input dict: ${dictPath}`);

  const input = await readJson(dictPath);
  if (!input?.layers || !Array.isArray(input.layers)) throw new Error("Dictionary schema unexpected (missing layers array).");

  const out = JSON.parse(JSON.stringify(input));
  const changes = [];

  for (const layer of out.layers) {
    const frozen = layer.frozen_file;
    if (!frozen || !fs.existsSync(frozen)) continue;

    const needsNameFix = (!layer.name_field) || String(layer.name_field).startsWith("__el_");
    const needsIdFix = (!layer.id_field) || String(layer.id_field).startsWith("__el_") || (layer.id_field === layer.name_field);

    if (!needsNameFix && !needsIdFix) {
      // still normalize display_name to ASCII-safe (hyphen)
      const suffix = toTitle(String(layer.layer_key || "").replace(/^\w+_/, ""));
      layer.display_name = `${layer.city} - ${suffix}`;
      continue;
    }

    const gj = await readGeoJSON(frozen);
    const { stats, n } = buildStats(gj.features, 500);

    const prev = { name_field: layer.name_field, id_field: layer.id_field, display_name: layer.display_name };

    if (needsNameFix) {
      layer.name_field = chooseNameField(stats, n);
    }
    if (needsIdFix) {
      layer.id_field = chooseIdField(stats, n, layer.name_field);
    }

    // Always normalize display_name to ASCII-safe hyphen
    const suffix = toTitle(String(layer.layer_key || "").replace(/^\w+_/, ""));
    layer.display_name = `${layer.city} - ${suffix}`;

    const next = { name_field: layer.name_field, id_field: layer.id_field, display_name: layer.display_name };

    changes.push({
      layer_key: layer.layer_key,
      city: layer.city,
      frozen_file: frozen,
      prev,
      next
    });
  }

  const s = stamp();
  await fsp.mkdir(auditDir, { recursive: true });

  const outPath = path.join(dictDir, `phase2_city_civics_dictionary__v1__${s}__REFINED.json`);
  const auditPath = path.join(auditDir, `phase2_city_civics_dictionary_refine__v1__${s}.json`);

  await fsp.writeFile(outPath, JSON.stringify(out, null, 2), "utf-8");
  await fsp.writeFile(auditPath, JSON.stringify({ created_at: new Date().toISOString(), input: dictPath, output: outPath, changes }, null, 2), "utf-8");

  await fsp.writeFile(pointerPath, JSON.stringify({ current: outPath, updated_at: new Date().toISOString() }, null, 2), "utf-8");

  console.log(`[out]   ${outPath}`);
  console.log(`[audit] ${auditPath}`);
  console.log(`[ptr]   ${pointerPath}`);
  console.log(`[done] refined layers changed: ${changes.length}`);
}

main().catch((e) => {
  console.error("[fatal]", e.message);
  process.exit(1);
});

