import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

function stamp() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
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

async function readJson(p) {
  return JSON.parse(await fsp.readFile(p, "utf-8"));
}
async function writeJson(p, obj) {
  await fsp.writeFile(p, JSON.stringify(obj, null, 2), "utf-8");
}

async function readGeoJSON(p) {
  const gj = JSON.parse(await fsp.readFile(p, "utf-8"));
  if (!gj || gj.type !== "FeatureCollection" || !Array.isArray(gj.features)) {
    throw new Error(`Invalid GeoJSON FeatureCollection: ${p}`);
  }
  return gj;
}

function safeBool(v) {
  if (v === true) return true;
  const s = String(v ?? "").toLowerCase().trim();
  return s === "1" || s === "true" || s === "yes";
}

function tryPick(obj, keys) {
  for (const k of keys) {
    if (obj && Object.prototype.hasOwnProperty.call(obj, k) && obj[k]) return obj[k];
  }
  return null;
}

async function main() {
  console.log("====================================================");
  console.log("PHASE 2 — CITY CIVICS PATCH ONE LAYER v1");
  console.log("====================================================");

  const args = parseArgs(process.argv);
  const root = args.root ? path.resolve(String(args.root)) : process.cwd();
  const layerKey = String(args.layerKey || "").trim();
  const source = args.source ? path.resolve(String(args.source)) : "";
  const updatePointers = safeBool(args.updatePointers);

  if (!layerKey) throw new Error("Missing --layerKey");
  if (!source || !fs.existsSync(source)) throw new Error(`Source file not found: ${source}`);

  // Pointers
  const dictPtrPath = path.join(root, "publicData", "overlays", "_frozen", "_dict", "CURRENT_PHASE2_CITY_CIVICS_DICT.json");
  const contractPtrPath = path.join(root, "publicData", "_contracts", "CURRENT_CONTRACT_VIEW_MA.json");

  if (!fs.existsSync(dictPtrPath)) throw new Error(`Missing dict pointer: ${dictPtrPath}`);

  const dictPtr = await readJson(dictPtrPath);
  const dictPath = dictPtr.current;
  if (!dictPath || !fs.existsSync(dictPath)) throw new Error(`Dict file missing: ${dictPath}`);

  // Contract path: pointer preferred; fallback to most recent phase2 city civics contract
  let contractPath = null;
  if (fs.existsSync(contractPtrPath)) {
    try {
      const cptr = await readJson(contractPtrPath);
      if (cptr?.current && fs.existsSync(cptr.current)) contractPath = cptr.current;
    } catch {}
  }
  if (!contractPath) {
    const contractsDir = path.join(root, "publicData", "_contracts");
    const cands = (await fsp.readdir(contractsDir))
      .filter((n) => n.startsWith("contract_view_ma__phase2_city_civics__") && n.endsWith(".json"))
      .map((n) => path.join(contractsDir, n));
    let newest = null;
    let newestM = 0;
    for (const p of cands) {
      const st = await fsp.stat(p);
      if (st.mtimeMs > newestM) {
        newestM = st.mtimeMs;
        newest = p;
      }
    }
    if (newest) contractPath = newest;
  }
  if (!contractPath || !fs.existsSync(contractPath)) throw new Error("Cannot locate Phase 2 city civics contract.");

  console.log(`[info] root: ${root}`);
  console.log(`[info] layerKey: ${layerKey}`);
  console.log(`[info] source: ${source}`);
  console.log(`[info] dict: ${dictPath}`);
  console.log(`[info] contract: ${contractPath}`);
  console.log(`[info] updatePointers: ${updatePointers}`);

  const dict = await readJson(dictPath);
  if (!Array.isArray(dict?.layers)) throw new Error("Dictionary schema missing layers[].");

  const contract = await readJson(contractPath);
  const phase = contract?.phase2_city_civics;
  if (!phase?.layers || !Array.isArray(phase.layers)) throw new Error("Contract missing phase2_city_civics.layers[].");

  const dLayer = dict.layers.find((l) => l.layer_key === layerKey);
  if (!dLayer) throw new Error(`Layer not found in dict: ${layerKey}`);

  const cLayer = phase.layers.find((l) => l.layer_key === layerKey);
  if (!cLayer) throw new Error(`Layer not found in contract: ${layerKey}`);

  // Determine old frozen path
  const oldFrozen = dLayer.frozen_file || tryPick(cLayer, ["frozen", "frozen_file", "frozenPath"]);
  if (!oldFrozen) throw new Error(`No frozen path present for layer ${layerKey}`);

  const oldDir = path.dirname(oldFrozen);
  const baseName = path.basename(oldFrozen, path.extname(oldFrozen));
  const ext = ".geojson";
  const newFrozen = path.join(oldDir, `${baseName}__PATCH__${stamp()}${ext}`);

  // Copy + count
  await fsp.mkdir(path.dirname(newFrozen), { recursive: true });
  await fsp.copyFile(source, newFrozen);

  const gj = await readGeoJSON(newFrozen);
  const fc = gj.features.length;

  // Update dict layer
  dLayer.frozen_file = newFrozen;
  dLayer.feature_count = fc;
  dLayer._patch = {
    patched_at: new Date().toISOString(),
    patch_type: "source_override",
    source,
    old_frozen: oldFrozen,
  };

  // Update contract layer (keep backwards compat)
  if (Object.prototype.hasOwnProperty.call(cLayer, "frozen")) cLayer.frozen = newFrozen;
  if (Object.prototype.hasOwnProperty.call(cLayer, "frozen_file")) cLayer.frozen_file = newFrozen;
  if (!Object.prototype.hasOwnProperty.call(cLayer, "frozen") && !Object.prototype.hasOwnProperty.call(cLayer, "frozen_file")) {
    cLayer.frozen = newFrozen;
  }
  cLayer.feature_count = fc;
  cLayer._patch = {
    patched_at: new Date().toISOString(),
    patch_type: "source_override",
    source,
    old_frozen: oldFrozen,
  };

  // Write new dict snapshot
  const dictDir = path.dirname(dictPath);
  const dictOut = path.join(dictDir, `phase2_city_civics_dictionary__v1__${stamp()}__PATCHED.json`);

  // Write new contract snapshot
  const contractsDir = path.join(root, "publicData", "_contracts");
  const contractOut = path.join(contractsDir, `contract_view_ma__phase2_city_civics__v1__${stamp()}__PATCHED.json`);

  // Audit
  const auditDir = path.join(root, "publicData", "_audit", "phase2_city_civics");
  await fsp.mkdir(auditDir, { recursive: true });
  const auditOut = path.join(auditDir, `phase2_city_civics_patch_layer__${layerKey}__v1__${stamp()}.json`);

  await writeJson(dictOut, dict);
  await writeJson(contractOut, contract);
  await writeJson(auditOut, {
    created_at: new Date().toISOString(),
    layer_key: layerKey,
    source,
    old_frozen: oldFrozen,
    new_frozen: newFrozen,
    feature_count: fc,
    dict_in: dictPath,
    dict_out: dictOut,
    contract_in: contractPath,
    contract_out: contractOut,
    pointers_updated: updatePointers,
  });

  if (updatePointers) {
    await writeJson(dictPtrPath, { current: dictOut, updated_at: new Date().toISOString() });
    await writeJson(contractPtrPath, { current: contractOut, updated_at: new Date().toISOString() });
  }

  console.log(`[out] frozen: ${newFrozen} (features=${fc})`);
  console.log(`[out] dict:   ${dictOut}`);
  console.log(`[out] contract:${contractOut}`);
  console.log(`[audit] ${auditOut}`);
  if (updatePointers) {
    console.log(`[ptr] updated dict pointer: ${dictPtrPath}`);
    console.log(`[ptr] updated contract pointer: ${contractPtrPath}`);
  } else {
    console.log("[warn] pointers not updated (use --updatePointers true).");
  }

  console.log("[done] patch complete.");
}

main().catch((e) => {
  console.error("[fatal]", e?.message || e);
  process.exit(1);
});
