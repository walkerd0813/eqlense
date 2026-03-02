import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

function arg(name, defv=null){
  const i = process.argv.indexOf("--"+name);
  return i>=0 ? (process.argv[i+1] ?? true) : defv;
}
function stamp(){
  const d=new Date(); const p=n=>String(n).padStart(2,"0");
  return `${d.getFullYear()}${p(d.getMonth()+1)}${p(d.getDate())}_${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;
}
async function readJson(p){ return JSON.parse(await fsp.readFile(p,"utf-8")); }
async function writeJson(p,obj){ await fsp.writeFile(p, JSON.stringify(obj,null,2), "utf-8"); }
async function countFeatures(p){
  const gj = JSON.parse(await fsp.readFile(p,"utf-8"));
  return (gj && Array.isArray(gj.features)) ? gj.features.length : null;
}

async function main(){
  console.log("====================================================");
  console.log("PHASE 2 — CITY CIVICS PATCH LAYER v1");
  console.log("====================================================");

  const root = path.resolve(String(arg("root", process.cwd())));
  const layerKey = String(arg("layerKey","")).trim();
  const source = String(arg("source","")).trim();
  const updatePointers = String(arg("updatePointers","false")).toLowerCase()==="true";

  if(!layerKey) throw new Error("Missing --layerKey");
  if(!source) throw new Error("Missing --source");
  if(!fs.existsSync(source)) throw new Error("Source not found: "+source);

  const dictPtr = path.join(root,"publicData","overlays","_frozen","_dict","CURRENT_PHASE2_CITY_CIVICS_DICT.json");
  if(!fs.existsSync(dictPtr)) throw new Error("Missing dict pointer: "+dictPtr);
  const dictPtrObj = await readJson(dictPtr);
  const dictPath = dictPtrObj.current;
  if(!dictPath || !fs.existsSync(dictPath)) throw new Error("Pointer dict missing: "+dictPath);

  const contractPtr = path.join(root,"publicData","_contracts","CURRENT_CONTRACT_VIEW_MA.json");
  if(!fs.existsSync(contractPtr)) throw new Error("Missing contract pointer: "+contractPtr);
  const contractPtrObj = await readJson(contractPtr);
  const contractPath = contractPtrObj.current;
  if(!contractPath || !fs.existsSync(contractPath)) throw new Error("Pointer contract missing: "+contractPath);

  const dict = await readJson(dictPath);
  const contract = await readJson(contractPath);

  const layers = dict.layers || [];
  const entry = layers.find(l => l.layer_key === layerKey);
  if(!entry) throw new Error("Layer key not found in dict: "+layerKey);

  const city = String(entry.city || "").toLowerCase();
  if(!city) throw new Error("Dict entry missing city for layer_key="+layerKey);

  const frozenDir = path.join(root,"publicData","overlays","_frozen");
  await fsp.mkdir(frozenDir,{recursive:true});

  const outFrozen = path.join(frozenDir, `civic_city__${city}__${layerKey}__v1__${stamp()}__PATCH.geojson`);
  await fsp.copyFile(source, outFrozen);
  const fc = await countFeatures(outFrozen);

  console.log("[info] layer_key:", layerKey);
  console.log("[info] city:", city);
  console.log("[info] source:", source);
  console.log("[freeze] ->", outFrozen, `(features=${fc})`);

  entry.frozen_file = outFrozen;
  entry.feature_count = (fc===null ? entry.feature_count : fc);
  entry.patched_at = new Date().toISOString();

  const dictDir = path.dirname(dictPtr);
  const outDict = path.join(dictDir, `phase2_city_civics_dictionary__v1__${stamp()}__PATCH.json`);
  await writeJson(outDict, dict);

  if(contract?.phase2_city_civics?.layers && Array.isArray(contract.phase2_city_civics.layers)){
    const cEntry = contract.phase2_city_civics.layers.find(l => l.layer_key === layerKey);
    if(cEntry){
      if("frozen" in cEntry) cEntry.frozen = outFrozen;
      if("frozen_file" in cEntry) cEntry.frozen_file = outFrozen;
      cEntry.feature_count = (fc===null ? cEntry.feature_count : fc);
      cEntry.patched_at = new Date().toISOString();
    }
  }

  const contractDir = path.dirname(contractPtr);
  const outContract = path.join(contractDir, `contract_view_ma__phase2_city_civics__v1__${stamp()}__PATCH.json`);
  await writeJson(outContract, contract);

  const auditDir = path.join(root,"publicData","_audit","phase2_city_civics");
  await fsp.mkdir(auditDir,{recursive:true});
  const auditPath = path.join(auditDir, `phase2_city_civics_patch_layer__v1__${stamp()}.json`);
  await writeJson(auditPath, {
    created_at: new Date().toISOString(),
    layer_key: layerKey,
    city,
    source,
    frozen_out: outFrozen,
    feature_count: fc,
    dict_in: dictPath,
    dict_out: outDict,
    contract_in: contractPath,
    contract_out: outContract,
    pointers_updated: updatePointers
  });

  if(updatePointers){
    await writeJson(dictPtr, { current: outDict, updated_at: new Date().toISOString() });
    await writeJson(contractPtr, { current: outContract, updated_at: new Date().toISOString() });
    console.log("[ptr] dict ->", outDict);
    console.log("[ptr] contract ->", outContract);
  }

  console.log("[out] dict:", outDict);
  console.log("[out] contract:", outContract);
  console.log("[audit]", auditPath);
  console.log("[done] patch complete.");
}

main().catch(e=>{ console.error("[fatal]", e.message); process.exit(1); });