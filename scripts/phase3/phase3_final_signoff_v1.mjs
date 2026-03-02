import fs from "node:fs";
import path from "node:path";
import { readJSON, writeJSON, sha256File, isoStamp } from "./_phase3_utils.mjs";

const ROOT = process.cwd();
const currentPtr = path.join(ROOT, "publicData", "overlays", "_frozen", "_dict", "CURRENT_PHASE3_UTILITIES_DICT.json");
const contractPtr = path.join(ROOT, "publicData", "_contracts", "CURRENT_CONTRACT_VIEW_MA.json");

if (!fs.existsSync(currentPtr)) {
  console.error("[err] missing CURRENT dict pointer:", currentPtr);
  process.exit(1);
}

const ptr = readJSON(currentPtr);
const dictPath = typeof ptr === "string" ? ptr : (ptr.path || ptr.dict_path || ptr.current || ptr.target);
const absDictPath = path.isAbsolute(dictPath) ? dictPath : path.join(ROOT, dictPath);
if (!fs.existsSync(absDictPath)) {
  console.error("[err] dict file not found:", absDictPath);
  process.exit(1);
}

const dict = readJSON(absDictPath);
const dictHash = sha256File(absDictPath);
const layers = dict.layers || dict.entries || dict.items || [];

const byCity = {};
for (const layer of layers) {
  const city = (layer.city || layer.source_city || layer.municipality || layer.town || "UNKNOWN").toString();
  if (!byCity[city]) byCity[city] = { total: 0 };
  byCity[city].total += 1;
}

const signoff = {
  created_at: new Date().toISOString(),
  phase: "phase3_utilities",
  current_dict_pointer: currentPtr,
  current_dict_pointer_value: ptr,
  final_dict_path: absDictPath,
  final_dict_sha256: dictHash,
  total_layers: Array.isArray(layers) ? layers.length : 0,
  per_city_total_layers: byCity,
  contract_view: fs.existsSync(contractPtr) ? contractPtr : null,
  contract_view_snapshot: fs.existsSync(contractPtr) ? readJSON(contractPtr) : null,
  notes: [
    "This is the Phase 3 sign-off summary artifact.",
    "It records the CURRENT dict pointer + final dict hash for auditability."
  ]
};

const outDir = path.join(ROOT, "publicData", "_audit", "phase3_utilities");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, `PHASE3_UTILITIES_SIGNOFF__${isoStamp()}.json`);
writeJSON(outPath, signoff);

console.log("[done] wrote sign-off:", outPath);
console.log("[info] final_dict_sha256:", dictHash);
