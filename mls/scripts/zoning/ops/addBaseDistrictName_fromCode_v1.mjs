// mls/scripts/zoning/ops/addBaseDistrictName_fromCode_v1.mjs
// ESM, streaming NDJSON: ensures base_district_name exists.
// Rule: if missing, set to base_district_code (or null if code null).

import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import crypto from "node:crypto";

function getArg(name) {
  const idx = process.argv.indexOf(name);
  if (idx === -1) return null;
  return process.argv[idx + 1] ?? null;
}

function usageAndExit(msg) {
  if (msg) console.error(`[FAIL] ${msg}`);
  console.error(
    "Usage: node addBaseDistrictName_fromCode_v1.mjs --in <ndjson> --out <ndjson> [--meta <json>] [--logEvery N] [--heartbeatSec N]"
  );
  process.exit(1);
}

const IN = getArg("--in");
const OUT = getArg("--out");
const META = getArg("--meta") || (OUT ? OUT.replace(/\.ndjson$/i, "") + "_meta.json" : null);
const logEvery = Number(getArg("--logEvery") || "250000");
const heartbeatSec = Number(getArg("--heartbeatSec") || "15");

if (!IN || !OUT) usageAndExit("Missing --in or --out");
if (!fs.existsSync(IN)) usageAndExit(`Input not found: ${IN}`);

fs.mkdirSync(path.dirname(OUT), { recursive: true });
if (META) fs.mkdirSync(path.dirname(META), { recursive: true });

console.log("====================================================");
console.log("[START] Ensure base_district_name (schema guard) v1");
console.log(`[INFO ] in   : ${IN}`);
console.log(`[INFO ] out  : ${OUT}`);
console.log(`[INFO ] meta : ${META || "(none)"}`);
console.log(`[INFO ] logEvery     : ${logEvery}`);
console.log(`[INFO ] heartbeatSec : ${heartbeatSec}s`);
console.log("====================================================");

const inStream = fs.createReadStream(IN, { encoding: "utf8" });
const outStream = fs.createWriteStream(OUT, { encoding: "utf8" });

const rl = readline.createInterface({ input: inStream, crlfDelay: Infinity });

let lines = 0;
let parseErr = 0;
let changed = 0;

const sha = crypto.createHash("sha256");
const startedAt = new Date().toISOString();

let lastBeat = Date.now();
const beatMs = heartbeatSec * 1000;

for await (const line of rl) {
  if (!line) continue;
  lines++;

  let obj;
  try {
    obj = JSON.parse(line);
  } catch {
    parseErr++;
    continue;
  }

  // Ensure field exists
  if (obj.base_district_name === undefined) {
    const code = obj.base_district_code ?? null;
    obj.base_district_name = code; // fallback per base-stage rules
    changed++;
  }

  const outLine = JSON.stringify(obj);
  outStream.write(outLine + "\n");
  sha.update(outLine + "\n");

  if (logEvery > 0 && lines % logEvery === 0) {
    console.log(`[PROG] ${new Date().toISOString()} lines=${lines.toLocaleString()} changed=${changed.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
  }

  const now = Date.now();
  if (now - lastBeat >= beatMs) {
    console.log(`[BEAT] ${new Date().toISOString()} lines=${lines.toLocaleString()} changed=${changed.toLocaleString()} parseErr=${parseErr.toLocaleString()}`);
    lastBeat = now;
  }
}

await new Promise((r) => outStream.end(r));

const finishedAt = new Date().toISOString();
const meta = {
  version: "addBaseDistrictName_fromCode_v1",
  created_at: finishedAt,
  started_at: startedAt,
  in: path.resolve(IN),
  out: path.resolve(OUT),
  counts: { lines, changed, parseErr },
  sha256_out: sha.digest("hex"),
  rule: "If base_district_name missing, set to base_district_code (else leave as-is).",
};

if (META) fs.writeFileSync(META, JSON.stringify(meta, null, 2), "utf8");

console.log("----------------------------------------------------");
console.log("[DONE] base_district_name ensured.");
console.log(`[DONE] lines   : ${lines.toLocaleString()}`);
console.log(`[DONE] changed : ${changed.toLocaleString()}`);
console.log(`[DONE] parseErr: ${parseErr.toLocaleString()}`);
if (META) console.log(`[DONE] meta    : ${META}`);
console.log("====================================================");
