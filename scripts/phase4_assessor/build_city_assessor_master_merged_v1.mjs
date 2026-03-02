#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import crypto from "node:crypto";

function parseArgs(argv){
  const out = {};
  for (let i=2;i<argv.length;i++){
    const a = argv[i];
    if (a === "--ptr") out.ptr = argv[++i];
    else if (a === "--outDir") out.outDir = argv[++i];
  }
  return out;
}

function readJSON(p){
  const raw = fs.readFileSync(p);
  const s = raw[0] === 0xEF && raw[1] === 0xBB && raw[2] === 0xBF ? raw.slice(3).toString("utf8") : raw.toString("utf8");
  return JSON.parse(s);
}

function sha256File(p){
  const h = crypto.createHash("sha256");
  const fd = fs.openSync(p, "r");
  const buf = Buffer.allocUnsafe(1024*1024);
  let n;
  while ((n = fs.readSync(fd, buf, 0, buf.length, null)) > 0){
    h.update(buf.subarray(0,n));
  }
  fs.closeSync(fd);
  return h.digest("hex");
}

async function appendFileTo(outStream, srcPath){
  if (!fs.existsSync(srcPath)) throw new Error("missing city master ndjson: " + srcPath);
  const rl = readline.createInterface({ input: fs.createReadStream(srcPath, { encoding:"utf8" }), crlfDelay: Infinity });
  for await (const line of rl){
    if (!line) continue;
    outStream.write(line + "\n");
  }
}

async function main(){
  const args = parseArgs(process.argv);
  if (!args.ptr) {
    console.error("usage: node build_city_assessor_master_merged_v1.mjs --ptr <CURRENT_PHASE4_ASSESSOR_MASTER.json> [--outDir <dir>]");
    process.exit(2);
  }
  const ptrPath = path.resolve(args.ptr);
  const outDir = path.resolve(args.outDir ?? path.join(path.dirname(ptrPath)));
  if (!fs.existsSync(ptrPath)) throw new Error("pointer not found: " + ptrPath);
  fs.mkdirSync(outDir, { recursive: true });

  const ptr = readJSON(ptrPath);
  const cities = ptr.cities ?? [];
  if (!Array.isArray(cities) || cities.length === 0) throw new Error("pointer has no cities[]: " + ptrPath);

  const ts = new Date().toISOString().replaceAll(":","-").replaceAll(".","-");
  const outNd = path.join(outDir, `city_assessor_master__MERGED__${ts}__V1.ndjson`);
  const out = fs.createWriteStream(outNd, { encoding:"utf8" });

  console.log("[start] merging city assessor masters:", cities.length, "files");
  for (const c of cities){
    const p = c.master_ndjson;
    if (!p) throw new Error("city entry missing master_ndjson for city=" + (c.city ?? "?"));
    console.log("[info] +", c.city, "->", p);
    await appendFileTo(out, p);
  }
  await new Promise(r=>out.end(r));

  const hash = sha256File(outNd);
  const pointerOut = path.join(outDir, "CURRENT_CITY_ASSESSOR_MASTER_MERGED.json");
  const meta = {
    updated_at: new Date().toISOString(),
    note: "AUTO: merged city assessor masters into single NDJSON for global attach",
    source_ptr: ptrPath,
    cities_count: cities.length,
    merged_ndjson: outNd,
    merged_sha256: hash
  };
  fs.writeFileSync(pointerOut, JSON.stringify(meta, null, 2), "utf8");

  console.log("[ok] wrote merged:", outNd);
  console.log("[ok] sha256:", hash);
  console.log("[ok] wrote pointer:", pointerOut);
}

main().catch(e=>{
  console.error("[fatal]", e);
  process.exit(1);
});
