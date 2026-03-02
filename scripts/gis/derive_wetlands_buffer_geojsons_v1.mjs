import fs from "node:fs";
import readline from "node:readline";
import path from "node:path";
import buffer from "@turf/buffer";

function arg(name, def=null) {
  const i = process.argv.indexOf(name);
  return i === -1 ? def : (process.argv[i+1] ?? def);
}

const IN  = arg("--in");
const OUT = arg("--out");
const meters = Number(arg("--meters", "30.48")); // 100 ft default
if (!IN || !OUT) {
  console.error("usage: node derive_wetlands_buffer_geojsons_v1.mjs --in <in.geojsons> --out <out.geojsons> [--meters 30.48]");
  process.exit(2);
}

fs.mkdirSync(path.dirname(OUT), { recursive: true });

const rl = readline.createInterface({
  input: fs.createReadStream(IN, { encoding: "utf8" }),
  crlfDelay: Infinity,
});

const out = fs.createWriteStream(OUT, { encoding: "utf8" });

let read=0, wrote=0, skipped=0;

console.log(`[info] derive wetlands buffer`);
console.log(`[info] in:  ${IN}`);
console.log(`[info] out: ${OUT}`);
console.log(`[info] meters: ${meters}`);

for await (const line of rl) {
  if (!line) continue;
  read++;

  let f;
  try { f = JSON.parse(line); } catch { skipped++; continue; }
  if (!f || f.type !== "Feature" || !f.geometry) { skipped++; continue; }

  // Turf buffer uses kilometers; geodesic-ish buffer (good enough for screening)
  const km = meters / 1000.0;

  let b;
  try {
    b = buffer(f, km, { units: "kilometers" });
  } catch {
    skipped++;
    continue;
  }

  // Keep provenance
  b.properties = {
    ...(f.properties || {}),
    __derived_from: "env_wetlands__ma__v1",
    __buffer_meters: meters,
    __derived_kind: "buffer"
  };

  out.write(JSON.stringify(b) + "\n");
  wrote++;

  if (read % 20000 === 0) {
    console.log(`[prog] read=${read} wrote=${wrote} skipped=${skipped}`);
  }
}

await new Promise(r => out.end(r));
console.log(`[done] read=${read} wrote=${wrote} skipped=${skipped}`);
