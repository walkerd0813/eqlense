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
const prefixRS = arg("--prefixRS", "1") !== "0"; // keep GeoJSONSeq RS prefix by default

if (!IN || !OUT) {
  console.error("usage: node derive_wetlands_buffer_geojsons_v2.mjs --in <in.geojsons> --out <out.geojsons> [--meters 30.48] [--prefixRS 1|0]");
  process.exit(2);
}

fs.mkdirSync(path.dirname(OUT), { recursive: true });

const rl = readline.createInterface({
  input: fs.createReadStream(IN, { encoding: "utf8" }),
  crlfDelay: Infinity,
});

const out = fs.createWriteStream(OUT, { encoding: "utf8" });

let read=0, wrote=0, skipped=0;

console.log(`[info] derive wetlands buffer (v2)`);
console.log(`[info] in:  ${IN}`);
console.log(`[info] out: ${OUT}`);
console.log(`[info] meters: ${meters} (100ft=30.48m)`);
console.log(`[info] prefixRS: ${prefixRS}`);

for await (const line0 of rl) {
  if (line0 === undefined || line0 === null) continue;
  read++;

  // Strip GeoJSON Text Sequences Record Separator (0x1E) if present
  let line = line0;
  if (line.length > 0 && line.charCodeAt(0) === 0x1e) line = line.slice(1);

  // Also strip BOM if present
  if (line.length > 0 && line.charCodeAt(0) === 0xfeff) line = line.slice(1);

  line = line.trim();
  if (!line) { skipped++; continue; }

  let f;
  try { f = JSON.parse(line); } catch { skipped++; continue; }
  if (!f || f.type !== "Feature" || !f.geometry) { skipped++; continue; }

  // Turf buffer uses kilometers
  const km = meters / 1000.0;

  let b;
  try {
    b = buffer(f, km, { units: "kilometers" });
  } catch {
    skipped++;
    continue;
  }

  b.properties = {
    ...(f.properties || {}),
    __derived_from: "env_wetlands__ma__v1",
    __buffer_meters: meters,
    __derived_kind: "buffer"
  };

  const json = JSON.stringify(b);
  out.write((prefixRS ? "\u001e" : "") + json + "\n");
  wrote++;

  if (read % 20000 === 0) {
    console.log(`[prog] read=${read} wrote=${wrote} skipped=${skipped}`);
  }
}

await new Promise(r => out.end(r));
console.log(`[done] read=${read} wrote=${wrote} skipped=${skipped}`);
