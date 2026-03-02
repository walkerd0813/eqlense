import fs from "node:fs";
import readline from "node:readline";

async function loadIds(file, set){
  if(!fs.existsSync(file)) return;
  const rl = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity });
  for await (const line of rl){
    const l = line.trim();
    if(!l) continue;
    try { const o = JSON.parse(l); if(o?.listingId) set.add(o.listingId); } catch {}
  }
}

const [INPUT, GOOD, BAD, OUT] = process.argv.slice(2);
if(!INPUT || !GOOD || !BAD || !OUT){
  console.error("Usage: node buildRemainingFromGeocode.mjs <INPUT> <GOOD> <BAD> <OUT>");
  process.exit(1);
}

const seen = new Set();
await loadIds(GOOD, seen);
await loadIds(BAD, seen);

let scanned=0, kept=0, skipped=0, badJson=0;
const out = fs.createWriteStream(OUT, { flags:"w", encoding:"utf8" });

const rl = readline.createInterface({ input: fs.createReadStream(INPUT), crlfDelay: Infinity });
for await (const line of rl){
  const l = line.trim();
  if(!l) continue;
  scanned++;

  let id = null;
  try { id = JSON.parse(l)?.listingId; } catch { badJson++; continue; }

  if(id && seen.has(id)){ skipped++; continue; }

  out.write(l + "\n");
  kept++;

  if(scanned % 5000 === 0) console.log(`[remain] scanned=${scanned} kept=${kept} skipped=${skipped}`);
}

out.end();
console.log(`DONE. remaining=${kept} skipped=${skipped} badJson=${badJson} -> ${OUT}`);
