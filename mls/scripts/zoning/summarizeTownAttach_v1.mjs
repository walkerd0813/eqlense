import fs from "node:fs";
import readline from "node:readline";

const f = process.argv[2];
if (!f) { console.error("Usage: node summarizeTownAttach_v1.mjs <ndjson>"); process.exit(1); }

const m = new Map();
const rl = readline.createInterface({ input: fs.createReadStream(f,"utf8"), crlfDelay: Infinity });

for await (const line of rl) {
  if (!line) continue;
  const o = JSON.parse(line);
  if (o.address_tier !== "A") continue;

  const town = String(o.town || "UNKNOWN").toUpperCase();
  const cc = Number(o?.zoning?.attach?.candidateCount ?? 0);
  const mh = !!o?.zoning?.attach?.multiHit;

  if (!m.has(town)) m.set(town, { A:0, attached:0, multi:0 });
  const r = m.get(town); r.A++;
  if (cc > 0) { r.attached++; if (mh) r.multi++; }
}

const rows = [...m.entries()].map(([town,r]) => ({
  town, A:r.A, attached:r.attached,
  attachPct: r.A ? +(100*r.attached/r.A).toFixed(2) : 0,
  multiPct: r.attached ? +(100*r.multi/r.attached).toFixed(2) : 0
})).sort((a,b)=>b.A-a.A);

console.table(rows.slice(0,30));
