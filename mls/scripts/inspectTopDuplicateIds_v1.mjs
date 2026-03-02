import fs from "node:fs";
import readline from "node:readline";

const inPath = process.argv[2];
const outPath = process.argv[3] || "topDuplicateIds.json";
const TOPN = Number(process.argv[4] || 50);

if (!inPath) {
  console.error("Usage: node inspectTopDuplicateIds_v1.mjs <in.ndjson> [out.json] [topN]");
  process.exit(1);
}

const normTown = (s) =>
  String(s ?? "")
    .toUpperCase()
    .trim()
    .replace(/\bTOWN OF\b/g, "")
    .replace(/\bCITY OF\b/g, "")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const pickId = (o) => o?.property_id || o?.parcel_id || null;

const counts = new Map();
let total = 0;

const rl = readline.createInterface({
  input: fs.createReadStream(inPath, { encoding: "utf8" }),
  crlfDelay: Infinity,
});

for await (const line of rl) {
  const t = line.trim();
  if (!t) continue;
  total++;
  const o = JSON.parse(t);
  const id = pickId(o);
  if (!id) continue;
  counts.set(id, (counts.get(id) || 0) + 1);
  if (total % 500000 === 0) console.log(`...counted ${total.toLocaleString()} rows`);
}

// pick topN ids by count
let top = [];
for (const [id, n] of counts.entries()) {
  if (top.length < TOPN) top.push([id, n]);
  else {
    // replace smallest if bigger
    let minIdx = 0;
    for (let i = 1; i < top.length; i++) if (top[i][1] < top[minIdx][1]) minIdx = i;
    if (n > top[minIdx][1]) top[minIdx] = [id, n];
  }
}
top.sort((a, b) => b[1] - a[1]);

// second pass: collect towns + sample rows for those ids
const targetIds = new Set(top.map(([id]) => id));
const townsById = new Map();
const sampleById = new Map();

const rl2 = readline.createInterface({
  input: fs.createReadStream(inPath, { encoding: "utf8" }),
  crlfDelay: Infinity,
});

let scanned2 = 0;
for await (const line of rl2) {
  const t = line.trim();
  if (!t) continue;
  scanned2++;
  const o = JSON.parse(t);
  const id = pickId(o);
  if (!id || !targetIds.has(id)) continue;

  const town = normTown(o.town);
  if (!townsById.has(id)) townsById.set(id, new Map());
  const m = townsById.get(id);
  m.set(town || "(missing town)", (m.get(town || "(missing town)") || 0) + 1);

  if (!sampleById.has(id)) sampleById.set(id, []);
  const arr = sampleById.get(id);
  if (arr.length < 3) {
    arr.push({
      town: o.town ?? null,
      parcel_id: o.parcel_id ?? null,
      property_id: o.property_id ?? null,
      lat: o.lat ?? null,
      lng: o.lng ?? o.lon ?? null,
      address_label: o.address_label ?? null,
    });
  }

  if (scanned2 % 500000 === 0) console.log(`...scanned2 ${scanned2.toLocaleString()} rows`);
}

const result = {
  in: inPath,
  total_rows: total,
  top: top.map(([id, n]) => {
    const townMap = townsById.get(id) || new Map();
    const towns = Array.from(townMap.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);
    return { id, count: n, topTowns: towns, samples: sampleById.get(id) || [] };
  }),
};

fs.writeFileSync(outPath, JSON.stringify(result, null, 2));
console.log("DONE. Wrote:", outPath);
