import fs from "node:fs";
import readline from "node:readline";

function parseArgs() {
  const a = process.argv.slice(2);
  const out = {};
  for (let i = 0; i < a.length; i++) {
    if (a[i].startsWith("--")) out[a[i].slice(2)] = a[i + 1], i++;
  }
  return out;
}

const { in: inPath } = parseArgs();
if (!inPath) {
  console.error("Usage: node .\\mls\\scripts\\quickAddressStats_v1.mjs --in <path_to_ndjson>");
  process.exit(1);
}

const isZip5 = (z) => /^\d{5}$/.test(String(z ?? "").trim());
const isMissing = (v) => v == null || String(v).trim() === "";
const isBadNo = (v) => {
  const s = String(v ?? "").trim();
  if (!s) return true;
  if (/^0+$/.test(s)) return true;           // 0, 00, 000
  return false;
};


// “mail-like valid” (institutional-safe) — adjust as you wish
const isValidStreetNo = (v) => {
  const s = String(v ?? "").trim();
  if (!s) return false;
  if (/^0+$/.test(s)) return false;
  if (/^\d+$/.test(s)) return true;          // 12
  if (/^\d+[A-Za-z]$/.test(s)) return true;  // 12A
  if (/^\d+\s*1\/2$/.test(s)) return true;   // 12 1/2
  if (/^\d+\-\d+$/.test(s)) return true;     // 12-14
  return false;
};

const counts = {
  total: 0,
  mail_like: 0,
  missNo: 0,
  badNo: 0,
  missName: 0,
  missZip: 0,
  tiers: {},
  revalidate_outcome: {},
};

const rl = readline.createInterface({
  input: fs.createReadStream(inPath, { encoding: "utf8" }),
  crlfDelay: Infinity,
});

for await (const line of rl) {
  const t = line.trim();
  if (!t) continue;
  let row;
  try { row = JSON.parse(t); } catch { continue; }

  counts.total++;

  const street_no = row.street_no;
  const street_name = row.street_name;
  const zip = row.zip;

  if (isMissing(street_no)) counts.missNo++;
  else if (!isValidStreetNo(street_no)) counts.badNo++;

  if (isMissing(street_name)) counts.missName++;
  if (!isZip5(zip)) counts.missZip++;

  const tier = row.address_tier ?? "UNKNOWN";
  counts.tiers[tier] = (counts.tiers[tier] ?? 0) + 1;

  const outc = row.addr_authority_revalidate?.outcome;
  if (outc) counts.revalidate_outcome[outc] = (counts.revalidate_outcome[outc] ?? 0) + 1;

  const mailLike =
    isValidStreetNo(street_no) &&
    !isMissing(street_name) &&
    isZip5(zip);

  if (mailLike) counts.mail_like++;
}

const pct = (n) => (counts.total ? (100 * n / counts.total).toFixed(3) : "0.000");

console.log("=====================================");
console.log("Address Stats");
console.log("=====================================");
console.log("IN:", inPath);
console.log("total:", counts.total);
console.log("mail_like:", counts.mail_like, `(${pct(counts.mail_like)}%)`);
console.log("missNo:", counts.missNo, `(${pct(counts.missNo)}%)`);
console.log("badNo:", counts.badNo, `(${pct(counts.badNo)}%)`);
console.log("missName:", counts.missName, `(${pct(counts.missName)}%)`);
console.log("missZip:", counts.missZip, `(${pct(counts.missZip)}%)`);

console.log("\naddress_tier counts:");
console.log(counts.tiers);

console.log("\nrevalidate outcome counts:");
console.log(counts.revalidate_outcome);
