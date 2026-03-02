import fs from "node:fs";
import readline from "node:readline";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
    out[k] = v;
  }
  return out;
}

const args = parseArgs(process.argv);

const IN = args.in || "./publicData/properties/v44_baseDistricts_TierA_only.ndjson";
const OUT = args.out || "./publicData/zoning/zoningDistrictCoverage_allowlist_v1.json";
const MIN = Number(args.minAttachPct ?? 60);

const m = new Map();

const rl = readline.createInterface({
  input: fs.createReadStream(IN, { encoding: "utf8" }),
  crlfDelay: Infinity
});

for await (const line of rl) {
  const s = line.trim();
  if (!s) continue;

  let o;
  try { o = JSON.parse(s); } catch { continue; }

  if (o.address_tier !== "A") continue;

  const town = String(o.town || "UNKNOWN").toUpperCase();
  const st = o?.zoning?.attach?.status || "none";
  const mh = !!o?.zoning?.attach?.multiHit;

  if (!m.has(town)) m.set(town, { A: 0, attached: 0, multi: 0 });
  const r = m.get(town);
  r.A++;
  if (st === "attached") {
    r.attached++;
    if (mh) r.multi++;
  }
}

const rows = [...m.entries()].map(([town, r]) => {
  const attachPct = r.A ? (100 * r.attached / r.A) : 0;
  const multiPct = r.attached ? (100 * r.multi / r.attached) : 0;
  const covered = attachPct >= MIN;

  return {
    town,
    A: r.A,
    attached: r.attached,
    attachPct: Number(attachPct.toFixed(2)),
    multiPctOfAttached: Number(multiPct.toFixed(2)),
    covered
  };
}).sort((a, b) => b.attached - a.attached);

const allowlist = rows.filter(r => r.covered).map(r => r.town);

const payload = {
  created_at: new Date().toISOString(),
  source: IN,
  rule: `covered = attachPct >= ${MIN} (Tier A only)`,
  allowlist,
  rows
};

fs.mkdirSync("./publicData/zoning", { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(payload, null, 2), "utf8");

console.log("Wrote:", OUT);
console.log("Allowlist count:", allowlist.length);
console.log("Allowlist:", allowlist);
