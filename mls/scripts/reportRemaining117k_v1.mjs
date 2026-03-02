import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

function parseArgs() {
  const a = process.argv.slice(2);
  const out = {};
  for (let i = 0; i < a.length; i++) {
    if (a[i].startsWith("--")) {
      const k = a[i].slice(2);
      const v = a[i + 1] && !a[i + 1].startsWith("--") ? a[++i] : "1";
      out[k] = v;
    }
  }
  return out;
}

const args = parseArgs();
const inPath = args.in;
const outPath =
  args.out ||
  path.join(process.cwd(), "publicData", "properties", "remaining117k_report.json");

if (!inPath) {
  console.error(
    "Usage: node .\\mls\\scripts\\reportRemaining117k_v1.mjs --in <ndjson> [--out <report.json>]"
  );
  process.exit(1);
}

const isMissing = (v) => v == null || String(v).trim() === "";

const isZip5 = (z) => /^\d{5}$/.test(String(z ?? "").trim());
const isZip5or9 = (z) => {
  const s = String(z ?? "").trim();
  return /^\d{5}$/.test(s) || /^\d{5}-\d{4}$/.test(s);
};

const isValidStreetNoStrict = (v) => {
  const s = String(v ?? "").trim();
  if (!s) return false;
  if (/^0+$/.test(s)) return false;
  if (/^\d+$/.test(s)) return true;          // 12
  if (/^\d+[A-Za-z]$/.test(s)) return true;  // 12A
  if (/^\d+\s*1\/2$/.test(s)) return true;   // 12 1/2
  if (/^\d+\-\d+$/.test(s)) return true;     // 12-14
  return false;
};

// “Populated” (for overlap insight)
const isPopulatedStreetNo = (v) => {
  const s = String(v ?? "").trim();
  if (!s) return false;
  if (/^0+$/.test(s)) return false;
  return true;
};

// BadNo pattern buckets (for diagnosis)
function badNoBuckets(streetNoRaw) {
  const s0 = String(streetNoRaw ?? "").trim();
  const s = s0.toUpperCase();

  const b = {
    zero_like: /^0+$/.test(s0),                      // 0 / 00 / 000
    non_numeric_start: s0.length ? !/^\d/.test(s0) : true,
    has_letters: /[A-Z]/.test(s),
    has_slash: /\//.test(s0),
    has_dash: /\-/.test(s0),
    has_space: /\s/.test(s0),
    has_hash: /#/.test(s0),
    has_amp: /&/.test(s0),
    range_like: /^\d+\-\d+$/.test(s0),
    fraction_like: /^\d+\s*1\/2$/.test(s0),
    alpha_suffix_like: /^\d+[A-Z]$/.test(s),
    alpha_suffix_long: /^\d+[A-Z]{2,}$/.test(s),     // 12AB (would be “maybe-valid” if you allowed)
    decimal_like: /^\d+\.\d+$/.test(s0),
    placeholder_token: /(UNKNOWN|UNK|N\/A|NA|NONE|NO\s?NUMBER|NONUM|TBD)/.test(s),
    rear_like: /\bREAR\b/.test(s),
    lot_like: /\bLOT\b|\bTRACT\b|\bPARCEL\b/.test(s),
    unit_like: /\bUNIT\b|\bAPT\b|\bSUITE\b/.test(s),
    po_box_like: /\bP\.?\s*O\.?\s*BOX\b|\bPO BOX\b/.test(s),
  };

  // A quick “would become strict-valid if we only cleaned formatting” flag:
  // ex: "12 A" -> could normalize to "12A"
  b.maybe_fixable_by_compaction =
    /^\d+\s+[A-Z]$/.test(s) || /^\d+\s+\d\/\d$/.test(s);

  return b;
}

function tierKey(row) {
  const t = row.address_tier ?? "UNKNOWN";
  return String(t);
}

function inc(obj, k, n = 1) {
  obj[k] = (obj[k] ?? 0) + n;
}

function inc2(obj, k1, k2, n = 1) {
  obj[k1] = obj[k1] ?? {};
  obj[k1][k2] = (obj[k1][k2] ?? 0) + n;
}

const report = {
  in: inPath,
  out: outPath,
  totals: {
    total_rows: 0,
    strict_mail_like: 0,
    strict_fail: 0,
    populated_triplet: 0, // has street_no(not 0), street_name, zip(5 or 9)
  },
  fail_breakdown: {
    missNo: 0,
    badNo: 0,
    missName: 0,
    missZip: 0,
    // overlaps (bitmask counts)
    overlap_mask_counts: {}, // e.g., "No|Name" etc.
  },
  fail_by_tier: {}, // tier -> counts
  fail_badNo_subbuckets: {}, // bucket -> counts
  badNo_by_tier: {}, // tier -> counts
  missNo_by_tier: {}, // tier -> counts
  missName_by_tier: {}, // tier -> counts
  missZip_by_tier: {}, // tier -> counts
  sample_badNo_values: {
    enabled: true,
    max_unique: 5000,
    counts: {}, // raw street_no -> count (capped)
    truncated: false,
  },
  timestamp: new Date().toISOString(),
};

const rs = fs.createReadStream(inPath, { encoding: "utf8" });
const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });

for await (const line of rl) {
  const t = line.trim();
  if (!t) continue;

  let row;
  try {
    row = JSON.parse(t);
  } catch {
    continue;
  }

  report.totals.total_rows++;

  const street_no = row.street_no;
  const street_name = row.street_name;
  const zip = row.zip;

  const strictMailLike =
    isValidStreetNoStrict(street_no) &&
    !isMissing(street_name) &&
    isZip5(zip);

  const populatedTriplet =
    isPopulatedStreetNo(street_no) &&
    !isMissing(street_name) &&
    isZip5or9(zip);

  if (populatedTriplet) report.totals.populated_triplet++;
  if (strictMailLike) {
    report.totals.strict_mail_like++;
    continue;
  }

  // Fail case
  report.totals.strict_fail++;

  const tier = tierKey(row);
  inc(report.fail_by_tier, tier);

  const missNo = isMissing(street_no) || /^0+$/.test(String(street_no ?? "").trim());
  const missName = isMissing(street_name);
  const missZip = !isZip5(zip);

  const badNo = !missNo && !isValidStreetNoStrict(street_no);

  if (missNo) { report.fail_breakdown.missNo++; inc(report.missNo_by_tier, tier); }
  if (badNo) { report.fail_breakdown.badNo++; inc(report.badNo_by_tier, tier); }
  if (missName) { report.fail_breakdown.missName++; inc(report.missName_by_tier, tier); }
  if (missZip) { report.fail_breakdown.missZip++; inc(report.missZip_by_tier, tier); }

  // overlap mask (human readable)
  const parts = [];
  if (missNo) parts.push("missNo");
  if (badNo) parts.push("badNo");
  if (missName) parts.push("missName");
  if (missZip) parts.push("missZip");
  const mask = parts.length ? parts.join("|") : "other";
  inc(report.fail_breakdown.overlap_mask_counts, mask);

  // badNo deeper buckets
  if (badNo) {
    const buckets = badNoBuckets(street_no);
    for (const [k, v] of Object.entries(buckets)) {
      if (v) inc(report.fail_badNo_subbuckets, k);
    }

    // optional sample values (capped)
    const samp = report.sample_badNo_values;
    if (samp.enabled && !samp.truncated) {
      const raw = String(street_no ?? "").trim();
      if (raw) {
        if (samp.counts[raw] != null) samp.counts[raw]++;
        else {
          const uniq = Object.keys(samp.counts).length;
          if (uniq < samp.max_unique) samp.counts[raw] = 1;
          else samp.truncated = true;
        }
      }
    }
  }

  // progress
  if (report.totals.total_rows % 500000 === 0) {
    console.log(
      `...scanned ${report.totals.total_rows.toLocaleString()} | strict_fail=${report.totals.strict_fail.toLocaleString()}`
    );
  }
}

// Sort sample_badNo_values counts (top 50) for readability
if (report.sample_badNo_values.enabled) {
  const entries = Object.entries(report.sample_badNo_values.counts);
  entries.sort((a, b) => b[1] - a[1]);
  const top = entries.slice(0, 50);
  report.sample_badNo_values.top50 = top.map(([value, count]) => ({ value, count }));
  // Keep full map too (capped), but top50 is what you’ll look at first
}

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

const pct = (n, d) => (d ? (100 * n / d).toFixed(3) : "0.000");

console.log("=====================================");
console.log("Remaining Strict-Fail Report");
console.log("=====================================");
console.log("IN :", inPath);
console.log("OUT:", outPath);
console.log("total_rows:", report.totals.total_rows);
console.log("strict_mail_like:", report.totals.strict_mail_like, `(${pct(report.totals.strict_mail_like, report.totals.total_rows)}%)`);
console.log("strict_fail:", report.totals.strict_fail, `(${pct(report.totals.strict_fail, report.totals.total_rows)}%)`);
console.log("populated_triplet:", report.totals.populated_triplet, `(${pct(report.totals.populated_triplet, report.totals.total_rows)}%)`);
console.log("\nFail breakdown (non-exclusive):");
console.log(report.fail_breakdown);
console.log("\nFail by tier:");
console.log(report.fail_by_tier);
console.log("\nBadNo sub-buckets:");
console.log(report.fail_badNo_subbuckets);
if (report.sample_badNo_values.enabled) {
  console.log("\nTop 10 badNo raw values:");
  console.log(report.sample_badNo_values.top50?.slice(0, 10) ?? []);
  if (report.sample_badNo_values.truncated) {
    console.log("[note] badNo value sampling truncated at max_unique =", report.sample_badNo_values.max_unique);
  }
}
