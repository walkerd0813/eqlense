import fs from "fs";
import path from "path";
import crypto from "crypto";

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const next = argv[i + 1];
    const v = (next && !next.startsWith("--")) ? (i++, next) : true;
    out[k] = v;
  }
  return out;
}

function normTown(t) {
  return String(t || "").trim().toLowerCase().replace(/\s+/g, "_");
}

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function scoreKeyForCode(k) {
  const s = k.toLowerCase();
  let score = 0;
  if (s.includes("code")) score += 6;
  if (s.includes("zone")) score += 5;
  if (s.includes("zoning")) score += 5;
  if (s.includes("dist")) score += 4;
  if (s.includes("symbol")) score += 3;
  if (s.includes("abbr")) score += 3;
  if (s.includes("type")) score += 1;
  if (s.includes("objectid") || s === "fid" || s.includes("shape") || s.includes("globalid")) score -= 10;
  return score;
}

function scoreKeyForName(k) {
  const s = k.toLowerCase();
  let score = 0;
  if (s.includes("name")) score += 7;
  if (s.includes("label")) score += 6;
  if (s.includes("desc")) score += 5;
  if (s.includes("district")) score += 4;
  if (s.includes("zone")) score += 2;
  if (s.includes("zoning")) score += 2;
  if (s.includes("objectid") || s === "fid" || s.includes("shape") || s.includes("globalid")) score -= 10;
  if (s.includes("code")) score -= 3; // push away from code-ish keys
  return score;
}

function normCode(x) {
  if (x == null) return null;
  const s = String(x).trim();
  if (!s) return null;
  // normalize spacing and case, keep hyphens
  return s
    .replace(/\s+/g, " ")
    .replace(/[–—]/g, "-")
    .trim()
    .toUpperCase();
}

function normName(x) {
  if (x == null) return null;
  const s = String(x).trim();
  if (!s) return null;
  return s.replace(/\s+/g, " ").trim();
}

function pickKeys(features, which) {
  // Inspect a sample of features and compute key stats
  const sample = features.slice(0, Math.min(features.length, 300));
  const stats = new Map(); // key -> {strCount, uniq:Set, avgLenSum, seen}
  for (const f of sample) {
    const p = (f && f.properties) || {};
    for (const [k, v] of Object.entries(p)) {
      if (v == null) continue;
      const sv = (typeof v === "string" || typeof v === "number") ? String(v).trim() : null;
      if (!sv) continue;

      const rec = stats.get(k) || { strCount: 0, uniq: new Set(), lenSum: 0, seen: 0 };
      rec.strCount += 1;
      rec.uniq.add(sv);
      rec.lenSum += sv.length;
      rec.seen += 1;
      stats.set(k, rec);
    }
  }

  const scored = [];
  for (const [k, rec] of stats.entries()) {
    const uniqCount = rec.uniq.size;
    const avgLen = rec.seen ? (rec.lenSum / rec.seen) : 0;
    // filter out obvious garbage
    if (uniqCount <= 1) continue;

    let base = 0;
    if (which === "code") base = scoreKeyForCode(k);
    if (which === "name") base = scoreKeyForName(k);

    // heuristic boosts:
    // - code: shorter-ish strings preferred
    // - name: longer-ish strings preferred
    if (which === "code") {
      if (avgLen >= 1 && avgLen <= 18) base += 3;
      if (avgLen > 40) base -= 3;
    } else {
      if (avgLen >= 6) base += 3;
      if (avgLen <= 4) base -= 2;
    }

    // uniqueness usually good
    base += Math.min(5, Math.log10(uniqCount + 1) * 2);

    scored.push({ key: k, score: base, uniqCount, avgLen: Math.round(avgLen * 10) / 10 });
  }

  scored.sort((a, b) => b.score - a.score);
  return scored.length ? scored[0].key : null;
}

async function main() {
  const args = parseArgs(process.argv);
  const zoningRoot = args.zoningRoot || ".\\publicData\\zoning";
  const asOf = args.asOf || new Date().toISOString().slice(0, 10);

  const rootAbs = path.resolve(zoningRoot);
  if (!fs.existsSync(rootAbs)) throw new Error(`Missing zoningRoot: ${rootAbs}`);

  const audit = {
    ran_at: new Date().toISOString(),
    zoningRoot: rootAbs,
    as_of: asOf,
    towns: [],
  };

  const townDirs = fs.readdirSync(rootAbs, { withFileTypes: true }).filter(d => d.isDirectory());

  for (const td of townDirs) {
    const town = td.name;
    const townNorm = normTown(town);
    const districtsDir = path.join(rootAbs, town, "districts");
    const basePath = path.join(districtsDir, "zoning_base.geojson");

    if (!fs.existsSync(basePath)) continue;

    const buf = fs.readFileSync(basePath);
    const srcHash = sha256(buf);

    let gj;
    try {
      gj = JSON.parse(buf.toString("utf8"));
    } catch {
      audit.towns.push({ town, townNorm, path: basePath, ok: false, reason: "JSON_PARSE_FAILED" });
      continue;
    }

    if (!gj || !Array.isArray(gj.features)) {
      audit.towns.push({ town, townNorm, path: basePath, ok: false, reason: "NO_FEATURES_ARRAY" });
      continue;
    }

    const features = gj.features;
    const codeKey = pickKeys(features, "code");
    const nameKey = pickKeys(features, "name");

    let codeFilled = 0;
    let nameFilled = 0;

    for (const f of features) {
      if (!f) continue;
      if (!f.properties) f.properties = {};
      const p = f.properties;

      const rawCode = codeKey ? p[codeKey] : null;
      const rawName = nameKey ? p[nameKey] : null;

      const dcRaw = rawCode != null ? String(rawCode).trim() : null;
      const dnRaw = rawName != null ? String(rawName).trim() : null;

      const dcNorm = normCode(dcRaw);
      const dnNorm = normName(dnRaw);

      if (dcRaw) codeFilled++;
      if (dnRaw) nameFilled++;

      // Standardized fields (what your engines consume later)
      p.jurisdiction_state = "MA";
      p.jurisdiction_name = townNorm;

      p.district_code_raw = dcRaw;
      p.district_name_raw = dnRaw;

      p.district_code_norm = dcNorm;
      p.district_name_norm = dnNorm;

      p.as_of_date = asOf;
      p.dataset_hash = srcHash;
      p.audit_disclaimer_tag = "informational_not_determination";
    }

    // Backup original
    const ts = new Date().toISOString().replace(/[:.]/g, "").replace("T", "_").slice(0, 15);
    const bak = path.join(districtsDir, `zoning_base__PRE_STD__${ts}.geojson`);
    fs.writeFileSync(bak, buf);

    // Write updated
    fs.writeFileSync(basePath, JSON.stringify(gj));

    audit.towns.push({
      town,
      townNorm,
      path: basePath,
      ok: true,
      features: features.length,
      inferred_code_key: codeKey,
      inferred_name_key: nameKey,
      codeFilled,
      nameFilled,
      srcHash,
      backup: bak,
    });

    console.log(`[DONE] ${townNorm} std fields added (codeKey=${codeKey || "null"} nameKey=${nameKey || "null"}) features=${features.length}`);
  }

  const auditPath = path.resolve(".\\publicData\\_audit", `zoning_base_std_fields__${new Date().toISOString().replace(/[:.]/g, "").replace("T","_").slice(0, 15)}.json`);
  fs.writeFileSync(auditPath, JSON.stringify(audit, null, 2));
  console.log("=====================================================");
  console.log(`[OK ] wrote audit: ${auditPath}`);
  console.log("=====================================================");
}

main().catch((e) => {
  console.error("[FATAL]", e);
  process.exit(1);
});
