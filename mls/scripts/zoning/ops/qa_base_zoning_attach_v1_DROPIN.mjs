import fs from "fs";
import path from "path";
import readline from "readline";

function nowIso() { return new Date().toISOString(); }

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const key = a.slice(2);
    const val = argv[i + 1];
    if (val && !val.startsWith("--")) { out[key] = val; i++; }
    else { out[key] = true; }
  }
  return out;
}

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

function normTown(s) {
  return String(s ?? "").trim().toLowerCase().replace(/\s+/g, "_");
}

function pickLon(o) {
  if (o && Number.isFinite(o.lon)) return o.lon;
  if (o && Number.isFinite(o.lng)) return o.lng;
  return null;
}

function inRange(x, lo, hi) { return x >= lo && x <= hi; }

/**
 * Compute a bounding box for a town zoning file WITHOUT full JSON parse.
 * We scan numbers and only keep MA-plausible lon/lat ranges.
 * This is a QA guard, not a geodetic truth source.
 */
async function computeTownBBoxStreaming(filePath, opts = {}) {
  const {
    lonLo = -74.0, lonHi = -69.0,
    latLo = 41.0, latHi = 43.7,
    logEveryBytes = 0
  } = opts;

  let minLon = Infinity, maxLon = -Infinity;
  let minLat = Infinity, maxLat = -Infinity;
  let lonCount = 0, latCount = 0;

  const rs = fs.createReadStream(filePath, { encoding: "utf8" });

  let bytes = 0;
  for await (const chunk of rs) {
    bytes += chunk.length;

    // Pull all numbers (int/float/exponent)
    const re = /-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g;
    let m;
    while ((m = re.exec(chunk)) !== null) {
      const n = Number(m[0]);
      if (!Number.isFinite(n)) continue;

      // MA plausible lon/lat windows
      if (inRange(n, lonLo, lonHi)) {
        lonCount++;
        if (n < minLon) minLon = n;
        if (n > maxLon) maxLon = n;
      } else if (inRange(n, latLo, latHi)) {
        latCount++;
        if (n < minLat) minLat = n;
        if (n > maxLat) maxLat = n;
      }
    }

    if (logEveryBytes && bytes >= logEveryBytes) {
      // reset counter but keep totals
      bytes = 0;
    }
  }

  if (lonCount === 0 || latCount === 0) return null;
  return { minLon, minLat, maxLon, maxLat, lonCount, latCount };
}

async function buildTownBBoxCache(zoningRoot, outDir) {
  const cachePath = path.join(outDir, "qa_town_bbox_cache.json");
  let cache = {};
  if (fs.existsSync(cachePath)) {
    try { cache = JSON.parse(fs.readFileSync(cachePath, "utf8")); }
    catch { cache = {}; }
  }

  const dirs = fs.readdirSync(zoningRoot, { withFileTypes: true })
    .filter(d => d.isDirectory())
    .map(d => d.name);

  console.log("====================================================");
  console.log("[STEP A] Build/Load town bbox cache (from zoning_base.geojson)");
  console.log("[INFO ] zoningRoot:", zoningRoot);
  console.log("[INFO ] cachePath :", cachePath);
  console.log("====================================================");

  let computed = 0, skipped = 0, missing = 0;
  for (const townDir of dirs) {
    const townNorm = normTown(townDir);
    const zFile = path.join(zoningRoot, townDir, "districts", "zoning_base.geojson");
    if (!fs.existsSync(zFile)) { missing++; continue; }

    const stat = fs.statSync(zFile);
    const cacheKey = `${townNorm}::${stat.size}::${stat.mtimeMs}`;

    if (cache[cacheKey]?.bbox) { skipped++; continue; }

    console.log(`[BBOX] computing ${townNorm} (sizeMB=${(stat.size/1024/1024).toFixed(2)}) ...`);
    const bbox = await computeTownBBoxStreaming(zFile);
    if (!bbox) {
      cache[cacheKey] = { town: townNorm, file: zFile, note: "NO_BBOX_FOUND" };
      console.log(`[BBOX] ${townNorm} -> NO_BBOX_FOUND`);
    } else {
      cache[cacheKey] = { town: townNorm, file: zFile, bbox };
      console.log(`[BBOX] ${townNorm} -> ${bbox.minLon.toFixed(6)},${bbox.minLat.toFixed(6)} .. ${bbox.maxLon.toFixed(6)},${bbox.maxLat.toFixed(6)}`);
    }
    computed++;
  }

  fs.writeFileSync(cachePath, JSON.stringify(cache, null, 2), "utf8");
  console.log("----------------------------------------------------");
  console.log(`[DONE] bbox cache saved. computed=${computed} skipped=${skipped} missingZFile=${missing}`);
  console.log("----------------------------------------------------");

  // Create a simple town->bbox map using the latest entry per town (any is fine for QA)
  const townBboxes = {};
  for (const k of Object.keys(cache)) {
    const rec = cache[k];
    if (rec?.town && rec?.bbox) townBboxes[rec.town] = rec.bbox;
  }
  return { townBboxes, cachePath };
}

function qaCodesForRow(o, townBboxes) {
  const codes = [];

  const townRaw = o?.town ?? "";
  const town = normTown(townRaw);
  const jur  = normTown(o?.jurisdiction_name ?? "");
  const lat  = Number.isFinite(o?.lat) ? o.lat : null;
  const lon  = pickLon(o);
  const tier = String(o?.address_tier ?? "").trim().toUpperCase();
  const method = String(o?.base_zone_attach_method ?? "").trim();
  const conf = Number.isFinite(o?.base_zone_confidence) ? o.base_zone_confidence : null;

  // TRA = Town/Region/Address / structural QA
  if (!townRaw || !String(townRaw).trim()) codes.push("TRA002_MISSING_TOWN");
  if (!(Number.isFinite(lat) && Number.isFinite(lon))) codes.push("TRA001_MISSING_COORDS");

  if (town && jur && town !== jur) codes.push("TRA003_TOWN_JURIS_MISMATCH");

  const bb = townBboxes[town];
  if (bb && Number.isFinite(lat) && Number.isFinite(lon)) {
    const inside =
      lon >= bb.minLon && lon <= bb.maxLon &&
      lat >= bb.minLat && lat <= bb.maxLat;
    if (!inside) codes.push("TRA004_OUTSIDE_TOWN_BBOX");
  }

  // ZON = zoning attach QA
  const isNoHit = (method === "point_in_poly_no_hit") || (conf === 0);
  if (isNoHit) {
    if (tier === "A") codes.push("ZON001_TIERA_NOHIT");
    else if (tier === "B") codes.push("ZON002_TIERB_NOHIT");
    else if (tier === "C") codes.push("ZON003_TIERC_NOHIT");
    else codes.push("ZON004_NOTIER_NOHIT");
  }

  const isHit = (conf === 1) || (method === "point_in_poly");
  if (isHit) {
    if (!o?.base_zone_evidence) codes.push("ZON005_HIT_MISSING_EVIDENCE");
    if (!String(o?.base_district_code ?? "").trim()) codes.push("ZON006_HIT_MISSING_CODE");
    if (!String(o?.base_district_name ?? "").trim()) codes.push("ZON007_HIT_MISSING_NAME");
  }

  if (codes.length === 0) codes.push("OK");
  return codes;
}

function toCsv(rows, headers) {
  const esc = (v) => {
    const s = String(v ?? "");
    if (/[,"\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  return [
    headers.join(","),
    ...rows.map(r => headers.map(h => esc(r[h])).join(",")),
  ].join("\n");
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const infile = args.in;
  const outDir = args.outDir || ".\\publicData\\_audit\\qa_base_zoning";
  const zoningRoot = args.zoningRoot || ".\\publicData\\zoning";
  const sampleN = Number(args.sampleN ?? 300);
  const logEvery = Number(args.logEvery ?? 250000);
  const heartbeatSec = Number(args.heartbeatSec ?? 15);

  if (!infile || !fs.existsSync(infile)) {
    console.error("Usage: node qa_base_zoning_attach_v1_DROPIN.mjs --in <ndjson> [--outDir <dir>] [--zoningRoot <dir>] [--sampleN 300] [--logEvery 250000] [--heartbeatSec 15]");
    process.exit(1);
  }

  ensureDir(outDir);

  console.log("====================================================");
  console.log("[START] QA Base Zoning Attach (codes + TRA + coord_source) v1");
  console.log("[INFO ] in        :", infile);
  console.log("[INFO ] outDir    :", outDir);
  console.log("[INFO ] zoningRoot:", zoningRoot);
  console.log("[INFO ] sampleN   :", sampleN);
  console.log("[INFO ] logEvery  :", logEvery);
  console.log("[INFO ] heartbeat :", `${heartbeatSec}s`);
  console.log("====================================================");

  const { townBboxes, cachePath } = await buildTownBBoxCache(zoningRoot, outDir);

  // Aggregations
  const qaTotals = new Map();                 // qa_code -> count
  const qaByCoordSource = new Map();          // `${qa_code}\t${coord_source}` -> count
  const coordSourceTotals = new Map();        // coord_source -> count
  const townTotals = new Map();               // town -> {lines, hit, nohit, traMismatch, outsideBBox}
  const codesPerRowMax = 8;

  // Samples (bounded)
  const samples = {
    tierA_nohit: [],
    tierB_nohit: [],
    tierC_nohit: [],
    town_juris_mismatch: [],
    outside_town_bbox: []
  };

  let lines = 0;
  let parseErr = 0;
  let lastBeat = Date.now();

  const rl = readline.createInterface({
    input: fs.createReadStream(infile, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    if (!line) continue;
    lines++;

    let o;
    try { o = JSON.parse(line); }
    catch { parseErr++; continue; }

    const town = normTown(o?.town ?? "");
    const coord_source = String(o?.coord_source ?? "UNKNOWN").trim() || "UNKNOWN";

    // totals by coord_source
    coordSourceTotals.set(coord_source, (coordSourceTotals.get(coord_source) ?? 0) + 1);

    // QA codes
    const codes = qaCodesForRow(o, townBboxes);

    // Per-town bucket
    if (!townTotals.has(town)) {
      townTotals.set(town, { town, lines: 0, hit: 0, nohit: 0, traMismatch: 0, outsideBBox: 0 });
    }
    const tRec = townTotals.get(town);
    tRec.lines++;

    const isHit = (o?.base_zone_confidence === 1) || (o?.base_zone_attach_method === "point_in_poly");
    const isNoHit = (o?.base_zone_attach_method === "point_in_poly_no_hit") || (o?.base_zone_confidence === 0);
    if (isHit) tRec.hit++;
    if (isNoHit) tRec.nohit++;

    if (codes.includes("TRA003_TOWN_JURIS_MISMATCH")) tRec.traMismatch++;
    if (codes.includes("TRA004_OUTSIDE_TOWN_BBOX")) tRec.outsideBBox++;

    // Aggregate counts
    for (const c of codes.slice(0, codesPerRowMax)) {
      qaTotals.set(c, (qaTotals.get(c) ?? 0) + 1);
      const k = `${c}\t${coord_source}`;
      qaByCoordSource.set(k, (qaByCoordSource.get(k) ?? 0) + 1);
    }

    // Samples (keep first N)
    if (codes.includes("ZON001_TIERA_NOHIT") && samples.tierA_nohit.length < sampleN) {
      samples.tierA_nohit.push({
        property_id: o?.property_id, parcel_id: o?.parcel_id, town: o?.town, jurisdiction_name: o?.jurisdiction_name,
        zip: o?.zip, lat: o?.lat, lon: pickLon(o),
        coord_source: o?.coord_source, coord_key_used: o?.coord_key_used,
        base_zone_attach_method: o?.base_zone_attach_method, base_zone_confidence: o?.base_zone_confidence
      });
    }
    if (codes.includes("ZON002_TIERB_NOHIT") && samples.tierB_nohit.length < sampleN) {
      samples.tierB_nohit.push({
        property_id: o?.property_id, parcel_id: o?.parcel_id, town: o?.town, jurisdiction_name: o?.jurisdiction_name,
        zip: o?.zip, lat: o?.lat, lon: pickLon(o),
        coord_source: o?.coord_source, coord_key_used: o?.coord_key_used,
        base_zone_attach_method: o?.base_zone_attach_method, base_zone_confidence: o?.base_zone_confidence
      });
    }
    if (codes.includes("ZON003_TIERC_NOHIT") && samples.tierC_nohit.length < sampleN) {
      samples.tierC_nohit.push({
        property_id: o?.property_id, parcel_id: o?.parcel_id, town: o?.town, jurisdiction_name: o?.jurisdiction_name,
        zip: o?.zip, lat: o?.lat, lon: pickLon(o),
        coord_source: o?.coord_source, coord_key_used: o?.coord_key_used,
        base_zone_attach_method: o?.base_zone_attach_method, base_zone_confidence: o?.base_zone_confidence
      });
    }
    if (codes.includes("TRA003_TOWN_JURIS_MISMATCH") && samples.town_juris_mismatch.length < sampleN) {
      samples.town_juris_mismatch.push({
        property_id: o?.property_id, parcel_id: o?.parcel_id,
        town: o?.town, jurisdiction_name: o?.jurisdiction_name,
        zip: o?.zip, lat: o?.lat, lon: pickLon(o),
        address_tier: o?.address_tier,
        coord_source: o?.coord_source, coord_key_used: o?.coord_key_used,
        base_zone_attach_method: o?.base_zone_attach_method, base_zone_confidence: o?.base_zone_confidence
      });
    }
    if (codes.includes("TRA004_OUTSIDE_TOWN_BBOX") && samples.outside_town_bbox.length < sampleN) {
      samples.outside_town_bbox.push({
        property_id: o?.property_id, parcel_id: o?.parcel_id,
        town: o?.town, jurisdiction_name: o?.jurisdiction_name,
        zip: o?.zip, lat: o?.lat, lon: pickLon(o),
        address_tier: o?.address_tier,
        coord_source: o?.coord_source, coord_key_used: o?.coord_key_used,
        base_zone_attach_method: o?.base_zone_attach_method, base_zone_confidence: o?.base_zone_confidence
      });
    }

    // Progress / heartbeat
    if (logEvery && lines % logEvery === 0) {
      const hit = qaTotals.get("OK") ?? 0;
      console.log(`[PROG] ${nowIso()} lines=${lines.toLocaleString()} parseErr=${parseErr} OK=${hit.toLocaleString()}`);
    }
    if (heartbeatSec && (Date.now() - lastBeat) >= heartbeatSec * 1000) {
      lastBeat = Date.now();
      console.log(`[BEAT] ${nowIso()} lines=${lines.toLocaleString()} parseErr=${parseErr} towns=${townTotals.size}`);
    }
  }

  // Write outputs
  const qaTotalsArr = Array.from(qaTotals.entries())
    .map(([qa_code, count]) => ({ qa_code, count }))
    .sort((a, b) => b.count - a.count);

  const qaByCoordArr = Array.from(qaByCoordSource.entries())
    .map(([k, count]) => {
      const [qa_code, coord_source] = k.split("\t");
      return { qa_code, coord_source, count };
    })
    .sort((a, b) => b.count - a.count);

  const coordTotalsArr = Array.from(coordSourceTotals.entries())
    .map(([coord_source, count]) => ({ coord_source, count }))
    .sort((a, b) => b.count - a.count);

  const townArr = Array.from(townTotals.values())
    .map(r => ({
      town: r.town,
      lines: r.lines,
      hit: r.hit,
      nohit: r.nohit,
      hitRatePct: r.lines ? (100 * r.hit / r.lines).toFixed(2) : "0.00",
      nohitRatePct: r.lines ? (100 * r.nohit / r.lines).toFixed(2) : "0.00",
      traMismatch: r.traMismatch,
      outsideBBox: r.outsideBBox
    }))
    .sort((a, b) => b.lines - a.lines);

  const outSummary = {
    created_at: nowIso(),
    infile,
    outDir,
    zoningRoot,
    bbox_cache: cachePath,
    lines,
    parseErr,
    note: "base_zone_confidence is 0/1 where 1 means attached=true (point_in_poly).",
    totals: qaTotalsArr,
    sample_counts: Object.fromEntries(Object.entries(samples).map(([k, v]) => [k, v.length]))
  };

  fs.writeFileSync(path.join(outDir, "qa_summary.json"), JSON.stringify(outSummary, null, 2), "utf8");
  fs.writeFileSync(path.join(outDir, "qa_totals.csv"), toCsv(qaTotalsArr, ["qa_code", "count"]), "utf8");
  fs.writeFileSync(path.join(outDir, "qa_by_coord_source.csv"), toCsv(qaByCoordArr, ["qa_code", "coord_source", "count"]), "utf8");
  fs.writeFileSync(path.join(outDir, "coord_source_totals.csv"), toCsv(coordTotalsArr, ["coord_source", "count"]), "utf8");
  fs.writeFileSync(path.join(outDir, "qa_by_town.csv"), toCsv(townArr, ["town","lines","hit","nohit","hitRatePct","nohitRatePct","traMismatch","outsideBBox"]), "utf8");

  fs.writeFileSync(path.join(outDir, "qa_sample_tierA_nohit.json"), JSON.stringify(samples.tierA_nohit, null, 2), "utf8");
  fs.writeFileSync(path.join(outDir, "qa_sample_tierB_nohit.json"), JSON.stringify(samples.tierB_nohit, null, 2), "utf8");
  fs.writeFileSync(path.join(outDir, "qa_sample_tierC_nohit.json"), JSON.stringify(samples.tierC_nohit, null, 2), "utf8");
  fs.writeFileSync(path.join(outDir, "qa_sample_town_juris_mismatch.json"), JSON.stringify(samples.town_juris_mismatch, null, 2), "utf8");
  fs.writeFileSync(path.join(outDir, "qa_sample_outside_town_bbox.json"), JSON.stringify(samples.outside_town_bbox, null, 2), "utf8");

  console.log("----------------------------------------------------");
  console.log("[DONE] QA outputs written:");
  console.log(" - qa_summary.json");
  console.log(" - qa_totals.csv");
  console.log(" - qa_by_coord_source.csv   (THIS is the 'accounts by code source')");
  console.log(" - coord_source_totals.csv");
  console.log(" - qa_by_town.csv");
  console.log(" - qa_sample_*.json");
  console.log("----------------------------------------------------");
  console.log(`[DONE] lines=${lines.toLocaleString()} parseErr=${parseErr.toLocaleString()} towns=${townTotals.size}`);
  console.log("====================================================");
}

main().catch(err => {
  console.error("[FAIL]", err?.stack || err);
  process.exit(1);
});
