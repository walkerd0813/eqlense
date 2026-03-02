import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

function nowIso() { return new Date().toISOString(); }
function log(msg) { process.stdout.write(msg + "\n"); }
function mb(n) { return (n / 1024 / 1024).toFixed(1) + " MB"; }
function memSnap() {
  const m = process.memoryUsage();
  return { rss: mb(m.rss), heapUsed: mb(m.heapUsed), heapTotal: mb(m.heapTotal) };
}

function normKey(s) {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .replace(/_+/g, "_");
}

function listDirs(p) {
  if (!fs.existsSync(p)) return [];
  return fs.readdirSync(p, { withFileTypes: true })
    .filter(d => d.isDirectory())
    .map(d => d.name);
}

function listGeoJSONFilesRec(p) {
  const out = [];
  if (!fs.existsSync(p)) return out;
  const st = fs.statSync(p);
  if (st.isFile()) {
    if (p.toLowerCase().endsWith(".geojson")) out.push(p);
    return out;
  }
  for (const ent of fs.readdirSync(p, { withFileTypes: true })) {
    const full = path.join(p, ent.name);
    if (ent.isDirectory()) out.push(...listGeoJSONFilesRec(full));
    else if (ent.isFile() && ent.name.toLowerCase().endsWith(".geojson")) out.push(full);
  }
  return out;
}

function safeReadJSON(file) {
  const size = fs.statSync(file).size;
  log("[FILE] reading " + file + " (" + mb(size) + ")");
  const raw = fs.readFileSync(file, "utf8");
  log("[FILE] parsing " + file + " (" + mb(raw.length) + ")");
  const obj = JSON.parse(raw);
  log("[FILE] parsed  " + file + " features=" + (Array.isArray(obj.features) ? obj.features.length : 0));
  return obj;
}

function inferTownField(sampleObj) {
  const keys = Object.keys(sampleObj || {});
  const lower = keys.map(k => k.toLowerCase());
  const prefer = ["town","city","municipality","muni","mail_city","site_city"];
  for (const p of prefer) {
    const idx = lower.indexOf(p);
    if (idx !== -1) return keys[idx];
  }
  return null;
}

function coordLooksLatLon(x, y) {
  const lon = Number(x), lat = Number(y);
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return false;
  if (lon < -180 || lon > 180) return false;
  if (lat < -90 || lat > 90) return false;
  return true;
}
function coordLooksMA(lon, lat) {
  lon = Number(lon); lat = Number(lat);
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return false;
  return (lat >= 41.0 && lat <= 43.8 && lon >= -73.6 && lon <= -69.0);
}

function bboxFromCoords(coords, bbox) {
  for (const c of coords) {
    if (Array.isArray(c[0])) bboxFromCoords(c, bbox);
    else {
      const x = Number(c[0]), y = Number(c[1]);
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
      bbox.minX = Math.min(bbox.minX, x);
      bbox.minY = Math.min(bbox.minY, y);
      bbox.maxX = Math.max(bbox.maxX, x);
      bbox.maxY = Math.max(bbox.maxY, y);
    }
  }
}
function computeBbox(geom) {
  const bbox = { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity };
  bboxFromCoords(geom.coordinates, bbox);
  return bbox;
}

function looksWebMercator(b) {
  // EPSG:3857 typical ranges (meters)
  const okX = Number.isFinite(b.minX) && Number.isFinite(b.maxX) && Math.max(Math.abs(b.minX), Math.abs(b.maxX)) > 180 && Math.max(Math.abs(b.minX), Math.abs(b.maxX)) <= 20037508.4;
  const okY = Number.isFinite(b.minY) && Number.isFinite(b.maxY) && Math.max(Math.abs(b.minY), Math.abs(b.maxY)) > 90  && Math.max(Math.abs(b.minY), Math.abs(b.maxY)) <= 20048966.1;
  return okX && okY;
}
function mercatorToLonLat(x, y) {
  const R = 6378137;
  const lon = (Number(x) / R) * (180 / Math.PI);
  const lat = (2 * Math.atan(Math.exp(Number(y) / R)) - (Math.PI / 2)) * (180 / Math.PI);
  return [lon, lat];
}
function transformCoords3857to4326(coords) {
  if (!Array.isArray(coords)) return coords;
  if (coords.length && Array.isArray(coords[0])) return coords.map(transformCoords3857to4326);
  if (coords.length >= 2 && Number.isFinite(Number(coords[0])) && Number.isFinite(Number(coords[1]))) {
    return mercatorToLonLat(coords[0], coords[1]);
  }
  return coords;
}
function transformGeom3857to4326(geom) {
  return { ...geom, coordinates: transformCoords3857to4326(geom.coordinates) };
}

function pointInRing(pt, ring) {
  const x = pt[0], y = pt[1];
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0], yi = ring[i][1];
    const xj = ring[j][0], yj = ring[j][1];
    const intersect = ((yi > y) !== (yj > y)) &&
      (x < (xj - xi) * (y - yi) / ((yj - yi) || 1e-12) + xi);
    if (intersect) inside = !inside;
  }
  return inside;
}
function pointInPolygon(pt, polyCoords) {
  const outer = polyCoords[0];
  if (!outer || outer.length < 3) return false;
  if (!pointInRing(pt, outer)) return false;
  for (let h = 1; h < polyCoords.length; h++) {
    const hole = polyCoords[h];
    if (hole && hole.length >= 3 && pointInRing(pt, hole)) return false;
  }
  return true;
}
function pointInGeom(pt, geom) {
  if (!geom || !geom.type) return false;
  if (geom.type === "Polygon") return pointInPolygon(pt, geom.coordinates);
  if (geom.type === "MultiPolygon") {
    for (const poly of geom.coordinates) {
      if (pointInPolygon(pt, poly)) return true;
    }
    return false;
  }
  return false;
}

function pickZoneFields(features) {
  if (!features.length) return { code: null, name: null };
  const props = features[0].properties || {};
  const keys = Object.keys(props);
  const lowerMap = new Map(keys.map(k => [k.toLowerCase(), k]));

  const codePrefs = ["zonecode","zone_code","zoning","zone","district","zoneclass","zonedist","zoningdist","district_n"];
  const namePrefs = ["zonename","zone_name","name","zoningname","district_n","district","zone","zoneclass"];

  function find(prefs) {
    for (const p of prefs) {
      const hit = lowerMap.get(p.toLowerCase());
      if (hit) return hit;
    }
    return null;
  }
  return { code: find(codePrefs), name: find(namePrefs) };
}

function classifyZoningFiles(zoningRoot, cityFolder) {
  const cityPath = path.join(zoningRoot, cityFolder);

  const buckets = [
    { sub: "districts", kind: "base" },
    { sub: "overlays", kind: "overlay" },
    { sub: "subdistricts", kind: "overlay" },
    { sub: "historic", kind: "overlay" },
    { sub: "environmental", kind: "overlay" },
    { sub: "_normalized", kind: "overlay" }
  ];

  const files = [];
  for (const b of buckets) {
    const p = path.join(cityPath, b.sub);
    for (const f of listGeoJSONFilesRec(p)) files.push({ file: f, kind: b.kind, bucket: b.sub });
  }
  return files;
}

function loadLayer(file, kind, bucket) {
  const gj = safeReadJSON(file);
  const feats = Array.isArray(gj.features) ? gj.features : [];

  const clean = [];
  let crsMode = "4326";
  let reprojCount = 0;
  let badCount = 0;

  for (let idx = 0; idx < feats.length; idx++) {
    const f = feats[idx];
    let g = f && f.geometry;
    if (!g || !g.type || !g.coordinates) continue;
    if (g.type !== "Polygon" && g.type !== "MultiPolygon") continue;

    // CRS preflight per-feature via bbox
    let bb = computeBbox(g);

    if (!coordLooksLatLon(bb.minX, bb.minY)) {
      // Try WebMercator->WGS84
      if (looksWebMercator(bb)) {
        g = transformGeom3857to4326(g);
        bb = computeBbox(g);
        reprojCount++;
        crsMode = "3857->4326";
      } else {
        badCount++;
      }
    }

    clean.push({ file, idx, kind, bucket, properties: f.properties || {}, geometry: g, bbox: bb });
  }

  // Determine file-level "ok" by sampling first kept feature bbox
  let firstCoordOk = true;
  if (clean.length) {
    const bb = clean[0].bbox;
    firstCoordOk = coordLooksLatLon(bb.minX, bb.minY);
  }

  if (!firstCoordOk) {
    const sample = clean.length ? clean[0].bbox : null;
    log("[WARN] CRS check still failing for file: " + file + " sample_bbox=" + JSON.stringify(sample));
  }

  log("[FILE] kept   " + clean.length.toLocaleString() + " polygon feats :: " + file + " :: crs=" + crsMode + " reproj=" + reprojCount + " bad=" + badCount);
  return { feats: clean, firstCoordOk, crsMode, reprojCount, badCount };
}

function parseArgs() {
  const a = process.argv.slice(2);
  const get = (name, def = null) => {
    const i = a.indexOf("--" + name);
    if (i === -1) return def;
    return a[i + 1] ?? def;
  };
  return {
    parcelsIn: get("parcelsIn"),
    out: get("out"),
    zoningRoot: get("zoningRoot"),
    auditOut: get("auditOut"),
    logEvery: Number(get("logEvery", "5000")),
    heartbeatSec: Number(get("heartbeatSec", "10")),
    failOnBadCRS: String(get("failOnBadCRS", "false")).toLowerCase() === "true"
  };
}

async function main() {
  const args = parseArgs();
  if (!args.parcelsIn || !args.out || !args.zoningRoot || !args.auditOut) {
    throw new Error("Missing required args: --parcelsIn --out --zoningRoot --auditOut");
  }

  const started = Date.now();
  let phase = "boot";
  let seen = 0;
  let written = 0;

  log("=====================================================");
  log("[START] Tier-A zoning attach (all cities) v3");
  log("[INFO ] parcelsIn : " + args.parcelsIn);
  log("[INFO ] zoningRoot: " + args.zoningRoot);
  log("[INFO ] out       : " + args.out);
  log("[INFO ] auditOut  : " + args.auditOut);
  log("[INFO ] logEvery  : " + args.logEvery);
  log("[INFO ] heartbeat : " + args.heartbeatSec + "s");
  log("[INFO ] failOnBadCRS : " + args.failOnBadCRS);
  log("=====================================================");

  const hb = setInterval(() => {
    const secs = Math.max(1, Math.round((Date.now() - started) / 1000));
    const rate = (seen / secs).toFixed(1);
    const m = memSnap();
    log("[HB  ] phase=" + phase + " seen=" + seen.toLocaleString() + " written=" + written.toLocaleString() +
      " rate=" + rate + "/s rss=" + m.rss + " heap=" + m.heapUsed);
  }, Math.max(2, args.heartbeatSec) * 1000);

  phase = "discover_cities";
  const cityFolders = listDirs(args.zoningRoot).filter(c =>
    !c.startsWith("_") && c !== "articles" && c !== "audit" && c !== "compare"
  );
  log("[INFO ] zoningRoot city folders detected: " + cityFolders.length);

  phase = "load_zoning_layers";
  const cityData = new Map();
  const badCrsCities = [];

  for (const cityFolder of cityFolders) {
    const zoningFiles = classifyZoningFiles(args.zoningRoot, cityFolder);
    if (!zoningFiles.length) continue;

    log("[CITY ] " + cityFolder + " :: files=" + zoningFiles.length);

    let all = [];
    let anyBadSR = false;
    let reprojTotal = 0;

    for (const zf of zoningFiles) {
      const loaded = loadLayer(zf.file, zf.kind, zf.bucket);
      if (!loaded.firstCoordOk) anyBadSR = true;
      reprojTotal += loaded.reprojCount;
      all.push(...loaded.feats);
    }

    const baseFeats = all.filter(f => f.kind === "base");
    const overlayFeats = all.filter(f => f.kind === "overlay");

    const entry = {
      cityFolder,
      crsInvalid: anyBadSR,
      reprojTotal,
      base: { feats: baseFeats, fields: pickZoneFields(baseFeats) },
      overlay: { feats: overlayFeats, fields: pickZoneFields(overlayFeats) }
    };

    cityData.set(cityFolder, entry);

    if (anyBadSR) {
      badCrsCities.push({ city: cityFolder, reprojTotal, base: baseFeats.length, overlay: overlayFeats.length });
      log("[WARN] city CRS invalid after WebMercator fix: " + cityFolder + " (base=" + baseFeats.length + " overlay=" + overlayFeats.length + " reproj=" + reprojTotal + ")");
    } else {
      log("[LOAD] " + cityFolder + " :: base=" + baseFeats.length + " overlay=" + overlayFeats.length + " reproj=" + reprojTotal);
    }
  }

  if (!cityData.size) {
    clearInterval(hb);
    throw new Error("No usable zoning layers found under zoningRoot.");
  }

  if (badCrsCities.length && args.failOnBadCRS) {
    clearInterval(hb);
    throw new Error("CRS preflight failed for " + badCrsCities.length + " city folder(s). First failing city: " + badCrsCities[0].city);
  }

  phase = "stream_parcels_and_attach";
  const inStream = fs.createReadStream(args.parcelsIn, { encoding: "utf8" });
  const rl = readline.createInterface({ input: inStream, crlfDelay: Infinity });
  const outStream = fs.createWriteStream(args.out, { encoding: "utf8" });

  const perCity = {};
  function bump(city, key, n = 1) {
    perCity[city] = perCity[city] || { seen: 0, baseHit: 0, overlayHits: 0, noTown: 0, townNoZoning: 0, noCoords: 0, crsInvalid: 0 };
    perCity[city][key] = (perCity[city][key] || 0) + n;
  }

  let townField = null;

  for await (const line of rl) {
    if (!line.trim()) continue;

    let obj;
    try { obj = JSON.parse(line); }
    catch { continue; }

    if (!townField) {
      townField = inferTownField(obj);
      log("[INFO] detected town field: " + (townField ?? "(none)"));
    }

    seen++;
    if (seen % args.logEvery === 0) {
      log("[PROG] processed " + seen.toLocaleString() + " parcels... written " + written.toLocaleString());
    }

    const lon = obj.lon ?? obj.lng ?? obj.longitude ?? obj.x;
    const lat = obj.lat ?? obj.latitude ?? obj.y;
    const hasCoord = coordLooksLatLon(lon, lat) && coordLooksMA(lon, lat);

    let townRaw = null;
    if (townField) townRaw = obj[townField];
    if (!townRaw) townRaw = obj.town || obj.city || obj.municipality || null;

    const townKey = normKey(townRaw);

    if (!townKey) {
      obj.zoning_base = null;
      obj.zoning_overlays = [];
      obj.zoning_attach = { status: "no_town", at: nowIso(), version: "tierA_allCities_v3" };
      outStream.write(JSON.stringify(obj) + "\n");
      written++;
      bump("_unknown", "noTown", 1);
      continue;
    }

    bump(townKey, "seen", 1);

    const cd = cityData.get(townKey);

    if (!cd) {
      obj.zoning_base = null;
      obj.zoning_overlays = [];
      obj.zoning_attach = { status: "no_zoning_for_town", at: nowIso(), version: "tierA_allCities_v3" };
      outStream.write(JSON.stringify(obj) + "\n");
      written++;
      bump(townKey, "townNoZoning", 1);
      continue;
    }

    if (cd.crsInvalid) {
      obj.zoning_base = null;
      obj.zoning_overlays = [];
      obj.zoning_attach = { status: "zoning_crs_invalid", at: nowIso(), version: "tierA_allCities_v3" };
      outStream.write(JSON.stringify(obj) + "\n");
      written++;
      bump(townKey, "crsInvalid", 1);
      continue;
    }

    if (!hasCoord) {
      obj.zoning_base = null;
      obj.zoning_overlays = [];
      obj.zoning_attach = { status: "no_coords_or_outside_ma", at: nowIso(), version: "tierA_allCities_v3" };
      outStream.write(JSON.stringify(obj) + "\n");
      written++;
      bump(townKey, "noCoords", 1);
      continue;
    }

    const pt = [Number(lon), Number(lat)];

    let baseHit = null;
    for (const f of cd.base.feats) {
      const bb = f.bbox;
      if (pt[0] < bb.minX || pt[0] > bb.maxX || pt[1] < bb.minY || pt[1] > bb.maxY) continue;
      if (pointInGeom(pt, f.geometry)) {
        const codeKey = cd.base.fields.code;
        const nameKey = cd.base.fields.name;
        baseHit = {
          city: townKey,
          code: codeKey ? (f.properties[codeKey] ?? null) : null,
          name: nameKey ? (f.properties[nameKey] ?? null) : null,
          bucket: f.bucket,
          source_file: f.file,
          feature_idx: f.idx,
          method: "pip_bbox"
        };
        break;
      }
    }

    const overlays = [];
    for (const f of cd.overlay.feats) {
      const bb = f.bbox;
      if (pt[0] < bb.minX || pt[0] > bb.maxX || pt[1] < bb.minY || pt[1] > bb.maxY) continue;
      if (pointInGeom(pt, f.geometry)) {
        const codeKey = cd.overlay.fields.code;
        const nameKey = cd.overlay.fields.name;
        overlays.push({
          city: townKey,
          code: codeKey ? (f.properties[codeKey] ?? null) : null,
          name: nameKey ? (f.properties[nameKey] ?? null) : null,
          bucket: f.bucket,
          source_file: f.file,
          feature_idx: f.idx,
          method: "pip_bbox"
        });
      }
    }

    obj.zoning_base = baseHit;
    obj.zoning_overlays = overlays;
    obj.zoning_attach = { status: "ok", at: nowIso(), version: "tierA_allCities_v3", townField: townField ?? null };

    outStream.write(JSON.stringify(obj) + "\n");
    written++;

    if (baseHit) bump(townKey, "baseHit", 1);
    if (overlays.length) bump(townKey, "overlayHits", overlays.length);
  }

  phase = "write_audit";
  outStream.end();

  const audit = {
    version: "tierA_allCities_v3",
    created_at: nowIso(),
    parcelsIn: args.parcelsIn,
    zoningRoot: args.zoningRoot,
    out: args.out,
    townField_detected: townField,
    totals: { seen, written },
    cities_loaded: Array.from(cityData.keys()),
    badCrsCities,
    perCity
  };

  fs.writeFileSync(args.auditOut, JSON.stringify(audit, null, 2), "utf8");

  phase = "done";
  clearInterval(hb);

  log("=====================================================");
  log("[DONE] Tier-A zoning attach complete (v3)");
  log("[DONE] seen    : " + seen.toLocaleString());
  log("[DONE] written : " + written.toLocaleString());
  log("[DONE] out     : " + args.out);
  log("[DONE] audit   : " + args.auditOut);
  log("[DONE] bad CRS cities skipped: " + badCrsCities.length);
  log("=====================================================");
}

main().catch(err => {
  process.stderr.write("\n[FAIL] " + (err && err.stack ? err.stack : String(err)) + "\n");
  process.exit(1);
});
