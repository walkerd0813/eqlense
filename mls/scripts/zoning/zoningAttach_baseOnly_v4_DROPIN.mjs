import fs from "fs";
import path from "path";
import crypto from "crypto";
import readline from "readline";

function nowIso() {
  return new Date().toISOString();
}

function logBanner(msg) {
  console.log("=====================================================");
  console.log(msg);
  console.log("=====================================================");
}

function sha256File(filePath) {
  return new Promise((resolve, reject) => {
    const h = crypto.createHash("sha256");
    const s = fs.createReadStream(filePath);
    s.on("error", reject);
    s.on("data", (d) => h.update(d));
    s.on("end", () => resolve(h.digest("hex")));
  });
}

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const key = a.slice(2);
      const next = argv[i + 1];
      if (!next || next.startsWith("--")) {
        args[key] = true;
      } else {
        args[key] = next;
        i++;
      }
    }
  }
  return args;
}

function toNum(x) {
  if (x === null || x === undefined) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

// MA sanity bounds gate (prevents “Medford, OR” ghosts)
function inMABounds(lon, lat) {
  return (
    Number.isFinite(lat) &&
    Number.isFinite(lon) &&
    lat >= 41.0 &&
    lat <= 43.5 &&
    lon >= -73.6 &&
    lon <= -69.5
  );
}

function normalizeTownKey(s) {
  if (!s) return "";
  return String(s)
    .trim()
    .toLowerCase()
    .replace(/[\.\,]/g, "")
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_]/g, "")
    .replace(/_+/g, "_");
}

function pickFirst(obj, candidates) {
  for (const k of candidates) {
    if (obj && Object.prototype.hasOwnProperty.call(obj, k)) {
      const v = obj[k];
      if (v !== null && v !== undefined && String(v).trim() !== "") return v;
    }
  }
  return null;
}

function normalizeDistrictCode(x) {
  if (!x) return null;
  return String(x)
    .trim()
    .toUpperCase()
    .replace(/\s+/g, " ")
    .replace(/[–—]/g, "-");
}

// Point-in-polygon (ray casting), supports Polygon + MultiPolygon
// Expects GeoJSON geometry in EPSG:4326
function pointInRing(pt, ring) {
  const [x, y] = pt;
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0], yi = ring[i][1];
    const xj = ring[j][0], yj = ring[j][1];
    const intersect =
      yi > y !== yj > y &&
      x < ((xj - xi) * (y - yi)) / (yj - yi + 0.0) + xi;
    if (intersect) inside = !inside;
  }
  return inside;
}

function pointInPolygonGeometry(pt, geom) {
  if (!geom) return false;
  if (geom.type === "Polygon") {
    const rings = geom.coordinates || [];
    if (!rings.length) return false;
    // first ring = exterior, subsequent rings = holes
    if (!pointInRing(pt, rings[0])) return false;
    for (let r = 1; r < rings.length; r++) {
      if (pointInRing(pt, rings[r])) return false;
    }
    return true;
  }
  if (geom.type === "MultiPolygon") {
    const polys = geom.coordinates || [];
    for (const poly of polys) {
      const rings = poly || [];
      if (!rings.length) continue;
      if (!pointInRing(pt, rings[0])) continue;
      let inHole = false;
      for (let r = 1; r < rings.length; r++) {
        if (pointInRing(pt, rings[r])) {
          inHole = true;
          break;
        }
      }
      if (!inHole) return true;
    }
    return false;
  }
  return false;
}

function bboxOfCoords(coords, bbox) {
  // bbox: [minX, minY, maxX, maxY]
  for (const c of coords) {
    if (Array.isArray(c[0])) {
      bboxOfCoords(c, bbox);
    } else {
      const x = c[0], y = c[1];
      if (x < bbox[0]) bbox[0] = x;
      if (y < bbox[1]) bbox[1] = y;
      if (x > bbox[2]) bbox[2] = x;
      if (y > bbox[3]) bbox[3] = y;
    }
  }
}

function computeGeomBbox(geom) {
  const bbox = [Infinity, Infinity, -Infinity, -Infinity];
  if (!geom || !geom.coordinates) return null;
  bboxOfCoords(geom.coordinates, bbox);
  if (!Number.isFinite(bbox[0])) return null;
  return bbox;
}

function bboxContainsPoint(b, lon, lat) {
  return lon >= b[0] && lon <= b[2] && lat >= b[1] && lat <= b[3];
}

async function loadZoningTown(townKey, zoningRoot, codeFields, nameFields) {
  const filePath = path.join(zoningRoot, townKey, "districts", "zoning_base.geojson");
  if (!fs.existsSync(filePath)) {
    return { townKey, filePath, ok: false, reason: "MISSING_FILE" };
  }

  console.log(`[zoning] loading ${townKey}: ${filePath}`);
  const raw = await fs.promises.readFile(filePath, "utf8");
  let gj;
  try {
    gj = JSON.parse(raw);
  } catch (e) {
    return { townKey, filePath, ok: false, reason: "BAD_JSON" };
  }

  const feats = Array.isArray(gj.features) ? gj.features : [];
  const prepared = [];
  for (const f of feats) {
    const geom = f?.geometry;
    if (!geom) continue;
    const bb = computeGeomBbox(geom);
    if (!bb) continue;

    const props = f?.properties || {};
    const codeRaw = pickFirst(props, codeFields);
    const nameRaw = pickFirst(props, nameFields);

    prepared.push({
      bbox: bb,
      geom,
      props,
      code_norm: normalizeDistrictCode(codeRaw),
      name_norm: nameRaw ? String(nameRaw).trim() : null,
      code_raw: codeRaw ? String(codeRaw) : null,
      name_raw: nameRaw ? String(nameRaw) : null
    });
  }

  const hash = await sha256File(filePath);

  console.log(`[zoning] loaded ${townKey}: polygons=${prepared.length}`);
  return {
    townKey,
    filePath,
    ok: true,
    hash,
    polygons: prepared
  };
}

async function main() {
  const args = parseArgs(process.argv);

  const IN = args.in;
  const OUT = args.out;
  const META = args.meta || "";
  const zoningRoot = args.zoningRoot || "C:\\seller-app\\backend\\publicData\\zoning";
  const asOf = args.asOf || new Date().toISOString().slice(0, 10);
  const progressEvery = Number(args.progressEvery || 100000);

  if (!IN || !OUT) {
    logBanner("[ERR] Missing required args: --in <ndjson> --out <ndjson>");
    process.exit(1);
  }

  // Candidate fields (safe defaults)
  const townFields = (args.townFields || "town,municipality,city,Jurisdiction,TOWN")
    .split(",").map(s => s.trim()).filter(Boolean);

  const latFields = (args.latFields || "lat,latitude,Lat,Latitude,centroid_lat")
    .split(",").map(s => s.trim()).filter(Boolean);

  const lonFields = (args.lonFields || "lon,lng,longitude,Lon,Lng,Longitude,centroid_lon")
    .split(",").map(s => s.trim()).filter(Boolean);

  const propertyIdFields = (args.propertyIdFields || "property_id,parcel_id,PARCEL_ID,parcelId")
    .split(",").map(s => s.trim()).filter(Boolean);

  // Zoning property fields inside GeoJSON polygons
  const zoneCodeFields = (args.zoneCodeFields ||
    "district_code_norm,district_code,DISTRICT,ZONE,ZONE_CODE,ZONECLASS,ZN_CODE,ZONE_1,ZONING")
    .split(",").map(s => s.trim()).filter(Boolean);

  const zoneNameFields = (args.zoneNameFields ||
    "district_name_norm,district_name,NAME,ZONE_NAME,ZONEDESC,DESCRIPTION,DESCRIPTIO,ZONINGDESC")
    .split(",").map(s => s.trim()).filter(Boolean);

  logBanner(`[zoningAttach] START ${nowIso()}`);
  console.log(`[zoningAttach] in:        ${IN}`);
  console.log(`[zoningAttach] out:       ${OUT}`);
  console.log(`[zoningAttach] zoningRoot:${zoningRoot}`);
  console.log(`[zoningAttach] asOf:      ${asOf}`);
  console.log(`[zoningAttach] progressEvery: ${progressEvery}`);
  console.log("");

  await fs.promises.mkdir(path.dirname(OUT), { recursive: true });

  const rl = readline.createInterface({
    input: fs.createReadStream(IN, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  const outStream = fs.createWriteStream(OUT, { encoding: "utf8" });

  const townCache = new Map(); // townKey -> loaded object

  let total = 0;
  let matched = 0;
  let missingTownFile = 0;
  let badCoords = 0;
  let noTown = 0;

  const townStats = new Map(); // townKey -> {rows, matched}

  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    total++;

    let row;
    try {
      row = JSON.parse(line);
    } catch {
      continue;
    }

    const townRaw = pickFirst(row, townFields);
    const townKey = normalizeTownKey(townRaw);

    const lat = toNum(pickFirst(row, latFields));
    const lon = toNum(pickFirst(row, lonFields));

    const pid = pickFirst(row, propertyIdFields);

    // attach scaffolding (always present)
    row.jurisdiction_name = townKey || (townRaw ? String(townRaw) : null);

    row.base_district_code = null;
    row.base_district_name = null;
    row.base_zone_confidence = 0.0;
    row.base_zone_attach_method = "unknown";
    row.base_zone_distance_m = null;
    row.base_zone_evidence = null;

    row.split_parcel_flag = false;
    row.edge_proximity_flag = false;
    row.flags = [];

    if (!townKey) {
      noTown++;
      row.flags.push("missing_town");
      outStream.write(JSON.stringify(row) + "\n");
      continue;
    }

    if (!Number.isFinite(lat) || !Number.isFinite(lon) || !inMABounds(lon, lat)) {
      badCoords++;
      row.flags.push("bad_or_missing_coords");
      outStream.write(JSON.stringify(row) + "\n");
      continue;
    }

    // town zoning load (lazy)
    let tz = townCache.get(townKey);
    if (!tz) {
      tz = await loadZoningTown(townKey, zoningRoot, zoneCodeFields, zoneNameFields);
      townCache.set(townKey, tz);
    }

    if (!tz.ok) {
      missingTownFile++;
      row.flags.push(`zoning_missing:${tz.reason}`);
      outStream.write(JSON.stringify(row) + "\n");
      continue;
    }

    // attempt point-in-poly (A confidence only when inside)
    const pt = [lon, lat];

    let hit = null;
    for (const poly of tz.polygons) {
      if (!bboxContainsPoint(poly.bbox, lon, lat)) continue;
      if (pointInPolygonGeometry(pt, poly.geom)) {
        hit = poly;
        break;
      }
    }

    // update town stats
    const ts = townStats.get(townKey) || { rows: 0, matched: 0 };
    ts.rows++;
    if (hit) ts.matched++;
    townStats.set(townKey, ts);

    if (hit) {
      matched++;
      row.base_district_code = hit.code_norm || (hit.code_raw ? normalizeDistrictCode(hit.code_raw) : null);
      row.base_district_name = hit.name_norm || (hit.name_raw ? String(hit.name_raw).trim() : null);
      row.base_zone_confidence = 1.0;
      row.base_zone_attach_method = "point_in_poly";
      row.base_zone_distance_m = 0;

      row.base_zone_evidence = {
        town: townKey,
        zoning_file: tz.filePath,
        zoning_as_of: asOf,
        zoning_hash_sha256: tz.hash,
        attach_method: "point_in_poly",
        property_id: pid ?? null
      };
    } else {
      row.base_zone_confidence = 0.0;
      row.base_zone_attach_method = "point_in_poly_no_hit";
      row.flags.push("no_zone_hit");
    }

    outStream.write(JSON.stringify(row) + "\n");

    if (progressEvery > 0 && total % progressEvery === 0) {
      const rate = total ? (matched / total) : 0;
      console.log(`[zoningAttach] progress rows=${total.toLocaleString()} matched=${matched.toLocaleString()} rate=${(rate*100).toFixed(2)}% memTownsLoaded=${townCache.size}`);
    }
  }

  outStream.end();

  const report = {
    generated_at_utc: nowIso(),
    in: IN,
    out: OUT,
    zoning_root: zoningRoot,
    zoning_as_of: asOf,
    totals: {
      rows: total,
      matched,
      match_rate: total ? matched / total : 0,
      bad_or_missing_coords: badCoords,
      missing_town: noTown,
      missing_town_zoning_file: missingTownFile
    },
    towns: Array.from(townStats.entries())
      .sort((a, b) => b[1].rows - a[1].rows)
      .map(([k, v]) => ({ town: k, rows: v.rows, matched: v.matched, match_rate: v.rows ? v.matched / v.rows : 0 }))
  };

  if (META) {
    await fs.promises.mkdir(path.dirname(META), { recursive: true });
    await fs.promises.writeFile(META, JSON.stringify(report, null, 2), "utf8");
    console.log(`[zoningAttach] wrote report: ${META}`);
  }

  logBanner(`[zoningAttach] DONE ${nowIso()}`);
  console.log(JSON.stringify(report.totals, null, 2));
}

main().catch((err) => {
  console.error("[FATAL]", err);
  process.exit(1);
});
