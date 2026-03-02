import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";

export function nowIso() {
  return new Date().toISOString();
}

export function sha256FileSync(filePath) {
  const h = crypto.createHash("sha256");
  const fd = fs.openSync(filePath, "r");
  try {
    const buf = Buffer.alloc(1024 * 1024);
    let bytes = 0;
    while ((bytes = fs.readSync(fd, buf, 0, buf.length, null)) > 0) {
      h.update(buf.subarray(0, bytes));
    }
  } finally {
    fs.closeSync(fd);
  }
  return h.digest("hex");
}

export function sha256String(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}

export function ensureDirSync(p) {
  fs.mkdirSync(p, { recursive: true });
}

export async function fileExists(p) {
  try { await fsp.access(p); return true; } catch { return false; }
}

export function normalizeCityName(s) {
  return String(s ?? "").trim().toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/town of /g, "")
    .replace(/city of /g, "");
}

export function pick(obj, keys) {
  for (const k of keys) {
    if (k in obj && obj[k] != null && obj[k] !== "") return obj[k];
  }
  return null;
}

export function detectContractPointerFiles(root) {
  const candidates = [];
  const searchRoots = [
    path.join(root, "publicData"),
    path.join(root, "publicData", "_contracts"),
    path.join(root, "publicData", "contracts"),
    path.join(root, "publicData", "_contract_views"),
    path.join(root, "publicData", "_audit"),
  ];
  for (const sr of searchRoots) {
    if (fs.existsSync(sr)) candidates.push(sr);
  }
  return candidates;
}

export function walkFindFilesSync(startDir, predicate, maxDepth = 6) {
  const out = [];
  function walk(dir, depth) {
    if (depth > maxDepth) return;
    let entries = [];
    try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const e of entries) {
      const p = path.join(dir, e.name);
      if (e.isDirectory()) {
        walk(p, depth + 1);
      } else if (e.isFile()) {
        if (predicate(p)) out.push(p);
      }
    }
  }
  walk(startDir, 0);
  return out;
}

export function safeReadJsonSync(filePath) {
  const raw = fs.readFileSync(filePath, "utf-8");
  return JSON.parse(raw);
}

export function safeWriteJsonSync(filePath, obj) {
  fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), "utf-8");
}

export function formatDateStamp(d = new Date()) {
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

export function withinMABbox(lat, lon, bbox) {
  if (lat == null || lon == null) return false;
  return lat >= bbox.min_lat && lat <= bbox.max_lat && lon >= bbox.min_lon && lon <= bbox.max_lon;
}

export function tryExtractLatLon(rec) {
  // common keys
  const lat = pick(rec, ["lat","latitude","centroid_lat","y","LAT","Latitude"]);
  const lon = pick(rec, ["lon","lng","longitude","centroid_lon","x","LON","Longitude","Lng"]);
  if (lat != null && lon != null) return { lat: Number(lat), lon: Number(lon) };

  // nested: location:{lat,lon}
  if (rec.location && typeof rec.location === "object") {
    const lat2 = pick(rec.location, ["lat","latitude","y"]);
    const lon2 = pick(rec.location, ["lon","lng","longitude","x"]);
    if (lat2 != null && lon2 != null) return { lat: Number(lat2), lon: Number(lon2) };
  }

  // geometry field (GeoJSON)
  if (rec.geometry && rec.geometry.type === "Point" && Array.isArray(rec.geometry.coordinates)) {
    const [x, y] = rec.geometry.coordinates;
    return { lat: Number(y), lon: Number(x) };
  }

  return { lat: null, lon: null };
}

export function guessCityField(rec) {
  const candidates = [
    "town","city","municipality","municipality_name","town_name","townNorm","cityNorm",
    "admin_town","adminTown","admin.municipality","admin.town"
  ];
  for (const k of candidates) {
    if (k.includes(".")) {
      const parts = k.split(".");
      let cur = rec;
      let ok = true;
      for (const p of parts) {
        if (cur && typeof cur === "object" && p in cur) cur = cur[p]; else { ok = false; break; }
      }
      if (ok && cur != null) return { key: k, value: String(cur) };
    } else if (k in rec && rec[k] != null) {
      return { key: k, value: String(rec[k]) };
    }
  }
  return { key: null, value: null };
}

export function inferPropertiesPathFromContract(contractObj, root) {
  const candidates = [];
  function pushIfString(v) {
    if (typeof v === "string" && v.toLowerCase().endsWith(".ndjson")) candidates.push(v);
  }
  // common locations
  if (contractObj) {
    for (const k of ["properties","properties_path","properties_ndjson","propertiesFile","properties_file"]) {
      pushIfString(contractObj[k]);
    }
    if (contractObj.paths && typeof contractObj.paths === "object") {
      for (const k of ["properties","properties_ndjson","properties_file"]) pushIfString(contractObj.paths[k]);
    }
  }
  // normalize to absolute if relative
  for (const p of candidates) {
    const abs = path.isAbsolute(p) ? p : path.join(root, p);
    if (fs.existsSync(abs)) return abs;
  }
  return null;
}

export function findLatestPropertiesNdjson(root) {
  const dir = path.join(root, "publicData", "properties");
  if (!fs.existsSync(dir)) return null;
  const files = fs.readdirSync(dir)
    .filter(f => f.toLowerCase().endsWith(".ndjson") && f.toLowerCase().includes("properties"))
    .map(f => path.join(dir, f));
  if (files.length === 0) return null;
  files.sort((a,b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
  return files[0];
}

export function backupFileSync(filePath) {
  const dir = path.dirname(filePath);
  const stamp = formatDateStamp();
  const base = path.basename(filePath);
  const bak = path.join(dir, `${base}.bak_${stamp}`);
  fs.copyFileSync(filePath, bak);
  return bak;
}

