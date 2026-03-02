import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

export function ensureDirSync(p) {
  fs.mkdirSync(p, { recursive: true });
}

export function readJSON(p) {
  let s = fs.readFileSync(p, "utf8");
  // strip UTF-8 BOM if present (Node JSON.parse will fail otherwise)
  if (s && s.charCodeAt(0) === 0xFEFF) s = s.slice(1);
  return JSON.parse(s);
}

export function writeJSON(p, obj) {
  ensureDirSync(path.dirname(p));
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}

export function sha1File(p) {
  const h = crypto.createHash("sha1");
  h.update(fs.readFileSync(p));
  return h.digest("hex");
}

export function nowStamp() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

export function normCity(s) {
  return String(s || "").trim().toLowerCase().replace(/\s+/g, "");
}

export function containsAny(hay, arr) {
  const t = String(hay || "").toLowerCase();
  return arr.some(k => t.includes(String(k).toLowerCase()));
}

export function haversineMeters(aLon, aLat, bLon, bLat) {
  const R = 6371000;
  const toRad = (x) => (x * Math.PI) / 180;
  const dLat = toRad(bLat - aLat);
  const dLon = toRad(bLon - aLon);
  const lat1 = toRad(aLat);
  const lat2 = toRad(bLat);
  const s = Math.sin(dLat/2)**2 + Math.cos(lat1)*Math.cos(lat2)*Math.sin(dLon/2)**2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(s)));
}

export function safeLonLat(pt) {
  if (!pt || typeof pt[0] !== "number" || typeof pt[1] !== "number") return null;
  const lon = pt[0], lat = pt[1];
  if (lat < 41 || lat > 43.7) return null;
  if (lon > -69 || lon < -73.8) return null;
  return { lon, lat };
}

