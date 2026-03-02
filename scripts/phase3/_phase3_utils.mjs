import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

export function readJSON(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
export function writeJSON(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}
export function sha256File(p) {
  const buf = fs.readFileSync(p);
  return crypto.createHash("sha256").update(buf).digest("hex");
}
export function isoStamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}
export function norm(s) {
  return (s ?? "").toString().toLowerCase();
}
export function hasAny(hay, needles) {
  const t = norm(hay);
  return needles.some(k => t.includes(k));
}
export function layerHay(layer) {
  return [
    layer.display_name,
    layer.layer_name,
    layer.name,
    layer.title,
    layer.url,
    layer.out_path,
    layer.file,
    layer.geojson_path
  ].filter(Boolean).join(" | ");
}
