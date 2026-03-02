import fs from "node:fs";
import path from "node:path";

const DEFAULT_PATH = path.resolve(
  process.cwd(),
  "publicData/zoning/zoningDistrictCoverage_allowlist_v1.json"
);

let _cache = null;

export function loadZoningCoverage(filePath = DEFAULT_PATH) {
  const raw = fs.readFileSync(filePath, "utf8");
  const json = JSON.parse(raw);

  const allow = new Set((json.allowlist || []).map(t => String(t).toUpperCase()));
  const rows = new Map((json.rows || []).map(r => [String(r.town).toUpperCase(), r]));

  _cache = { filePath, allow, rows, created_at: json.created_at, rule: json.rule };
  return _cache;
}

export function getZoningCoverage() {
  if (!_cache) return loadZoningCoverage();
  return _cache;
}

export function isTownZoningCovered(town) {
  if (!town) return false;
  const { allow } = getZoningCoverage();
  return allow.has(String(town).toUpperCase());
}

// Optional: expose multi-hit / quality metadata per town
export function getTownCoverageRow(town) {
  if (!town) return null;
  const { rows } = getZoningCoverage();
  return rows.get(String(town).toUpperCase()) || null;
}
