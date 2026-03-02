import { spawnSync } from "node:child_process";
import path from "node:path";
import fs from "node:fs";

/**
 * Attempt to convert shapefile to GeoJSON EPSG:4326.
 * Strategy:
 * 1) Try ogr2ogr if present.
 * 2) Try `npx mapshaper` if available.
 * If none is available, throw (caller can skip attachment gracefully).
 */
export function convertShpToGeoJSON({ shpPath, outGeoJSONPath }) {
  const absShp = path.resolve(shpPath);
  const absOut = path.resolve(outGeoJSONPath);

  // 1) ogr2ogr
  const ogr = spawnSync("ogr2ogr", ["-f", "GeoJSON", "-t_srs", "EPSG:4326", absOut, absShp], { stdio: "pipe" });
  if (ogr.status === 0 && fs.existsSync(absOut)) {
    return { tool: "ogr2ogr", out: absOut };
  }

  // 2) npx mapshaper
  const ms = spawnSync("npx", ["--yes", "mapshaper", absShp, "-proj", "wgs84", "-o", `format=geojson`, absOut], { stdio: "pipe" });
  if (ms.status === 0 && fs.existsSync(absOut)) {
    return { tool: "mapshaper", out: absOut };
  }

  const ogrErr = (ogr.stderr || Buffer.from("")).toString("utf-8").slice(0, 400);
  const msErr = (ms.stderr || Buffer.from("")).toString("utf-8").slice(0, 400);

  throw new Error(
    `No shapefile converter succeeded.\n` +
    `ogr2ogr stderr: ${ogrErr}\n` +
    `mapshaper stderr: ${msErr}\n` +
    `Install GDAL (ogr2ogr) OR add mapshaper to dev deps, then re-run.`
  );
}

