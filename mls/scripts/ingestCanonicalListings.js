/**
 * CANONICAL LISTINGS INGESTER (vNext v5)
 * -------------------------------------
 * Reads recursively under: mls/raw/<property_type>/<bucket>/*.csv
 * Writes: mls/normalized/listings.ndjson
 *
 * Hard guarantees (best-effort):
 *  - street_no, street_name, unit_no, zip, town_num, town exist in canonical snake_case
 *  - unit_no is never undefined (null if missing)
 *  - town is derived from towns.txt using town_num when present
 *
 * NOTE: Avoids patterns in comments that would close a block comment.
 */

import fs from "fs";
import path from "path";
import csv from "csv-parser";
import { fileURLToPath } from "url";
import { normalizeListingRow } from "../normalized/normalizeListingRow.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ROOT = path.resolve(__dirname, "..", "..");
const RAW_ROOT = path.join(ROOT, "mls", "raw");
const OUTPUT_FILE = path.join(ROOT, "mls", "normalized", "listings.ndjson");
const TOWNS_FILE = path.join(ROOT, "mls", "raw", "reference", "towns.txt");

let fileCount = 0;
let rowCount = 0;
let badRows = 0;
let missingAddr = 0;

fs.mkdirSync(path.dirname(OUTPUT_FILE), { recursive: true });
const output = fs.createWriteStream(OUTPUT_FILE, { flags: "w" });

function cleanStr(v) {
  if (v == null) return "";
  return String(v).trim();
}
function digitsOnly(v) {
  const s = cleanStr(v).replace(/[^\d]/g, "");
  return s || null;
}
function padZip(z) {
  const d = cleanStr(z).replace(/[^\d]/g, "");
  if (!d) return null;
  return d.padStart(5, "0").slice(0, 5);
}
function pickAny(obj, keys) {
  for (const k of keys) {
    if (obj && obj[k] != null && cleanStr(obj[k])) return obj[k];
  }
  return null;
}

function loadTownsMapIfPresent() {
  if (!fs.existsSync(TOWNS_FILE)) return null;
  const txt = fs.readFileSync(TOWNS_FILE, "utf8");
  const lines = txt.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  const map = new Map();

  for (const line of lines) {
    let parts = line.split("|");
    if (parts.length < 2) parts = line.split("\t");
    if (parts.length < 2) parts = line.split(",");
    if (parts.length < 2) parts = line.split(/\s{2,}/);

    if (parts.length >= 2) {
      const a = digitsOnly(parts[0]);
      const b = cleanStr(parts[1]);
      if (a && b) map.set(a, b);
      continue;
    }

    const m = line.match(/^\s*(\d+)\s+(.+)\s*$/);
    if (m) map.set(m[1], m[2]);
  }

  return map.size ? map : null;
}

function isListingCsv(filename) {
  const f = filename.toLowerCase();
  return (
    f.endsWith(".csv") &&
    !f.includes("agent") &&
    !f.includes("office") &&
    !f.includes("reference")
  );
}

function extractContextFromPath(filePath) {
  const parts = filePath.split(path.sep).map((p) => p.toLowerCase());

  const types = new Set(["single_family", "multi_family", "condo", "land"]);
  const typeIdx = parts.findIndex((p) => types.has(p));
  const propertyType = typeIdx >= 0 ? parts[typeIdx] : null;

  const bucket = typeIdx >= 0 && parts[typeIdx + 1] ? parts[typeIdx + 1] : "events";
  const lifecycle = bucket === "active" ? "active" : bucket === "sold" ? "sold" : "event";

  return { propertyType, bucket, lifecycle };
}

function extractUnitFromStreetName(streetNameRaw) {
  const s0 = cleanStr(streetNameRaw);
  if (!s0) return { street_name_clean: null, unit_no: null };

  const s = " " + s0.replace(/\s+/g, " ").trim() + " ";
  const m =
    s.match(/\s#\s*([A-Za-z0-9\-]+)\s/i) ||
    s.match(/\s(?:UNIT|APT|APARTMENT|STE|SUITE)\s+([A-Za-z0-9\-]+)\s/i);

  if (!m) return { street_name_clean: s0, unit_no: null };

  const unit_no = m[1];
  let cleaned = s0
    .replace(new RegExp(`#\\s*${unit_no}\\b`, "i"), "")
    .replace(new RegExp(`\\b(?:UNIT|APT|APARTMENT|STE|SUITE)\\s+${unit_no}\\b`, "i"), "")
    .replace(/\s+/g, " ")
    .trim();
  if (!cleaned) cleaned = s0;

  return { street_name_clean: cleaned, unit_no };
}

function parseStreetNoFromFullAddress(fullAddr) {
  const s = cleanStr(fullAddr);
  if (!s) return null;
  const m = s.match(/^\s*(\d+(?:\s*[-–]\s*\d+)?[A-Za-z]?)\b/);
  if (!m) return null;
  return m[1].replace(/\s+/g, "");
}

const townsMap = loadTownsMapIfPresent();

console.log("====================================================");
console.log("      INGEST CANONICAL MLS LISTINGS (vNext v5)");
console.log("====================================================");
console.log("RAW root:    ", RAW_ROOT);
console.log("Output file: ", OUTPUT_FILE);
console.log("Towns map:   ", townsMap ? `${townsMap.size.toLocaleString()} entries` : "NOT FOUND (ok)");
console.log("====================================================");

async function ingestCsv(filePath, context) {
  return new Promise((resolve) => {
    let rows = 0;

    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (raw) => {
        try {
          const base = normalizeListingRow(raw, context);
          if (!base) return;

          // Start with whatever normalizeListingRow returns
          const o = { ...base };

          // Preserve/force context
          o.propertyType = o.propertyType ?? context.propertyType ?? null;
          o.bucket = o.bucket ?? context.bucket ?? "events";
          o.lifecycle = o.lifecycle ?? context.lifecycle ?? "event";

          // ---- Canonicalize address fields (snake_case), from any casing + nested full_address
          const faObj = o.full_address && typeof o.full_address === "object" ? o.full_address : null;
          const faStr = typeof o.full_address === "string" ? o.full_address : null;

          const streetNoAny = pickAny(o, ["street_no", "streetNo", "STREET_NO"]) ?? faObj?.streetNumber;
          const streetNameAny = pickAny(o, ["street_name", "streetName", "STREET_NAME"]) ?? faObj?.streetName;
          const zipAny = pickAny(o, ["zip", "zip_code", "ZIP_CODE"]) ?? faObj?.zip;

          if (!o.street_no && streetNoAny) o.street_no = cleanStr(streetNoAny);
          if (!o.street_name && streetNameAny) o.street_name = cleanStr(streetNameAny);
          if (!o.zip && zipAny) o.zip = padZip(zipAny);

          // If still missing street_no but we have a full address string/object, parse it
          if (!o.street_no) {
            const fa = faStr || (faObj ? `${faObj.streetNumber ?? ""} ${faObj.streetName ?? ""}`.trim() : "");
            const parsed = parseStreetNoFromFullAddress(fa);
            if (parsed) o.street_no = parsed;
          }

          // Unit: never undefined; extract if embedded in street_name
          if (o.unit_no === undefined) o.unit_no = null;
          if (o.street_name) {
            const { street_name_clean, unit_no } = extractUnitFromStreetName(o.street_name);
            if (o.unit_no == null && unit_no) o.unit_no = unit_no;
            if (street_name_clean) o.street_name = street_name_clean;
          }

          // town_num from either normalized or raw (MLS typically has TOWN_NUM)
          const tn =
            digitsOnly(pickAny(o, ["town_num", "townNum", "TOWN_NUM"])) ||
            digitsOnly(pickAny(raw, ["TOWN_NUM", "town_num", "TOWNNUM", "Town_Num", "TOWNNO"]));
          if (tn) o.town_num = tn;

          // town from towns map (preferred), else raw town/city
          if (!o.town) {
            let t = null;
            if (tn && townsMap && townsMap.has(tn)) t = townsMap.get(tn);

            if (!t) {
              const rawTown = pickAny(raw, ["TOWN", "town", "CITY", "city", "MUNICIPALITY"]);
              if (rawTown) t = cleanStr(rawTown).split(",")[0].trim();
            }

            if (t) o.town = t;
          }

          // Missing addr (for linkability)
          if (!o.street_no || !o.street_name || !o.zip) missingAddr++;

          output.write(JSON.stringify(o) + "\n");
          rows++;
          rowCount++;
        } catch {
          badRows++;
        }
      })
      .on("end", () => {
        fileCount++;
        console.log(`✔ ${rows.toLocaleString()} rows → ${path.basename(filePath)} [${context.propertyType}/${context.bucket}]`);
        resolve();
      });
  });
}

async function walk(dir) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      await walk(fullPath);
      continue;
    }
    if (entry.isFile() && isListingCsv(entry.name)) {
      const context = extractContextFromPath(fullPath);
      await ingestCsv(fullPath, context);
    }
  }
}

if (!fs.existsSync(RAW_ROOT)) throw new Error(`Missing RAW_ROOT: ${RAW_ROOT}`);
await walk(RAW_ROOT);

output.end();

console.log("----------------------------------------------------");
console.log("✅ CANONICAL INGEST COMPLETE");
console.log("Files processed:", fileCount);
console.log("Rows ingested:  ", rowCount.toLocaleString());
console.log("Bad rows:       ", badRows.toLocaleString());
console.log("Missing addr:   ", missingAddr.toLocaleString());
console.log("Output:         ", OUTPUT_FILE);
console.log("====================================================");
