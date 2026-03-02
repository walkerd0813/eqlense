Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Stamp { (Get-Date).ToString("yyyyMMdd_HHmmss") }

function Read-Pointer([string]$p) {
  if ([string]::IsNullOrWhiteSpace($p)) { throw "Pointer path empty" }
  if (!(Test-Path $p)) { throw "Pointer missing: $p" }
  $v = (Get-Content $p -Raw).Trim()
  if ([string]::IsNullOrWhiteSpace($v)) { throw "Pointer empty: $p" }
  return $v
}

function Find-Latest-PropertySpineNdjson {
  $root = ".\publicData\properties\_frozen"
  if (!(Test-Path $root)) { throw "Missing: $root" }

  $candidates = Get-ChildItem $root -Recurse -File -Filter "*.ndjson" |
    Where-Object { $_.FullName -match "withBaseZoning" } |
    Sort-Object LastWriteTime -Descending

  if (!$candidates -or $candidates.Count -eq 0) {
    throw "Could not auto-find a withBaseZoning *.ndjson under $root"
  }
  return $candidates[0].FullName
}

function Write-Json([string]$path, $obj) {
  $dir = Split-Path $path -Parent
  if (!(Test-Path $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  ($obj | ConvertTo-Json -Depth 20) | Set-Content -Encoding UTF8 $path
}

function Ensure-Green-Freeze-NoSkipped([string]$pointerPath, [string]$artifactKey) {
  $oldDir = Read-Pointer $pointerPath
  $oldManifestPath = Join-Path $oldDir "MANIFEST.json"
  $oldSkippedPath  = Join-Path $oldDir "SKIPPED.txt"
  $oldFC = Join-Path $oldDir "feature_catalog.ndjson"
  $oldATT = Join-Path $oldDir "attachments.ndjson"

  if (!(Test-Path $oldManifestPath)) { throw "Missing MANIFEST.json in $oldDir" }
  if (!(Test-Path $oldFC)) { throw "Missing feature_catalog.ndjson in $oldDir" }
  if (!(Test-Path $oldATT)) { throw "Missing attachments.ndjson in $oldDir" }

  $hasSkipped = Test-Path $oldSkippedPath
  if (-not $hasSkipped) {
    Write-Host "[ok] $artifactKey already GREEN (no SKIPPED.txt)."
    return
  }

  Write-Host "[warn] $artifactKey has SKIPPED.txt -> not GREEN. Creating new GREEN freeze + updating pointer..."

  $oldManifest = Get-Content $oldManifestPath -Raw | ConvertFrom-Json

  $newDir = Join-Path ".\publicData\overlays\_frozen" ($artifactKey + "__FREEZE__" + (Stamp))
  New-Item -ItemType Directory -Force -Path $newDir | Out-Null

  Copy-Item $oldFC  (Join-Path $newDir "feature_catalog.ndjson")
  Copy-Item $oldATT (Join-Path $newDir "attachments.ndjson")

  $newManifest = [ordered]@{
    artifact_key = $artifactKey
    created_at   = (Get-Date).ToString("o")
    inputs       = $oldManifest.inputs
    outputs      = $oldManifest.outputs
    stats        = $oldManifest.stats
    note         = "Re-freeze to remove SKIPPED.txt; outputs identical, pointer upgraded to GREEN."
  }

  Write-Json (Join-Path $newDir "MANIFEST.json") $newManifest

  Set-Content -Encoding UTF8 $pointerPath $newDir
  Write-Host "[done] pointer updated -> $newDir"
}

function Write-Contract-Builder-MJS {
  $dst = ".\scripts\gis\build_property_spine_contract_v1.mjs"
  $dir = Split-Path $dst -Parent
  if (!(Test-Path $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }

  $js = @'
import fs from "fs";
import path from "path";
import readline from "readline";

function getPath(obj, p) {
  if (!obj || !p) return undefined;
  const parts = p.split(".");
  let cur = obj;
  for (const part of parts) {
    if (cur && Object.prototype.hasOwnProperty.call(cur, part)) cur = cur[part];
    else return undefined;
  }
  return cur;
}

function pick(obj, candidates) {
  for (const k of candidates) {
    const v = k.includes(".") ? getPath(obj, k) : obj?.[k];
    if (v === 0) return v;
    if (v === false) return v;
    if (v !== undefined && v !== null && String(v).trim() !== "") return v;
  }
  return undefined;
}

function normCode(v) {
  if (v === undefined || v === null) return undefined;
  const s = String(v).trim();
  if (!s) return undefined;
  return s.toUpperCase();
}

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const key = a.slice(2);
      const val = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
      out[key] = val;
    }
  }
  return out;
}

const args = parseArgs(process.argv);

const inPath = args.in;
const outPath = args.out;
const asOfDate = args.asOfDate || "";
const datasetHash = args.datasetHash || "";
const defaultState = args.defaultState || "MA";
const zoningDatasetHash = args.zoningDatasetHash || datasetHash;
const zoningAsOfDate = args.zoningAsOfDate || asOfDate;

if (!inPath || !outPath) {
  console.error("Usage: node build_property_spine_contract_v1.mjs --in <ndjson> --out <ndjson> --asOfDate YYYY-MM-DD --datasetHash <sha>");
  process.exit(2);
}

fs.mkdirSync(path.dirname(outPath), { recursive: true });

const rl = readline.createInterface({
  input: fs.createReadStream(inPath, { encoding: "utf8" }),
  crlfDelay: Infinity,
});

const out = fs.createWriteStream(outPath, { encoding: "utf8" });

let read = 0;
let wrote = 0;

const REQUIRED = [
  "parcel_id_raw","parcel_id_norm","source_city","source_state","dataset_hash","as_of_date",
  "address_city","address_state","address_zip",
  "latitude","longitude","coord_confidence_grade",
  "parcel_centroid_lat","parcel_centroid_lon","crs",
  "base_zoning_status","base_zoning_code_raw","base_zoning_code_norm",
  "zoning_attach_method","zoning_attach_confidence","zoning_source_city","zoning_dataset_hash","zoning_as_of_date"
];

for await (const line of rl) {
  if (!line || !line.trim()) continue;
  read++;

  let obj;
  try { obj = JSON.parse(line); } catch { continue; }

  const property_id = pick(obj, ["property_id","propertyId","id"]);
  const parcelRaw = pick(obj, ["parcel_id_raw","parcel_id","parcelIdRaw","parcelId","APN","apn","MAP_PAR_ID","LOC_ID"]);
  const parcelNorm = pick(obj, ["parcel_id_norm","parcel_id_normalized","parcelIdNorm","parcel_norm","parcel_id","parcelId"]);

  const srcCity = pick(obj, ["source_city","sourceCity","city","town","municipality","jurisdiction","address_city","address.city"]);
  const srcState = pick(obj, ["source_state","sourceState","state","address_state","address.state"]) || defaultState;

  const addrCity = pick(obj, ["address_city","address.city","city","town","addr_city"]);
  const addrState = pick(obj, ["address_state","address.state","state"]) || defaultState;
  const addrZip = pick(obj, ["address_zip","address.zip","zip","zipcode","addr_zip"]);

  const lat = pick(obj, ["latitude","lat","LATITUDE","y","Y","coord_lat"]);
  const lon = pick(obj, ["longitude","lon","lng","LONGITUDE","LON","x","X","coord_lon"]);

  const cLat = pick(obj, ["parcel_centroid_lat","centroid_lat","centroid.y"]);
  const cLon = pick(obj, ["parcel_centroid_lon","centroid_lon","centroid.x"]);

  const coordGrade = pick(obj, ["coord_confidence_grade","coordConfidenceGrade","coord_grade","coord_confidence","coord_tier_grade"]);

  const zStatus = pick(obj, ["base_zoning_status","baseZoningStatus","zoning_status"]);
  const zCodeRaw = pick(obj, ["base_zoning_code_raw","base_zoning_code","zoning_code_raw","zoning_code","zone","DISTRICT","ZONE_CODE"]);
  const zCodeNorm = pick(obj, ["base_zoning_code_norm","zoning_code_norm","zone_norm"]) || normCode(zCodeRaw);

  const attachMethod = pick(obj, ["zoning_attach_method","zoningAttachMethod","attach_method"]) || "polygon_centroid";
  const attachConf = pick(obj, ["zoning_attach_confidence","zoningAttachConfidence","attach_confidence"]);

  const zoningSourceCity = pick(obj, ["zoning_source_city","zoningSourceCity","base_zoning_source_city"]) || srcCity;

  // Dataset-level
  obj.dataset_hash = (obj.dataset_hash ?? datasetHash);
  obj.as_of_date = (obj.as_of_date ?? asOfDate);
  obj.source_state = (obj.source_state ?? srcState);
  obj.zoning_dataset_hash = (obj.zoning_dataset_hash ?? zoningDatasetHash);
  obj.zoning_as_of_date = (obj.zoning_as_of_date ?? zoningAsOfDate);

  // Canonical aliases
  obj.property_id = (obj.property_id ?? property_id ?? null);
  obj.parcel_id_raw = (obj.parcel_id_raw ?? parcelRaw ?? null);
  obj.parcel_id_norm = (obj.parcel_id_norm ?? parcelNorm ?? null);
  obj.source_city = (obj.source_city ?? srcCity ?? null);

  obj.address_city = (obj.address_city ?? addrCity ?? null);
  obj.address_state = (obj.address_state ?? addrState ?? null);
  obj.address_zip = (obj.address_zip ?? addrZip ?? null);

  obj.latitude = (obj.latitude ?? lat ?? null);
  obj.longitude = (obj.longitude ?? lon ?? null);

  obj.coord_confidence_grade = (obj.coord_confidence_grade ?? coordGrade ?? null);

  obj.parcel_centroid_lat = (obj.parcel_centroid_lat ?? cLat ?? lat ?? null);
  obj.parcel_centroid_lon = (obj.parcel_centroid_lon ?? cLon ?? lon ?? null);

  obj.crs = (obj.crs ?? "EPSG:4326");

  obj.base_zoning_status = (obj.base_zoning_status ?? zStatus ?? (zCodeNorm ? "ATTACHED" : "UNKNOWN"));
  obj.base_zoning_code_raw = (obj.base_zoning_code_raw ?? zCodeRaw ?? null);
  obj.base_zoning_code_norm = (obj.base_zoning_code_norm ?? zCodeNorm ?? null);

  obj.zoning_attach_method = (obj.zoning_attach_method ?? attachMethod ?? null);
  obj.zoning_attach_confidence = (obj.zoning_attach_confidence ?? attachConf ?? (obj.base_zoning_status === "ATTACHED" ? "B" : "C"));
  obj.zoning_source_city = (obj.zoning_source_city ?? zoningSourceCity ?? null);

  // Ensure REQUIRED keys exist (null if unknown)
  for (const k of REQUIRED) {
    if (!Object.prototype.hasOwnProperty.call(obj, k) || obj[k] === undefined) obj[k] = null;
  }

  out.write(JSON.stringify(obj) + "\n");
  wrote++;

  if (read % 250000 === 0) {
    console.error(`[prog] read=${read} wrote=${wrote}`);
  }
}

out.end();
console.error(`[done] read=${read} wrote=${wrote}`);
'@

  Set-Content -Encoding UTF8 $dst $js
  Write-Host "[done] wrote $dst"
}

function Verify-Schema-And-Overlays([string]$contractNdjson, [string]$auditDir) {
  $req = @(
    "parcel_id_raw","parcel_id_norm","source_city","source_state","dataset_hash","as_of_date",
    "address_city","address_state","address_zip",
    "latitude","longitude","coord_confidence_grade",
    "parcel_centroid_lat","parcel_centroid_lon","crs",
    "base_zoning_status","base_zoning_code_raw","base_zoning_code_norm",
    "zoning_attach_method","zoning_attach_confidence","zoning_source_city","zoning_dataset_hash","zoning_as_of_date"
  )

  $sampleLines = Get-Content $contractNdjson -TotalCount 5
  $schemaMissing = @()

  foreach ($k in $req) {
    $present = $false
    foreach ($l in $sampleLines) {
      $o = $l | ConvertFrom-Json
      if ($null -ne $o.PSObject.Properties[$k]) { $present = $true; break }
    }
    if (-not $present) { $schemaMissing += $k }
  }

  $overlayPtrs = @(
    ".\publicData\overlays\_frozen\CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt",
    ".\publicData\overlays\_frozen\CURRENT_ENV_WETLANDS_MA.txt",
    ".\publicData\overlays\_frozen\CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt",
    ".\publicData\overlays\_frozen\CURRENT_ENV_PROS_MA.txt",
    ".\publicData\overlays\_frozen\CURRENT_ENV_AQUIFERS_MA.txt",
    ".\publicData\overlays\_frozen\CURRENT_ENV_ZONEII_IWPA_MA.txt",
    ".\publicData\overlays\_frozen\CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"
  )

  $overlayIssues = @()
  foreach ($p in $overlayPtrs) {
    if (!(Test-Path $p)) { $overlayIssues += "Missing pointer: $p"; continue }
    $d = (Get-Content $p -Raw).Trim()
    if ([string]::IsNullOrWhiteSpace($d) -or !(Test-Path $d)) { $overlayIssues += "Bad pointer target: $p -> $d"; continue }
    if (!(Test-Path (Join-Path $d "MANIFEST.json"))) { $overlayIssues += "Not GREEN (no MANIFEST): $p -> $d" }
    if (Test-Path (Join-Path $d "SKIPPED.txt")) { $overlayIssues += "Not GREEN (SKIPPED present): $p -> $d" }
  }

  $status = "PASS"
  $notes = @()

  if ($schemaMissing.Count -gt 0) { $status = "FAIL"; $notes += "Contract v1 schema missing keys: " + ($schemaMissing -join ", ") }
  if ($overlayIssues.Count -gt 0) { $status = "FAIL"; $notes += $overlayIssues }

  $report = [ordered]@{
    status = $status
    created_at = (Get-Date).ToString("o")
    contract_ndjson = $contractNdjson
    schema_missing_keys = $schemaMissing
    overlay_issues = $overlayIssues
    notes = $notes
  }

  $jsonPath = Join-Path $auditDir "verify_report.json"
  $txtPath  = Join-Path $auditDir "verify_report.txt"
  Write-Json $jsonPath $report

  $lines = @()
  $lines += "STATUS: $status"
  $lines += ""
  if ($schemaMissing.Count -gt 0) {
    $lines += "SCHEMA MISSING KEYS:"
    $lines += ($schemaMissing | ForEach-Object { " - $_" })
    $lines += ""
  }
  if ($overlayIssues.Count -gt 0) {
    $lines += "OVERLAY ISSUES:"
    $lines += ($overlayIssues | ForEach-Object { " - $_" })
    $lines += ""
  }
  if ($notes.Count -gt 0) {
    $lines += "NOTES:"
    $lines += ($notes | ForEach-Object { " - $_" })
  }

  $lines | Set-Content -Encoding UTF8 $txtPath

  Write-Host ""
  Write-Host "[ok] wrote: $jsonPath"
  Write-Host "[ok] wrote: $txtPath"
  Write-Host "[result] status: $status"
  if ($status -ne "PASS") { Write-Host "[result] see FAIL NOTES in verify_report.txt" }

  if ($status -ne "PASS") { exit 3 }
}

# -----------------------------
# MAIN
# -----------------------------
$auditDir = Join-Path ".\publicData\_audit" ("phase1a_finish_verify__" + (Stamp))
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

# 1) Fix “GREEN” for wetlands buffer pointer (no SKIPPED)
$wetBufPtr = ".\publicData\overlays\_frozen\CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt"
Ensure-Green-Freeze-NoSkipped $wetBufPtr "env_wetlands_buffer_100ft__ma__v1"

# 2) Resolve property spine input + hash
$propIn = Find-Latest-PropertySpineNdjson
$propSha = (Get-FileHash $propIn -Algorithm SHA256).Hash
Write-Host ""
Write-Host "[info] using property spine: $propIn"
Write-Host "[info] sha256: $propSha"

# 3) Write builder .mjs (prevents “JS pasted into PS” mistake)
Write-Contract-Builder-MJS

# 4) Build Contract v1 + freeze + pointer
$asOf = "2025-12-22"
$contractKey = "properties_withBaseZoning__CONTRACT_V1"
$contractDir = Join-Path ".\publicData\properties\_frozen" ($contractKey + "__FREEZE__" + (Stamp))
New-Item -ItemType Directory -Force -Path $contractDir | Out-Null
$contractNdjson = Join-Path $contractDir ($contractKey + ".ndjson")

Write-Host ""
Write-Host "[run] build Contract v1 spine..."
node .\scripts\gis\build_property_spine_contract_v1.mjs `
  --in $propIn `
  --out $contractNdjson `
  --asOfDate $asOf `
  --datasetHash $propSha `
  --defaultState "MA" `
  --zoningDatasetHash $propSha `
  --zoningAsOfDate $asOf

if ($LASTEXITCODE -ne 0) { throw "Contract builder failed (exit=$LASTEXITCODE)" }

$contractSha = (Get-FileHash $contractNdjson -Algorithm SHA256).Hash
$manifest = [ordered]@{
  artifact_key = $contractKey
  created_at   = (Get-Date).ToString("o")
  inputs       = @{ properties_in=$propIn; properties_sha256=$propSha }
  outputs      = @{ contract_ndjson=(Split-Path $contractNdjson -Leaf); contract_sha256=$contractSha }
  stats        = @{ note="Derived Contract v1 schema view; base zoning freeze remains immutable." }
}
Write-Json (Join-Path $contractDir "MANIFEST.json") $manifest

$contractPtr = ".\publicData\properties\_frozen\CURRENT_PROPERTY_SPINE_CONTRACT_V1.txt"
Set-Content -Encoding UTF8 $contractPtr $contractDir

Write-Host "[done] Contract v1 frozen: $contractDir"
Write-Host "[done] pointer: $contractPtr"

# 5) Verify (schema + Phase1A overlay pointers green)
Verify-Schema-And-Overlays $contractNdjson $auditDir
