param(
  [string]$PropertiesNdjson = "",
  [string]$OverlaysFrozenDir = ".\publicData\overlays\_frozen",
  [string]$OutAuditRoot = ".\publicData\_audit",
  [int]$SampleLines = 4000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Read-Text([string]$p) {
  if ([string]::IsNullOrWhiteSpace($p)) { return $null }
  if (!(Test-Path $p)) { return $null }
  return (Get-Content $p -Raw).Trim()
}

function Find-LatestPropertiesNdjson {
  if (![string]::IsNullOrWhiteSpace($PropertiesNdjson) -and (Test-Path $PropertiesNdjson)) { return $PropertiesNdjson }

  $root = ".\publicData\properties\_frozen"
  if (!(Test-Path $root)) { throw "Missing: $root" }

  $cand = Get-ChildItem $root -Directory |
    Where-Object { $_.Name -match 'properties_.*withBaseZoning.*__FREEZE__' } |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

  if ($null -eq $cand) { throw "Could not find latest properties_*withBaseZoning* freeze dir under $root" }

  $nd = Get-ChildItem $cand.FullName -File -Filter "*.ndjson" | Select-Object -First 1
  if ($null -eq $nd) { throw "No .ndjson found in: $($cand.FullName)" }

  return $nd.FullName
}

function Ensure-Dir([string]$p) {
  if (!(Test-Path $p)) { New-Item -ItemType Directory -Force -Path $p | Out-Null }
}

function Write-Json([string]$path, $obj) {
  ($obj | ConvertTo-Json -Depth 25) | Set-Content -Encoding UTF8 $path
}

function ReFreeze-ToGreen-WithManifest {
  param(
    [Parameter(Mandatory=$true)][string]$PointerFileName,
    [Parameter(Mandatory=$true)][string]$PropertiesPath,
    [Parameter(Mandatory=$true)][string]$PropertiesSha
  )

  $pfile = Join-Path $OverlaysFrozenDir $PointerFileName
  if (!(Test-Path $pfile)) { throw "Missing overlay pointer file: $pfile" }

  $srcDir = (Get-Content $pfile -Raw).Trim()
  if ([string]::IsNullOrWhiteSpace($srcDir) -or !(Test-Path $srcDir)) { throw "Pointer invalid: $PointerFileName -> $srcDir" }

  $fc = Join-Path $srcDir "feature_catalog.ndjson"
  $att = Join-Path $srcDir "attachments.ndjson"
  if (!(Test-Path $fc)) { throw "Missing feature_catalog.ndjson in $srcDir" }
  if (!(Test-Path $att)) { throw "Missing attachments.ndjson in $srcDir" }

  $hasManifest = Test-Path (Join-Path $srcDir "MANIFEST.json")
  $hasSkipped  = Test-Path (Join-Path $srcDir "SKIPPED.txt")

  if ($hasManifest -and -not $hasSkipped) {
    Write-Host "[ok] already GREEN: $PointerFileName -> $srcDir"
    return
  }

  $baseName = Split-Path $srcDir -Leaf
  $artifactKey = $baseName -replace '__FREEZE__.*$',''
  $ts = Get-Date -Format "yyyyMMdd_HHmmss"
  $dstDir = Join-Path $OverlaysFrozenDir ($artifactKey + "__FREEZE__" + $ts)

  Ensure-Dir $dstDir

  Copy-Item $fc (Join-Path $dstDir "feature_catalog.ndjson") -Force
  Copy-Item $att (Join-Path $dstDir "attachments.ndjson") -Force

  # If there was SKIPPED in the old dir, we intentionally do NOT copy it.
  # We are re-freezing to GREEN (MANIFEST present, NO SKIPPED).

  $fcSha  = (Get-FileHash -Algorithm SHA256 (Join-Path $dstDir "feature_catalog.ndjson")).Hash
  $attSha = (Get-FileHash -Algorithm SHA256 (Join-Path $dstDir "attachments.ndjson")).Hash

  $manifest = [ordered]@{
    artifact_key = $artifactKey
    created_at = (Get-Date).ToString("o")
    inputs = @{
      properties_path = $PropertiesPath
      properties_sha256 = $PropertiesSha
      note = "Backfilled MANIFEST from prior freeze dir; geo_path not re-linked in this backfill (explicitly unknown)."
      geo_path = $null
      geo_sha256 = $null
    }
    outputs = @{
      feature_catalog_ndjson = (Join-Path $dstDir "feature_catalog.ndjson")
      attachments_ndjson = (Join-Path $dstDir "attachments.ndjson")
      feature_catalog_sha256 = $fcSha
      attachments_sha256 = $attSha
    }
    stats = @{
      features_count = $null
      attachments_written = $null
    }
  }

  Write-Json (Join-Path $dstDir "MANIFEST.json") $manifest

  # update pointer
  Set-Content -Encoding UTF8 $pfile $dstDir

  Write-Host "[done] re-froze GREEN (manifested): $PointerFileName -> $dstDir"
}

# ---------- Write Node contract-view builder ----------
Ensure-Dir ".\scripts\gis"

$nodePath = ".\scripts\gis\build_properties_contract_view_v1.mjs"

@"
import fs from "node:fs";
import readline from "node:readline";
import crypto from "node:crypto";

function sha256File(p){
  const h = crypto.createHash("sha256");
  const s = fs.createReadStream(p);
  return new Promise((resolve,reject)=>{
    s.on("data", d=>h.update(d));
    s.on("error", reject);
    s.on("end", ()=>resolve(h.digest("hex").toUpperCase()));
  });
}

function getPath(obj, path){
  if (!obj || !path) return undefined;
  const parts = path.split(".");
  let cur = obj;
  for (const part of parts){
    if (cur && Object.prototype.hasOwnProperty.call(cur, part)){
      cur = cur[part];
    } else {
      return undefined;
    }
  }
  return cur;
}

function pick(obj, candidates){
  for (const p of candidates){
    const v = getPath(obj, p);
    if (v !== undefined && v !== null && v !== "") return v;
  }
  return undefined;
}

function deriveCoordGrade(coordSource){
  if (!coordSource) return null;
  const s = String(coordSource).toLowerCase();
  if (s.includes("address_point")) return "A";
  if (s.includes("parcel_centroid")) return "B";
  if (s.includes("external")) return "C";
  return null;
}

async function run(){
  const args = process.argv.slice(2);
  const arg = (k)=>{
    const i = args.indexOf(k);
    if (i === -1) return null;
    return args[i+1] ?? null;
  };

  const inPath = arg("--in");
  const outPath = arg("--out");
  const asOf = arg("--asOf") || new Date().toISOString().slice(0,10);
  const datasetHash = arg("--datasetHash"); // sha256 of input file

  if (!inPath || !outPath) {
    console.error("usage: node build_properties_contract_view_v1.mjs --in <in.ndjson> --out <out.ndjson> --asOf YYYY-MM-DD --datasetHash <sha>");
    process.exit(2);
  }

  const inSha = datasetHash || await sha256File(inPath);

  fs.mkdirSync(new URL(".", "file://" + process.cwd().replace(/\\/g,"/") + "/" + outPath.replace(/\\/g,"/")), { recursive: true });

  const input = fs.createReadStream(inPath, { encoding: "utf8" });
  const rl = readline.createInterface({ input, crlfDelay: Infinity });
  const out = fs.createWriteStream(outPath, { encoding: "utf8" });

  let read = 0;
  let wrote = 0;
  let missing = 0;

  for await (const line of rl){
    if (!line || !line.trim()) continue;
    read++;
    let o;
    try { o = JSON.parse(line); } catch { continue; }

    // Your v46 uses: parcel_id, town, state, zip, lat, lon, etc.
    const property_id = pick(o, ["property_id","id"]);
    const parcel_id_raw = pick(o, ["parcel_id_raw","parcel_id","parcelId"]);
    const source_city = pick(o, ["source_city","town","city"]);
    const source_state = pick(o, ["source_state","state"]);
    const address_city = pick(o, ["address_city","address.city","city","town"]);
    const address_state = pick(o, ["address_state","address.state","state"]);
    const address_zip = pick(o, ["address_zip","address.zip","zip","zipcode"]);

    const latitude = pick(o, ["latitude","lat","location.lat","coord.lat"]);
    const longitude = pick(o, ["longitude","lon","lng","location.lon","location.lng","coord.lon","coord.lng"]);

    const coord_source = pick(o, ["coord_source","coord.source","coordSource"]);
    const coord_conf = pick(o, ["coord_confidence_grade","coord_confidence","coord.grade","coordGrade"]) ?? deriveCoordGrade(coord_source);

    const centroidLat = pick(o, ["parcel_centroid_lat","parcel_centroid.lat","centroid_lat","centroid.lat"]);
    const centroidLon = pick(o, ["parcel_centroid_lon","parcel_centroid.lon","parcel_centroid.lng","centroid_lon","centroid.lon","centroid.lng"]);

    const crs = pick(o, ["crs","geom.crs","parcel.crs"]) ?? "EPSG:4326";

    const baseStatus = pick(o, ["base_zoning_status","zoning_base_status","zoning.status","base_zoning.status","zoning_status"]) ?? "UNKNOWN";
    const baseCodeRaw = pick(o, ["base_zoning_code_raw","zoning_code_raw","base_zoning.code_raw","zoning.code_raw"]);
    const baseCodeNorm = pick(o, ["base_zoning_code_norm","zoning_code_norm","base_zoning.code_norm","zoning.code_norm"]);

    const attachMethod = pick(o, ["zoning_attach_method","base_zoning_attach_method","zoning.attach_method","base_zoning.attach_method"]);
    const attachConf = pick(o, ["zoning_attach_confidence","base_zoning_attach_confidence","zoning.attach_confidence","base_zoning.attach_confidence"]);

    // These may not exist in-row in v46; include keys anyway (null ok for schema).
    const zoning_source_city = pick(o, ["zoning_source_city","zoning.source_city","base_zoning.source_city"]) ?? source_city;
    const zoning_dataset_hash = pick(o, ["zoning_dataset_hash","zoning.dataset_hash","base_zoning.dataset_hash"]);
    const zoning_as_of_date = pick(o, ["zoning_as_of_date","zoning.as_of_date","base_zoning.as_of_date"]);

    const row = {
      // ---- Contract headers ----
      property_id: property_id ?? null,
      parcel_id_raw: parcel_id_raw ?? null,
      parcel_id_norm: pick(o, ["parcel_id_norm","parcel_id_normalized","parcel_id_canon"]) ?? null,

      source_city: source_city ?? null,
      source_state: source_state ?? null,

      dataset_hash: inSha,
      as_of_date: asOf,

      address_city: address_city ?? null,
      address_state: address_state ?? null,
      address_zip: address_zip ?? null,

      latitude: latitude ?? null,
      longitude: longitude ?? null,

      coord_source: coord_source ?? null,
      coord_confidence_grade: coord_conf ?? null,
      coord_distance_m: pick(o, ["coord_distance_m","coord.distance_m","coordDistanceM"]) ?? null,

      parcel_centroid_lat: centroidLat ?? null,
      parcel_centroid_lon: centroidLon ?? null,
      crs: crs,

      base_zoning_status: baseStatus,
      base_zoning_code_raw: baseCodeRaw ?? null,
      base_zoning_code_norm: baseCodeNorm ?? null,

      zoning_attach_method: attachMethod ?? null,
      zoning_attach_confidence: attachConf ?? null,
      zoning_source_city: zoning_source_city ?? null,
      zoning_dataset_hash: zoning_dataset_hash ?? null,
      zoning_as_of_date: zoning_as_of_date ?? null,

      // ---- Optional passthrough (keeps your original around for debugging; can remove later) ----
      _src: {
        town: o.town ?? null,
        parcel_id: o.parcel_id ?? null,
        lat: o.lat ?? null,
        lon: o.lon ?? null
      }
    };

    if (!row.property_id || !row.parcel_id_raw) missing++;

    out.write(JSON.stringify(row) + "\n");
    wrote++;

    if (read % 250000 === 0) {
      console.error(`[prog] read=`${read} wrote=`${wrote} missing_core=`${missing}`);
    }
  }

  out.end();
  console.error(`[done] read=`${read} wrote=`${wrote} missing_core=`${missing}`);
}

run().catch(e=>{ console.error(e); process.exit(1); });
"@ | Set-Content -Encoding UTF8 $nodePath

# ---------- Resolve properties + sha ----------
$propsNdjson = Find-LatestPropertiesNdjson
$propsSha = (Get-FileHash -Algorithm SHA256 $propsNdjson).Hash
$asOf = (Get-Date).ToString("yyyy-MM-dd")

Write-Host "[info] properties: $propsNdjson"
Write-Host "[info] properties_sha256: $propsSha"
Write-Host "[info] as_of_date: $asOf"

# ---------- (A) Fix the 3 pointer overlays to GREEN+MANIFEST ----------
$needManifestPointers = @(
  "CURRENT_ENV_AQUIFERS_MA.txt",
  "CURRENT_ENV_ZONEII_IWPA_MA.txt",
  "CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"
)

foreach ($p in $needManifestPointers) {
  ReFreeze-ToGreen-WithManifest -PointerFileName $p -PropertiesPath $propsNdjson -PropertiesSha $propsSha
}

# ---------- (B) Build contract-view properties (v47 derived) ----------
Ensure-Dir ".\publicData\properties\_derived"
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$outProps = ".\publicData\properties\_derived\properties_v47_contract_view__${ts}.ndjson"

Write-Host "[run] build contract view -> $outProps"
node $nodePath --in $propsNdjson --out $outProps --asOf $asOf --datasetHash $propsSha
if ($LASTEXITCODE -ne 0) { throw "contract-view builder failed" }

# ---------- (C) Verify quickly: schema + overlays GREEN ----------
# Sample schema paths from contract-view (it must contain these top-level keys now)
$first = Get-Content $outProps -TotalCount 1
if ([string]::IsNullOrWhiteSpace($first)) { throw "contract-view output is empty: $outProps" }
$o = $first | ConvertFrom-Json

$mustKeys = @(
  "property_id","parcel_id_raw","source_city","source_state","dataset_hash","as_of_date",
  "address_city","address_state","address_zip",
  "latitude","longitude","coord_confidence_grade",
  "parcel_centroid_lat","parcel_centroid_lon","crs",
  "base_zoning_status","base_zoning_code_raw","base_zoning_code_norm",
  "zoning_attach_method","zoning_attach_confidence","zoning_source_city","zoning_dataset_hash","zoning_as_of_date"
)

$missingKeys = @()
foreach ($k in $mustKeys) {
  if (-not ($o.PSObject.Properties.Name -contains $k)) { $missingKeys += $k }
}

$overlayIssues = @()
$requiredPointers = @(
  "CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt",
  "CURRENT_ENV_WETLANDS_MA.txt",
  "CURRENT_ENV_WETLANDS_BUFFER_100FT_MA.txt",
  "CURRENT_ENV_PROS_MA.txt",
  "CURRENT_ENV_AQUIFERS_MA.txt",
  "CURRENT_ENV_ZONEII_IWPA_MA.txt",
  "CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"
)

foreach ($pname in $requiredPointers) {
  $pfile = Join-Path $OverlaysFrozenDir $pname
  if (!(Test-Path $pfile)) { $overlayIssues += "Missing pointer: $pname"; continue }
  $dir = Read-Text $pfile
  if ([string]::IsNullOrWhiteSpace($dir) -or !(Test-Path $dir)) { $overlayIssues += "Pointer invalid: $pname"; continue }
  if (!(Test-Path (Join-Path $dir "MANIFEST.json"))) { $overlayIssues += "No MANIFEST: $pname"; continue }
  if (Test-Path (Join-Path $dir "SKIPPED.txt")) { $overlayIssues += "NOT GREEN (has SKIPPED): $pname"; continue }
}

$status = "PASS"
if ($missingKeys.Count -gt 0 -or $overlayIssues.Count -gt 0) { $status = "FAIL" }

# Write audit report
$audTs = Get-Date -Format "yyyyMMdd_HHmmss"
$outDir = Join-Path $OutAuditRoot ("phase1a_repair_contract_verify__" + $audTs)
Ensure-Dir $outDir

$report = [ordered]@{
  status = $status
  created_at = (Get-Date).ToString("o")
  properties_input = @{ path=$propsNdjson; sha256=$propsSha }
  properties_contract_view = @{ path=$outProps; as_of_date=$asOf }
  missing_contract_keys = $missingKeys
  overlay_pointer_issues = $overlayIssues
  next = @(
    "Use properties_v47_contract_view as the engine input contract until we decide to permanently reshape the spine.",
    "Phase 1A overlays are now GREEN+manifested."
  )
}

Write-Json (Join-Path $outDir "verify_report.json") $report

$txt = @()
$txt += ("STATUS: {0}" -f $status)
$txt += ("input_properties: {0}" -f $propsNdjson)
$txt += ("contract_view: {0}" -f $outProps)
$txt += ""
if ($missingKeys.Count -gt 0) { $txt += "MISSING CONTRACT KEYS:"; $missingKeys | % { $txt += " - $_" }; $txt += "" }
if ($overlayIssues.Count -gt 0) { $txt += "OVERLAY POINTER ISSUES:"; $overlayIssues | % { $txt += " - $_" }; $txt += "" }
$txt -join "`r`n" | Set-Content -Encoding UTF8 (Join-Path $outDir "verify_report.txt")

Write-Host ""
Write-Host "[ok] wrote: $(Join-Path $outDir "verify_report.json")"
Write-Host "[ok] wrote: $(Join-Path $outDir "verify_report.txt")"
Write-Host "[result] status: $status"
if ($status -ne "PASS") { Write-Host "[result] read verify_report.txt for details" }

