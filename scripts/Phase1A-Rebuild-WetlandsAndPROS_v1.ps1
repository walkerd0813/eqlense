param(
  [string]$PropsPointer = ".\publicData\properties\_frozen\CURRENT_BASE_ZONING.txt",
  [string]$AttachNode   = ".\scripts\gis\attach_overlay_geojson_polygons_to_properties_v5.mjs",
  [string]$AsOfDate     = ""
)

$ErrorActionPreference = "Stop"
if (-not $AsOfDate) { $AsOfDate = (Get-Date).ToString("yyyy-MM-dd") }

function Ensure-Dir([string]$p){ New-Item -ItemType Directory -Force $p | Out-Null }

function Get-PropsFile([string]$ptr){
  $dir = (Get-Content $ptr -Raw).Trim()
  if (!(Test-Path $dir)) { throw "properties freeze dir not found: $dir" }
  $f = Get-ChildItem $dir -File -Filter "*.ndjson" | Sort-Object Length -Descending | Select-Object -First 1
  if (-not $f) { throw "no .ndjson found in $dir" }
  return $f.FullName
}

function Freeze-Folder([string]$layerKey, [string]$workDir, [string]$pointerPath, [hashtable]$meta, [string]$propsSha){
  $stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
  $frozenRoot = ".\publicData\overlays\_frozen"
  Ensure-Dir $frozenRoot

  $freezeDir = Join-Path $frozenRoot ("${layerKey}__FREEZE__" + $stamp)
  Ensure-Dir $freezeDir
  Copy-Item (Join-Path $workDir "*") $freezeDir -Recurse -Force

  $files = Get-ChildItem $freezeDir -Recurse -File
  $hashes = @()
  foreach ($f in $files) {
    $hashes += [pscustomobject]@{
      rel = $f.FullName.Substring($freezeDir.Length).TrimStart("\","/")
      sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $f.FullName).Hash.ToUpper()
      size_bytes = $f.Length
    }
  }

  $freezeManifest = @{
    artifact_key = $layerKey
    frozen_at = (Get-Date).ToString("s")
    properties_source_sha256 = $propsSha
    meta = $meta
    files = $hashes
  }
  ($freezeManifest | ConvertTo-Json -Depth 8) | Set-Content -Encoding UTF8 (Join-Path $freezeDir "FREEZE_MANIFEST.json")

  $freezeDir | Set-Content -Encoding UTF8 $pointerPath
  Write-Host "[done] froze:" $freezeDir
  Write-Host "[done] pointer:" $pointerPath
}

function Assert-GreenWorkDir([string]$workDir, [string]$layerKey){
  if (!(Test-Path (Join-Path $workDir "MANIFEST.json"))) { throw "missing MANIFEST.json for $layerKey (attach incomplete)" }
  if (Test-Path (Join-Path $workDir "SKIPPED.txt")) { throw "unexpected SKIPPED.txt for $layerKey (should be polygon attach)" }
  $att = Join-Path $workDir "attachments.ndjson"
  if (!(Test-Path $att)) { throw "missing attachments.ndjson for $layerKey" }
  $sz = (Get-Item $att).Length
  if ($sz -lt 1000) { throw "attachments.ndjson too small ($sz bytes) for $layerKey — attach likely failed/aborted" }
}

# Resolve properties
$propsFile = Get-PropsFile $PropsPointer
$propsSha  = (Get-FileHash -Algorithm SHA256 -LiteralPath $propsFile).Hash.ToUpper()

Write-Host ""
Write-Host "[info] property spine:" $propsFile
Write-Host "[info] property spine sha256:" $propsSha
Write-Host "[info] as_of_date:" $AsOfDate
Write-Host ""

if (!(Test-Path $AttachNode)) { throw "Missing attach node script: $AttachNode" }

$workRoot = ".\publicData\overlays\_work"
Ensure-Dir $workRoot

$layers = @(
  @{
    key="env_wetlands__ma__v1"
    geo=".\publicData\overlays\_statewide\env_wetlands\_normalized\wetlands_ma__clip_bbox.geojsons"
    pointer=".\publicData\overlays\_frozen\CURRENT_ENV_WETLANDS_MA.txt"
    source="MassDEP/MassGIS Wetlands (GeoJSONSeq clip bbox)"
  },
  @{
    key="env_pros__ma__v1"
    geo=".\publicData\overlays\_statewide\env_open_space_pros\_normalized\pros_ma_polygons__clip_bbox.geojsons"
    pointer=".\publicData\overlays\_frozen\CURRENT_ENV_PROS_MA.txt"
    source="MassGIS PROS polygons (GeoJSONSeq clip bbox)"
  }
)

foreach ($L in $layers) {
  if (!(Test-Path $L.geo)) { throw "missing geo input: $($L.geo)" }

  $workDir = Join-Path $workRoot $L.key
  Remove-Item $workDir -Recurse -Force -ErrorAction SilentlyContinue
  Ensure-Dir $workDir

  Write-Host ""
  Write-Host "[run] attach:" $L.key
  Write-Host "      geo:" $L.geo
  Write-Host "      out:" $workDir

  # Give node more heap (wetlands/pros can be heavy)
  node --max-old-space-size=16384 $AttachNode `
    --properties $propsFile `
    --geojson $L.geo `
    --layerKey $L.key `
    --outDir $workDir `
    --asOfDate $AsOfDate `
    --sourceSystem $L.source `
    --jurisdictionName "Massachusetts"

  if ($LASTEXITCODE -ne 0) { throw ("node attach failed for " + $L.key + " exit=" + $LASTEXITCODE) }

  Assert-GreenWorkDir $workDir $L.key

  Freeze-Folder $L.key $workDir $L.pointer @{
    geo_used=$L.geo
    sourceSystem=$L.source
    as_of_date=$AsOfDate
  } $propsSha
}

Write-Host ""
Write-Host "[ok] Wetlands + PROS rebuilt and CURRENT pointers updated (green-only)."

