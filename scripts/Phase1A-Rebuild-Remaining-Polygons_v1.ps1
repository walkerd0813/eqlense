param(
  [string]$PropsPointer = ".\publicData\properties\_frozen\CURRENT_BASE_ZONING.txt",
  [string]$AttachNode = ".\scripts\gis\attach_overlay_geojson_polygons_to_properties_v4.mjs",
  [string]$OverlaysRoot = ".\publicData\overlays\_statewide",
  [string]$AsOfDate = ""
)

$ErrorActionPreference = "Stop"
if (-not $AsOfDate) { $AsOfDate = (Get-Date).ToString("yyyy-MM-dd") }

function Ensure-Dir([string]$p){ New-Item -ItemType Directory -Force $p | Out-Null }
function First-GeoJson([string]$dir){
  if (!(Test-Path $dir)) { return $null }
  $g = Get-ChildItem $dir -File -Filter "*.geojson" -ErrorAction SilentlyContinue | Sort-Object Length -Descending | Select-Object -First 1
  if ($g) { return $g.FullName }
  return $null
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

# Resolve frozen properties
$freezeDir = (Get-Content $PropsPointer -Raw).Trim()
if (!(Test-Path $freezeDir)) { throw "Properties freeze dir not found: $freezeDir" }
$propsFile = Get-ChildItem $freezeDir -File -Filter "*.ndjson" | Sort-Object Length -Descending | Select-Object -First 1
if (-not $propsFile) { throw "No ndjson found in $freezeDir" }
$propsSha = (Get-FileHash -Algorithm SHA256 -LiteralPath $propsFile.FullName).Hash.ToUpper()

if (!(Test-Path $AttachNode)) { throw "Missing node attach script: $AttachNode" }

Write-Host ""
Write-Host "[info] property spine:" $propsFile.FullName
Write-Host "[info] property spine sha256:" $propsSha
Write-Host "[info] as_of_date:" $AsOfDate
Write-Host ""

# Use the already-built clipped GeoJSONSeq files (do NOT re-clip here)
$wetSeq  = "$OverlaysRoot\env_wetlands\_normalized\wetlands_ma__clip_bbox.geojsons"
$prosSeq = "$OverlaysRoot\env_open_space_pros\_normalized\pros_ma_polygons__clip_bbox.geojsons"

# Regular GeoJSON polygon layers
$swspGeo = "$OverlaysRoot\env_surface_water_supply_protection\_normalized\swsp_zones_abc_ma.geojson"
$zoneGeo = First-GeoJson "$OverlaysRoot\env_wellhead_zoneii_iwpa\_normalized"
$aqGeo   = First-GeoJson "$OverlaysRoot\env_aquifers\_normalized"

$layers = @(
  @{ key="env_wetlands__ma__v1";         geo=$wetSeq;  pointer=".\publicData\overlays\_frozen\CURRENT_ENV_WETLANDS_MA.txt";          source="MassDEP/MassGIS Wetlands (GeoJSONSeq clip bbox)" },
  @{ key="env_pros__ma__v1";             geo=$prosSeq; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_PROS_MA.txt";              source="MassGIS PROS polygons (GeoJSONSeq clip bbox)" },
  @{ key="env_swsp_zones_abc__ma__v1";   geo=$swspGeo; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_SWSP_ZONES_ABC_MA.txt";    source="MassGIS SWSP Zones A/B/C" },
  @{ key="env_zoneii_iwpa__ma__v1";      geo=$zoneGeo; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_ZONEII_IWPA_MA.txt";        source="MassDEP Wellhead (Zone II/IWPA)" },
  @{ key="env_aquifers__ma__v1";         geo=$aqGeo;   pointer=".\publicData\overlays\_frozen\CURRENT_ENV_AQUIFERS_MA.txt";          source="MassGIS Aquifers" }
)

$workRoot = ".\publicData\overlays\_work"
Ensure-Dir $workRoot

foreach ($L in $layers) {
  if (-not $L.geo -or !(Test-Path -LiteralPath $L.geo)) {
    Write-Host "[skip] missing geo for" $L.key ":" $L.geo
    continue
  }

  $workDir = Join-Path $workRoot $L.key
  Remove-Item $workDir -Recurse -Force -ErrorAction SilentlyContinue
  Ensure-Dir $workDir

  Write-Host ""
  Write-Host "[run] attach:" $L.key
  Write-Host "      geo:" $L.geo

  node $AttachNode `
    --properties $propsFile.FullName `
    --geojson $L.geo `
    --layerKey $L.key `
    --outDir $workDir `
    --asOfDate $AsOfDate `
    --sourceSystem $L.source `
    --jurisdictionName "Massachusetts"

  if ($LASTEXITCODE -ne 0) { throw ("node attach failed for " + $L.key + " exit=" + $LASTEXITCODE) }
  if (Test-Path (Join-Path $workDir "SKIPPED.txt")) { throw ("unexpected SKIPPED for " + $L.key) }

  Freeze-Folder $L.key $workDir $L.pointer @{
    geo_used = $L.geo
    sourceSystem = $L.source
    as_of_date = $AsOfDate
  } $propsSha
}

Write-Host ""
Write-Host "[ok] Phase 1A remaining polygon layers attached + frozen."
Write-Host "     NFHL pointer untouched (already green)."

