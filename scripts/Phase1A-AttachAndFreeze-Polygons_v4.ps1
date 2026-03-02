param(
  [string]$PropsPointer = ".\publicData\properties\_frozen\CURRENT_BASE_ZONING.txt",
  [string]$OverlaysRoot = ".\publicData\overlays\_statewide",
  [string]$AsOfDate = "",
  [double]$MarginDeg = 0.05
)

$ErrorActionPreference = "Stop"
if (-not $AsOfDate) { $AsOfDate = (Get-Date).ToString("yyyy-MM-dd") }

function Ensure-Dir([string]$p){ New-Item -ItemType Directory -Force $p | Out-Null }

function Pick-Zip([string]$dir, [string[]]$prefer){
  if (!(Test-Path $dir)) { return $null }
  $z = Get-ChildItem $dir -File -Filter "*.zip" -ErrorAction SilentlyContinue
  if (-not $z) { return $null }
  foreach ($r in $prefer){
    $hit = $z | Where-Object { $_.Name -match $r } | Sort-Object Length -Descending | Select-Object -First 1
    if ($hit) { return $hit.FullName }
  }
  return ($z | Sort-Object Length -Descending | Select-Object -First 1).FullName
}

function Ensure-Unzipped([string]$zipPath, [string]$rawDir){
  if (!(Test-Path $zipPath)) { throw "Missing zip: $zipPath" }
  Ensure-Dir $rawDir
  Expand-Archive -LiteralPath $zipPath -DestinationPath $rawDir -Force
}

function Pick-Shp([string]$rawDir, [string[]]$preferRegex){
  $shps = Get-ChildItem $rawDir -Recurse -File -Filter "*.shp" -ErrorAction SilentlyContinue
  if (-not $shps) { return $null }
  foreach ($r in $preferRegex) {
    $hit = $shps | Where-Object { $_.Name -match $r } | Sort-Object Length -Descending | Select-Object -First 1
    if ($hit) { return $hit.FullName }
  }
  return ($shps | Sort-Object Length -Descending | Select-Object -First 1).FullName
}

function Ogr-Clip-To-GeoJSONSeq([string]$shpPath, [string]$outGeoSeq, [double]$minLon,[double]$minLat,[double]$maxLon,[double]$maxLat){
  $ogr = Get-Command ogr2ogr -ErrorAction SilentlyContinue
  if (-not $ogr) { throw "ogr2ogr not found on PATH. Install GDAL." }

  Ensure-Dir (Split-Path $outGeoSeq)
  if (Test-Path $outGeoSeq) { Remove-Item $outGeoSeq -Force }

  # IMPORTANT: -spat is interpreted in source SRS unless you set -spat_srs
  & $ogr.Source `
    -spat_srs EPSG:4326 -spat $minLon $minLat $maxLon $maxLat `
    -t_srs EPSG:4326 `
    -nlt PROMOTE_TO_MULTI `
    -f GeoJSONSeq $outGeoSeq $shpPath | Out-Null
}

function Freeze-Folder([string]$layerKey, [string]$workDir, [string]$pointerPath, [hashtable]$extraMeta, [string]$propsSha){
  $stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
  $frozenRoot = ".\publicData\overlays\_frozen"
  Ensure-Dir $frozenRoot
  $freezeDir = Join-Path $frozenRoot ("${layerKey}__FREEZE__" + $stamp)
  Ensure-Dir $freezeDir

  Copy-Item (Join-Path $workDir "*") $freezeDir -Recurse -Force

  $files = Get-ChildItem $freezeDir -Recurse -File
  $hashes = @()
  foreach ($f in $files) {
    $hashes += @{ path = $f.FullName; sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $f.FullName).Hash.ToUpper(); size_bytes = $f.Length }
  }

  $freezeManifest = @{
    artifact_key = $layerKey
    frozen_at = (Get-Date).ToString("s")
    properties_source_sha256 = $propsSha
    extra = $extraMeta
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

$nodeAttach = ".\scripts\gis\attach_overlay_geojson_polygons_to_properties_v3.mjs"
$bboxTool   = ".\scripts\gis\compute_zoned_bbox_from_properties_v2.mjs"
if (!(Test-Path $nodeAttach)) { throw "Missing node attach script: $nodeAttach" }
if (!(Test-Path $bboxTool))   { throw "Missing bbox tool: $bboxTool" }

Write-Host ""
Write-Host "[info] property spine:" $propsFile.FullName
Write-Host "[info] property spine sha256:" $propsSha
Write-Host "[info] as_of_date:" $AsOfDate
Write-Host ""

# Compute bbox (works now in your run)
$bboxJson = node $bboxTool --in $propsFile.FullName --onlyZoned 1 --marginDeg $MarginDeg
if ($LASTEXITCODE -ne 0) { throw "bbox tool failed" }
$bbox = $bboxJson | ConvertFrom-Json
$minLon = [double]$bbox.minLon; $minLat = [double]$bbox.minLat; $maxLon = [double]$bbox.maxLon; $maxLat = [double]$bbox.maxLat
Write-Host "[info] bbox:" $minLon $minLat $maxLon $maxLat
Write-Host ""

# --- NFHL + Wetlands (big) => CLIP + GeoJSONSeq
$nfhlZip = Pick-Zip "$OverlaysRoot\env_nfhl\_inbox" @("NFHL","Flood","FEMA","Hazard")
$wetZip  = Pick-Zip "$OverlaysRoot\env_wetlands\_inbox" @("wetlands","WETLANDS","dep")

Ensure-Unzipped $nfhlZip "$OverlaysRoot\env_nfhl\_raw"
Ensure-Unzipped $wetZip  "$OverlaysRoot\env_wetlands\_raw"

$nfhlShp = Pick-Shp "$OverlaysRoot\env_nfhl\_raw" @("S_Fld_Haz_Ar","Fld_Haz.*Ar","Haz.*Ar","S_Fld_Haz","haz")
$wetShp  = Pick-Shp "$OverlaysRoot\env_wetlands\_raw" @("WETLANDSDEP.*POLY","WETLAND.*POLY","DEP.*POLY.*WET","WET.*POLY")

if (-not $nfhlShp) { throw "NFHL polygon shp not found under env_nfhl/_raw" }
if (-not $wetShp)  { throw "Wetlands polygon shp not found under env_wetlands/_raw" }

$nfhlSeq = "$OverlaysRoot\env_nfhl\_normalized\nfhl_flood_hazard_ma__clip_bbox.geojsons"
$wetSeq  = "$OverlaysRoot\env_wetlands\_normalized\wetlands_ma__clip_bbox.geojsons"

Write-Host "[info] NFHL shp:" $nfhlShp
Write-Host "[info] Wetlands shp:" $wetShp

Write-Host "[info] clipping->geojsonseq NFHL:" $nfhlSeq
Ogr-Clip-To-GeoJSONSeq $nfhlShp $nfhlSeq $minLon $minLat $maxLon $maxLat
Write-Host "[info] clipping->geojsonseq Wetlands:" $wetSeq
Ogr-Clip-To-GeoJSONSeq $wetShp  $wetSeq  $minLon $minLat $maxLon $maxLat

# --- PROS (ensure POLYGON layer, not ARC)
$prosZip = Pick-Zip "$OverlaysRoot\env_open_space_pros\_inbox" @("OpenSpace","PROS")
if ($prosZip) { Ensure-Unzipped $prosZip "$OverlaysRoot\env_open_space_pros\_raw" }

$prosPolyShp = Pick-Shp "$OverlaysRoot\env_open_space_pros\_raw" @("OPENSPACE.*POLY","OPENSPACE.*PLY","PROS.*POLY","Open_Space.*POLY","OpenSpace.*POLY")
if (-not $prosPolyShp) {
  Write-Host "[warn] PROS polygon shp not found; PROS will be skipped this run."
}

$prosSeq = "$OverlaysRoot\env_open_space_pros\_normalized\pros_ma_polygons__clip_bbox.geojsons"
if ($prosPolyShp) {
  Write-Host "[info] clipping->geojsonseq PROS (polygons):" $prosSeq
  Ogr-Clip-To-GeoJSONSeq $prosPolyShp $prosSeq $minLon $minLat $maxLon $maxLat
}

# Smaller polygon layers (already normalized)
$swspGeo = "$OverlaysRoot\env_surface_water_supply_protection\_normalized\swsp_zones_abc_ma.geojson"
$zoneGeo = Get-ChildItem "$OverlaysRoot\env_wellhead_zoneii_iwpa\_normalized" -File -Filter "*.geojson" -ErrorAction SilentlyContinue | Select-Object -First 1
$aqGeo   = Get-ChildItem "$OverlaysRoot\env_aquifers\_normalized"           -File -Filter "*.geojson" -ErrorAction SilentlyContinue | Select-Object -First 1

$layers = @(
  @{ key="env_nfhl_flood_hazard__ma__v1"; geo=$nfhlSeq; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt"; source="FEMA NFHL (GeoJSONSeq clip bbox; spat_srs=4326)" },
  @{ key="env_wetlands__ma__v1";         geo=$wetSeq;  pointer=".\publicData\overlays\_frozen\CURRENT_ENV_WETLANDS_MA.txt";          source="MassDEP/MassGIS Wetlands (GeoJSONSeq clip bbox; spat_srs=4326)" }
)

if ($prosPolyShp) {
  $layers += @{ key="env_pros__ma__v1"; geo=$prosSeq; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_PROS_MA.txt"; source="MassGIS PROS polygons (GeoJSONSeq clip bbox; spat_srs=4326)" }
}

if (Test-Path $swspGeo) {
  $layers += @{ key="env_swsp_zones_abc__ma__v1"; geo=$swspGeo; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"; source="MassGIS SWSP Zones A/B/C" }
}

if ($zoneGeo) {
  $layers += @{ key="env_zoneii_iwpa__ma__v1"; geo=$zoneGeo.FullName; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_ZONEII_IWPA_MA.txt"; source="MassDEP Wellhead (Zone II/IWPA)" }
}
if ($aqGeo) {
  $layers += @{ key="env_aquifers__ma__v1"; geo=$aqGeo.FullName; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_AQUIFERS_MA.txt"; source="MassGIS Aquifers" }
}

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

  node $nodeAttach `
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
    clip_bbox = @{ minLon=$minLon; minLat=$minLat; maxLon=$maxLon; maxLat=$maxLat; marginDeg=$MarginDeg }
  } $propsSha
}

Write-Host ""
Write-Host "[ok] Phase 1A polygon constraints attached + frozen (polygons only)."
Write-Host "     Deferred: PWS (points), Vernal Pools (points), Hydrography (lines)."

