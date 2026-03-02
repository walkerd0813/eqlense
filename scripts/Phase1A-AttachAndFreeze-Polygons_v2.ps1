param(
  [string]$PropsPointer = ".\publicData\properties\_frozen\CURRENT_BASE_ZONING.txt",
  [string]$OverlaysRoot = ".\publicData\overlays\_statewide",
  [string]$AsOfDate = ""
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
function Pick-GeoJSON([string]$dir, [string]$fallbackPath){
  if (Test-Path $fallbackPath) { return $fallbackPath }
  if (!(Test-Path $dir)) { return $null }
  $g = Get-ChildItem $dir -File -Filter "*.geojson" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if ($g) { return $g.FullName }
  return $null
}
function Pick-Shp([string]$rawDir, [string[]]$preferRegex){
  $shps = Get-ChildItem $rawDir -Recurse -File -Filter "*.shp" -ErrorAction SilentlyContinue
  if (-not $shps) { return $null }
  foreach ($r in $preferRegex) {
    $hit = $shps | Where-Object { $_.Name -match $r } | Sort-Object Length -Descending | Select-Object -First 1
    if ($hit) { return $hit }
  }
  return ($shps | Sort-Object Length -Descending | Select-Object -First 1)
}
function Ensure-GeoJSONFromZip([string]$zipPath, [string]$rawDir, [string]$outGeo, [string[]]$prefer){
  $ogr = Get-Command ogr2ogr -ErrorAction SilentlyContinue
  if (-not $ogr) { throw "ogr2ogr not found on PATH. Install GDAL first." }

  if (Test-Path -LiteralPath $outGeo) { return $outGeo }
  if (-not $zipPath) { throw "No zip file found for conversion into $outGeo" }

  Ensure-Dir $rawDir
  Expand-Archive -LiteralPath $zipPath -DestinationPath $rawDir -Force

  $shp = Pick-Shp $rawDir $prefer
  if (-not $shp) { throw "No .shp found under $rawDir" }

  Ensure-Dir (Split-Path $outGeo)
  & $ogr.Source -t_srs EPSG:4326 -f GeoJSON $outGeo $shp.FullName | Out-Null
  return $outGeo
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

$nodeAttach = ".\scripts\gis\attach_overlay_geojson_polygons_to_properties_v1.mjs"
if (!(Test-Path $nodeAttach)) { throw "Missing node attach script: $nodeAttach" }

Write-Host ""
Write-Host "[info] property spine:" $propsFile.FullName
Write-Host "[info] property spine sha256:" $propsSha
Write-Host "[info] as_of_date:" $AsOfDate
Write-Host ""

# Build/ensure GeoJSON paths
$nfhlGeo = "$OverlaysRoot\env_nfhl\_normalized\nfhl_flood_hazard_ma.geojson"
$wetGeo  = "$OverlaysRoot\env_wetlands\_normalized\wetlands_ma.geojson"

$nfhlZip = Pick-Zip "$OverlaysRoot\env_nfhl\_inbox" @("NFHL","Flood","FEMA","Hazard")
$wetZip  = Pick-Zip "$OverlaysRoot\env_wetlands\_inbox" @("wetlands","WETLANDS","dep")

$nfhlGeo = Ensure-GeoJSONFromZip $nfhlZip "$OverlaysRoot\env_nfhl\_raw" $nfhlGeo @("S_Fld_Haz_Ar","Fld_Haz","Haz.*Ar","flood.*haz","haz")
$wetGeo  = Ensure-GeoJSONFromZip $wetZip  "$OverlaysRoot\env_wetlands\_raw" $wetGeo  @("WETLAND","Wetland","DEP.*WET","WET.*POLY","WETLANDS.*POLY")

$prosGeo = "$OverlaysRoot\env_open_space_pros\_normalized\pros_ma.geojson"
$swspGeo = "$OverlaysRoot\env_surface_water_supply_protection\_normalized\swsp_zones_abc_ma.geojson"

$zoneDir = "$OverlaysRoot\env_wellhead_zoneii_iwpa\_normalized"
$aqDir   = "$OverlaysRoot\env_aquifers\_normalized"
$zoneGeo = Pick-GeoJSON $zoneDir "$zoneDir\zoneii_iwpa_ma.geojson"
$aqGeo   = Pick-GeoJSON $aqDir   "$aqDir\aquifers_ma.geojson"

$layers = @(
  @{ key="env_nfhl_flood_hazard__ma__v1"; geo=$nfhlGeo; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_NFHL_FLOOD_HAZARD_MA.txt"; source="FEMA NFHL" },
  @{ key="env_wetlands__ma__v1";         geo=$wetGeo;  pointer=".\publicData\overlays\_frozen\CURRENT_ENV_WETLANDS_MA.txt";          source="MassDEP/MassGIS Wetlands" },

  @{ key="env_pros__ma__v1";             geo=$prosGeo; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_PROS_MA.txt";              source="MassGIS PROS" },
  @{ key="env_swsp_zones_abc__ma__v1";   geo=$swspGeo; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_SWSP_ZONES_ABC_MA.txt";    source="MassGIS SWSP Zones A/B/C" },

  @{ key="env_zoneii_iwpa__ma__v1";      geo=$zoneGeo; pointer=".\publicData\overlays\_frozen\CURRENT_ENV_ZONEII_IWPA_MA.txt";       source="MassDEP Wellhead (Zone II/IWPA)" },
  @{ key="env_aquifers__ma__v1";         geo=$aqGeo;   pointer=".\publicData\overlays\_frozen\CURRENT_ENV_AQUIFERS_MA.txt";          source="MassGIS Aquifers" }
)

$workRoot = ".\publicData\overlays\_work"
Ensure-Dir $workRoot

foreach ($L in $layers) {
  if (-not $L.geo -or !(Test-Path -LiteralPath $L.geo)) {
    Write-Host "[skip] missing geojson for" $L.key ":" $L.geo
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

  Freeze-Folder $L.key $workDir $L.pointer @{ geojson=$L.geo; sourceSystem=$L.source; as_of_date=$AsOfDate } $propsSha
}

Write-Host ""
Write-Host "[ok] Phase 1A polygon constraints attached + frozen (polygons only)."
Write-Host "     Deferred: PWS (points), Vernal Pools (points), Hydrography (lines)."
