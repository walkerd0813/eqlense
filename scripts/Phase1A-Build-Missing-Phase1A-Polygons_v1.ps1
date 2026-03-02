param(
  [string]$PropertiesNdjson = "C:\seller-app\backend\publicData\properties\_frozen\properties_v46_withBaseZoning__20251220_from_v44__FREEZE__20251221_123730\properties_v46_withBaseZoning__20251220_from_v44.ndjson",
  [string]$AsOfDate = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-Dir([string]$p) { if (!(Test-Path $p)) { New-Item -ItemType Directory -Path $p | Out-Null } }
function Sha256([string]$p) { (Get-FileHash -Algorithm SHA256 $p).Hash }

# Massachusetts bbox you already computed from zoned points (good for clipping performance).
$bbox = @{
  minX = -73.554662807589
  minY =  41.1904621318313
  maxX = -69.8830823068283
  maxY =  42.9363915635423
}

if ([string]::IsNullOrWhiteSpace($AsOfDate)) { $AsOfDate = (Get-Date -Format "yyyy-MM-dd") }

if (!(Test-Path $PropertiesNdjson)) { throw "Missing properties ndjson: $PropertiesNdjson" }

$overlaysStatewide = ".\publicData\overlays\_statewide"
$overlaysWork      = ".\publicData\overlays\_work"
$overlaysFrozen    = ".\publicData\overlays\_frozen"
Ensure-Dir $overlaysWork
Ensure-Dir $overlaysFrozen

$attachTool = ".\scripts\gis\attach_overlay_geojson_polygons_to_properties_v4.mjs"
if (!(Test-Path $attachTool)) { throw "Missing attach tool: $attachTool" }

function Find-FirstFile([string]$root, [string[]]$patterns) {
  foreach ($pat in $patterns) {
    $hit = Get-ChildItem -Path $root -Recurse -File -Filter $pat -ErrorAction SilentlyContinue |
      Sort-Object LastWriteTime -Descending |
      Select-Object -First 1
    if ($null -ne $hit) { return $hit.FullName }
  }
  return $null
}

function Build-GeoJSONSeq([string]$src, [string]$dstGeojsons) {
  Ensure-Dir (Split-Path $dstGeojsons -Parent)

  # ogr2ogr supports reading shp/geojson and writing GeoJSONSeq streaming-style.
  $minX = $bbox.minX; $minY = $bbox.minY; $maxX = $bbox.maxX; $maxY = $bbox.maxY

  Write-Host "[info] ogr2ogr -> GeoJSONSeq"
  Write-Host "       in:  $src"
  Write-Host "       out: $dstGeojsons"

  & ogr2ogr -f GeoJSONSeq $dstGeojsons $src `
    -t_srs EPSG:4326 -makevalid -skipfailures `
    -clipsrc $minX $minY $maxX $maxY | Out-Null

  if (!(Test-Path $dstGeojsons)) { throw "Failed to create geojsons: $dstGeojsons" }
}

function Try-NodeAttach([string]$layerKey, [string]$geojsonsPath, [string]$outDir) {
  Ensure-Dir $outDir

  # We try a few common flag layouts, and succeed if the outputs exist.
  $attempts = @(
    @("--properties", $PropertiesNdjson, "--geo", $geojsonsPath, "--outDir", $outDir, "--layerKey", $layerKey, "--asOfDate", $AsOfDate),
    @("--propertiesPath", $PropertiesNdjson, "--geoPath", $geojsonsPath, "--outDir", $outDir, "--artifact_key", $layerKey, "--as_of_date", $AsOfDate),
    @("--in", $PropertiesNdjson, "--geo", $geojsonsPath, "--out", $outDir, "--key", $layerKey, "--asOf", $AsOfDate)
  )

  foreach ($args in $attempts) {
    Write-Host "[run] node attach attempt: $layerKey"
    & node $attachTool @args
    $fc  = Join-Path $outDir "feature_catalog.ndjson"
    $att = Join-Path $outDir "attachments.ndjson"
    if ((Test-Path $fc) -and (Test-Path $att)) {
      return @{ feature_catalog = $fc; attachments = $att }
    }
  }

  throw "Node attach did not produce feature_catalog/attachments for $layerKey. Run: node $attachTool --help to confirm flags."
}

function Freeze-FromWork([string]$layerKey, [string]$pointerFileName, [string]$workDir, [string]$geoSha) {
  $fc  = Join-Path $workDir "feature_catalog.ndjson"
  $att = Join-Path $workDir "attachments.ndjson"
  if (!(Test-Path $fc))  { throw "Missing feature_catalog.ndjson in $workDir" }
  if (!(Test-Path $att)) { throw "Missing attachments.ndjson in $workDir" }

  $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
  $freezeDir = Join-Path $overlaysFrozen ("{0}__FREEZE__{1}" -f $layerKey, $stamp)
  Ensure-Dir $freezeDir

  Copy-Item $fc  (Join-Path $freezeDir "feature_catalog.ndjson") -Force
  Copy-Item $att (Join-Path $freezeDir "attachments.ndjson")    -Force

  # Create MANIFEST (GREEN requires MANIFEST and NO SKIPPED)
  $manifest = @{
    artifact_key = $layerKey
    created_at   = (Get-Date).ToString("o")
    inputs       = @{
      properties_path = $PropertiesNdjson
      properties_sha256 = (Sha256 $PropertiesNdjson)
      geo_sha256 = $geoSha
      as_of_date = $AsOfDate
    }
    outputs      = @{
      feature_catalog_ndjson = "feature_catalog.ndjson"
      attachments_ndjson     = "attachments.ndjson"
    }
  } | ConvertTo-Json -Depth 6

  Set-Content -Encoding UTF8 (Join-Path $freezeDir "MANIFEST.json") $manifest

  $pointerPath = Join-Path $overlaysFrozen $pointerFileName
  Set-Content -Encoding UTF8 $pointerPath (".\publicData\overlays\_frozen\" + (Split-Path $freezeDir -Leaf))

  Write-Host "[done] GREEN freeze: $pointerFileName -> $freezeDir"
}

$layers = @(
  @{
    key = "env_aquifers__ma__v1"
    pointer = "CURRENT_ENV_AQUIFERS_MA.txt"
    root = Join-Path $overlaysStatewide "env_aquifers"
    outGeojsons = Join-Path $overlaysStatewide "env_aquifers\_normalized\aquifers_ma__clip_bbox.geojsons"
    patterns = @("*AQUIFER*.shp","*aquifer*.shp","*aquifers*.shp","*AQUIFER*.geojson","*aquifer*.geojson","*aquifers*.geojson")
  },
  @{
    key = "env_zoneii_iwpa__ma__v1"
    pointer = "CURRENT_ENV_ZONEII_IWPA_MA.txt"
    root = Join-Path $overlaysStatewide "env_zoneii_iwpa"
    outGeojsons = Join-Path $overlaysStatewide "env_zoneii_iwpa\_normalized\zoneii_iwpa_ma__clip_bbox.geojsons"
    patterns = @("*ZONE*II*.shp","*IWPA*.shp","*Wellhead*Protection*.shp","*ZONE*II*.geojson","*IWPA*.geojson","*Wellhead*Protection*.geojson")
  },
  @{
    key = "env_swsp_zones_abc__ma__v1"
    pointer = "CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"
    root = Join-Path $overlaysStatewide "env_surface_water_supply_protection"
    outGeojsons = Join-Path $overlaysStatewide "env_surface_water_supply_protection\_normalized\swsp_zones_abc_ma__clip_bbox.geojsons"
    patterns = @("*SWP*ZONES*.shp","*SWSP*.shp","*Surface*Water*Supply*Protection*.shp","*SWP*ZONES*.geojson","*SWSP*.geojson","*Surface*Water*Supply*Protection*.geojson")
  }
)

foreach ($L in $layers) {
  Write-Host ""
  Write-Host ("================  BUILD {0}  ================" -f $L.key)

  $src = Find-FirstFile $L.root $L.patterns
  if ([string]::IsNullOrWhiteSpace($src)) {
    throw "Could not find source for $($L.key) under $($L.root)"
  }

  Build-GeoJSONSeq $src $L.outGeojsons
  $geoSha = Sha256 $L.outGeojsons

  $workDir = Join-Path $overlaysWork $L.key
  $out = Try-NodeAttach $L.key $L.outGeojsons $workDir

  Freeze-FromWork $L.key $L.pointer $workDir $geoSha
}

Write-Host ""
Write-Host "[ok] Missing Phase1A polygons built and pointers updated."

# quick status print
Get-ChildItem $overlaysFrozen -File -Filter "CURRENT_ENV_*.txt" |
  Sort-Object Name |
  ForEach-Object {
    $dir = (Get-Content $_.FullName -Raw).Trim()
    $man = Test-Path (Join-Path $dir "MANIFEST.json")
    $sk  = Test-Path (Join-Path $dir "SKIPPED.txt")
    "{0} | MANIFEST={1} | SKIPPED={2} | {3}" -f $_.Name, $man, $sk, $dir
  } | Write-Host

