param(
  [string]$PropertiesNdjson = "",
  [string]$AttachScript = ".\scripts\gis\attach_overlay_geojson_polygons_to_properties_v4.mjs",
  [string]$StatewideRoot = ".\publicData\overlays\_statewide",
  [string]$WorkRoot = ".\publicData\overlays\_work",
  [string]$FrozenRoot = ".\publicData\overlays\_frozen",
  # Safe default MA bbox (fallback). You already computed ~this earlier.
  [double]$MinLon = -73.554662807589,
  [double]$MinLat =  41.1904621318313,
  [double]$MaxLon = -69.8830823068283,
  [double]$MaxLat =  42.9363915635423
)

$ErrorActionPreference = "Stop"

function Ensure-Dir([string]$p) {
  if (!(Test-Path $p)) { New-Item -ItemType Directory -Path $p | Out-Null }
}

function Find-FirstFile([string]$root, [string[]]$patterns) {
  foreach ($pat in $patterns) {
    $hit = Get-ChildItem -Path $root -Recurse -File -Filter $pat -ErrorAction SilentlyContinue |
      Sort-Object LastWriteTime -Descending |
      Select-Object -First 1
    if ($null -ne $hit) { return $hit.FullName }
  }
  return $null
}

function Count-Lines([string]$path) {
  $c = 0
  $sr = New-Object System.IO.StreamReader($path)
  try {
    while ($null -ne $sr.ReadLine()) { $c++ }
  } finally {
    $sr.Close()
  }
  return $c
}

function Freeze-FromWork([string]$key, [string]$pointerFile, [string]$propsPath, [string]$geoPath) {
  $src = Join-Path $WorkRoot $key
  $fc  = Join-Path $src "feature_catalog.ndjson"
  $att = Join-Path $src "attachments.ndjson"
  if (!(Test-Path $fc))  { throw "Missing feature_catalog.ndjson in $src" }
  if (!(Test-Path $att)) { throw "Missing attachments.ndjson in $src" }

  $ts = Get-Date -Format yyyyMMdd_HHmmss
  $dst = Join-Path $FrozenRoot ("{0}__FREEZE__{1}" -f $key, $ts)
  Ensure-Dir $dst

  Copy-Item $fc  (Join-Path $dst "feature_catalog.ndjson") -Force
  Copy-Item $att (Join-Path $dst "attachments.ndjson") -Force

  $fcCount  = Count-Lines (Join-Path $dst "feature_catalog.ndjson")
  $attCount = Count-Lines (Join-Path $dst "attachments.ndjson")

  $propsSha = (Get-FileHash -Algorithm SHA256 $propsPath).Hash
  $geoSha   = (Get-FileHash -Algorithm SHA256 $geoPath).Hash

  $manifest = [pscustomobject]@{
    artifact_key = $key
    created_at   = (Get-Date).ToString("o")
    inputs       = @{
      properties_path  = $propsPath
      properties_sha256 = $propsSha
      geo_path         = $geoPath
      geo_sha256       = $geoSha
    }
    outputs      = @{
      feature_catalog_ndjson = "publicData\overlays\_work\$key\feature_catalog.ndjson"
      attachments_ndjson     = "publicData\overlays\_work\$key\attachments.ndjson"
    }
    stats        = @{
      features_count      = $fcCount
      attachments_written = $attCount
    }
  }

  ($manifest | ConvertTo-Json -Depth 8) | Set-Content -Encoding UTF8 (Join-Path $dst "MANIFEST.json")

  # GREEN means: MANIFEST exists and NO SKIPPED.txt
  $sk = Join-Path $dst "SKIPPED.txt"
  if (Test-Path $sk) { Remove-Item $sk -Force }

  Ensure-Dir $FrozenRoot
  Set-Content -Encoding UTF8 (Join-Path $FrozenRoot $pointerFile) $dst

  Write-Host ("[done] froze GREEN: {0} -> {1}" -f $pointerFile, $dst)
}

function Run-Attach([string]$key, [string]$geoClip, [string]$propsPath) {
  if (!(Test-Path $AttachScript)) { throw "Missing attach script: $AttachScript" }
  if (!(Test-Path $geoClip)) { throw "Missing geo clip file: $geoClip" }

  $outDir = Join-Path $WorkRoot $key
  if (Test-Path $outDir) { Remove-Item $outDir -Recurse -Force }
  Ensure-Dir $outDir

  Write-Host ("[run] node attach: {0}" -f $key)
  Write-Host ("      props: {0}" -f $propsPath)
  Write-Host ("      geo:   {0}" -f $geoClip)
  Write-Host ("      out:   {0}" -f $outDir)

  # IMPORTANT: pass args as separate tokens (NOT one string)
  & node $AttachScript `
    "--properties" $propsPath `
    "--geojson"    $geoClip `
    "--layerKey"   $key `
    "--outDir"     $outDir

  if ($LASTEXITCODE -ne 0) { throw "node attach failed for $key exit=$LASTEXITCODE" }

  $fc  = Join-Path $outDir "feature_catalog.ndjson"
  $att = Join-Path $outDir "attachments.ndjson"
  if (!(Test-Path $fc) -or !(Test-Path $att)) {
    throw "Node attach did not produce outputs for $key (expected feature_catalog.ndjson + attachments.ndjson in $outDir)"
  }
}

function Clip-ToGeoJSONSeq([string]$inPath, [string]$outGeojsons) {
  Ensure-Dir (Split-Path $outGeojsons -Parent)
  if (Test-Path $outGeojsons) { Remove-Item $outGeojsons -Force }

  Write-Host "[info] ogr2ogr clip -> GeoJSONSeq"
  Write-Host ("       in:  {0}" -f $inPath)
  Write-Host ("       out: {0}" -f $outGeojsons)
  Write-Host ("       bbox: {0} {1} {2} {3}" -f $MinLon, $MinLat, $MaxLon, $MaxLat)

  & ogr2ogr -f GeoJSONSeq -t_srs EPSG:4326 -spat $MinLon $MinLat $MaxLon $MaxLat $outGeojsons $inPath
  if ($LASTEXITCODE -ne 0) { throw "ogr2ogr clip failed for: $inPath" }
  if (!(Test-Path $outGeojsons)) { throw "clip output missing: $outGeojsons" }
}

function Resolve-PropertiesPath() {
  if ($PropertiesNdjson -and (Test-Path $PropertiesNdjson)) { return (Resolve-Path $PropertiesNdjson).Path }

  $cand = Get-ChildItem ".\publicData\properties\_frozen" -Recurse -File -Filter "*.ndjson" -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -match "withBaseZoning" } |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

  if ($null -eq $cand) { throw "Could not auto-find a withBaseZoning properties NDJSON under .\publicData\properties\_frozen" }
  return $cand.FullName
}

# -------- MAIN --------
Ensure-Dir $WorkRoot
Ensure-Dir $FrozenRoot

$propsPath = Resolve-PropertiesPath
Write-Host ("[info] properties: {0}" -f $propsPath)

# 1) Aquifers
$aquifersIn = Find-FirstFile (Join-Path $StatewideRoot "env_aquifers") @(
  "aquifers_ma.geojson","*aquifer*.geojson","*.shp"
)
if ($null -eq $aquifersIn) { throw "Could not find Aquifers input under $StatewideRoot\env_aquifers" }
$aquifersClip = Join-Path $StatewideRoot "env_aquifers\_normalized\aquifers_ma__clip_bbox.geojsons"
Clip-ToGeoJSONSeq $aquifersIn $aquifersClip
Run-Attach "env_aquifers__ma__v1" $aquifersClip $propsPath
Freeze-FromWork "env_aquifers__ma__v1" "CURRENT_ENV_AQUIFERS_MA.txt" $propsPath $aquifersClip

# 2) Zone II / IWPA
$zoneiiIn = Find-FirstFile (Join-Path $StatewideRoot "env_zoneii_iwpa") @(
  "*zone*ii*.geojson","*iwpa*.geojson","*wellhead*.geojson","*.shp"
)
if ($null -eq $zoneiiIn) {
  # some earlier staging used different folder naming; fallback sweep
  $zoneiiIn = Find-FirstFile $StatewideRoot @("*zone*ii*.geojson","*iwpa*.geojson","*wellhead*.geojson")
}
if ($null -eq $zoneiiIn) { throw "Could not find ZoneII/IWPA input under $StatewideRoot (env_zoneii_iwpa)" }
$zoneiiClip = Join-Path $StatewideRoot "env_zoneii_iwpa\_normalized\zoneii_iwpa_ma__clip_bbox.geojsons"
Clip-ToGeoJSONSeq $zoneiiIn $zoneiiClip
Run-Attach "env_zoneii_iwpa__ma__v1" $zoneiiClip $propsPath
Freeze-FromWork "env_zoneii_iwpa__ma__v1" "CURRENT_ENV_ZONEII_IWPA_MA.txt" $propsPath $zoneiiClip

# 3) SWSP Zones A/B/C
$swspIn = Find-FirstFile $StatewideRoot @(
  "swsp_zones_abc_ma.geojson","*SWP*ZONE*.shp","*swsp*abc*.geojson","*surface*water*supply*protection*.geojson","*.shp"
)
if ($null -eq $swspIn) { throw "Could not find SWSP Zones A/B/C input under $StatewideRoot" }
$swspClip = Join-Path $StatewideRoot "env_surface_water_supply_protection\_normalized\swsp_zones_abc_ma__clip_bbox.geojsons"
Clip-ToGeoJSONSeq $swspIn $swspClip
Run-Attach "env_swsp_zones_abc__ma__v1" $swspClip $propsPath
Freeze-FromWork "env_swsp_zones_abc__ma__v1" "CURRENT_ENV_SWSP_ZONES_ABC_MA.txt" $propsPath $swspClip

Write-Host ""
Write-Host "[ok] Missing Phase1A polygons built + frozen GREEN:"
Write-Host "     CURRENT_ENV_AQUIFERS_MA.txt"
Write-Host "     CURRENT_ENV_ZONEII_IWPA_MA.txt"
Write-Host "     CURRENT_ENV_SWSP_ZONES_ABC_MA.txt"
