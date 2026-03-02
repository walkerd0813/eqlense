param(
  [string]$OverlaysRoot = ".\publicData\overlays\_statewide"
)

$ErrorActionPreference = "Stop"

function Ensure-Dir([string]$p){ New-Item -ItemType Directory -Force $p | Out-Null }
function Pick-Shp([string]$rawDir, [string[]]$preferRegex){
  $shps = Get-ChildItem $rawDir -Recurse -File -Filter "*.shp" -ErrorAction SilentlyContinue
  if (-not $shps) { return $null }
  foreach ($r in $preferRegex) {
    $hit = $shps | Where-Object { $_.Name -match $r } | Sort-Object Length -Descending | Select-Object -First 1
    if ($hit) { return $hit }
  }
  return ($shps | Sort-Object Length -Descending | Select-Object -First 1)
}

$ogr = Get-Command ogr2ogr -ErrorAction SilentlyContinue
if (-not $ogr) { throw "ogr2ogr not found. Install GDAL and ensure ogr2ogr is on PATH." }

$layers = @(
  @{
    key="env_open_space_pros"
    zip="$OverlaysRoot\env_open_space_pros\_inbox\PROS_MA.zip"
    raw="$OverlaysRoot\env_open_space_pros\_raw"
    norm="$OverlaysRoot\env_open_space_pros\_normalized"
    out="$OverlaysRoot\env_open_space_pros\_normalized\pros_ma.geojson"
    prefer=@("PROS","Open.*Space","Protected","Recreational")
  },
  @{
    key="env_surface_water_supply_protection"
    zip="$OverlaysRoot\env_surface_water_supply_protection\_inbox\SWSP_ZONES_ABC_MA.zip"
    raw="$OverlaysRoot\env_surface_water_supply_protection\_raw"
    norm="$OverlaysRoot\env_surface_water_supply_protection\_normalized"
    out="$OverlaysRoot\env_surface_water_supply_protection\_normalized\swsp_zones_abc_ma.geojson"
    prefer=@("Supply","Protection","Zone","SWSP","Zone_A|ZoneA","Zone_B|ZoneB","Zone_C|ZoneC")
  },
  @{
    key="env_public_water_supplies"
    zip="$OverlaysRoot\env_public_water_supplies\_inbox\PUBLIC_WATER_SUPPLIES_MA.zip"
    raw="$OverlaysRoot\env_public_water_supplies\_raw"
    norm="$OverlaysRoot\env_public_water_supplies\_normalized"
    out="$OverlaysRoot\env_public_water_supplies\_normalized\public_water_supplies_ma.geojson"
    prefer=@("Public.*Water","PWS","Water.*Supply")
  },
  @{
    key="env_vernal_pools"
    zip="$OverlaysRoot\env_vernal_pools\_inbox\NHESP_CERTIFIED_VERNAL_POOLS_MA.zip"
    raw="$OverlaysRoot\env_vernal_pools\_raw"
    norm="$OverlaysRoot\env_vernal_pools\_normalized"
    out="$OverlaysRoot\env_vernal_pools\_normalized\certified_vernal_pools_ma.geojson"
    prefer=@("Vernal","Pool","NHESP")
  },
  @{
    key="env_hydrography"
    zip="$OverlaysRoot\env_hydrography\_inbox\HYDROGRAPHY_MA.zip"
    raw="$OverlaysRoot\env_hydrography\_raw"
    norm="$OverlaysRoot\env_hydrography\_normalized"
    out="$OverlaysRoot\env_hydrography\_normalized\hydrography_ma.geojson"
    prefer=@("Hydro","River","Stream","Water")
  }
)

foreach ($L in $layers) {
  if (!(Test-Path -LiteralPath $L.zip)) {
    Write-Host "[skip] missing zip:" $L.zip
    continue
  }

  Ensure-Dir $L.raw
  Ensure-Dir $L.norm

  Write-Host ""
  Write-Host "[info] unzip:" $L.key
  Expand-Archive -LiteralPath $L.zip -DestinationPath $L.raw -Force

  $shp = Pick-Shp $L.raw $L.prefer
  if (-not $shp) { throw "No .shp found under $($L.raw)" }

  Write-Host "[info] picked shp:" $shp.FullName
  Write-Host "[info] convert ->" $L.out

  & $ogr.Source -t_srs EPSG:4326 -f GeoJSON $L.out $shp.FullName | Out-Null

  $manifest = @{
    layer_key = $L.key
    created_at = (Get-Date).ToString("s")
    zip = $L.zip
    picked_shp = $shp.FullName
    out_geojson = $L.out
  }
  ($manifest | ConvertTo-Json -Depth 6) | Set-Content -Encoding UTF8 (Join-Path $L.norm "CONVERT_MANIFEST.json")

  Write-Host "[done]" $L.key
}

Write-Host ""
Write-Host "[ok] Phase 1A ZIP conversion complete."
