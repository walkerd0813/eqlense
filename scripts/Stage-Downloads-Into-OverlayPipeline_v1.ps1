param(
  [string]$Downloads = "",
  [switch]$Move
)

$ErrorActionPreference = "Stop"

function Ensure-Dir([string]$p){ New-Item -ItemType Directory -Force $p | Out-Null }
function Sha256([string]$p){ (Get-FileHash -Algorithm SHA256 -LiteralPath $p).Hash }
function Norm([string]$s){ if($null -eq $s){""} else {$s.Trim()} }

if (-not $Downloads) { $Downloads = Join-Path $env:USERPROFILE "Downloads" }
if (!(Test-Path $Downloads)) { throw "Downloads folder not found: $Downloads" }

$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$auditDir = ".\publicData\_audit\downloads_stage__${stamp}"
Ensure-Dir $auditDir

$pointerDir = ".\publicData\overlays\_statewide\_inbox"
Ensure-Dir $pointerDir
$pointerPath = Join-Path $pointerDir "CURRENT_DOWNLOADS_STAGE.txt"

# Map: source filename -> destination path + phase + canonical flag
$items = @(
  @{ src="MassGIS Data  FEMA National Flood Hazard Layer.zip"; phase="PHASE_1A_STATEWIDE_ENV"; canonical=$true;  dst=".\publicData\overlays\_statewide\env_nfhl\_inbox\NFHL_MA.zip" },
  @{ src="wetlandsdep.zip"; phase="PHASE_1A_STATEWIDE_ENV"; canonical=$true;  dst=".\publicData\overlays\_statewide\env_wetlands\_inbox\WETLANDSDEP_MA.zip" },
  @{ src="MassGIS Data Protected and Recreational OpenSpace.zip"; phase="PHASE_1A_STATEWIDE_ENV"; canonical=$true;  dst=".\publicData\overlays\_statewide\env_open_space_pros\_inbox\PROS_MA.zip" },
  @{ src="MassGIS Data Surface Water Supply Protection Areas (ZONE A, B, C).zip"; phase="PHASE_1A_STATEWIDE_ENV"; canonical=$true;  dst=".\publicData\overlays\_statewide\env_surface_water_supply_protection\_inbox\SWSP_ZONES_ABC_MA.zip" },
  @{ src="MassGIS Data NHESP Certified Vernal Pools.zip"; phase="PHASE_1A_STATEWIDE_ENV"; canonical=$true;  dst=".\publicData\overlays\_statewide\env_vernal_pools\_inbox\NHESP_CERTIFIED_VERNAL_POOLS_MA.zip" },
  @{ src="MassGIS Data Public Water Supplies.zip"; phase="PHASE_1A_STATEWIDE_ENV"; canonical=$true;  dst=".\publicData\overlays\_statewide\env_public_water_supplies\_inbox\PUBLIC_WATER_SUPPLIES_MA.zip" },
  @{ src="MassGIS Data Hydrography.zip"; phase="PHASE_1A_STATEWIDE_ENV_REFERENCE"; canonical=$true; dst=".\publicData\overlays\_statewide\env_hydrography\_inbox\HYDROGRAPHY_MA.zip" },

  @{ src="MassDEP_Wellhead_Protection_Areas_(Zone_II%2C_IWPA).geojson"; phase="PHASE_1A_STATEWIDE_ENV"; canonical=$true;  dst=".\publicData\overlays\_statewide\env_wellhead_zoneii_iwpa\_normalized\zoneii_iwpa_ma.geojson" },
  @{ src="mass Aquifers.geojson"; phase="PHASE_1A_STATEWIDE_ENV"; canonical=$true;  dst=".\publicData\overlays\_statewide\env_aquifers\_normalized\aquifers_ma.geojson" },

  # Wetlands alt formats (keep, do not ingest)
  @{ src="MA_geopackage_wetlands.zip"; phase="PHASE_1A_ALT_FORMAT"; canonical=$false; dst=".\publicData\overlays\_statewide\env_wetlands\_alt_formats\MA_geopackage_wetlands.zip" },
  @{ src="MA_geodatabase_wetlands.zip"; phase="PHASE_1A_ALT_FORMAT"; canonical=$false; dst=".\publicData\overlays\_statewide\env_wetlands\_alt_formats\MA_geodatabase_wetlands.zip" },

  # Phase 3 transit (deferred)
  @{ src="Commuter_Rail_Routes.geojson"; phase="PHASE_3_UTILITIES_INFRA"; canonical=$false; dst=".\publicData\overlays\_statewide\infra_transit_mbta\_deferred\commuter_rail_routes.geojson" },
  @{ src="MBTA_Systemwide_GTFS_Map.geojson"; phase="PHASE_3_UTILITIES_INFRA_REFERENCE"; canonical=$false; dst=".\publicData\overlays\_statewide\infra_transit_mbta\_deferred\mbta_systemwide_gtfs_map.geojson" },
  @{ src="Rapid_Transit_Routes.geojson"; phase="PHASE_3_UTILITIES_INFRA"; canonical=$false; dst=".\publicData\overlays\_statewide\infra_transit_mbta\_deferred\rapid_transit_routes.geojson" },
  @{ src="Rapid_Transit_Stops.geojson"; phase="PHASE_3_UTILITIES_INFRA"; canonical=$false; dst=".\publicData\overlays\_statewide\infra_transit_mbta\_deferred\rapid_transit_stops.geojson" },
  @{ src="MBTA_Extended_Service_Area.geojson"; phase="PHASE_3_UTILITIES_INFRA"; canonical=$false; dst=".\publicData\overlays\_statewide\infra_transit_mbta\_deferred\mbta_extended_service_area.geojson" },

  # Transit signals (phase 5 signals)
  @{ src="Rail_Ridership_by_Season_Time_Period_RouteLine_and_Stop_3959847833495046779.geojson"; phase="PHASE_5_SIGNALS"; canonical=$false; dst=".\publicData\overlays\_statewide\infra_transit_mbta\_signals\ridership_by_season.geojson" },
  @{ src="MBTA Gated Station Entries.geojson"; phase="PHASE_5_SIGNALS"; canonical=$false; dst=".\publicData\overlays\_statewide\infra_transit_mbta\_signals\gated_station_entries.geojson" },
  @{ src="mbta_Alerts_2025.zip"; phase="PHASE_5_SIGNALS"; canonical=$false; dst=".\publicData\overlays\_statewide\infra_transit_mbta\_signals\mbta_alerts_2025.zip" },

  # Phase 4 permits/capital signals
  @{ src="MassDEP_Ground_Water_Discharge_Permits.geojson"; phase="PHASE_4_PERMITS_CAPITAL"; canonical=$false; dst=".\publicData\overlays\_statewide\permits_massdep\_deferred\ground_water_discharge_permits.geojson" },

  # City overlay (Phase ZO, env duplicate)
  @{ src="Zoning_Groundwater_Conservation_Overlay_District_(GCOD).geojson"; phase="PHASE_ZO_MUNICIPAL_ZONING_OVERLAYS (ENV_DUPLICATE)"; canonical=$false; dst=".\publicData\zoning\boston\overlays\_incoming\zoning_overlay__gcod__boston.geojson" }
)

$results = New-Object System.Collections.Generic.List[object]

foreach ($it in $items) {
  $srcName = Norm $it.src
  $srcPath = Join-Path $Downloads $srcName
  if (!(Test-Path -LiteralPath $srcPath)) {
    $results.Add([pscustomobject]@{
      src = $srcName; found = $false; phase = $it.phase; canonical = $it.canonical; dst = $it.dst; sha256 = ""; note = "NOT_FOUND_IN_DOWNLOADS"
    })
    continue
  }

  $dstPath = $it.dst
  Ensure-Dir (Split-Path $dstPath)

  if ($Move) {
    Move-Item -LiteralPath $srcPath -Destination $dstPath -Force
    $op = "MOVE"
  } else {
    Copy-Item -LiteralPath $srcPath -Destination $dstPath -Force
    $op = "COPY"
  }

  $hash = Sha256 $dstPath

  $results.Add([pscustomobject]@{
    src = $srcName
    found = $true
    op = $op
    phase = $it.phase
    canonical = [bool]$it.canonical
    dst = $dstPath
    sha256 = $hash
    size_bytes = (Get-Item -LiteralPath $dstPath).Length
  })
}

# Write audit artifacts
$manifestPath = Join-Path $auditDir "STAGING_MANIFEST.json"
$csvPath = Join-Path $auditDir "STAGING_SUMMARY.csv"

$manifest = @{
  created_at = (Get-Date).ToString("s")
  downloads = $Downloads
  move = [bool]$Move
  outputs = @{
    summary_csv = $csvPath
    manifest_json = $manifestPath
  }
  items = $results
}

($manifest | ConvertTo-Json -Depth 8) | Set-Content -Encoding UTF8 $manifestPath
$results | Export-Csv -NoTypeInformation -Encoding UTF8 $csvPath

$auditDir | Set-Content -Encoding UTF8 $pointerPath

Write-Host ""
Write-Host "[done] staged downloads -> pipeline folders"
Write-Host "  audit:   $auditDir"
Write-Host "  pointer: $pointerPath"
Write-Host ""
Write-Host "Missing files (if any):"
$results | Where-Object { -not $_.found } | Select-Object src, phase, dst, note | Format-Table -AutoSize
