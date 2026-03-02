\
param(
  [string]$AsOfDate = "",
  [string]$Cities = "Boston,Brookline,Cambridge,Somerville,Chelsea,Quincy,Newton,Revere,Springfield,Waltham,Wareham,West_Springfield,Worcester,Dedham,CapeCod",
  [switch]$SkipArcGIS
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function NowStamp {
  return (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmssZ")
}

function Resolve-BackendRoot {
  # assume run from backend root
  return (Resolve-Path ".").Path
}

function Ensure-Dir([string]$p){
  New-Item -ItemType Directory -Force -Path $p | Out-Null
}

function Write-Text([string]$path, [string[]]$lines){
  $lines | Out-File -FilePath $path -Encoding UTF8
}

$BackendRoot = Resolve-BackendRoot
if([string]::IsNullOrWhiteSpace($AsOfDate)){
  $AsOfDate = (Get-Date).ToString("yyyy-MM-dd")
}

$auditDir = Join-Path $BackendRoot (Join-Path "publicData\_audit" ("phase2_civic_prep__" + (NowStamp)))
Ensure-Dir $auditDir

Write-Host "[info] BackendRoot: $BackendRoot"
Write-Host "[info] AsOfDate: $AsOfDate"
Write-Host "[info] Cities: $Cities"
Write-Host "[info] auditDir: $auditDir"

# Your known candidate files (from your message)
# NOTE: These are *references* for the inventory report. We don't move/rename anything in this script.
$candidates = @(
  @{ city="STATEWIDE"; phase="PHASE2"; path="backend\publicData\boundaries\_statewide\mbta\mbta" ; note="Folder (MBTA shapes / routes)" },
  @{ city="STATEWIDE"; phase="PHASE2"; path="backend\publicData\boundaries\_statewide\towns" ; note="Folder (town boundaries)" },
  @{ city="STATEWIDE"; phase="PHASE2"; path="backend\publicData\boundaries\_statewide\zipcodes\zipcodes" ; note="Folder (ZIP polygons)" },

  @{ city="Boston"; phase="PHASE2"; path="backend\publicData\boundaries\neighborhoodBoundaries.geojson" ; note="Boston neighborhoods (root-level)" },
  @{ city="Boston"; phase="PHASE2"; path="backend\publicData\boundaries\boston\neighborhoods\neighborhoods__precincts__7.geojson" ; note="Precincts" },
  @{ city="Boston"; phase="PHASE2"; path="backend\publicData\boundaries\boston\neighborhoods\neighborhoods__wards__8.geojson" ; note="Wards" },
  @{ city="Boston"; phase="PHASE2"; path="backend\publicData\zoning\boston\overlays\urban_renewal.geojson" ; note="Urban renewal (stored under zoning/overlays; treat as civic/regulatory boundary)" },

  @{ city="Brookline"; phase="PHASE2"; path="backend\publicData\boundaries\brookline\neighborhoods\neighborhoods__precincts__6.geojson" ; note="Precincts" },
  @{ city="Brookline"; phase="PHASE2"; path="backend\publicData\boundaries\brookline\neighborhoods\neighborhoods__voting_precincts__15.geojson" ; note="Voting precincts" },

  @{ city="Cambridge"; phase="PHASE2"; path="backend\publicData\boundaries\cambridge\neighborhoods\BOUNDARY_CDDNeighborhoods.geojson" ; note="CDD neighborhoods" },
  @{ city="Cambridge"; phase="PHASE2"; path="backend\publicData\boundaries\cambridge\neighborhoods\neighborhoods__wards_precincts__7.geojson" ; note="Wards/precincts" },

  @{ city="CapeCod"; phase="PHASE2"; path="backend\publicData\zoning\capecod\overlays\Cape_Cod_Chapter_H_Boundaries.geojson" ; note="Regional planning boundary (keep separate; civic/regulatory)" },

  @{ city="Somerville"; phase="PHASE2"; path="backend\publicData\boundaries\somerville\neighborhoods\neighborhoods__neighborhoods__8.geojson" ; note="Neighborhoods" },
  @{ city="Somerville"; phase="PHASE2"; path="backend\publicData\boundaries\somerville\neighborhoods\neighborhoods__neighborhood_plan_boundaries__9.geojson" ; note="Neighborhood plan boundaries" },
  @{ city="Somerville"; phase="PHASE2"; path="backend\publicData\boundaries\somerville\neighborhoods\neighborhoods__precincts__4.geojson" ; note="Precincts" },
  @{ city="Somerville"; phase="PHASE2"; path="backend\publicData\boundaries\somerville\neighborhoods\neighborhoods__wards__3.geojson" ; note="Wards" },
  @{ city="Somerville"; phase="PHASE2"; path="backend\publicData\boundaries\somerville\neighborhoods\conservation__neighboring_cities__24.geojson" ; note="Neighboring cities (context)" },
  @{ city="Somerville"; phase="PHASE3"; path="backend\publicData\boundaries\somerville\transportation\Streets.geojson" ; note="Streets (transport/infrastructure; Phase 3)" },

  @{ city="Springfield"; phase="PHASE2"; path="backend\publicData\boundaries\springfield\neighborhoods\neighborhoods__springfield_gis_neighborhoods__1.geojson" ; note="Neighborhoods" },
  @{ city="Springfield"; phase="PHASE2"; path="backend\publicData\boundaries\springfield\neighborhoods\neighborhoods__springfield__webgisdynamicmap__1.geojson" ; note="Neighborhoods alt copy (dedupe later)" },
  @{ city="Springfield"; phase="PHASE3"; path="backend\publicData\boundaries\springfield\transportation\major_streets__springfield__webgisdynamicmap__28.geojson" ; note="Major streets (Phase 3)" },
  @{ city="Springfield"; phase="PHASE2"; path="backend\publicData\zoning\springfield\overlays\urban_renewal_plans__springfield__webgisdynamicmap__36.geojson" ; note="Urban renewal (civic/regulatory boundary)" },

  @{ city="Waltham"; phase="PHASE2"; path="backend\publicData\boundaries\waltham\civic\neighborhoods.geojson" ; note="Neighborhoods" },
  @{ city="Waltham"; phase="PHASE3"; path="backend\publicData\boundaries\waltham\civic\buildings.geojson" ; note="Buildings points/polys (Phase 3/amenities; not parcel-attach required yet)" },

  @{ city="Quincy"; phase="PHASE2"; path="backend\publicData\boundaries\quincy\neighborhoods\neighborhoods__quincy__gdb__Neighborhoods.geojson" ; note="Neighborhoods" },

  @{ city="Newton"; phase="PHASE2"; path="backend\publicData\boundaries\newton\neighborhoods\neighborhoods__precincts__16.geojson" ; note="Precincts" },
  @{ city="Newton"; phase="PHASE3"; path="backend\publicData\zoning\newton\overlays\conservation__major_buildings__16.geojson" ; note="Major buildings (Phase 3/amenities)" },
  @{ city="Newton"; phase="PHASE3"; path="backend\publicData\zoning\newton\overlays\conservation__blue_heron_trail__63.geojson" ; note="Trail (Phase 3/amenities)" },
  @{ city="Newton"; phase="PHASE3"; path="backend\publicData\zoning\newton\overlays\conservation__aqueduct_trails__73.geojson" ; note="Trail (Phase 3/amenities)" },
  @{ city="Newton"; phase="PHASE3"; path="backend\publicData\zoning\newton\overlays\conservation__accessible_trails__43.geojson" ; note="Trail (Phase 3/amenities)" },

  @{ city="Revere"; phase="PHASE3"; path="backend\publicData\zoning\revere\overlays\conservation__revere_streets__0.geojson" ; note="Streets (Phase 3)" },

  @{ city="Worcester"; phase="PHASE3"; path="backend\publicData\boundaries\worcester\transportation\Street_Name_Table.geojson" ; note="Street name table (Phase 3; may be non-geometry; review)" }
)

# ArcGIS pulls you listed (Brookline MapServer layers)
$arcgisPulls = @(
  @{ city="brookline"; url="https://gisweb.brooklinema.gov/arcgis/rest/services/MyGov/GeneralPurpose/MapServer/21"; outRel="publicData\boundaries\brookline\_incoming\arcgis__brookline__mygov_generalpurpose__21.geojson" },
  @{ city="brookline"; url="https://gisweb.brooklinema.gov/arcgis/rest/services/MyGov/GeneralPurpose/MapServer/15"; outRel="publicData\boundaries\brookline\_incoming\arcgis__brookline__mygov_generalpurpose__15.geojson" },
  @{ city="brookline"; url="https://gisweb.brooklinema.gov/arcgis/rest/services/MyGov/GeneralPurpose/MapServer/9";  outRel="publicData\boundaries\brookline\_incoming\arcgis__brookline__mygov_generalpurpose__9.geojson" }
)

$downloader = Join-Path $BackendRoot "mls\scripts\gis\arcgisDownloadLayerToGeoJSON_v1.mjs"
if(-not (Test-Path $downloader)){
  throw "Missing downloader: $downloader`n(Expected existing script: mls\scripts\gis\arcgisDownloadLayerToGeoJSON_v1.mjs)"
}

$downloadLog = @()

if(-not $SkipArcGIS){
  Write-Host ""
  Write-Host "================  ARC GIS PULLS  ================"
  foreach($p in $arcgisPulls){
    $outAbs = Join-Path $BackendRoot $p.outRel
    Ensure-Dir (Split-Path $outAbs)
    Write-Host "[run] $($p.city) -> $($p.url)"
    Write-Host "      out: $outAbs"

    # call node downloader
    & node $downloader --layerUrl $p.url --out $outAbs
    if($LASTEXITCODE -ne 0){
      throw "ArcGIS download failed exit=$LASTEXITCODE for $($p.url)"
    }

    $downloadLog += @{
      city = $p.city
      url = $p.url
      out = $outAbs
      bytes = (Get-Item $outAbs).Length
      last_write = (Get-Item $outAbs).LastWriteTime.ToString("s")
    }
  }
}else{
  Write-Host "[info] SkipArcGIS enabled (no downloads)."
}

# Inventory report
Write-Host ""
Write-Host "================  INVENTORY REPORT  ================"

$citySet = $Cities.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
$inv = @()

foreach($c in $candidates){
  $city = $c.city
  if($city -ne "STATEWIDE" -and ($citySet -notcontains $city)){
    continue
  }
  $rel = $c.path.Replace("backend\", "")  # user used 'backend\...' prefix
  $abs = Join-Path $BackendRoot $rel
  $exists = Test-Path $abs
  $inv += @{
    city=$city
    phase=$c.phase
    path=$abs
    exists=$exists
    note=$c.note
  }
}

# also include downloaded files in inventory
foreach($d in $downloadLog){
  $inv += @{
    city=$d.city
    phase="PHASE2"
    path=$d.out
    exists=$true
    note=("Downloaded from ArcGIS: " + $d.url)
  }
}

# Write JSON + TXT
$invJson = Join-Path $auditDir "phase2_civic_inventory.json"
$invTxt  = Join-Path $auditDir "phase2_civic_inventory.txt"
($inv | ConvertTo-Json -Depth 6) | Out-File -FilePath $invJson -Encoding UTF8

$lines = @()
$lines += "PHASE2_CIVIC_PREP"
$lines += "as_of_date: $AsOfDate"
$lines += "backend_root: $BackendRoot"
$lines += ""
$lines += "Downloads:"
if($downloadLog.Count -eq 0){
  $lines += "  (none)"
}else{
  foreach($d in $downloadLog){
    $lines += ("  - " + $d.city + ": " + $d.url)
    $lines += ("      -> " + $d.out + " (" + $d.bytes + " bytes)")
  }
}
$lines += ""
$lines += "Candidates (exists?):"
foreach($row in $inv | Sort-Object city, phase, path){
  $flag = if($row.exists){"OK"}else{"MISSING"}
  $lines += ("- [" + $flag + "] " + $row.city + " :: " + $row.phase + " :: " + $row.path)
  if(-not [string]::IsNullOrWhiteSpace($row.note)){
    $lines += ("    note: " + $row.note)
  }
}
Write-Text $invTxt $lines

Write-Host "[ok] wrote $invJson"
Write-Host "[ok] wrote $invTxt"
Write-Host ""
Write-Host "[next] If inventory looks right, we freeze+attach Phase 2 civic boundaries per city (neighborhoods/wards/precincts/urban renewal/planning districts)."
