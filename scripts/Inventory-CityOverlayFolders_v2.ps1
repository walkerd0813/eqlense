param(
  [string]$ZoningRoot = ".\publicData\zoning",
  [string]$OutDir = ".\publicData\zoning\_audit"
)

$ErrorActionPreference = "Stop"

function NowStamp() { (Get-Date).ToString("yyyyMMdd_HHmmss") }

function SafeLower([string]$s) {
  if ($null -eq $s) { return "" }
  return $s.ToLower()
}

function Detect-OverlayType([string]$pathOrName) {
  $s = SafeLower $pathOrName

  if ($s -match "wetland") { return "wetlands" }
  if ($s -match "fema|nfhl|flood|fldhaz|floodway") { return "flood_nfhl" }
  if ($s -match "open\s*space|protected|recreat|pros|conserv") { return "open_space_or_conservation" }
  if ($s -match "nhesp|priority\s*habitat|biomap|rare|habitat") { return "nhesp_habitat" }
  if ($s -match "vernal") { return "vernal_pools" }
  if ($s -match "public\s*water|pws") { return "public_water_supplies" }
  if ($s -match "surface\s*water\s*supply|watersupply|wsp") { return "surface_water_supply_protection" }
  if ($s -match "aquifer|groundwater|wellhead|zone\s*ii|zoneii") { return "groundwater_aquifer" }
  if ($s -match "hydro|stream|river|lake|pond|waterbody|hydrography") { return "hydrography" }
  if ($s -match "historic|district|landmark|cultural|preserv") { return "historic_preservation" }

  return "unknown"
}

function Suggested-Action([string]$overlayType) {
  switch ($overlayType) {
    "wetlands" { return "REDUNDANT_USE_STATEWIDE" }
    "flood_nfhl" { return "REDUNDANT_USE_STATEWIDE" }
    "open_space_or_conservation" { return "REDUNDANT_USE_STATEWIDE" }
    "nhesp_habitat" { return "REDUNDANT_USE_STATEWIDE" }
    "vernal_pools" { return "REDUNDANT_USE_STATEWIDE" }
    "public_water_supplies" { return "REDUNDANT_USE_STATEWIDE" }
    "surface_water_supply_protection" { return "REDUNDANT_USE_STATEWIDE" }
    "groundwater_aquifer" { return "REDUNDANT_USE_STATEWIDE" }
    "hydrography" { return "STATEWIDE_REFERENCE_OK" }
    "historic_preservation" { return "LOCAL_PATCH_CANDIDATE_REVIEW" }
    default { return "REVIEW" }
  }
}

function FolderBucket([string]$fullPath) {
  $p = SafeLower $fullPath
  if ($p -match "\\overlay(s)?\\") { return "overlay_folder" }
  if ($p -match "\\district(s)?\\") { return "district_folder" }
  if ($p -match "\\boundar(y|ies)\\") { return "boundaries_folder" }
  if ($p -match "\\historic\\") { return "historic_folder" }
  return "other_folder"
}

if (!(Test-Path $ZoningRoot)) { throw "ZoningRoot not found: $ZoningRoot" }
New-Item -ItemType Directory -Force $OutDir | Out-Null

$stamp = NowStamp
$outCsv = Join-Path $OutDir "city_gis_layers_inventory__${stamp}.csv"
$outSummaryCsv = Join-Path $OutDir "city_gis_layers_summary__${stamp}.csv"

# Heuristic: city folders live under .\publicData\zoning\cities (if present), else under ZoningRoot
$citiesPath1 = Join-Path $ZoningRoot "cities"
$citiesRoot = if (Test-Path $citiesPath1) { $citiesPath1 } else { $ZoningRoot }

$cityDirs = Get-ChildItem -Path $citiesRoot -Directory -ErrorAction SilentlyContinue
if ($cityDirs.Count -eq 0) { throw "No city directories found under: $citiesRoot" }

# Only inventory GIS-relevant file types + geodatabases
$extSet = @(
  ".shp", ".geojson", ".json", ".gpkg", ".kml", ".kmz",
  ".zip", ".7z", ".gdb.zip", ".tif", ".tiff"
)

$rows = New-Object System.Collections.Generic.List[object]

foreach ($city in $cityDirs) {
  # 1) Files (GIS-ish) anywhere under the city
  $allFiles = Get-ChildItem -Path $city.FullName -Recurse -File -ErrorAction SilentlyContinue

  foreach ($f in $allFiles) {
    $ext = SafeLower $f.Extension
    $nameLower = SafeLower $f.Name

    $isMatch = $false
    if ($extSet -contains $ext) { $isMatch = $true }
    elseif ($nameLower -like "*.gdb.zip") { $isMatch = $true }

    if (-not $isMatch) { continue }

    $overlayType = Detect-OverlayType ($f.Name + " " + $f.DirectoryName)
    $action = Suggested-Action $overlayType
    $bucket = FolderBucket $f.FullName

    $rows.Add([pscustomobject]@{
      city = $city.Name
      folder_bucket = $bucket
      file_path = $f.FullName
      file_name = $f.Name
      ext = $ext
      size_bytes = $f.Length
      modified = $f.LastWriteTime.ToString("s")
      overlay_type_guess = $overlayType
      suggested_action = $action
    })
  }

  # 2) Also record any .gdb directories (geodatabases) even though they’re folders
  $gdbDirs = Get-ChildItem -Path $city.FullName -Recurse -Directory -ErrorAction SilentlyContinue | Where-Object { (SafeLower $_.Name) -like "*.gdb" }
  foreach ($d in $gdbDirs) {
    $overlayType = Detect-OverlayType ($d.Name + " " + $d.FullName)
    $action = Suggested-Action $overlayType
    $bucket = FolderBucket $d.FullName

    $rows.Add([pscustomobject]@{
      city = $city.Name
      folder_bucket = $bucket
      file_path = $d.FullName
      file_name = $d.Name
      ext = ".gdb(dir)"
      size_bytes = $null
      modified = $d.LastWriteTime.ToString("s")
      overlay_type_guess = $overlayType
      suggested_action = $action
    })
  }
}

if ($rows.Count -eq 0) {
  Write-Host "No GIS-ish files found under: $citiesRoot"
  exit 0
}

$rows | Export-Csv -NoTypeInformation -Encoding UTF8 $outCsv

# Summary: city × overlay_type_guess × suggested_action × bucket
$summary =
  $rows |
  Group-Object city, overlay_type_guess, suggested_action, folder_bucket |
  ForEach-Object {
    $parts = $_.Name.Split(",")
    [pscustomobject]@{
      city = $parts[0].Trim()
      overlay_type_guess = $parts[1].Trim()
      suggested_action = $parts[2].Trim()
      folder_bucket = $parts[3].Trim()
      item_count = $_.Count
    }
  } |
  Sort-Object city, overlay_type_guess, suggested_action, folder_bucket

$summary | Export-Csv -NoTypeInformation -Encoding UTF8 $outSummaryCsv

Write-Host ""
Write-Host "[done] Inventory written:"
Write-Host "  $outCsv"
Write-Host "  $outSummaryCsv"
Write-Host ""
Write-Host "Top 30 summary rows (city/type/action/bucket/count):"
$summary | Select-Object -First 30 | Format-Table -AutoSize
