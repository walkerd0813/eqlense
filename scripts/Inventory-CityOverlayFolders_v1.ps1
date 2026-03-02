param(
  [string]$ZoningRoot = ".\publicData\zoning",
  [string]$OutDir = ".\publicData\zoning\_audit"
)

$ErrorActionPreference = "Stop"

function NowStamp() { (Get-Date).ToString("yyyyMMdd_HHmmss") }

function Detect-OverlayType([string]$pathOrName) {
  $s = ($pathOrName ?? "").ToLower()

  if ($s -match "wetland") { return "wetlands" }
  if ($s -match "fema|nfhl|flood|fldhaz") { return "flood_nfhl" }
  if ($s -match "open\s*space|protected|recreat|conserv") { return "open_space_or_conservation" }
  if ($s -match "nhesp|priority\s*habitat|biomap|rare|habitat") { return "nhesp_habitat" }
  if ($s -match "vernal") { return "vernal_pools" }
  if ($s -match "public\s*water|pws") { return "public_water_supplies" }
  if ($s -match "surface\s*water\s*supply|watersupply|wsp") { return "surface_water_supply_protection" }
  if ($s -match "aquifer|groundwater|wellhead|zone\s*ii|zoneii") { return "groundwater_aquifer" }
  if ($s -match "hydro|stream|river|lake|pond|waterbody") { return "hydrography" }
  if ($s -match "historic|district|landmark|cultural") { return "historic_preservation" }
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
    default { return "REVIEW" }
  }
}

if (!(Test-Path $ZoningRoot)) { throw "ZoningRoot not found: $ZoningRoot" }
New-Item -ItemType Directory -Force $OutDir | Out-Null

$stamp = NowStamp
$outCsv = Join-Path $OutDir "city_overlays_inventory__${stamp}.csv"
$outSummaryCsv = Join-Path $OutDir "city_overlays_summary__${stamp}.csv"

# Heuristic: city folders are immediate children under ZoningRoot (or under ZoningRoot\cities)
$citiesPath1 = Join-Path $ZoningRoot "cities"
$citiesRoot = if (Test-Path $citiesPath1) { $citiesPath1 } else { $ZoningRoot }

$cityDirs = Get-ChildItem -Path $citiesRoot -Directory -ErrorAction SilentlyContinue
if ($cityDirs.Count -eq 0) { throw "No city directories found under: $citiesRoot" }

$rows = New-Object System.Collections.Generic.List[object]

foreach ($city in $cityDirs) {
  # Find overlay folders anywhere inside the city folder:
  $overlayDirs =
    Get-ChildItem -Path $city.FullName -Directory -Recurse -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match '^(overlay|overlays|Overlay|Overlays)$' -or $_.FullName -match '\\overlay(s)?\\' }

  # Sometimes overlays live under "District(s)" in your note
  $districtDirs =
    Get-ChildItem -Path $city.FullName -Directory -Recurse -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match '^(district|districts|District|Districts)$' -or $_.FullName -match '\\district(s)?\\' }

  $targets = @($overlayDirs + $districtDirs) | Sort-Object FullName -Unique
  if ($targets.Count -eq 0) { continue }

  foreach ($dir in $targets) {
    $files = Get-ChildItem -Path $dir.FullName -File -Recurse -ErrorAction SilentlyContinue
    foreach ($f in $files) {
      $relCity = $city.Name
      $relDir = Resolve-Path $dir.FullName
      $relFile = Resolve-Path $f.FullName

      $overlayType = Detect-OverlayType ($f.Name + " " + $dir.FullName)
      $action = Suggested-Action $overlayType

      $rows.Add([pscustomobject]@{
        city = $relCity
        overlay_folder = $dir.FullName
        file_path = $f.FullName
        file_name = $f.Name
        ext = $f.Extension.ToLower()
        size_bytes = $f.Length
        modified = $f.LastWriteTime.ToString("s")
        overlay_type_guess = $overlayType
        suggested_action = $action
      })
    }
  }
}

if ($rows.Count -eq 0) {
  Write-Host "No overlay/district folders found under: $citiesRoot"
  exit 0
}

$rows | Export-Csv -NoTypeInformation -Encoding UTF8 $outCsv

# Summary: counts by city + overlay_type_guess
$summary =
  $rows |
  Group-Object city, overlay_type_guess |
  ForEach-Object {
    [pscustomobject]@{
      city = ($_.Name.Split(",")[0]).Trim()
      overlay_type_guess = ($_.Name.Split(",")[1]).Trim()
      file_count = $_.Count
      suggested_action = (Suggested-Action (($_.Name.Split(",")[1]).Trim()))
    }
  } |
  Sort-Object city, overlay_type_guess

$summary | Export-Csv -NoTypeInformation -Encoding UTF8 $outSummaryCsv

Write-Host ""
Write-Host "[done] Inventory written:"
Write-Host "  $outCsv"
Write-Host "  $outSummaryCsv"
Write-Host ""
Write-Host "Top 25 summary rows (city/type/count):"
$summary | Select-Object -First 25 | Format-Table -AutoSize
