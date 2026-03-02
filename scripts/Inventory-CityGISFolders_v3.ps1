param(
  [string]$ZoningRoot = ".\publicData\zoning",
  [string]$BoundariesRoot = ".\publicData\boundaries",
  [string]$OutDir = ".\publicData\_audit\city_gis_inventory",
  [string]$Cities = ""   # optional: "boston,cambridge,somerville"
)

$ErrorActionPreference = "Stop"

function NowStamp() { (Get-Date).ToString("yyyyMMdd_HHmmss") }
function SafeLower([string]$s) { if ($null -eq $s) { "" } else { $s.ToLower() } }

function FolderBucket([string]$fullPath) {
  $p = SafeLower $fullPath
  if ($p -match "\\overlay(s)?\\") { return "overlay_folder" }
  if ($p -match "\\district(s)?\\") { return "district_folder" }
  if ($p -match "\\boundar(y|ies)\\") { return "boundaries_folder" }
  if ($p -match "\\historic\\|\\history\\|\\preserv") { return "historic_folder" }
  if ($p -match "\\wetland") { return "wetlands_folder" }
  if ($p -match "\\flood") { return "flood_folder" }
  return "other_folder"
}

function Detect-LayerType([string]$pathOrName) {
  $s = SafeLower $pathOrName

  # Boundaries / municipal
  if ($s -match "municipal|town\s*boundary|city\s*boundary|\bboundary\b") { return "municipal_boundary" }

  # Base zoning vs overlays
  if ($s -match "zoning" -and $s -notmatch "overlay") { return "zoning_or_districts" }

  # Phase 1 env/legal
  if ($s -match "wetland") { return "wetlands" }
  if ($s -match "fema|nfhl|flood|fldhaz|floodway") { return "flood_nfhl" }
  if ($s -match "open\s*space|protected|recreat|pros") { return "open_space_pros" }
  if ($s -match "nhesp|priority\s*habitat|biomap|rare|habitat") { return "nhesp_habitat" }
  if ($s -match "vernal") { return "vernal_pools" }
  if ($s -match "public\s*water|pws") { return "public_water_supplies" }
  if ($s -match "surface\s*water\s*supply|watersupply|wsp") { return "surface_water_supply_protection" }
  if ($s -match "aquifer|groundwater|wellhead|zone\s*ii|zoneii") { return "groundwater_aquifer" }
  if ($s -match "hydro|stream|river|lake|pond|waterbody|hydrography") { return "hydrography" }

  # Civic-ish / historic
  if ($s -match "historic|district|landmark|cultural|preserv") { return "historic_preservation" }

  return "unknown"
}

function Suggested-Action([string]$layerType) {
  switch ($layerType) {
    "municipal_boundary" { return "KEEP_CORE_BOUNDARY" }
    "zoning_or_districts" { return "REVIEW_ZONING_ARTIFACTS" }

    # Redundant vs statewide Phase 1 canon
    "wetlands" { return "REDUNDANT_USE_STATEWIDE" }
    "flood_nfhl" { return "REDUNDANT_USE_STATEWIDE" }
    "open_space_pros" { return "REDUNDANT_USE_STATEWIDE" }
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

function Get-CityDirs([string]$root) {
  if (!(Test-Path $root)) { return @() }
  return Get-ChildItem -Path $root -Directory -ErrorAction SilentlyContinue
}

# Validate roots
if (!(Test-Path $ZoningRoot)) { throw "ZoningRoot not found: $ZoningRoot" }
if (!(Test-Path $BoundariesRoot)) { Write-Host "[warn] BoundariesRoot not found (will still scan zoning): $BoundariesRoot" }

New-Item -ItemType Directory -Force $OutDir | Out-Null
$stamp = NowStamp

$outInventory = Join-Path $OutDir "city_gis_files_inventory__${stamp}.csv"
$outSummary   = Join-Path $OutDir "city_gis_files_summary__${stamp}.csv"
$outFolders   = Join-Path $OutDir "city_top_folders__${stamp}.csv"

# Determine cities (union of zoning + boundaries city folders)
$zCities = Get-CityDirs $ZoningRoot
$bCities = Get-CityDirs $BoundariesRoot

$cityNames = @($zCities.Name + $bCities.Name) | Where-Object { $_ } | Sort-Object -Unique

if ($Cities -and $Cities.Trim().Length -gt 0) {
  $wanted = $Cities.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
  $wantedLower = $wanted | ForEach-Object { $_.ToLower() }
  $cityNames = $cityNames | Where-Object { $wantedLower -contains $_.ToLower() }
}

if ($cityNames.Count -eq 0) { throw "No city folders found under: $ZoningRoot (and/or $BoundariesRoot)" }

# File extensions to include (GIS-ish + archives)
$extSet = @(
  ".shp",".shx",".dbf",".prj",".cpg",
  ".geojson",".json",
  ".gpkg",
  ".kml",".kmz",
  ".tif",".tiff",
  ".zip",".7z",".rar",
  ".csv",".txt",".pdf"
)

$rows = New-Object System.Collections.Generic.List[object]
$folderRows = New-Object System.Collections.Generic.List[object]

function Scan-CityRoot([string]$city, [string]$rootType, [string]$rootPath) {
  if (!(Test-Path $rootPath)) { return }

  # top-level folder snapshot
  $topDirs = Get-ChildItem -Path $rootPath -Directory -ErrorAction SilentlyContinue
  foreach ($d in $topDirs) {
    $folderRows.Add([pscustomobject]@{
      city = $city
      root_type = $rootType
      top_folder = $d.Name
      full_path = $d.FullName
      modified = $d.LastWriteTime.ToString("s")
    })
  }

  # files
  $files = Get-ChildItem -Path $rootPath -Recurse -File -ErrorAction SilentlyContinue
  foreach ($f in $files) {
    $ext = SafeLower $f.Extension
    $nameLower = SafeLower $f.Name

    $isMatch = $false
    if ($extSet -contains $ext) { $isMatch = $true }
    elseif ($nameLower -like "*.gdb.zip") { $isMatch = $true }

    if (-not $isMatch) { continue }

    $layerType = Detect-LayerType ($f.Name + " " + $f.DirectoryName)
    $action = Suggested-Action $layerType
    $bucket = FolderBucket $f.FullName

    $rows.Add([pscustomobject]@{
      city = $city
      root_type = $rootType                # zoning | boundaries
      folder_bucket = $bucket
      file_path = $f.FullName
      file_name = $f.Name
      ext = $ext
      size_bytes = $f.Length
      modified = $f.LastWriteTime.ToString("s")
      layer_type_guess = $layerType
      suggested_action = $action
    })
  }

  # .gdb directories (geodatabases)
  $gdbDirs = Get-ChildItem -Path $rootPath -Recurse -Directory -ErrorAction SilentlyContinue |
    Where-Object { (SafeLower $_.Name) -like "*.gdb" }

  foreach ($d in $gdbDirs) {
    $layerType = Detect-LayerType ($d.Name + " " + $d.FullName)
    $action = Suggested-Action $layerType
    $bucket = FolderBucket $d.FullName

    $rows.Add([pscustomobject]@{
      city = $city
      root_type = $rootType
      folder_bucket = $bucket
      file_path = $d.FullName
      file_name = $d.Name
      ext = ".gdb(dir)"
      size_bytes = $null
      modified = $d.LastWriteTime.ToString("s")
      layer_type_guess = $layerType
      suggested_action = $action
    })
  }
}

foreach ($city in $cityNames) {
  $zPath = Join-Path $ZoningRoot $city
  $bPath = Join-Path $BoundariesRoot $city
  Scan-CityRoot -city $city -rootType "zoning" -rootPath $zPath
  Scan-CityRoot -city $city -rootType "boundaries" -rootPath $bPath
}

if ($rows.Count -eq 0) {
  Write-Host "No GIS-ish files found under zoning/boundaries city folders."
  exit 0
}

$rows | Export-Csv -NoTypeInformation -Encoding UTF8 $outInventory
$folderRows | Export-Csv -NoTypeInformation -Encoding UTF8 $outFolders

$summary =
  $rows |
  Group-Object city, root_type, layer_type_guess, suggested_action, folder_bucket |
  ForEach-Object {
    $parts = $_.Name.Split(",")
    [pscustomobject]@{
      city = $parts[0].Trim()
      root_type = $parts[1].Trim()
      layer_type_guess = $parts[2].Trim()
      suggested_action = $parts[3].Trim()
      folder_bucket = $parts[4].Trim()
      item_count = $_.Count
    }
  } |
  Sort-Object city, root_type, layer_type_guess, suggested_action, folder_bucket

$summary | Export-Csv -NoTypeInformation -Encoding UTF8 $outSummary

Write-Host ""
Write-Host "[done] Wrote:"
Write-Host "  Inventory: $outInventory"
Write-Host "  Summary:   $outSummary"
Write-Host "  Folders:   $outFolders"
Write-Host ""
Write-Host "Top 30 summary rows:"
$summary | Select-Object -First 30 | Format-Table -AutoSize
