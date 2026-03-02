param(
  [string]$ZoningRoot = ".\publicData\zoning",
  [string]$OutDir = ".\publicData\_audit\zoning_overlay_inventory",
  [string]$Cities = ""   # optional: "boston,cambridge,somerville,newton"
)

$ErrorActionPreference = "Stop"

function NowStamp() { (Get-Date).ToString("yyyyMMdd_HHmmss") }
function SafeLower([string]$s) { if ($null -eq $s) { "" } else { $s.ToLower() } }

function Is-RealCityFolder([string]$name) {
  $n = SafeLower $name
  if ($n.StartsWith("_")) { return $false }
  if ($n -in @("audit","_audit","_build","build","normalized","_normalized","statewide","_statewide")) { return $false }
  if ($n -match "^base\s*only" -or $n -match "_base_only") { return $false }
  return $true
}

function Detect-OverlayType([string]$pathOrName) {
  $s = SafeLower $pathOrName

  # Phase 1 env/legal (mostly redundant with statewide)
  if ($s -match "wetland") { return "wetlands" }
  if ($s -match "fema|nfhl|flood|fldhaz|floodway") { return "flood_nfhl" }
  if ($s -match "open\s*space|protected|recreat|pros") { return "open_space_pros" }
  if ($s -match "conservat|conservation") { return "open_space_or_conservation_local" } # you said: ignore local, use statewide
  if ($s -match "nhesp|priority\s*habitat|biomap|rare|habitat") { return "nhesp_habitat" }
  if ($s -match "vernal") { return "vernal_pools" }
  if ($s -match "public\s*water|pws") { return "public_water_supplies" }
  if ($s -match "surface\s*water\s*supply|watersupply|wsp") { return "surface_water_supply_protection" }
  if ($s -match "aquifer|groundwater|wellhead|zone\s*ii|zoneii") { return "groundwater_aquifer" }

  # City overlays that may matter later
  if ($s -match "historic|landmark|cultural|preserv") { return "historic_preservation" }
  if ($s -match "overlay\s*district|overlay_zone|overlay") { return "zoning_overlay_district" }
  if ($s -match "neighborhood|ward|precinct") { return "civic_neighborhood_ward_precinct" }

  return "unknown"
}

function Suggested-Action([string]$t) {
  switch ($t) {
    "wetlands" { "REDUNDANT_USE_STATEWIDE" }
    "flood_nfhl" { "REDUNDANT_USE_STATEWIDE" }
    "open_space_pros" { "REDUNDANT_USE_STATEWIDE" }
    "open_space_or_conservation_local" { "REDUNDANT_USE_STATEWIDE" } # per your rule
    "nhesp_habitat" { "REDUNDANT_USE_STATEWIDE" }
    "vernal_pools" { "REDUNDANT_USE_STATEWIDE" }
    "public_water_supplies" { "REDUNDANT_USE_STATEWIDE" }
    "surface_water_supply_protection" { "REDUNDANT_USE_STATEWIDE" }
    "groundwater_aquifer" { "REDUNDANT_USE_STATEWIDE" }
    "historic_preservation" { "LOCAL_PATCH_CANDIDATE_REVIEW" }
    "zoning_overlay_district" { "LOCAL_CITY_OVERLAY_KEEP_REVIEW" }
    "civic_neighborhood_ward_precinct" { "PHASE2_CIVIC_LATER" }
    default { "REVIEW" }
  }
}

function LayerKeyFromFile([string]$filePath) {
  $bn = [System.IO.Path]::GetFileNameWithoutExtension($filePath)
  return $bn
}

if (!(Test-Path $ZoningRoot)) { throw "ZoningRoot not found: $ZoningRoot" }
New-Item -ItemType Directory -Force $OutDir | Out-Null

$stamp = NowStamp
$outLayers = Join-Path $OutDir "zoning_city_overlay_layers__${stamp}.csv"
$outFiles  = Join-Path $OutDir "zoning_city_overlay_files__${stamp}.csv"
$outSummary= Join-Path $OutDir "zoning_city_overlay_summary__${stamp}.csv"

# city list
$cityDirs = Get-ChildItem -Path $ZoningRoot -Directory -ErrorAction SilentlyContinue |
  Where-Object { Is-RealCityFolder $_.Name }

if ($Cities -and $Cities.Trim().Length -gt 0) {
  $wanted = $Cities.Split(",") | ForEach-Object { $_.Trim().ToLower() } | Where-Object { $_ }
  $cityDirs = $cityDirs | Where-Object { $wanted -contains $_.Name.ToLower() }
}

if ($cityDirs.Count -eq 0) { throw "No city folders found under: $ZoningRoot (after filtering)" }

# Scan only overlay folders inside each city
$layerRows = New-Object System.Collections.Generic.List[object]
$fileRows  = New-Object System.Collections.Generic.List[object]

$wantExt = @(".shp",".shx",".dbf",".prj",".cpg",".geojson",".json",".gpkg",".kml",".kmz",".zip")

foreach ($city in $cityDirs) {
  $overlayCandidates = @(
    (Join-Path $city.FullName "overlay"),
    (Join-Path $city.FullName "overlays"),
    (Join-Path $city.FullName "Overlay"),
    (Join-Path $city.FullName "Overlays")
  ) | Where-Object { Test-Path $_ }

# De-dupe folders by resolved full path (handles Overlay vs overlays, symlinks, etc.)
$overlayCandidates = $overlayCandidates |
  ForEach-Object { (Resolve-Path $_).Path } |
  Sort-Object -Unique

  if ($overlayCandidates.Count -eq 0) { continue }

  foreach ($ovDir in $overlayCandidates) {
    $files = Get-ChildItem -Path $ovDir -Recurse -File -ErrorAction SilentlyContinue |
      Where-Object { $wantExt -contains (SafeLower $_.Extension) -or (SafeLower $_.Name) -like "*.gdb.zip" }

    foreach ($f in $files) {
      $fileRows.Add([pscustomobject]@{
        city = $city.Name
        overlay_dir = $ovDir
        file_path = $f.FullName
        file_name = $f.Name
        ext = (SafeLower $f.Extension)
        size_bytes = $f.Length
        modified = $f.LastWriteTime.ToString("s")
      })
    }

    # Group into "layers":
    # - Shapefile layer = basename where .shp exists
    # - GeoJSON layer = each geojson/json
    # - gdb.zip layer = each gdb.zip
    $byBase = @{}

    foreach ($f in $files) {
      $nameLower = (SafeLower $f.Name)
      $ext = (SafeLower $f.Extension)

      $isGdbZip = $nameLower -like "*.gdb.zip"
      if ($isGdbZip) {
        $layerName = $f.Name
        $lk = $layerName
        if (-not $byBase.ContainsKey($lk)) {
          $byBase[$lk] = @{
            layer_name = $layerName
            kind = "gdb_zip"
            has_shp = $false
            shp_parts = 0
            file_count = 0
            bytes = 0
            sample_path = $f.FullName
          }
        }
        $byBase[$lk].file_count++
        $byBase[$lk].bytes += $f.Length
        continue
      }

      if ($ext -in @(".geojson",".json",".gpkg",".kml",".kmz")) {
        $lk = $f.Name
        if (-not $byBase.ContainsKey($lk)) {
          $byBase[$lk] = @{
            layer_name = $f.Name
            kind = $ext.TrimStart(".")
            has_shp = $false
            shp_parts = 0
            file_count = 0
            bytes = 0
            sample_path = $f.FullName
          }
        }
        $byBase[$lk].file_count++
        $byBase[$lk].bytes += $f.Length
        continue
      }

      # shapefile parts
      $base = [System.IO.Path]::GetFileNameWithoutExtension($f.Name)
      if (-not $byBase.ContainsKey($base)) {
        $byBase[$base] = @{
          layer_name = $base
          kind = "shapefile"
          has_shp = $false
          shp_parts = 0
          file_count = 0
          bytes = 0
          sample_path = $f.FullName
        }
      }
      $byBase[$base].file_count++
      $byBase[$base].bytes += $f.Length
      $byBase[$base].shp_parts++
      if ($ext -eq ".shp") { $byBase[$base].has_shp = $true }
    }

    foreach ($k in $byBase.Keys) {
      $v = $byBase[$k]
      if ($v.kind -eq "shapefile" -and -not $v.has_shp) { continue } # don't list orphan .dbf/.shx sets

      $typeGuess = Detect-OverlayType ($v.layer_name + " " + $ovDir)
      $action = Suggested-Action $typeGuess

      $layerRows.Add([pscustomobject]@{
        city = $city.Name
        overlay_dir = $ovDir
        layer_name = $v.layer_name
        layer_kind = $v.kind
        file_count = $v.file_count
        total_bytes = $v.bytes
        layer_type_guess = $typeGuess
        suggested_action = $action
        sample_path = $v.sample_path
      })
    }
  }
}

if ($layerRows.Count -eq 0) {
  Write-Host "No overlay layers found under zoning/<city>/overlay."
  exit 0
}

$layerRows | Export-Csv -NoTypeInformation -Encoding UTF8 $outLayers
$fileRows  | Export-Csv -NoTypeInformation -Encoding UTF8 $outFiles

$summary =
  $layerRows |
  Group-Object city, layer_type_guess, suggested_action |
  ForEach-Object {
    $p = $_.Name.Split(",")
    [pscustomobject]@{
      city = $p[0].Trim()
      layer_type_guess = $p[1].Trim()
      suggested_action = $p[2].Trim()
      layer_count = $_.Count
    }
  } |
  Sort-Object city, layer_type_guess, suggested_action

$summary | Export-Csv -NoTypeInformation -Encoding UTF8 $outSummary

Write-Host ""
Write-Host "[done] Wrote:"
Write-Host "  Layers:  $outLayers"
Write-Host "  Files:   $outFiles"
Write-Host "  Summary: $outSummary"
Write-Host ""
Write-Host "Top 30 summary rows (city/type/action/layer_count):"
$summary | Select-Object -First 30 | Format-Table -AutoSize

