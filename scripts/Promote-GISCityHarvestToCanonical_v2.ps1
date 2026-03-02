param(
  [string]$Root = "C:\seller-app\backend",
  [ValidateSet("copy","move")] [string]$Mode = "copy",
  [switch]$DryRun = $true,
  [switch]$IncludeNormToZoning = $true
)

function Normalize-Slug([string]$s){
  if([string]::IsNullOrWhiteSpace($s)){ return "" }
  $t = $s.ToLowerInvariant()
  $t = $t -replace "[^a-z0-9]+", "_"
  $t = $t -replace "_{2,}", "_"
  $t = $t.Trim("_")
  return $t
}

$CitySlugOverrides = @{
  "westspringfield"  = "west_springfield"
  "west-springfield" = "west_springfield"
}

function Get-CanonicalCitySlug([string]$folderName){
  $slug = Normalize-Slug $folderName
  if($CitySlugOverrides.ContainsKey($slug)){ return $CitySlugOverrides[$slug] }
  return $slug
}

function Ensure-Dir([string]$path){
  if(!(Test-Path $path)){
    New-Item -ItemType Directory -Force -Path $path | Out-Null
  }
}

function Unique-Dest([string]$destPath){
  if(!(Test-Path $destPath)){ return $destPath }
  $dir  = Split-Path $destPath -Parent
  $base = [IO.Path]::GetFileNameWithoutExtension($destPath)
  $ext  = [IO.Path]::GetExtension($destPath)
  $i=1
  do {
    $p = Join-Path $dir ("{0}__dup{1}{2}" -f $base,$i,$ext)
    $i++
  } while(Test-Path $p)
  return $p
}

function Classify([string]$fileName){
  $n = $fileName.ToLowerInvariant()

  # Skip assessor/parcels tables
  if($n -match "(assessor|assessment|parcelownership|parcel_owner|parcels_assessor|property_assessment|propertyassessment|asg_|fy\d{2})"){
    return @{ type="skip"; bucket=""; reason="assessor/parcels (skip)" }
  }

  # ---- ZONING (more than just the word 'zoning') ----
  if($n -match "(zoning|zone_)"){
    if($n -match "subdistrict"){ return @{ type="zoning"; bucket="subdistricts"; reason="zoning subdistricts" } }
    if($n -match "(zoning_base|zoning_district|zoningdistrict|districts)"){ return @{ type="zoning"; bucket="districts"; reason="base zoning districts" } }
    if($n -match "(historic|preserv)"){ return @{ type="zoning"; bucket="historic"; reason="historic/preservation" } }
    if($n -match "(wetland|flood|hazard|buffer|hydro|river|coastal)"){ return @{ type="zoning"; bucket="environmental"; reason="environmental constraints" } }
    return @{ type="zoning"; bucket="overlays"; reason="zoning overlay/other" }
  }

  # Non-zoning overlays we still want living under zoning/
  if($n -match "(historic|preserv)"){ return @{ type="zoning"; bucket="historic"; reason="historic/preservation overlay" } }
  if($n -match "(wetland|wetlands|flood|hazard|buffer|hydrography|hydro|river|coastal)"){ return @{ type="zoning"; bucket="environmental"; reason="environmental overlay" } }
  if($n -match "(airport|housing_priority|urban_renewal|priority_development|easement|easements)"){
    return @{ type="zoning"; bucket="overlays"; reason="planning/constraint overlay (kept under zoning/overlays)" }
  }

  # ---- BOUNDARIES / CIVIC ----
  if($n -match "(^boundary|_boundary|cityboundary|city_boundary|townboundary|town_boundary|municipal_boundary|maldenboundary)"){
    return @{ type="boundary"; bucket="civic"; reason="city/town boundary" }
  }
  if($n -match "(neigh|neighborhood)"){ return @{ type="boundary"; bucket="neighborhoods"; reason="neighborhoods" } }
  if($n -match "(zip|zipcode|zipcodes)"){ return @{ type="boundary"; bucket="zipcodes"; reason="zipcodes" } }
  if($n -match "(ward|wards|precinct)"){ return @{ type="boundary"; bucket="wards"; reason="wards/precincts" } }
  if($n -match "(police)"){ return @{ type="boundary"; bucket="police"; reason="police" } }
  if($n -match "(fire)"){ return @{ type="boundary"; bucket="fire"; reason="fire" } }
  if($n -match "(mbta|transit)"){ return @{ type="boundary"; bucket="mbta"; reason="mbta/transit" } }
  if($n -match "(transport|road|roads|street|streets|major_streets)"){ return @{ type="boundary"; bucket="transportation"; reason="transportation" } }
  if($n -match "(park|parks|open_space|openspace)"){ return @{ type="boundary"; bucket="parks"; reason="parks/open space" } }
  if($n -match "(snow)"){ return @{ type="boundary"; bucket="snow"; reason="snow" } }
  if($n -match "(trash|recycl|waste)"){ return @{ type="boundary"; bucket="trash"; reason="trash/recycling" } }
  if($n -match "(water|sewer)"){ return @{ type="boundary"; bucket="water"; reason="water/sewer" } }

  return @{ type="skip"; bucket=""; reason="unclassified" }
}

# ---------------- PATHS ----------------
$citiesRoot = Join-Path $Root "publicData\gis\cities"
$zoningRoot = Join-Path $Root "publicData\zoning"
$boundRoot  = Join-Path $Root "publicData\boundaries"
$auditRoot  = Join-Path $Root "publicData\_audit"
$zoningNormRoot = Join-Path $zoningRoot "_normalized"

if(!(Test-Path $citiesRoot)){ throw "Missing cities root: $citiesRoot" }
Ensure-Dir $zoningRoot
Ensure-Dir $boundRoot
Ensure-Dir $auditRoot
Ensure-Dir $zoningNormRoot

$ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
$auditPath = Join-Path $auditRoot ("promote_gis_to_canonical_v2_{0}.json" -f $ts)

Write-Host "====================================================="
Write-Host "[START] PROMOTE GIS → CANONICAL (v2)"
Write-Host "CitiesRoot : $citiesRoot"
Write-Host "ZoningRoot : $zoningRoot"
Write-Host "BoundRoot  : $boundRoot"
Write-Host "ZoningNorm : $zoningNormRoot"
Write-Host "Mode       : $Mode"
Write-Host "DryRun     : $DryRun"
Write-Host "IncludeNorm: $IncludeNormToZoning"
Write-Host "Audit      : $auditPath"
Write-Host "====================================================="

# Only treat folders as "cities" if they contain raw/ or standardized/ or norm/
$allDirs = Get-ChildItem $citiesRoot -Directory | Sort-Object Name
$cityDirs = @()
foreach($d in $allDirs){
  $hasRaw = Test-Path (Join-Path $d.FullName "raw")
  $hasStd = Test-Path (Join-Path $d.FullName "standardized")
  $hasNorm = Test-Path (Join-Path $d.FullName "norm")
  if($hasRaw -or $hasStd -or $hasNorm){
    $cityDirs += $d
  }
}

Write-Host ("[INFO ] City folders detected: {0}" -f $cityDirs.Count) -ForegroundColor Cyan
Write-Host ("[INFO ] Cities: {0}" -f (($cityDirs | ForEach-Object Name) -join ", ")) -ForegroundColor DarkCyan

$actions = New-Object System.Collections.Generic.List[object]
$counts = @{
  cities=$cityDirs.Count; geojson=0; zoning=0; boundary=0; skipped=0; normFiles=0; errors=0
}

foreach($city in $cityDirs){
  $citySlug = Get-CanonicalCitySlug $city.Name
  $rawDir = Join-Path $city.FullName "raw"
  $stdDir = Join-Path $city.FullName "standardized"

  Write-Host "-----------------------------------------------------"
  Write-Host ("[CITY ] {0}  → {1}" -f $city.Name,$citySlug) -ForegroundColor Cyan

  # prefer standardized; raw only if no std equivalent
  $stdFiles = @()
  if(Test-Path $stdDir){
    $stdFiles = Get-ChildItem $stdDir -File -Filter "*.geojson" -ErrorAction SilentlyContinue
  }

  $rawFiles = @()
  if(Test-Path $rawDir){
    $rawFiles = Get-ChildItem $rawDir -File -Filter "*.geojson" -ErrorAction SilentlyContinue
  }

  $stdBase = @{}
  foreach($sf in $stdFiles){
    $m = [regex]::Match($sf.Name, "^(.*)_std\.geojson$", "IgnoreCase")
    if($m.Success){ $stdBase[$m.Groups[1].Value.ToLowerInvariant()] = $true }
  }

  $final = New-Object System.Collections.Generic.List[object]
  foreach($sf in $stdFiles){ $final.Add($sf) | Out-Null }
  foreach($rf in $rawFiles){
    $rawBaseName = [IO.Path]::GetFileNameWithoutExtension($rf.Name).ToLowerInvariant()
    if($stdBase.ContainsKey($rawBaseName)){ continue }
    $final.Add($rf) | Out-Null
  }

  Write-Host ("[INFO ] GeoJSON candidates: {0}" -f $final.Count)

  $i=0
  foreach($f in $final){
    $i++
    $counts.geojson++

    if(($i % 10) -eq 0){
      Write-Host ("[LIVE ] {0} {1}/{2}" -f $citySlug,$i,$final.Count)
    }

    $cls = Classify $f.Name
    if($cls.type -eq "skip"){
      $counts.skipped++
      $actions.Add([pscustomobject]@{ts=(Get-Date).ToString("o"); city=$citySlug; action="skip"; src=$f.FullName; dest=$null; type="skip"; bucket=""; reason=$cls.reason}) | Out-Null
      continue
    }

    $destRoot = if($cls.type -eq "zoning"){ $zoningRoot } else { $boundRoot }
    $destDir  = Join-Path (Join-Path $destRoot $citySlug) $cls.bucket
    Ensure-Dir $destDir

    $destPath = Unique-Dest (Join-Path $destDir $f.Name)

    $verb = if($Mode -eq "move"){ "MOVE" } else { "COPY" }
    Write-Host ("[{0}] {1} -> {2}\{3}\{4}  ({5})" -f $verb,$f.Name,$cls.type,$citySlug,$cls.bucket,$cls.reason) -ForegroundColor Yellow

    $actions.Add([pscustomobject]@{ts=(Get-Date).ToString("o"); city=$citySlug; action=$Mode; src=$f.FullName; dest=$destPath; type=$cls.type; bucket=$cls.bucket; reason=$cls.reason}) | Out-Null

    if(-not $DryRun){
      try{
        if($Mode -eq "move"){ Move-Item -LiteralPath $f.FullName -Destination $destPath }
        else { Copy-Item -LiteralPath $f.FullName -Destination $destPath }

        if($cls.type -eq "zoning"){ $counts.zoning++ } else { $counts.boundary++ }
      } catch {
        $counts.errors++
        Write-Host ("[ERR ] {0}" -f $_.Exception.Message) -ForegroundColor Red
      }
    }
  }

  # ---- Norm folder → zoning/_normalized/<city>/ ----
  if($IncludeNormToZoning){
    $normDir = Join-Path $city.FullName "norm"
    if(Test-Path $normDir){
      $normOut = Join-Path $zoningNormRoot $citySlug
      Ensure-Dir $normOut

      $normFiles = Get-ChildItem $normDir -Recurse -File -ErrorAction SilentlyContinue |
        Where-Object { $_.Extension -in @(".json",".ndjson",".geojson") }

      if($normFiles.Count -gt 0){
        Write-Host ("[NORM] Found {0} files in {1}" -f $normFiles.Count,$normDir) -ForegroundColor Magenta
        foreach($nf in $normFiles){
          $rel = $nf.FullName.Substring($normDir.Length).TrimStart("\")
          $destN = Join-Path $normOut $rel
          Ensure-Dir (Split-Path $destN -Parent)
          $destN = Unique-Dest $destN

          $verb = if($Mode -eq "move"){ "MOVE" } else { "COPY" }
          Write-Host ("[{0}] norm\{1} -> zoning\_normalized\{2}\{1}" -f $verb,$rel,$citySlug) -ForegroundColor DarkMagenta

          $actions.Add([pscustomobject]@{ts=(Get-Date).ToString("o"); city=$citySlug; action=($Mode + "_norm"); src=$nf.FullName; dest=$destN; type="zoning_norm"; bucket="_normalized"; reason="promote norm folder"}) | Out-Null

          if(-not $DryRun){
            try{
              if($Mode -eq "move"){ Move-Item -LiteralPath $nf.FullName -Destination $destN }
              else { Copy-Item -LiteralPath $nf.FullName -Destination $destN }
              $counts.normFiles++
            } catch {
              $counts.errors++
              Write-Host ("[ERR ] norm: {0}" -f $_.Exception.Message) -ForegroundColor Red
            }
          }
        }
      } else {
        Write-Host "[NORM] norm/ exists but no json/ndjson/geojson files found." -ForegroundColor DarkGray
      }
    }
  }

  Write-Host ("[DONE] {0}" -f $citySlug) -ForegroundColor Green
}

# Write audit JSON
$audit = [pscustomobject]@{
  run_at = (Get-Date).ToString("o")
  root = $Root
  mode = $Mode
  dry_run = [bool]$DryRun
  include_norm_to_zoning = [bool]$IncludeNormToZoning
  counts = $counts
  actions = $actions
}

$audit | ConvertTo-Json -Depth 10 | Out-File -Encoding UTF8 $auditPath

Write-Host "====================================================="
Write-Host "[DONE] PROMOTION RUN COMPLETE (v2)"
Write-Host ("Cities   : {0}" -f $counts.cities)
Write-Host ("GeoJSON  : {0}" -f $counts.geojson)
if(-not $DryRun){
  Write-Host ("Zoning   : {0}" -f $counts.zoning)
  Write-Host ("Boundary : {0}" -f $counts.boundary)
  Write-Host ("NormFiles: {0}" -f $counts.normFiles)
}
Write-Host ("Skipped  : {0}" -f $counts.skipped)
Write-Host ("Errors   : {0}" -f $counts.errors)
Write-Host ("Audit    : {0}" -f $auditPath)
Write-Host "====================================================="
