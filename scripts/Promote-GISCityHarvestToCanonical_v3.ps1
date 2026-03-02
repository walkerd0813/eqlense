param(
  [string]$Root = "C:\seller-app\backend",
  [ValidateSet("copy","move")] [string]$Mode = "copy",
  [switch]$DryRun = $true,
  [switch]$ReportOnly = $false,
  [switch]$IncludeNormToZoning = $true,
  [switch]$CaptureUnclassified = $true,
  [switch]$SkipIfExists = $true
)

function Ensure-Dir([string]$path){
  if(!(Test-Path $path)){
    New-Item -ItemType Directory -Force -Path $path | Out-Null
  }
}

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

function Prefer-StdThenRaw([string]$stdDir, [string]$rawDir){
  $stdFiles = @()
  if(Test-Path $stdDir){ $stdFiles = Get-ChildItem $stdDir -File -Filter "*.geojson" -ErrorAction SilentlyContinue }

  $rawFiles = @()
  if(Test-Path $rawDir){ $rawFiles = Get-ChildItem $rawDir -File -Filter "*.geojson" -ErrorAction SilentlyContinue }

  $stdBase = @{}
  foreach($sf in $stdFiles){
    $m = [regex]::Match($sf.Name, "^(.*)_std\.geojson$", "IgnoreCase")
    if($m.Success){ $stdBase[$m.Groups[1].Value.ToLowerInvariant()] = $true }
  }

  $final = New-Object System.Collections.Generic.List[object]
  foreach($sf in $stdFiles){ $final.Add($sf) | Out-Null }
  foreach($rf in $rawFiles){
    $rawBase = [IO.Path]::GetFileNameWithoutExtension($rf.Name).ToLowerInvariant()
    if($stdBase.ContainsKey($rawBase)){ continue }
    $final.Add($rf) | Out-Null
  }
  return $final
}

function Classify-File([string]$fileName){
  $n = $fileName.ToLowerInvariant()

  # ---- PARCELS (often include assessor fields) ----
  if($n -match "(^parcels|_parcels|parcel_|parcelownership|parcel_owner|parcelowner|parcels_assessor)"){
    return @{ type="parcels"; bucket=""; reason="parcel layer (may include assessor fields)" }
  }

  # ---- ASSESSORS (property assessment joins, tables, etc.) ----
  if($n -match "(assessor|assessment|property_assessment|propertyassessment|asg_|fy\d{2})"){
    return @{ type="assessors"; bucket=""; reason="assessor/assessment layer" }
  }

  # ---- ZONING (and zoning-adjacent overlays) ----
  if($n -match "(zoning|zone_)"){
    if($n -match "subdistrict"){ return @{ type="zoning"; bucket="subdistricts"; reason="zoning subdistricts" } }
    if($n -match "(zoning_base|zoning_district|zoningdistrict|districts)"){ return @{ type="zoning"; bucket="districts"; reason="base zoning districts" } }
    if($n -match "(historic|preserv)"){ return @{ type="zoning"; bucket="historic"; reason="historic/preservation zoning overlay" } }
    if($n -match "(wetland|wetlands|flood|hazard|buffer|hydro|river|coastal)"){
      return @{ type="zoning"; bucket="environmental"; reason="environmental zoning overlay" }
    }
    return @{ type="zoning"; bucket="overlays"; reason="zoning overlay/other" }
  }

  # Put these under zoning overlays (constraint layers you’ll attach later)
  if($n -match "(historic|preserv)"){ return @{ type="zoning"; bucket="historic"; reason="historic/preservation overlay" } }
  if($n -match "(wetland|wetlands|flood|hazard|buffer|hydrography|hydro|river|coastal)"){
    return @{ type="zoning"; bucket="environmental"; reason="environmental overlay" }
  }
  if($n -match "(airport|housing_priority|urban_renewal|priority_development|easement|easements)"){
    return @{ type="zoning"; bucket="overlays"; reason="planning/constraint overlay" }
  }

  # ---- BOUNDARIES / CIVIC ----
  if($n -match "(^boundary|_boundary|cityboundary|city_boundary|townboundary|town_boundary|municipal_boundary|maldenboundary)"){
    return @{ type="boundaries"; bucket="civic"; reason="city/town boundary" }
  }
  if($n -match "(neigh|neighborhood)"){ return @{ type="boundaries"; bucket="neighborhoods"; reason="neighborhood boundaries" } }
  if($n -match "(zip|zipcode|zipcodes)"){ return @{ type="boundaries"; bucket="zipcodes"; reason="zip code boundaries" } }
  if($n -match "(ward|wards|precinct)"){ return @{ type="boundaries"; bucket="wards"; reason="wards/precincts" } }
  if($n -match "(police)"){ return @{ type="boundaries"; bucket="police"; reason="police boundaries" } }
  if($n -match "(fire)"){ return @{ type="boundaries"; bucket="fire"; reason="fire boundaries" } }
  if($n -match "(mbta|transit)"){ return @{ type="boundaries"; bucket="mbta"; reason="mbta/transit" } }
  if($n -match "(transport|road|roads|street|streets|major_streets)"){ return @{ type="boundaries"; bucket="transportation"; reason="transportation/streets" } }
  if($n -match "(park|parks|open_space|openspace)"){ return @{ type="boundaries"; bucket="parks"; reason="parks/open space" } }
  if($n -match "(snow)"){ return @{ type="boundaries"; bucket="snow"; reason="snow layers" } }
  if($n -match "(trash|recycl|waste)"){ return @{ type="boundaries"; bucket="trash"; reason="trash/recycling" } }
  if($n -match "(water|sewer)"){ return @{ type="boundaries"; bucket="water"; reason="water/sewer" } }

  return @{ type="unclassified"; bucket=""; reason="no match" }
}

# ---- PATHS ----
$citiesRoot = Join-Path $Root "publicData\gis\cities"
$zoningRoot = Join-Path $Root "publicData\zoning"
$boundRoot  = Join-Path $Root "publicData\boundaries"
$assessRoot = Join-Path $Root "publicData\assessors"
$parcelsRoot= Join-Path $Root "publicData\parcels"
$cityHarvestRoot = Join-Path $Root "publicData\_cityHarvest"
$unclassifiedRoot= Join-Path $cityHarvestRoot "_unclassified"
$auditRoot  = Join-Path $Root "publicData\_audit"

if(!(Test-Path $citiesRoot)){ throw "Missing: $citiesRoot" }

Ensure-Dir $zoningRoot
Ensure-Dir $boundRoot
Ensure-Dir $assessRoot
Ensure-Dir $parcelsRoot
Ensure-Dir $cityHarvestRoot
Ensure-Dir $unclassifiedRoot
Ensure-Dir $auditRoot

$ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
$auditPath = Join-Path $auditRoot ("promote_gis_to_canonical_v3_{0}.json" -f $ts)

Write-Host "====================================================="
Write-Host "[START] PROMOTE GIS → CANONICAL (v3)"
Write-Host "CitiesRoot : $citiesRoot"
Write-Host "Zoning     : $zoningRoot"
Write-Host "Boundaries : $boundRoot"
Write-Host "Assessors  : $assessRoot"
Write-Host "Parcels    : $parcelsRoot"
Write-Host "Unclassified stash: $unclassifiedRoot"
Write-Host "Mode       : $Mode"
Write-Host "DryRun     : $DryRun"
Write-Host "ReportOnly : $ReportOnly"
Write-Host "SkipIfExists: $SkipIfExists"
Write-Host "Audit      : $auditPath"
Write-Host "====================================================="

# Detect cities by presence of raw/ standardized/ norm/
$allDirs = Get-ChildItem $citiesRoot -Directory | Sort-Object Name
$cityDirs = @()
foreach($d in $allDirs){
  $hasRaw  = Test-Path (Join-Path $d.FullName "raw")
  $hasStd  = Test-Path (Join-Path $d.FullName "standardized")
  $hasNorm = Test-Path (Join-Path $d.FullName "norm")
  if($hasRaw -or $hasStd -or $hasNorm){ $cityDirs += $d }
}

Write-Host ("[INFO ] Cities detected: {0}" -f $cityDirs.Count) -ForegroundColor Cyan
Write-Host ("[INFO ] Cities: {0}" -f (($cityDirs | ForEach-Object Name) -join ", ")) -ForegroundColor DarkCyan

$actions = New-Object System.Collections.Generic.List[object]
$counts = @{
  cities=$cityDirs.Count
  geojsonCandidates=0
  promoted=0
  zoning=0
  boundaries=0
  assessors=0
  parcels=0
  unclassified=0
  exists=0
  skipped=0
  normPromoted=0
  errors=0
}

foreach($city in $cityDirs){
  $citySlug = Get-CanonicalCitySlug $city.Name

  $rawDir = Join-Path $city.FullName "raw"
  $stdDir = Join-Path $city.FullName "standardized"
  $normDir= Join-Path $city.FullName "norm"

  $rawCount = if(Test-Path $rawDir){ (Get-ChildItem $rawDir -Recurse -File -ErrorAction SilentlyContinue | Measure-Object).Count } else { 0 }
  $stdCount = if(Test-Path $stdDir){ (Get-ChildItem $stdDir -Recurse -File -ErrorAction SilentlyContinue | Measure-Object).Count } else { 0 }
  $normCount= if(Test-Path $normDir){ (Get-ChildItem $normDir -Recurse -File -ErrorAction SilentlyContinue | Measure-Object).Count } else { 0 }

  Write-Host "-----------------------------------------------------"
  Write-Host ("[CITY ] {0} → {1}   (raw files={2}, std files={3}, norm files={4})" -f $city.Name,$citySlug,$rawCount,$stdCount,$normCount) -ForegroundColor Cyan

  $finalGeo = Prefer-StdThenRaw $stdDir $rawDir
  $counts.geojsonCandidates += $finalGeo.Count
  Write-Host ("[INFO ] GeoJSON candidates (prefer std): {0}" -f $finalGeo.Count)

  $i=0
  foreach($f in $finalGeo){
    $i++
    if(($i % 25) -eq 0){
      Write-Host ("[LIVE ] {0} geojson {1}/{2}" -f $citySlug,$i,$finalGeo.Count)
    }

    $cls = Classify-File $f.Name
    $dest = $null

    if($cls.type -eq "zoning"){
      $destDir = Join-Path (Join-Path $zoningRoot $citySlug) $cls.bucket
      Ensure-Dir $destDir
      $dest = Join-Path $destDir $f.Name
    }
    elseif($cls.type -eq "boundaries"){
      $destDir = Join-Path (Join-Path $boundRoot $citySlug) $cls.bucket
      Ensure-Dir $destDir
      $dest = Join-Path $destDir $f.Name
    }
    elseif($cls.type -eq "assessors"){
      $srcKind = if($f.FullName.ToLowerInvariant().Contains("\standardized\")){"standardized"} else {"raw"}
      $destDir = Join-Path (Join-Path $assessRoot $citySlug) $srcKind
      Ensure-Dir $destDir
      $dest = Join-Path $destDir $f.Name
    }
    elseif($cls.type -eq "parcels"){
      $srcKind = if($f.FullName.ToLowerInvariant().Contains("\standardized\")){"standardized"} else {"raw"}
      $destDir = Join-Path (Join-Path $parcelsRoot $citySlug) $srcKind
      Ensure-Dir $destDir
      $dest = Join-Path $destDir $f.Name
    }
    else {
      if($CaptureUnclassified){
        $srcKind = if($f.FullName.ToLowerInvariant().Contains("\standardized\")){"standardized"} else {"raw"}
        $destDir = Join-Path (Join-Path $unclassifiedRoot $citySlug) $srcKind
        Ensure-Dir $destDir
        $dest = Join-Path $destDir $f.Name
      } else {
        $counts.skipped++
        $actions.Add([pscustomobject]@{ts=(Get-Date).ToString("o"); city=$citySlug; action="skip"; src=$f.FullName; dest=$null; type=$cls.type; bucket=$cls.bucket; reason=$cls.reason}) | Out-Null
        continue
      }
    }

    if($SkipIfExists -and $dest -and (Test-Path $dest)){
      $counts.exists++
      $actions.Add([pscustomobject]@{ts=(Get-Date).ToString("o"); city=$citySlug; action="exists"; src=$f.FullName; dest=$dest; type=$cls.type; bucket=$cls.bucket; reason="destination already exists"}) | Out-Null
      continue
    }

    $verb = if($Mode -eq "move"){"MOVE"} else {"COPY"}
    Write-Host ("[{0}] {1} -> {2} ({3})" -f $verb,$f.Name,$dest,$cls.reason) -ForegroundColor Yellow

    $actions.Add([pscustomobject]@{ts=(Get-Date).ToString("o"); city=$citySlug; action=$Mode; src=$f.FullName; dest=$dest; type=$cls.type; bucket=$cls.bucket; reason=$cls.reason}) | Out-Null

    if(-not $ReportOnly){
      if(-not $DryRun){
        try{
          if($Mode -eq "move"){ Move-Item -LiteralPath $f.FullName -Destination $dest }
          else { Copy-Item -LiteralPath $f.FullName -Destination $dest }

          $counts.promoted++
          switch($cls.type){
            "zoning"      { $counts.zoning++ }
            "boundaries"  { $counts.boundaries++ }
            "assessors"   { $counts.assessors++ }
            "parcels"     { $counts.parcels++ }
            default       { $counts.unclassified++ }
          }
        } catch {
          $counts.errors++
          Write-Host ("[ERR ] {0}" -f $_.Exception.Message) -ForegroundColor Red
        }
      }
    }
  }

  # ---- Norm → zoning/_normalized/<city> (Medford style) ----
  if($IncludeNormToZoning -and (Test-Path $normDir)){
    $zoningNormRoot = Join-Path $zoningRoot "_normalized"
    Ensure-Dir $zoningNormRoot
    $outNorm = Join-Path $zoningNormRoot $citySlug
    Ensure-Dir $outNorm

    $normFiles = Get-ChildItem $normDir -Recurse -File -ErrorAction SilentlyContinue |
      Where-Object { $_.Extension -in @(".geojson",".json",".ndjson",".csv") }

    if($normFiles.Count -gt 0){
      Write-Host ("[NORM] Found {0} files in {1}" -f $normFiles.Count,$normDir) -ForegroundColor Magenta
      foreach($nf in $normFiles){
        $rel = $nf.FullName.Substring($normDir.Length).TrimStart("\")
        $destN = Join-Path $outNorm $rel
        Ensure-Dir (Split-Path $destN -Parent)

        if($SkipIfExists -and (Test-Path $destN)){
          $counts.exists++
          $actions.Add([pscustomobject]@{ts=(Get-Date).ToString("o"); city=$citySlug; action="exists_norm"; src=$nf.FullName; dest=$destN; type="zoning_norm"; bucket="_normalized"; reason="destination already exists"}) | Out-Null
          continue
        }

        $verb = if($Mode -eq "move"){"MOVE"} else {"COPY"}
        Write-Host ("[{0}] norm\{1} -> zoning\_normalized\{2}\{1}" -f $verb,$rel,$citySlug) -ForegroundColor DarkMagenta

        $actions.Add([pscustomobject]@{ts=(Get-Date).ToString("o"); city=$citySlug; action=($Mode + "_norm"); src=$nf.FullName; dest=$destN; type="zoning_norm"; bucket="_normalized"; reason="promote norm folder"}) | Out-Null

        if(-not $ReportOnly){
          if(-not $DryRun){
            try{
              if($Mode -eq "move"){ Move-Item -LiteralPath $nf.FullName -Destination $destN }
              else { Copy-Item -LiteralPath $nf.FullName -Destination $destN }
              $counts.normPromoted++
            } catch {
              $counts.errors++
              Write-Host ("[ERR ] norm: {0}" -f $_.Exception.Message) -ForegroundColor Red
            }
          }
        }
      }
    }
  }

  Write-Host ("[DONE] {0}" -f $citySlug) -ForegroundColor Green
}

$audit = [pscustomobject]@{
  run_at = (Get-Date).ToString("o")
  root = $Root
  mode = $Mode
  dry_run = [bool]$DryRun
  report_only = [bool]$ReportOnly
  include_norm_to_zoning = [bool]$IncludeNormToZoning
  capture_unclassified = [bool]$CaptureUnclassified
  skip_if_exists = [bool]$SkipIfExists
  counts = $counts
  actions = $actions
}

$audit | ConvertTo-Json -Depth 10 | Out-File -Encoding UTF8 $auditPath

Write-Host "====================================================="
Write-Host "[DONE] PROMOTION RUN COMPLETE (v3)"
Write-Host ("Cities            : {0}" -f $counts.cities)
Write-Host ("GeoJSON candidates: {0}" -f $counts.geojsonCandidates)
Write-Host ("Promoted          : {0}" -f $counts.promoted)
Write-Host ("Zoning            : {0}" -f $counts.zoning)
Write-Host ("Boundaries        : {0}" -f $counts.boundaries)
Write-Host ("Assessors         : {0}" -f $counts.assessors)
Write-Host ("Parcels           : {0}" -f $counts.parcels)
Write-Host ("Unclassified stash: {0}" -f $counts.unclassified)
Write-Host ("Norm promoted      : {0}" -f $counts.normPromoted)
Write-Host ("Exists skipped     : {0}" -f $counts.exists)
Write-Host ("Other skipped      : {0}" -f $counts.skipped)
Write-Host ("Errors            : {0}" -f $counts.errors)
Write-Host ("Audit             : {0}" -f $auditPath)
Write-Host "====================================================="
