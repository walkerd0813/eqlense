param(
  [string]$Root = "C:\seller-app\backend"
)

function Ensure-Dir([string]$p){ if(!(Test-Path $p)){ New-Item -ItemType Directory -Force -Path $p | Out-Null } }

function Read-HeadText([string]$path, [int]$maxBytes=131072){
  $fs = [System.IO.File]::OpenRead($path)
  try{
    $len = [Math]::Min($maxBytes, [int]$fs.Length)
    $buf = New-Object byte[] $len
    [void]$fs.Read($buf,0,$len)
    return [System.Text.Encoding]::UTF8.GetString($buf)
  } finally { $fs.Close() }
}

function LooksLikeGeoJSON([string]$path){
  try{
    $head = Read-HeadText $path 131072
    if($head -match '"type"\s*:\s*"FeatureCollection"' -or $head -match '"type"\s*:\s*"Feature"'){
      return $true
    }
  } catch {}
  return $false
}

function DetectGeometryHint([string]$path){
  try{
    $head = Read-HeadText $path 262144
    if($head -match '"geometry"\s*:\s*\{\s*"type"\s*:\s*"([^"]+)"'){
      return $Matches[1]
    }
    if($head -match '"geometry"\s*:\s*null'){
      return "null"
    }
  } catch {}
  return ""
}

function CityFromPath([string]$full){
  if($full -match "\\publicData\\gis\\cities\\([^\\]+)\\"){ return $Matches[1] }
  return "unknown_city"
}

function IsParcelSource([string]$nameLower){
  # high precision parcel-ish identifiers (avoid floodplain_parcels false positives)
  return (
    $nameLower -match "parcels_assessor" -or
    $nameLower -match "parcelownership" -or
    $nameLower -match "assessor__parcels" -or
    $nameLower -match "parcels__\d+" -or
    $nameLower -match "propertyoverlay__13" -or
    $nameLower -match "assessment_parcel_join" -or
    $nameLower -match "parcels_join" -or
    ($nameLower -match "parcels" -and $nameLower -notmatch "flood|wetland|buffer|overlay")
  )
}

function IsAssessorSource([string]$nameLower){
  return (
    $nameLower -match "assessor_online" -or
    $nameLower -match "assessing" -or
    $nameLower -match "assessment" -or
    $nameLower -match "tax" -or
    $nameLower -match "fy\d+" -or
    $nameLower -match "property_assessment"
  )
}

$srcRoot = Join-Path $Root "publicData\gis\cities"
$dstPar  = Join-Path $Root "publicData\parcels"
$dstAss  = Join-Path $Root "publicData\assessors"
$auditDir= Join-Path $Root "publicData\_audit"
Ensure-Dir $dstPar
Ensure-Dir $dstAss
Ensure-Dir $auditDir

$ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
$auditPath = Join-Path $auditDir "fix_promote_parcels_assessors_v1_$ts.json"

Write-Host "====================================================="
Write-Host "[START] FIX + PROMOTE PARCELS & ASSESSORS (v1)"
Write-Host "From : $srcRoot"
Write-Host "To   : $dstPar (parcels)"
Write-Host "To   : $dstAss (assessors)"
Write-Host "====================================================="

if(!(Test-Path $srcRoot)){ throw "Missing: $srcRoot" }

$allFiles = Get-ChildItem $srcRoot -Recurse -File -ErrorAction SilentlyContinue

$counts = [ordered]@{
  cities = (Get-ChildItem $srcRoot -Directory -ErrorAction SilentlyContinue).Count
  files_seen = $allFiles.Count
  fixed_extensions = 0
  parcels_copied = 0
  assessors_copied = 0
  skipped_exists = 0
  skipped_not_match = 0
  errors = 0
}

$actions = New-Object System.Collections.Generic.List[object]

# 1) FIX: rename extensionless GeoJSON-looking files (Boston case)
Write-Host "-----------------------------------------------------"
Write-Host "[STEP] Fix extensionless GeoJSON files (if any)" -ForegroundColor Cyan

$noExt = $allFiles | Where-Object { [string]::IsNullOrWhiteSpace($_.Extension) }
foreach($f in $noExt){
  $nameLower = $f.Name.ToLowerInvariant()
  if($nameLower -match "assessor|assess|parcel|parcels|ownership|assessment"){
    if(LooksLikeGeoJSON $f.FullName){
      $newPath = $f.FullName + ".geojson"
      if(!(Test-Path $newPath)){
        try{
          Rename-Item -LiteralPath $f.FullName -NewName ($f.Name + ".geojson")
          $counts.fixed_extensions++
          $actions.Add([pscustomobject]@{ action="rename_add_geojson_ext"; src=$f.FullName; dest=$newPath }) | Out-Null
          Write-Host ("[FIX ] {0} -> {1}" -f $f.Name, ($f.Name + ".geojson")) -ForegroundColor Green
        } catch {
          $counts.errors++
          $actions.Add([pscustomobject]@{ action="error_rename"; src=$f.FullName; error=$_.Exception.Message }) | Out-Null
        }
      }
    }
  }
}

# Refresh file list after renames
$allGeo = Get-ChildItem $srcRoot -Recurse -File -Filter "*.geojson" -ErrorAction SilentlyContinue

Write-Host "-----------------------------------------------------"
Write-Host "[STEP] Promote parcel + assessor sources" -ForegroundColor Cyan

$i=0
foreach($f in $allGeo){
  $i++
  if(($i % 75) -eq 0){
    Write-Host ("[LIVE] scanned {0}/{1}" -f $i, $allGeo.Count)
  }

  $city = CityFromPath $f.FullName
  $nameLower = $f.Name.ToLowerInvariant()

  $parcelish = IsParcelSource $nameLower
  $assessorish = IsAssessorSource $nameLower

  if(-not ($parcelish -or $assessorish)){
    $counts.skipped_not_match++
    continue
  }

  # Decide destination by geometry hint:
  # - If polygon-ish + parcelish => parcels
  # - If geometry null/unknown + assessorish => assessors
  $geomHint = DetectGeometryHint $f.FullName
  $destType = $null

  if($parcelish -and ($geomHint -match "Polygon|MultiPolygon" -or $geomHint -eq "")){
    $destType = "parcels"
  } elseif($assessorish -and ($geomHint -eq "null" -or $geomHint -eq "")){
    $destType = "assessors"
  } else {
    # fallback rule: if parcelish, keep in parcels (anchor layer)
    $destType = $parcelish ? "parcels" : "assessors"
  }

  $destBase = ($destType -eq "parcels") ? (Join-Path $dstPar $city) : (Join-Path $dstAss $city)
  $destDir = Join-Path $destBase "raw"
  Ensure-Dir $destDir

  $dest = Join-Path $destDir $f.Name

  try{
    if(Test-Path $dest){
      $counts.skipped_exists++
      $actions.Add([pscustomobject]@{ action="skip_exists"; type=$destType; city=$city; src=$f.FullName; dest=$dest }) | Out-Null
      continue
    }
    Copy-Item -LiteralPath $f.FullName -Destination $dest -Force
    if($destType -eq "parcels"){ $counts.parcels_copied++ } else { $counts.assessors_copied++ }
    $actions.Add([pscustomobject]@{ action="copy"; type=$destType; city=$city; src=$f.FullName; dest=$dest; geomHint=$geomHint }) | Out-Null
  } catch {
    $counts.errors++
    $actions.Add([pscustomobject]@{ action="error_copy"; type=$destType; city=$city; src=$f.FullName; dest=$dest; error=$_.Exception.Message }) | Out-Null
  }
}

$audit = [pscustomobject]@{
  run_at = (Get-Date).ToString("o")
  counts = $counts
  actions = $actions
}

$audit | ConvertTo-Json -Depth 8 | Out-File -Encoding UTF8 $auditPath

Write-Host "====================================================="
Write-Host "[DONE] FIX + PROMOTE COMPLETE (v1)"
Write-Host ("Cities           : {0}" -f $counts.cities)
Write-Host ("Files seen       : {0}" -f $counts.files_seen)
Write-Host ("Fixed extensions : {0}" -f $counts.fixed_extensions)
Write-Host ("Parcels copied   : {0}" -f $counts.parcels_copied)
Write-Host ("Assessors copied : {0}" -f $counts.assessors_copied)
Write-Host ("Skipped exists   : {0}" -f $counts.skipped_exists)
Write-Host ("Skipped notmatch : {0}" -f $counts.skipped_not_match)
Write-Host ("Errors           : {0}" -f $counts.errors)
Write-Host ("Audit            : {0}" -f $auditPath)
Write-Host "====================================================="
