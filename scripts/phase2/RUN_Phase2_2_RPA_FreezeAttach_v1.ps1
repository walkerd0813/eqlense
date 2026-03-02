param(
  [Parameter(Mandatory=$true)][string]$AsOfDate,
  [int]$VerifySampleLines = 4000
)

$ErrorActionPreference = "Stop"

function Resolve-Abs([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  if(Test-Path $p){
    return (Resolve-Path $p).Path
  }
  if($p -match '^[A-Za-z]:\\'){
    return $p
  }
  $cand = Join-Path (Get-Location) $p
  if(Test-Path $cand){ return (Resolve-Path $cand).Path }
  return $cand
}

$BackendRoot = (Resolve-Path ".").Path
Write-Host "[info] BackendRoot: $BackendRoot"
Write-Host "[info] as_of_date: $AsOfDate"

# contract view pointer (current)
$ptr = Join-Path $BackendRoot "publicData\properties\_frozen\CURRENT_CONTRACT_VIEW_MA.txt"
if(!(Test-Path $ptr)){ throw "Missing pointer: $ptr" }
$contractIn = (Get-Content $ptr -Raw).Trim()
if([string]::IsNullOrWhiteSpace($contractIn)){ throw "Pointer empty: $ptr" }
$contractInAbs = Resolve-Abs $contractIn
if(!(Test-Path $contractInAbs)){ throw "Contract view file not found: $contractInAbs (from $ptr)" }

Write-Host "[info] contract_in: $contractInAbs"

# Source zip
$srcZip = Join-Path $BackendRoot "publicData\boundaries\_statewide\Regional Planning Agencies.zip"
if(!(Test-Path $srcZip)){ throw "Missing source zip: $srcZip" }

# Expand the zip to a work dir (so we can find shp/geojson)
$ts = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmssZ")
$workExtract = Join-Path $BackendRoot ("publicData\_work\rpa_extract__" + $ts)
New-Item -ItemType Directory -Force -Path $workExtract | Out-Null
Expand-Archive -Force $srcZip $workExtract

# Find an existing geojson inside the zip
$rpaGeojson = Get-ChildItem $workExtract -Recurse -File -Include *.geojson,*.json | Select-Object -First 1
if($rpaGeojson){
  $rpaGeojsonAbs = $rpaGeojson.FullName
  Write-Host "[info] found geojson in zip: $rpaGeojsonAbs"
} else {
  # Find a shp (prefer Polygon/MultiPolygon layers)
  $shps = Get-ChildItem $workExtract -Recurse -File -Filter *.shp
  if(!$shps -or $shps.Count -eq 0){ throw "No .geojson/.json and no .shp found after expanding: $srcZip" }

  # Prefer polygon geometry using ogrinfo if present
  $ogrinfo = Get-Command ogrinfo -ErrorAction SilentlyContinue
  $shp = $null
  if($ogrinfo){
    foreach($s in $shps){
      try {
        $ogrinfoExe = if($ogrinfo.Path){ $ogrinfo.Path } else { $ogrinfo.Source }
        $info = (& $ogrinfoExe -ro -so $s.FullName 2>$null) | Out-String
        if($info -match '(?im)^\s*Geometry:\s*(.+)\s*$'){
          $geom = $Matches[1].Trim()
          if($geom -match '(?i)polygon'){
            $shp = $s
            break
          }
        }
      } catch {
        # ignore
      }
    }
  }

  if(-not $shp){
    # Fallback to filename heuristics
    $preferred = $shps | Where-Object {
      $_.Name -match '(?i)poly|region|rpa' -and $_.Name -notmatch '(?i)arc|line'
    } | Select-Object -First 1
    if($preferred){
      $shp = $preferred
    } else {
      $shp = $shps | Select-Object -First 1
    }
  }

  Write-Host "[info] selected shp: $($shp.FullName)" -ForegroundColor Cyan

  $outGeo = Join-Path $workExtract "regional_planning_agencies__converted.geojson"

  # Prefer local mapshaper (npm dev dep)
  $mapshaper = Join-Path $BackendRoot "node_modules\.bin\mapshaper.cmd"
  if(Test-Path $mapshaper){
    Write-Host "[info] converting via mapshaper: $($shp.FullName)"
    & $mapshaper $shp.FullName -clean -proj wgs84 -o format=geojson $outGeo | Out-Null
  } else {
    # Try ogr2ogr if installed
    $ogr = Get-Command ogr2ogr -ErrorAction SilentlyContinue
    if($ogr){
      Write-Host "[info] converting via ogr2ogr: $($shp.FullName)"
      $ogrExe = if($ogr.Path){ $ogr.Path } else { $ogr.Source }
      & $ogrExe -t_srs EPSG:4326 $outGeo $shp.FullName | Out-Null
    } else {
      throw "No converter found. Install mapshaper then rerun: npm i -D mapshaper  (or install GDAL/ogr2ogr)."
    }
  }

  if(!(Test-Path $outGeo)){ throw "Conversion failed; missing output geojson: $outGeo" }
  $rpaGeojsonAbs = (Resolve-Path $outGeo).Path
  Write-Host "[ok] wrote converted geojson: $rpaGeojsonAbs"
}

# Run node attach/freeze
$node = Get-Command node -ErrorAction Stop
$nodeExe = if($node.Path){ $node.Path } else { $node.Source }
$script = Join-Path $BackendRoot "mls\scripts\gis\phase2_2_rpa_freeze_attach_v1.mjs"
if(!(Test-Path $script)){ throw "Missing node script: $script" }

Write-Host "[run] attach+freeze civic RPA (statewide)"
& $nodeExe $script `
  --backendRoot "$BackendRoot" `
  --asOfDate "$AsOfDate" `
  --contractIn "$contractInAbs" `
  --rpaGeojson "$rpaGeojsonAbs" `
  --verifySampleLines "$VerifySampleLines"

if($LASTEXITCODE -ne 0){ throw "Phase2.2 RPA run failed exit=$LASTEXITCODE" }

Write-Host "[done] Phase2.2 RPA complete."
