param(
  [Parameter(Mandatory=$true)][string[]]$Cities,
  [ValidateSet("IndexOnly","CopyCandidates")][string]$Mode = "IndexOnly"
)

$ErrorActionPreference = "Stop"

function Find-CityDir([string]$root, [string]$city) {
  if (!(Test-Path $root)) { return $null }
  $hit = Get-ChildItem $root -Directory -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -ieq $city } |
    Select-Object -First 1
  if ($hit) { return $hit.FullName }

  # fallback: try lowercase match
  $hit2 = Get-ChildItem $root -Directory -ErrorAction SilentlyContinue |
    Where-Object { $_.Name.ToLower() -eq $city.ToLower() } |
    Select-Object -First 1
  if ($hit2) { return $hit2.FullName }

  return $null
}

function Write-Tree([string]$dir, [string]$outPath) {
  $lines = @()
  $lines += ("DIR: {0}" -f $dir)
  $lines += ""

  $subDirs = Get-ChildItem $dir -Directory -Recurse -ErrorAction SilentlyContinue | Select-Object FullName
  $files   = Get-ChildItem $dir -File -Recurse -ErrorAction SilentlyContinue | Select-Object FullName, Length, LastWriteTime

  $lines += ("subdirs: {0}" -f ($subDirs | Measure-Object).Count)
  $lines += ("files:   {0}" -f ($files | Measure-Object).Count)
  $lines += ""

  $lines += "FILES (top 250 by newest):"
  $files | Sort-Object LastWriteTime -Descending | Select-Object -First 250 | ForEach-Object {
    $lines += ("- {0} | {1} bytes | {2}" -f $_.FullName, $_.Length, $_.LastWriteTime)
  }

  $lines -join "`r`n" | Set-Content -Encoding UTF8 $outPath
}

function Copy-Candidates([string]$srcDir, [string]$dstDir) {
  New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

  $exts = @(".geojson",".geojsons",".json",".gpkg",".shp",".dbf",".shx",".prj",".cpg",".gdb.zip",".zip")
  $files = Get-ChildItem $srcDir -File -Recurse -ErrorAction SilentlyContinue | Where-Object {
    $ext = $_.Extension.ToLower()
    $name = $_.Name.ToLower()
    ($exts -contains $ext) -or ($name.EndsWith(".gdb.zip"))
  }

  $copied = 0
  foreach ($f in $files) {
    $dest = Join-Path $dstDir $f.Name
    Copy-Item -Force $f.FullName $dest
    $copied++
  }
  return $copied
}

$now = Get-Date -Format "yyyyMMdd_HHmmss"
$auditDir = ".\publicData\_audit\phasezo_city_pull_review__$now"
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

$zoningRoot = ".\publicData\zoning"
$boundRoot  = ".\publicData\boundaries"

$summary = @()

foreach ($city in $Cities) {
  Write-Host ""
  Write-Host ("================  CITY REVIEW: {0}  ================" -f $city)

  $zCity = Find-CityDir $zoningRoot $city
  $bCity = Find-CityDir $boundRoot  $city

  $zOverlays = $null
  if ($zCity) {
    $zOverlays = Join-Path $zCity "overlays"
    if (!(Test-Path $zOverlays)) { $zOverlays = $null }
  }

  $zOut = Join-Path $auditDir ("zoning_overlays_tree__" + $city.ToLower() + ".txt")
  $bOut = Join-Path $auditDir ("boundaries_tree__" + $city.ToLower() + ".txt")

  if ($zOverlays) {
    Write-Tree $zOverlays $zOut
    Write-Host "[ok] wrote overlays tree:" $zOut
  } else {
    ("MISSING overlays dir for city: {0}" -f $city) | Set-Content -Encoding UTF8 $zOut
    Write-Host "[warn] no overlays folder:" $zOut
  }

  if ($bCity) {
    Write-Tree $bCity $bOut
    Write-Host "[ok] wrote boundaries tree:" $bOut
  } else {
    ("MISSING boundaries dir for city: {0}" -f $city) | Set-Content -Encoding UTF8 $bOut
    Write-Host "[warn] no boundaries folder:" $bOut
  }

  $copiedOverlays = 0
  $copiedBounds   = 0

  if ($Mode -eq "CopyCandidates") {
    if ($zOverlays) {
      $dst = Join-Path $zOverlays "_inbox_review"
      $copiedOverlays = Copy-Candidates $zOverlays $dst
      Write-Host ("[ok] copied overlay candidates -> {0} (files={1})" -f $dst, $copiedOverlays)
    }
    if ($bCity) {
      $dst = Join-Path $bCity "_inbox_review"
      $copiedBounds = Copy-Candidates $bCity $dst
      Write-Host ("[ok] copied boundary candidates -> {0} (files={1})" -f $dst, $copiedBounds)
    }
  }

  $summary += [pscustomobject]@{
    city = $city
    zoning_overlays_dir = $zOverlays
    boundaries_dir = $bCity
    wrote_overlays_tree = $zOut
    wrote_boundaries_tree = $bOut
    copied_overlay_files = $copiedOverlays
    copied_boundary_files = $copiedBounds
  }
}

$sumPathJson = Join-Path $auditDir "summary.json"
$sumPathTxt  = Join-Path $auditDir "summary.txt"
($summary | ConvertTo-Json -Depth 5) | Set-Content -Encoding UTF8 $sumPathJson
$summary | Format-Table -Auto | Out-String | Set-Content -Encoding UTF8 $sumPathTxt

Write-Host ""
Write-Host "[ok] auditDir:" $auditDir
Write-Host "[ok] wrote:" $sumPathJson
Write-Host "[ok] wrote:" $sumPathTxt
