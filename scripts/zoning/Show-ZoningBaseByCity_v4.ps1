[CmdletBinding()]
param(
  [Parameter(Mandatory=$false)]
  [string]$ZoningRoot = ".\publicData\zoning",

  [switch]$ComputeBBox,

  # MA sanity bounds (EPSG:4326 lon/lat)
  [double]$MaMinLon = -73.6,
  [double]$MaMaxLon = -69.5,
  [double]$MaMinLat = 41.0,
  [double]$MaMaxLat = 43.6
)

$nodeStats = Join-Path $PSScriptRoot "_lib\geojsonStats_v1.mjs"
if (-not (Test-Path $nodeStats)) { throw "Missing Node helper: $nodeStats" }
if (-not (Test-Path $ZoningRoot)) { throw "Missing zoning root: $ZoningRoot" }

function Get-GeoJsonStats {
  param([Parameter(Mandatory=$true)][string]$Path)
  $json = & node $nodeStats $Path 2>$null
  if (-not $json) { return $null }
  try { return $json | ConvertFrom-Json } catch { return $null }
}

function In-MA {
  param([Parameter(Mandatory=$true)]$Bbox)
  if (-not $Bbox) { return $false }
  $minX = [double]$Bbox[0]; $minY = [double]$Bbox[1]; $maxX = [double]$Bbox[2]; $maxY = [double]$Bbox[3]
  return ($minX -ge $MaMinLon -and $maxX -le $MaMaxLon -and $minY -ge $MaMinLat -and $maxY -le $MaMaxLat)
}

Write-Host "====================================================="
Write-Host "[zoningBaseShow] START $(Get-Date -Format o)"
Write-Host "[zoningBaseShow] zoningRoot: $ZoningRoot"
Write-Host "[zoningBaseShow] computeBBox: $ComputeBBox"
Write-Host "====================================================="

$rows = @()
$townDirs = Get-ChildItem -Path $ZoningRoot -Directory -ErrorAction SilentlyContinue

foreach ($td in $townDirs) {
  $town = $td.Name
  $districtsDir = Join-Path $td.FullName "districts"
  $base = Join-Path $districtsDir "zoning_base.geojson"

  if (-not (Test-Path $districtsDir)) {
    $rows += [pscustomobject]@{ town=$town; baseFile=""; sizeMB=0; features=0; bbox=""; inMA=$false; note="NO_DISTRICTS_DIR" }
    continue
  }

  if (-not (Test-Path $base)) {
    $rows += [pscustomobject]@{ town=$town; baseFile=""; sizeMB=0; features=0; bbox=""; inMA=$false; note="NO_zoning_base.geojson" }
    continue
  }

  $mb = [Math]::Round(((Get-Item $base).Length / 1MB), 2)
  $features = 0
  $bbox = ""
  $inMA = $false

  if ($ComputeBBox) {
    $stats = Get-GeoJsonStats -Path $base
    if ($stats -and $stats.bbox) {
      $features = [int]$stats.features
      $bbox = ($stats.bbox -join ",")
      $inMA = In-MA -Bbox $stats.bbox
    }
  }

  $rows += [pscustomobject]@{
    town=$town; baseFile=$base; sizeMB=$mb; features=$features; bbox=$bbox; inMA=$inMA; note="OK"
  }
}

$rows | Sort-Object town | Format-Table town, sizeMB, features, inMA, note, baseFile
