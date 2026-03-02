[CmdletBinding()]
param(
  [Parameter(Mandatory=$false)]
  [string]$ZoningRoot = ".\publicData\zoning",

  [Parameter(Mandatory=$false)]
  [string]$AsOf = (Get-Date -Format "yyyy-MM-dd"),

  # how many top candidates to bbox-validate (increase if a town has a bad first pick like “historic” or “outline”)
  [Parameter(Mandatory=$false)]
  [int]$TopKToValidate = 5,

  [Parameter(Mandatory=$false)]
  [int]$ProgressEvery = 25,

  [switch]$DryRun,

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

function Name-Score {
  param([Parameter(Mandatory=$true)][string]$FilePath)
  $n = [IO.Path]::GetFileName($FilePath).ToLowerInvariant()
  $s = 0

  if ($n -eq "zoning_base.geojson") { $s += 2000 }
  elseif ($n -like "zoning_base*")  { $s += 1500 }
  elseif ($n -like "*zoning*district*") { $s += 900 }
  elseif ($n -like "*zoning*") { $s += 600 }
  elseif ($n -like "*district*") { $s += 300 }

  if ($n -like "*_std*")  { $s += 150 }
  if ($n -like "*norm*")  { $s += 120 }

  # penalties (common non-base traps)
  if ($n -like "*overlay*")  { $s -= 800 }
  if ($n -like "*historic*") { $s -= 900 }
  if ($n -like "*outline*")  { $s -= 600 }
  if ($n -like "*shaded*")   { $s -= 300 }

  return $s
}

Write-Host "====================================================="
Write-Host "[zoningBaseCanonical] START $(Get-Date -Format o)"
Write-Host "[zoningBaseCanonical] zoningRoot: $ZoningRoot"
Write-Host "[zoningBaseCanonical] asOf: $AsOf"
Write-Host "[zoningBaseCanonical] topKToValidate: $TopKToValidate"
Write-Host "[zoningBaseCanonical] dryRun: $DryRun"
Write-Host "====================================================="

$publicDataDir = Resolve-Path (Join-Path $ZoningRoot "..")
$auditDir = Join-Path $publicDataDir "_audit"
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

$townDirs = Get-ChildItem -Path $ZoningRoot -Directory -ErrorAction SilentlyContinue
$results = @()

$idx = 0
foreach ($td in $townDirs) {
  $idx++
  $town = $td.Name
  $districtsDir = Join-Path $td.FullName "districts"

  if (-not (Test-Path $districtsDir)) {
    $results += [pscustomobject]@{ town=$town; selected=$null; note="NO_DISTRICTS_DIR"; inMA=$false; features=0; sizeMB=0; selected_from=$null }
    continue
  }

  $files = Get-ChildItem -Path $districtsDir -Filter "*.geojson" -File -ErrorAction SilentlyContinue
  if (-not $files) {
    $results += [pscustomobject]@{ town=$town; selected=$null; note="NO_GEOJSON_IN_DISTRICTS"; inMA=$false; features=0; sizeMB=0; selected_from=$null }
    continue
  }

  $ranked = $files |
    ForEach-Object {
      $mb = [Math]::Round(($_.Length / 1MB), 2)
      [pscustomobject]@{ file=$_.FullName; sizeMB=$mb; nameScore=(Name-Score -FilePath $_.FullName) }
    } |
    Sort-Object -Property @{Expression="nameScore";Descending=$true}, @{Expression="sizeMB";Descending=$true}

  $picked = $null
  $pickedStats = $null
  $note = "OK"

  $try = 0
  foreach ($c in $ranked) {
    $try++
    if ($try -gt $TopKToValidate) { break }

    $stats = Get-GeoJsonStats -Path $c.file
    if (-not $stats -or -not $stats.bbox) { continue }

    if (In-MA -Bbox $stats.bbox) {
      $picked = $c.file
      $pickedStats = [pscustomobject]@{
        features = [int]$stats.features
        sizeMB = $c.sizeMB
        inMA = $true
      }
      break
    }
  }

  if (-not $picked) {
    $best = $ranked | Select-Object -First 1
    $picked = $best.file
    $pickedStats = [pscustomobject]@{ features=0; sizeMB=$best.sizeMB; inMA=$false }
    $note = "PICKED_WITHOUT_MA_BBOX_VALIDATION"
  }

  $dst = Join-Path $districtsDir "zoning_base.geojson"
  $src = $picked

  if (-not (Test-Path $src)) {
    $results += [pscustomobject]@{ town=$town; selected=$null; note="MISSING_SRC"; inMA=$false; features=0; sizeMB=0; selected_from=$src }
    continue
  }

  $same = $false
  try {
    $dstResolved = Resolve-Path $dst -ErrorAction SilentlyContinue
    $srcResolved = Resolve-Path $src -ErrorAction Stop
    if ($dstResolved -and ($dstResolved.Path -eq $srcResolved.Path)) { $same = $true }
  } catch {}

  if ($same) {
    Write-Host "[SKIP] $town zoning_base.geojson already correct (source is itself)"
  } else {
    if (Test-Path $dst) {
      $bak = Join-Path $districtsDir ("zoning_base__OLD__{0}.geojson" -f (Get-Date -Format yyyyMMdd_HHmmss))
      if (-not $DryRun) { Rename-Item -Force $dst $bak }
      Write-Host "[OK ] $town backed up zoning_base.geojson -> $(Split-Path $bak -Leaf)"
    }

    if (-not $DryRun) { Copy-Item -Force $src $dst }
    Write-Host "[DONE] $town zoning_base.geojson <= $(Split-Path $src -Leaf)"
  }

  $results += [pscustomobject]@{
    town=$town
    selected=$dst
    note=$note
    inMA=$pickedStats.inMA
    features=$pickedStats.features
    sizeMB=$pickedStats.sizeMB
    selected_from=$src
  }

  if (($idx % $ProgressEvery) -eq 0) {
    Write-Host "[zoningBaseCanonical] progress towns=$idx / $($townDirs.Count)"
  }
}

$ts = Get-Date -Format yyyyMMdd_HHmmss
$outJson = Join-Path $auditDir ("zoning_base_canonical__{0}.json" -f $ts)

$payload = [pscustomobject]@{
  ran_at = (Get-Date -Format o)
  zoningRoot = (Resolve-Path $ZoningRoot).Path
  as_of = $AsOf
  towns_total = $townDirs.Count
  towns_with_selected = ($results | Where-Object { $_.selected }).Count
  towns_inMA_validated = ($results | Where-Object { $_.inMA -eq $true }).Count
  results = $results
}

if (-not $DryRun) {
  $payload | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $outJson
}

Write-Host "====================================================="
Write-Host "[zoningBaseCanonical] DONE  $(Get-Date -Format o)"
Write-Host "[zoningBaseCanonical] wrote: $outJson"
Write-Host "====================================================="

$results | Sort-Object town | Format-Table town, note, inMA, features, sizeMB, selected_from
