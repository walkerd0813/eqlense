[CmdletBinding()]
param(
  [string]$ZoningRoot = ".\publicData\zoning",
  [string]$AsOf = (Get-Date -Format "yyyy-MM-dd"),
  [int]$TopKToValidate = 5,
  [int]$ProgressEvery = 25
)

$ErrorActionPreference = "Stop"

function Name-Score {
  param([Parameter(Mandatory=$true)][string]$FilePath)

  $n = [IO.Path]::GetFileName($FilePath).ToLowerInvariant()
  $s = 0

  if ($n -eq "zoning_base.geojson") { $s += 2000 }
  elseif ($n -like "zoning_base*")  { $s += 1500 }
  elseif ($n -like "*zoning*district*") { $s += 900 }
  elseif ($n -like "*zoning*")      { $s += 600 }
  elseif ($n -like "*district*")    { $s += 300 }

  if ($n -like "*_std*") { $s += 150 }
  if ($n -like "*norm*") { $s += 120 }
  if ($n -like "*outline*") { $s -= 400 }
  if ($n -like "*shaded*")  { $s -= 200 }
  if ($n -like "*historic*") { $s -= 250 }

  return $s
}

Write-Host "====================================================="
Write-Host "[zoningBaseCanonical] START $(Get-Date -Format o)"
Write-Host "[zoningBaseCanonical] zoningRoot: $ZoningRoot"
Write-Host "[zoningBaseCanonical] asOf: $AsOf"
Write-Host "[zoningBaseCanonical] topKToValidate: $TopKToValidate"
Write-Host "====================================================="

if (-not (Test-Path $ZoningRoot)) { throw "Missing zoning root: $ZoningRoot" }

$auditDir = Join-Path $ZoningRoot "..\_audit"
$auditDir = (Resolve-Path $auditDir).Path
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null

$townDirs = Get-ChildItem -Path $ZoningRoot -Directory -ErrorAction SilentlyContinue
$results = @()

$idx = 0
foreach ($td in $townDirs) {
  $idx++
  $town = $td.Name
  $districtsDir = Join-Path $td.FullName "districts"

  if (-not (Test-Path $districtsDir)) {
    $results += [pscustomobject]@{
      town=$town; selected=$null; selected_from=$null; note="NO_DISTRICTS_DIR"; features=0; sizeMB=0
    }
    continue
  }

  $files = Get-ChildItem -Path $districtsDir -Filter "*.geojson" -File -ErrorAction SilentlyContinue
  if (-not $files) {
    $results += [pscustomobject]@{
      town=$town; selected=$null; selected_from=$null; note="NO_GEOJSON_IN_DISTRICTS"; features=0; sizeMB=0
    }
    continue
  }

  $ranked = $files |
    ForEach-Object {
      $mb = [Math]::Round(($_.Length / 1MB), 2)
      [pscustomobject]@{ file=$_.FullName; sizeMB=$mb; nameScore=(Name-Score -FilePath $_.FullName) }
    } |
    Sort-Object -Property @{Expression="nameScore";Descending=$true}, @{Expression="sizeMB";Descending=$true}

  $picked = ($ranked | Select-Object -First 1).file
  $pickedMB = ($ranked | Select-Object -First 1).sizeMB
  $note = "OK"

  $dst = Join-Path $districtsDir "zoning_base.geojson"
  $src = $picked

  if (-not (Test-Path $src)) {
    $results += [pscustomobject]@{
      town=$town; selected=$null; selected_from=$src; note="MISSING_SRC"; features=0; sizeMB=0
    }
    continue
  }

  $srcPath = (Resolve-Path $src).Path
  $dstPath = (Resolve-Path $dst -ErrorAction SilentlyContinue)?.Path

  if ($dstPath -and ($srcPath -eq $dstPath)) {
    Write-Host "[SKIP] $town zoning_base.geojson already correct (source is itself)"
  } else {
    if (Test-Path $dst) {
      $bak = Join-Path $districtsDir ("zoning_base__OLD__{0}.geojson" -f (Get-Date -Format yyyyMMdd_HHmmss))
      Move-Item -Force -LiteralPath $dst -Destination $bak
      Write-Host "[OK ] $town backed up zoning_base.geojson -> $(Split-Path $bak -Leaf)"
    }
    Copy-Item -Force -LiteralPath $src -Destination $dst
    Write-Host "[DONE] $town zoning_base.geojson <= $(Split-Path $src -Leaf)"
  }

  $results += [pscustomobject]@{
    town=$town
    selected=$dst
    selected_from=$src
    note=$note
    features=0
    sizeMB=$pickedMB
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
  results = $results
}

$payload | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 $outJson

Write-Host "====================================================="
Write-Host "[zoningBaseCanonical] DONE  $(Get-Date -Format o)"
Write-Host "[zoningBaseCanonical] wrote: $outJson"
Write-Host "====================================================="

$results |
  Select-Object town, note, sizeMB, selected_from |
  Sort-Object town |
  Format-Table -AutoSize
