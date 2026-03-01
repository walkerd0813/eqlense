param(
  [Parameter(Mandatory=$true)][string]$PlanJson,
  [Parameter(Mandatory=$true)][string]$OutDir,
  [int]$OutSR = 4326,
  [int]$MaxPerCategory = 3,
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

if(-not (Test-Path $PlanJson)){ throw "PlanJson not found: $PlanJson" }

$plan = Get-Content $PlanJson -Raw | ConvertFrom-Json

$city = $plan.city
if(-not $city){ $city = "CITY" }

$nodeDownloader = ".\mls\scripts\gis\arcgisDownloadLayerToGeoJSON_v1.mjs"
if(-not (Test-Path $nodeDownloader)){
  throw "Missing Node downloader: $nodeDownloader"
}

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# group candidates by category, take top N by score
$groups = $plan.candidates | Group-Object category
foreach($g in $groups){
  $cat = $g.Name
  $top = $g.Group | Sort-Object score -Descending | Select-Object -First $MaxPerCategory

  $catDir = Join-Path $OutDir $cat
  New-Item -ItemType Directory -Force -Path $catDir | Out-Null

  foreach($c in $top){
    $layerUrl = $c.layerUrl
    $layerId  = $c.layerId
    $safeName = ($c.layerName -replace '[^\w\-]+','_').Trim('_')
    if([string]::IsNullOrWhiteSpace($safeName)){ $safeName = "layer_$layerId" }

    $outFile = Join-Path $catDir ("{0}_{1}_{2}.geojson" -f $city, $layerId, $safeName)

    Write-Host ""
    Write-Host ("==> {0} | score={1} | {2}" -f $cat, $c.score, $c.layerName)
    Write-Host ("    {0}" -f $layerUrl)
    Write-Host ("    OUT: {0}" -f $outFile)

    if($DryRun){ continue }

    node $nodeDownloader `
      --layerUrl "$layerUrl" `
      --out "$outFile" `
      --outSR $OutSR

    if(-not (Test-Path $outFile)){
      Write-Warning "Expected output not found (download may have failed): $outFile"
    }
  }
}

Write-Host ""
Write-Host "[done] downloads complete (or dry-run)."
