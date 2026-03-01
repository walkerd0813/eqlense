param(
  [Parameter(Mandatory=$true)][string]$City,
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [int]$TimeoutSec = 20,
  [int]$MaxFeatures = 25000,
  [int]$TopOverlay = 2,
  [string]$ExcludeCategories = "",
  [switch]$Force
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = Resolve-Path (Join-Path $scriptDir "..")
Set-Location $rootDir | Out-Null

function Slug([string]$s){
  if([string]::IsNullOrWhiteSpace($s)){ return "layer" }
  $x = $s.ToLower()
  $x = [regex]::Replace($x, "[^a-z0-9]+", "_")
  $x = $x.Trim("_")
  if($x.Length -gt 80){ $x = $x.Substring(0,80) }
  return $x
}

$cityKey = $City.ToLower()
$baseDir = ".\publicData\gis\cities\$cityKey"
$rawDir  = Join-Path $baseDir "raw"
$repDir  = Join-Path $baseDir "reports"
$scanDir = ".\publicData\gis\_scans"
$hitsPath = Join-Path $scanDir ($cityKey + "_hits_v2.json")
$manifestPath = Join-Path $baseDir ("manifest_" + $cityKey + "_v2.json")

New-Item -ItemType Directory -Path $rawDir -Force | Out-Null
New-Item -ItemType Directory -Path $repDir -Force | Out-Null
New-Item -ItemType Directory -Path $scanDir -Force | Out-Null

# Build hits
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\ArcGIS-FindLayerHits_v2.ps1 `
  -City $cityKey `
  -RootUrl $RootUrl `
  -TimeoutSec $TimeoutSec `
  -OutJson $hitsPath | Out-Host

if(-not (Test-Path $hitsPath)){
  Write-Host "❌ hits file missing: $hitsPath"
  exit 1
}

$hitsObj = Get-Content $hitsPath -Raw | ConvertFrom-Json
$hits = @($hitsObj.hits)

$exclude = @()
if(-not [string]::IsNullOrWhiteSpace($ExcludeCategories)){
  $exclude = $ExcludeCategories.Split(",") | ForEach-Object { $_.Trim().ToLower() } | Where-Object { $_ -ne "" }
}
if($exclude.Count -gt 0){
  $hits = $hits | Where-Object { $exclude -notcontains ($_.category.ToLower()) }
}

# Picks (zoning-first)
$pickBase    = $hits | Where-Object { $_.category -eq "zoning_base" }    | Select-Object -First 1
$pickOverlay = $hits | Where-Object { $_.category -eq "zoning_overlay" } | Select-Object -First $TopOverlay
$pickAssessor= $hits | Where-Object { $_.category -eq "assessor" }       | Select-Object -First 1
$pickPermits = $hits | Where-Object { $_.category -eq "permits" }        | Select-Object -First 1
$pickBounds  = $hits | Where-Object { $_.category -eq "boundaries" }     | Select-Object -First 1

$picks = @()
if($null -ne $pickBase){ $picks += $pickBase }
$picks += @($pickOverlay)
if($null -ne $pickAssessor){ $picks += $pickAssessor }
if($null -ne $pickPermits){  $picks += $pickPermits }
if($null -ne $pickBounds){   $picks += $pickBounds }

if($picks.Count -eq 0){
  Write-Host "⚠️  No target layers found for $City. Top 25 hits:"
  $hits | Select-Object -First 25 category,score,layerName,layerId,serviceUrl | Format-Table -Auto
  exit 0
}

$downloads = New-Object System.Collections.Generic.List[object]

foreach($p in $picks){
  $layerUrl = ($p.serviceUrl + "/" + $p.layerId)
  $slug = Slug $p.layerName
  $out = Join-Path $rawDir ($p.category + "__" + $slug + "__" + $p.layerId + ".geojson")
  $fields = Join-Path $repDir ($p.category + "__" + $slug + "__" + $p.layerId + "_fields.json")

  if((Test-Path $out) -and (-not $Force)){
    Write-Host "⏭️  exists: $out"
    continue
  }

  Write-Host "▶️  DL: [$($p.category)] $($p.layerName) => $layerUrl"
  node .\mls\scripts\gis\arcgisDownloadLayerToGeoJSON_v1.mjs `
    --layerUrl "$layerUrl" `
    --out "$out" `
    --outSR 4326 | Out-Host

  if(Test-Path $out){
    $fr = ".\mls\scripts\gis\geojsonFieldReport_v1.mjs"
    if(Test-Path $fr){
      node $fr --in "$out" --out "$fields" | Out-Host
    }

    $fieldsOut = ""
    if(Test-Path $fields){ $fieldsOut = $fields }

    $downloads.Add([pscustomobject]@{
      category = $p.category
      layerName = $p.layerName
      layerId = $p.layerId
      layerUrl = $layerUrl
      out = $out
      fields = $fieldsOut
    })
  } else {
    Write-Host "⚠️  download failed (no output file): $out"
  }
}

$manifest = [pscustomobject]@{
  created_at = (Get-Date).ToString("o")
  city = $cityKey
  rootUrl = $RootUrl
  downloads = @($downloads)
}

$fullManifest = [System.IO.Path]::GetFullPath($manifestPath)
[System.IO.File]::WriteAllText($fullManifest, ($manifest | ConvertTo-Json -Depth 10), (New-Object System.Text.UTF8Encoding $false))
Write-Host "✅ Harvest complete: $manifestPath"