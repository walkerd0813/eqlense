[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$City,
  [Parameter(Mandatory=$true)][string]$RootUrl,
  [int]$TimeoutSec = 20,
  [int]$MaxFeatures = 25000,
  [int]$TopOverlay = 2,
  [string]$ExcludeCategories = "",
  [string]$AllowFolderRegex = ".*",
  [switch]$Force
)

function EnsureDir([string]$p){
  if(-not (Test-Path $p)){ New-Item -ItemType Directory -Force -Path $p | Out-Null }
}

function WriteJsonFile([string]$path, $obj){
  EnsureDir (Split-Path -Parent $path)
  $json = $obj | ConvertTo-Json -Depth 80
  [System.IO.File]::WriteAllText($path, $json, [System.Text.Encoding]::UTF8)
}

function Slug($s){
  if($null -eq $s){ return "layer" }
  $t = $s.ToString().ToLower()
  $t = [regex]::Replace($t, "[^a-z0-9]+", "_")
  $t = [regex]::Replace($t, "_{2,}", "_").Trim("_")
  if([string]::IsNullOrWhiteSpace($t)){ $t = "layer" }
  return $t
}

$cityKey = ($City.ToLower())
$root = $RootUrl.TrimEnd("/")
$exclude = @()
if(-not [string]::IsNullOrWhiteSpace($ExcludeCategories)){
  $exclude = $ExcludeCategories.Split(",") | ForEach-Object { $_.Trim().ToLower() } | Where-Object { $_ }
}

$hitsPath = [System.IO.Path]::GetFullPath(".\publicData\gis\_scans\{0}_hits_v4.json" -f $cityKey)
if(-not (Test-Path $hitsPath)){
  $finder = ".\scripts\ArcGIS-FindLayerHits_v4.ps1"
  & powershell -NoProfile -ExecutionPolicy Bypass -File $finder `
    -City $cityKey -RootUrl $root -TimeoutSec $TimeoutSec -OutJson $hitsPath -AllowFolderRegex $AllowFolderRegex
}

if(-not (Test-Path $hitsPath)){
  Write-Host "⚠️  hits file missing: $hitsPath"
  exit 0
}

$hitsObj = Get-Content $hitsPath -Raw | ConvertFrom-Json
$hits = @($hitsObj.hits)

# filter excluded categories
if($exclude.Count -gt 0){
  $hits = @($hits | Where-Object {
    $c = ($_.category.ToString().ToLower())
    -not ($exclude -contains $c)
  })
}

$pickBase = @($hits | Where-Object { $_.category -eq "zoning_base" } | Select-Object -First 1)
$pickOver = @($hits | Where-Object { $_.category -eq "zoning_overlay" } | Select-Object -First $TopOverlay)
$pickAss  = @($hits | Where-Object { $_.category -eq "assessor" } | Select-Object -First 1)
$pickPerm = @($hits | Where-Object { $_.category -eq "permits" } | Select-Object -First 1)
$pickBnd  = @($hits | Where-Object { $_.category -eq "boundaries" } | Select-Object -First 1)
$pickOpp  = @($hits | Where-Object { $_.category -eq "opportunity" } | Select-Object -First 1)

$picks = @()
$picks += $pickBase
$picks += $pickOver
$picks += $pickAss
$picks += $pickPerm
$picks += $pickBnd
$picks += $pickOpp
$picks = @($picks | Where-Object { $null -ne $_ })

if($picks.Count -eq 0){
  Write-Host "⚠️  No zoning/overlay/assessor/permits/boundaries/opportunity picks for $cityKey."
  exit 0
}

$cityDir = [System.IO.Path]::GetFullPath(".\publicData\gis\cities\{0}" -f $cityKey)
$rawDir  = Join-Path $cityDir "raw"
$repDir  = Join-Path $cityDir "reports"
EnsureDir $rawDir
EnsureDir $repDir

$dlScript = ".\mls\scripts\gis\arcgisDownloadLayerToGeoJSON_v1.mjs"
$repScript = ".\mls\scripts\gis\geojsonFieldReport_v1.mjs"

$downloads = @()
foreach($p in $picks){
  $cat = $p.category.ToString().ToLower()
  $layerName = $p.layerName
  $layerId = $p.layerId
  $layerUrl = $p.layerUrl
  $outName = "{0}__{1}__{2}.geojson" -f $cat, (Slug $layerName), $layerId
  $outPath = Join-Path $rawDir $outName

  if((Test-Path $outPath) -and (-not $Force.IsPresent)){
    Write-Host "⏭️  exists: $outPath"
    $downloads += [pscustomobject]@{ category=$cat; layerName=$layerName; layerId=$layerId; layerUrl=$layerUrl; out=$outPath; skipped=$true }
    continue
  }

  Write-Host ("▶️  DL: [{0}] {1} => {2}" -f $cat, $layerName, $layerUrl)
  & node $dlScript --layerUrl $layerUrl --out $outPath --outSR 4326

  if(Test-Path $outPath){
    & node $repScript $outPath | Out-Null
    $downloads += [pscustomobject]@{ category=$cat; layerName=$layerName; layerId=$layerId; layerUrl=$layerUrl; out=$outPath; skipped=$false }
  } else {
    Write-Host "⚠️  download failed (no file): $outPath"
    $downloads += [pscustomobject]@{ category=$cat; layerName=$layerName; layerId=$layerId; layerUrl=$layerUrl; out=$outPath; skipped=$false; failed=$true }
  }
}

$manifestPath = Join-Path $cityDir ("manifest_{0}_v3.json" -f $cityKey)
WriteJsonFile $manifestPath ([pscustomobject]@{
  city=$cityKey
  rootUrl=$root
  createdAt=(Get-Date).ToString("o")
  excludeCategories=$exclude
  picks=$picks
  downloads=$downloads
  hitsFile=$hitsPath
})

Write-Host "✅ Harvest complete: $manifestPath"