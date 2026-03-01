[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$EndpointsJson,
  [int]$TimeoutSec = 20,
  [int]$MaxFeatures = 25000,
  [int]$TopOverlay = 2,
  [string]$ExcludeCategories = "",
  [switch]$SkipIfManifestExists,
  [switch]$Force
)

function EnsureDir([string]$p){
  if(-not (Test-Path $p)){ New-Item -ItemType Directory -Force -Path $p | Out-Null }
}

$epPath = [System.IO.Path]::GetFullPath($EndpointsJson)
if(-not (Test-Path $epPath)){
  throw "EndpointsJson not found: $epPath"
}

$endpoints = Get-Content $epPath -Raw | ConvertFrom-Json
$endpoints = @($endpoints | Where-Object { $_.enabled -eq $true -and $_.rootUrl })

EnsureDir ".\publicData\gis\_scans"

$report = @()

foreach($ep in $endpoints){
  $city = $ep.city.ToString().ToLower()
  $root = $ep.rootUrl.ToString()
  $allow = ".*"
  if($null -ne $ep.allowFolderRegex -and ($ep.allowFolderRegex.ToString().Length -gt 0)){
    $allow = $ep.allowFolderRegex.ToString()
  }

  $manifest = ".\publicData\gis\cities\{0}\manifest_{0}_v3.json" -f $city
  if($SkipIfManifestExists.IsPresent -and (Test-Path $manifest) -and (-not $Force.IsPresent)){
    Write-Host "⏭️  Skip (manifest exists): $city"
    $report += [pscustomobject]@{ city=$city; rootUrl=$root; skipped=$true; reason="manifest exists" }
    continue
  }

  Write-Host "===================================================="
  Write-Host "▶️  Harvest (zoning-first batch): $city => $root"
  Write-Host "===================================================="

  & powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\ArcGIS-HarvestCityTargets_v2.ps1" `
    -City $city -RootUrl $root -TimeoutSec $TimeoutSec -MaxFeatures $MaxFeatures -TopOverlay $TopOverlay `
    -ExcludeCategories $ExcludeCategories -AllowFolderRegex $allow -Force:$Force.IsPresent

  $report += [pscustomobject]@{ city=$city; rootUrl=$root; skipped=$false; manifest=$manifest }
}

$repPath = ".\publicData\gis\_scans\batch_zoningfirst_report_v2.json"
$repJson = $report | ConvertTo-Json -Depth 20
[System.IO.File]::WriteAllText((Resolve-Path ".\publicData\gis\_scans").Path + "\batch_zoningfirst_report_v2.json", $repJson, [System.Text.Encoding]::UTF8)
Write-Host "✅ batch report: $repPath"