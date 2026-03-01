param(
  [Parameter(Mandatory=$true)][string]$EndpointsJson,
  [int]$Top = 15,
  [int]$TimeoutSec = 20,
  [int]$MaxFeatures = 25000,
  [switch]$Force,
  [string[]]$OnlyCities = @()
)

$ErrorActionPreference = "Stop"

if(-not (Test-Path $EndpointsJson)){ throw "Missing endpoints file: $EndpointsJson" }
$endpoints = Get-Content $EndpointsJson -Raw | ConvertFrom-Json

$runner = Resolve-Path ".\scripts\ArcGIS-HarvestCityAuto_v4.ps1" -ErrorAction Stop | Select-Object -ExpandProperty Path

$enabled = @($endpoints | Where-Object { $_.enabled -and $_.rootUrl })
if($OnlyCities -and $OnlyCities.Count -gt 0){
  $set = @{}
  foreach($c in $OnlyCities){ $set[$c.ToLower()] = $true }
  $enabled = @($enabled | Where-Object { $set.ContainsKey(($_.city+"").ToLower()) })
}

Write-Host ""
Write-Host "===================================================="
Write-Host "BATCH HARVEST v5 START"
Write-Host "endpoints: $EndpointsJson"
Write-Host "Cities: $($enabled.Count)  Top: $Top  TimeoutSec: $TimeoutSec  MaxFeatures: $MaxFeatures  Force: $($Force.IsPresent)"
Write-Host "===================================================="

$results = @()

for($i=0; $i -lt $enabled.Count; $i++){
  $ep = $enabled[$i]
  $city = $ep.city
  $root = $ep.rootUrl
  $afr  = $ep.allowFolderRegex
  $exc  = $ep.excludeCategories

  if(-not $exc){
    $exc = @("flood_fema","transit")
  }

  Write-Host ("▶️  [{0}/{1}] Harvest {2} => {3}" -f ($i+1),$enabled.Count,$city,$root)

  $args = @(
    "-NoProfile","-ExecutionPolicy","Bypass",
    "-File",$runner,
    "-City",$city,
    "-RootUrl",$root,
    "-Top",$Top,
    "-TimeoutSec",$TimeoutSec,
    "-MaxFeatures",$MaxFeatures
  )

  if($Force.IsPresent){ $args += "-Force" }
  if($afr){ $args += @("-AllowFolderRegex",$afr) }
  if($exc){ $args += @("-ExcludeCategories", ($exc -join ",")) } # v4 accepts array in PS; passing comma string is OK too

  try{
    powershell @args
    $results += [pscustomobject]@{ city=$city; ok=$true }
  } catch {
    Write-Warning ("city failed: {0}" -f $_.Exception.Message)
    $results += [pscustomobject]@{ city=$city; ok=$false; error=$_.Exception.Message }
  }
}

$reportPath = ".\publicData\gis\_scans\batch_harvest_v5_report.json"
New-Item -ItemType Directory -Force (Split-Path $reportPath) | Out-Null
[pscustomobject]@{
  createdAt = (Get-Date).ToString("o")
  endpoints = $EndpointsJson
  top = $Top
  timeoutSec = $TimeoutSec
  maxFeatures = $MaxFeatures
  force = $Force.IsPresent
  onlyCities = $OnlyCities
  results = $results
} | ConvertTo-Json -Depth 50 | Set-Content -Encoding UTF8 $reportPath

Write-Host ""
Write-Host "✅ BATCH HARVEST v5 DONE"
Write-Host "Report: $reportPath"
