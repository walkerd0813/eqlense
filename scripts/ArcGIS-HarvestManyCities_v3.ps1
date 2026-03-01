[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$EndpointsJson,
  [int]$Top = 25,
  [int]$TimeoutSec = 20,
  [int]$MaxFeatures = 25000,
  [switch]$Force
)

$ErrorActionPreference = "Stop"

if(!(Test-Path $EndpointsJson)){
  throw "Missing endpoints file: $EndpointsJson"
}

$eps = Get-Content $EndpointsJson -Raw | ConvertFrom-Json
if($eps -isnot [System.Collections.IEnumerable]){ $eps = @($eps) }

$cityScript = Join-Path $PSScriptRoot "ArcGIS-HarvestCityAuto_v2.ps1"
if(!(Test-Path $cityScript)){
  throw "Missing city harvester: $cityScript"
}

Write-Host "===================================================="
Write-Host "BATCH HARVEST v3 START"
Write-Host "endpoints: $EndpointsJson"
Write-Host "Top: $Top  TimeoutSec: $TimeoutSec  MaxFeatures: $MaxFeatures  Force: $Force"
Write-Host "===================================================="

$ran=0; $skipped=0

foreach($ep in $eps){
  $city = ("" + $ep.city).Trim()

  # enabled flag (default true if missing)
  $enabled = $true
  if($null -ne $ep.enabled){ $enabled = [bool]$ep.enabled }
  elseif($null -ne $ep.is_enabled){ $enabled = [bool]$ep.is_enabled }

  # accept multiple possible url property names
  $root = $null
  foreach($k in @("rootUrl","url","serviceUrl","endpoint","root","pjson_endpoint")){
    if($null -ne $ep.$k -and ("" + $ep.$k).Trim()){
      $root = ("" + $ep.$k).Trim()
      break
    }
  }

  if(-not $enabled){
    Write-Host "⏭️  Skipping $city (disabled)"
    $skipped++; continue
  }
  if(-not $root){
    Write-Host "⏭️  Skipping $city (no root url found in JSON)"
    $skipped++; continue
  }

  $ran++
  Write-Host "▶️  [$ran] Harvest $city => $root"

  $args = @{
    City        = $city
    RootUrl     = $root
    Top         = $Top
    TimeoutSec  = $TimeoutSec
    MaxFeatures = $MaxFeatures
  }
  if($Force){ $args["Force"] = $true }

  & $cityScript @args
}

Write-Host ""
Write-Host "✅ BATCH HARVEST v3 DONE  ran=$ran skipped=$skipped"
