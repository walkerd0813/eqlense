param(
  [Parameter(Mandatory=$true)][string]$EndpointsJson,
  [int]$Top = 25,
  [int]$TimeoutSec = 20,
  [int]$MaxFeatures = 25000,
  [switch]$Force
)

$ErrorActionPreference = "Stop"
if(-not (Test-Path $EndpointsJson)) { throw "Missing endpoints file: $EndpointsJson" }

$endpoints = Get-Content $EndpointsJson -Raw | ConvertFrom-Json
if(-not $endpoints) { throw "No endpoints found in: $EndpointsJson" }

$harvestCity = ".\scripts\ArcGIS-HarvestCityAuto_v2.ps1"
if(-not (Test-Path $harvestCity)) { throw "Missing harvester script: $harvestCity" }

Write-Host ""
Write-Host "===================================================="
Write-Host "BATCH HARVEST v2 START"
Write-Host "endpoints:" $EndpointsJson
Write-Host "Top:" $Top " TimeoutSec:" $TimeoutSec " MaxFeatures:" $MaxFeatures " Force:" $Force
Write-Host "===================================================="

foreach($e in $endpoints) {
  $city = [string]$e.city
  $type = [string]$e.type
  $url  = [string]$e.rootUrl
  $notes= [string]$e.notes

  if($type -ne "arcgis_rest") {
    Write-Host "⏭️  Skipping $city ($type) $notes"
    continue
  }
  if([string]::IsNullOrWhiteSpace($url)) {
    Write-Host "⏭️  Skipping $city (no rootUrl)"
    continue
  }

  Write-Host ""
  Write-Host "---- CITY:" $city
  Write-Host "ROOT:" $url

  if($Force) {
    & $harvestCity -City $city -RootUrl $url -Top $Top -TimeoutSec $TimeoutSec -MaxFeatures $MaxFeatures -Force
  } else {
    & $harvestCity -City $city -RootUrl $url -Top $Top -TimeoutSec $TimeoutSec -MaxFeatures $MaxFeatures
  }
}

Write-Host ""
Write-Host "✅ BATCH HARVEST v2 DONE"
