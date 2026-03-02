[CmdletBinding()]
param(
  [string]$ZoningRoot = ".\publicData\zoning"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $ZoningRoot)) { throw "Missing zoning root: $ZoningRoot" }

Write-Host "====================================================="
Write-Host "[zoningBaseShow] START $(Get-Date -Format o)"
Write-Host "[zoningBaseShow] zoningRoot: $ZoningRoot"
Write-Host "====================================================="

$townDirs = Get-ChildItem -Path $ZoningRoot -Directory -ErrorAction SilentlyContinue

$rows = foreach ($td in $townDirs) {
  $town = $td.Name
  $districtsDir = Join-Path $td.FullName "districts"

  if (-not (Test-Path $districtsDir)) {
    [pscustomobject]@{
      town = $town
      hasDistrictsDir = $false
      hasZoningBase = $false
      zoningBasePath = $null
      geojsonCount = 0
    }
    continue
  }

  $zBase = Join-Path $districtsDir "zoning_base.geojson"
  $geo = @(Get-ChildItem -Path $districtsDir -Filter "*.geojson" -File -ErrorAction SilentlyContinue)

  [pscustomobject]@{
    town = $town
    hasDistrictsDir = $true
    hasZoningBase = (Test-Path $zBase)
    zoningBasePath = (if (Test-Path $zBase) { (Resolve-Path $zBase).Path } else { $null })
    geojsonCount = $geo.Count
  }
}

$rows | Sort-Object town | Format-Table -AutoSize

Write-Host "====================================================="
Write-Host "[zoningBaseShow] DONE  $(Get-Date -Format o)"
Write-Host "====================================================="
