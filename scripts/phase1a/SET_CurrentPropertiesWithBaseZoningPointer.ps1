param(
  [string]$PropertiesPath = ""
)
$ErrorActionPreference = "Stop"
. ".\scripts\_lib\Resolve-PropertiesWithBaseZoning.ps1"
$resolved = Resolve-PropertiesWithBaseZoning $PropertiesPath
if (!(Test-Path $resolved)) { throw "Resolved properties path does not exist: $resolved" }

$ptr = ".\publicData\properties\_frozen\CURRENT_PROPERTIES_WITH_BASEZONING_MA.txt"
New-Item -ItemType Directory -Force -Path (Split-Path $ptr) | Out-Null
Set-Content -Encoding UTF8 $ptr $resolved

Write-Host "[ok] pointer written: $ptr"
Write-Host ("     -> " + (Get-Content $ptr -Raw).Trim())
