param()

$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL Price Discovery P01 v0_1c (copies into .\scripts\market_radar)"

$root = (Get-Location).Path
$dstDir = Join-Path $root "scripts\market_radar"
New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

$srcPy  = Join-Path $PSScriptRoot "scripts\market_radar\build_price_discovery_p01_v0_1.py"
$srcRun = Join-Path $PSScriptRoot "scripts\market_radar\Run-PriceDiscovery-P01-v0_1_PS51SAFE.ps1"

Copy-Item -Force $srcPy  $dstDir
Copy-Item -Force $srcRun $dstDir

Write-Host "[ok] installed build_price_discovery_p01_v0_1.py"
Write-Host "[ok] installed Run-PriceDiscovery-P01-v0_1_PS51SAFE.ps1"
Write-Host "[done] INSTALL complete"
