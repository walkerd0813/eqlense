$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL Price Discovery P01 v0_1 (copies into .\scripts\market_radar)"

$srcPy  = Join-Path $PSScriptRoot "scripts\market_radar\build_price_discovery_p01_v0_1.py"
$srcRun = Join-Path $PSScriptRoot "scripts\market_radar\Run-PriceDiscovery-P01-v0_1_PS51SAFE.ps1"

$dstDir = Join-Path (Get-Location) "scripts\market_radar"
New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

Copy-Item -Force $srcPy  (Join-Path $dstDir "build_price_discovery_p01_v0_1.py")
Copy-Item -Force $srcRun (Join-Path $dstDir "Run-PriceDiscovery-P01-v0_1_PS51SAFE.ps1")

Write-Host "[ok] installed build_price_discovery_p01_v0_1.py"
Write-Host "[ok] installed Run-PriceDiscovery-P01-v0_1_PS51SAFE.ps1"
Write-Host "[done] INSTALL complete"
