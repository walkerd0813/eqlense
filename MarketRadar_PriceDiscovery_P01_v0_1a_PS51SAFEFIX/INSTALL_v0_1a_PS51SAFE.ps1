param()
$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL Price Discovery P01 v0_1a (copies into .\scripts\market_radar)"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$srcDir = Join-Path $here "scripts\market_radar"
$dstDir = Join-Path (Get-Location) "scripts\market_radar"
New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

Copy-Item -Force (Join-Path $srcDir "build_price_discovery_p01_v0_1.py") (Join-Path $dstDir "build_price_discovery_p01_v0_1.py")
Copy-Item -Force (Join-Path $srcDir "Run-PriceDiscovery-P01-v0_1a_PS51SAFE.ps1") (Join-Path $dstDir "Run-PriceDiscovery-P01-v0_1a_PS51SAFE.ps1")

# Backward-compat: overwrite v0_1 runner name so your existing command can still work
Copy-Item -Force (Join-Path $srcDir "Run-PriceDiscovery-P01-v0_1a_PS51SAFE.ps1") (Join-Path $dstDir "Run-PriceDiscovery-P01-v0_1_PS51SAFE.ps1")

Write-Host "[ok] installed build_price_discovery_p01_v0_1.py"
Write-Host "[ok] installed Run-PriceDiscovery-P01-v0_1a_PS51SAFE.ps1 (and patched v0_1 runner alias)"
Write-Host "[done] INSTALL complete"
