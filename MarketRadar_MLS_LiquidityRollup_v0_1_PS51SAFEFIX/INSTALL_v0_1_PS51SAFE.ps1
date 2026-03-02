$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL MarketRadar MLS Liquidity Rollup v0_1 (copies into .\scripts\market_radar)"

$srcPy  = Join-Path $PSScriptRoot "scripts\market_radar\rollup_mls_liquidity_zip_v0_1.py"
$srcRun = Join-Path $PSScriptRoot "scripts\market_radar\Run-Rollup-MLS-Liquidity-Zip-v0_1_PS51SAFE.ps1"

$dstDir = ".\scripts\market_radar"
if (!(Test-Path $dstDir)) { New-Item -ItemType Directory -Path $dstDir | Out-Null }

Copy-Item $srcPy  $dstDir -Force
Copy-Item $srcRun $dstDir -Force

Write-Host "[ok] installed rollup_mls_liquidity_zip_v0_1 + runner into .\scripts\market_radar"
Write-Host "[done] INSTALL complete"
