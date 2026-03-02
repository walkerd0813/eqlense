$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL MLS Liquidity rollup + Liquidity P01 v0_2 (PS51SAFE)..."

$root = (Get-Location).Path
$srcDir = Join-Path $PSScriptRoot "scripts\market_radar"
$dstDir = Join-Path $root "scripts\market_radar"

New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

Copy-Item (Join-Path $srcDir "rollup_mls_liquidity_zip_v0_2.py") $dstDir -Force
Copy-Item (Join-Path $srcDir "Run-Rollup-MLS-Liquidity-Zip-v0_2_PS51SAFE.ps1") $dstDir -Force
Copy-Item (Join-Path $srcDir "build_liquidity_p01_v0_2.py") $dstDir -Force
Copy-Item (Join-Path $srcDir "Run-Liquidity-P01-v0_2_PS51SAFE.ps1") $dstDir -Force

Write-Host "[ok] installed into .\scripts\market_radar"
Write-Host "[done] INSTALL complete."
