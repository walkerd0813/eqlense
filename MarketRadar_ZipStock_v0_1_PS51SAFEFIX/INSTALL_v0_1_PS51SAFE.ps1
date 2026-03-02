$ErrorActionPreference = "Stop"
Write-Host "[start] INSTALL Market Radar ZIP stock v0_1 (copies into .\scripts\market_radar)"

$srcDir = Join-Path $PSScriptRoot "scripts\market_radar"
$dstDir = Join-Path (Get-Location) "scripts\market_radar"

if (-not (Test-Path $srcDir)) { throw "[error] missing source dir: $srcDir" }
if (-not (Test-Path $dstDir)) { New-Item -ItemType Directory -Path $dstDir | Out-Null }

Copy-Item (Join-Path $srcDir "build_zip_stock_spine_v0_1.py") $dstDir -Force
Copy-Item (Join-Path $srcDir "Run-Build-Zip-Stock-Spine-v0_1_PS51SAFE.ps1") $dstDir -Force

Write-Host "[ok] installed build_zip_stock_spine_v0_1 + runner into .\scripts\market_radar"
Write-Host "[done] INSTALL complete"
