param([string]$Root=(Get-Location).Path)
$ErrorActionPreference="Stop"
Write-Host "[start] INSTALL Indicators Engine v0_1 (copies into .\scripts\market_radar\indicators + runners)"
$src=Join-Path $PSScriptRoot "scripts"
$dstInd=Join-Path $Root "scripts\market_radar\indicators"
$dstMR=Join-Path $Root "scripts\market_radar"
New-Item -ItemType Directory -Force -Path $dstInd,$dstMR | Out-Null
Copy-Item -Force (Join-Path $src "market_radar\indicators\*.py") $dstInd
Copy-Item -Force (Join-Path $src "market_radar\Run-Indicators-Contract-V1-v0_1_PS51SAFE.ps1") $dstMR
Copy-Item -Force (Join-Path $src "market_radar\Run-Indicators-P01-v0_1_PS51SAFE.ps1") $dstMR
Copy-Item -Force (Join-Path $src "market_radar\Run-Freeze-Indicators-P01-CURRENT-v0_1_PS51SAFE.ps1") $dstMR
Copy-Item -Force (Join-Path $src "market_radar\Run-MarketRadar-Indicators-Daily_v0_1_PS51SAFE.ps1") $dstMR
Write-Host "[ok] installed indicators engine files"
Write-Host "[done] INSTALL complete"
