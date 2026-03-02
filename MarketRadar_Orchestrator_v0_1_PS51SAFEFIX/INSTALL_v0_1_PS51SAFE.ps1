param(
  [string]$Root = (Get-Location).Path
)

$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL Market Radar Orchestrator v0_1 (copies into .\\scripts\\market_radar)"

$srcDir = Join-Path $PSScriptRoot "scripts\\market_radar"
$dstDir = Join-Path $Root "scripts\\market_radar"
New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

Copy-Item (Join-Path $srcDir "Run-MarketRadar-Daily_v0_1_PS51SAFE.ps1") $dstDir -Force
Copy-Item (Join-Path $srcDir "freeze_market_radar_pillars_currents_v0_1.py") $dstDir -Force
Copy-Item (Join-Path $srcDir "ensure_mls_current_listings_v0_1.py") $dstDir -Force

Write-Host "[ok] installed Run-MarketRadar-Daily_v0_1_PS51SAFE.ps1"
Write-Host "[ok] installed freeze_market_radar_pillars_currents_v0_1.py"
Write-Host "[ok] installed ensure_mls_current_listings_v0_1.py"
Write-Host "[done] INSTALL complete"
