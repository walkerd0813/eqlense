# INSTALL_v0_1_PS51SAFE.ps1
# PowerShell 5.1 safe installer for Market Radar liquidity CURRENT freezer.
param(
  [string]$Root = "."
)

$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL freeze liquidity CURRENT v0_1 (copies into .\scripts\market_radar)"

$srcPy = Join-Path $PSScriptRoot "scripts\market_radar\freeze_market_radar_liquidity_currents_v0_1.py"
$srcRun = Join-Path $PSScriptRoot "scripts\market_radar\Run-Freeze-Liquidity-Currents-v0_1_PS51SAFE.ps1"

$dstDir = Join-Path $Root "scripts\market_radar"
New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

Copy-Item -Force $srcPy (Join-Path $dstDir "freeze_market_radar_liquidity_currents_v0_1.py")
Copy-Item -Force $srcRun (Join-Path $dstDir "Run-Freeze-Liquidity-Currents-v0_1_PS51SAFE.ps1")

Write-Host "[ok] installed freeze_market_radar_liquidity_currents_v0_1.py + runner into $dstDir"
Write-Host "[done] INSTALL complete"
