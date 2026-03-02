param(
  [string]$Root = (Get-Location).Path
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL Explainability Debug v0_3 + Indicator Founder Guidance IB1 (copies into .\scripts\market_radar\debug + indicators)..."

$srcDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pkgRoot = Split-Path -Parent $srcDir

$dstDebug = Join-Path $Root "scripts\market_radar\debug"
$dstInd   = Join-Path $Root "scripts\market_radar\indicators"

New-Item -ItemType Directory -Force -Path $dstDebug | Out-Null
New-Item -ItemType Directory -Force -Path $dstInd   | Out-Null

Copy-Item -Force (Join-Path $pkgRoot "scripts\market_radar\debug\market_radar_explainability_debug_v0_3.py") $dstDebug
Copy-Item -Force (Join-Path $pkgRoot "scripts\market_radar\debug\Run-MarketRadar-Explainability-Debug_v0_3_PS51SAFE.ps1") $dstDebug
Copy-Item -Force (Join-Path $pkgRoot "scripts\market_radar\indicators\build_indicator_founder_guidance_contract_ib1_v0_1.py") $dstInd
Copy-Item -Force (Join-Path $pkgRoot "scripts\market_radar\indicators\Run-IndicatorFounderGuidance-Contract-IB1_v0_1_PS51SAFE.ps1") $dstInd

Write-Host "[ok] installed debug + indicator guidance scripts"
Write-Host "[done] INSTALL complete"
