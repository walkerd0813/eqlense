param(
  [Parameter(Mandatory=$true)][string]$Root
)

$ErrorActionPreference = "Stop"

$pkgRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "[start] INSTALL Explainability Debug v0_3b + Indicator Founder Guidance IB1 (copies into .\scripts\market_radar)..."

# Ensure target folders
$debugDir = Join-Path $Root "scripts\market_radar\debug"
$igDir    = Join-Path $Root "scripts\market_radar\indicators\contracts"

New-Item -ItemType Directory -Force -Path $debugDir | Out-Null
New-Item -ItemType Directory -Force -Path $igDir | Out-Null

# Copy debug python + runner
Copy-Item -Force (Join-Path $pkgRoot "scripts\market_radar\debug\market_radar_explainability_debug_v0_3.py") $debugDir
Copy-Item -Force (Join-Path $pkgRoot "scripts\market_radar\debug\Run-MarketRadar-Explainability-Debug_v0_3_PS51SAFE.ps1") $debugDir

# Copy indicator guidance builder + runner
Copy-Item -Force (Join-Path $pkgRoot "scripts\market_radar\indicators\contracts\build_indicator_founder_guidance_contract_ib1_v0_1.py") $igDir
Copy-Item -Force (Join-Path $pkgRoot "scripts\market_radar\indicators\contracts\Run-Build-IndicatorFounderGuidanceContract_IB1_v0_1_PS51SAFE.ps1") $igDir

Write-Host "[ok] installed v0_3b debug + IB1 contract builder files"
Write-Host "[done] INSTALL complete"
