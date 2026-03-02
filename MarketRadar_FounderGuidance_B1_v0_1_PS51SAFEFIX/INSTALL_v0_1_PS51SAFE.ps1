param(
  [string]$Root = "."
)

$ErrorActionPreference = "Stop"
$rootAbs = (Resolve-Path $Root).Path

$srcDir = Join-Path $rootAbs "MarketRadar_FounderGuidance_B1_v0_1_PS51SAFEFIX\scripts\market_radar"
$dstDir = Join-Path $rootAbs "scripts\market_radar"

$srcDebug = Join-Path $srcDir "debug"
$dstDebug = Join-Path $dstDir "debug"

$srcContracts = Join-Path $srcDir "contracts"
$dstContracts = Join-Path $dstDir "contracts"

Write-Host "[start] INSTALL Founder Guidance B1 + Explainability V1B v0_1 (copies into .\scripts\market_radar)..."

New-Item -ItemType Directory -Force -Path $dstDir | Out-Null
New-Item -ItemType Directory -Force -Path $dstDebug | Out-Null
New-Item -ItemType Directory -Force -Path $dstContracts | Out-Null

Copy-Item -Force (Join-Path $srcDir "build_explainability_contract_v1b_v0_1.py") $dstDir
Copy-Item -Force (Join-Path $srcDir "freeze_market_radar_explainability_currents_v0_2.py") $dstDir
Copy-Item -Force (Join-Path $srcDir "Run-FounderGuidance-Contract-B1-v0_1_PS51SAFE.ps1") $dstDir
Copy-Item -Force (Join-Path $srcDir "Run-Explainability-Contract-V1B-v0_1_PS51SAFE.ps1") $dstDir
Copy-Item -Force (Join-Path $srcDir "Run-Freeze-Explainability-Contract-V1B-CURRENT-v0_1_PS51SAFE.ps1") $dstDir

Copy-Item -Force (Join-Path $srcDebug "market_radar_explainability_debug_v0_2.py") $dstDebug
Copy-Item -Force (Join-Path $srcDebug "Run-MarketRadar-Explainability-Debug_v0_2_PS51SAFE.ps1") $dstDebug

Copy-Item -Force (Join-Path $srcContracts "build_founder_guidance_contract_b1_v0_1.py") $dstContracts

Write-Host "[ok] installed Founder Guidance + Explainability V1B files"
Write-Host "[done] INSTALL complete"
