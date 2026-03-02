param(
  [string]$Root = "."
)
$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL Explainability Contract V1 v0_1 (copies into .\\scripts\\market_radar)"

$rootPath = (Resolve-Path $Root).Path
$srcDir = Join-Path $PSScriptRoot "scripts\market_radar"
$dstDir = Join-Path $rootPath "scripts\market_radar"

New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

Copy-Item -Force (Join-Path $srcDir "build_explainability_contract_v1.py") (Join-Path $dstDir "build_explainability_contract_v1.py")
Copy-Item -Force (Join-Path $srcDir "freeze_market_radar_explainability_currents_v0_1.py") (Join-Path $dstDir "freeze_market_radar_explainability_currents_v0_1.py")
Copy-Item -Force (Join-Path $srcDir "Run-Explainability-Contract-V1-v0_1_PS51SAFE.ps1") (Join-Path $dstDir "Run-Explainability-Contract-V1-v0_1_PS51SAFE.ps1")
Copy-Item -Force (Join-Path $srcDir "Run-Freeze-Explainability-Contract-V1-CURRENT-v0_1_PS51SAFE.ps1") (Join-Path $dstDir "Run-Freeze-Explainability-Contract-V1-CURRENT-v0_1_PS51SAFE.ps1")

Write-Host "[ok] installed scripts/market_radar/build_explainability_contract_v1.py"
Write-Host "[ok] installed scripts/market_radar/freeze_market_radar_explainability_currents_v0_1.py"
Write-Host "[ok] installed scripts/market_radar/Run-Explainability-Contract-V1-v0_1_PS51SAFE.ps1"
Write-Host "[ok] installed scripts/market_radar/Run-Freeze-Explainability-Contract-V1-CURRENT-v0_1_PS51SAFE.ps1"
Write-Host "[done] INSTALL complete"
