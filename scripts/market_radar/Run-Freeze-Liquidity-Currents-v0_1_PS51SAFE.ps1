# Run-Freeze-Liquidity-Currents-v0_1_PS51SAFE.ps1
param(
  [Parameter(Mandatory=$true)][string]$MlsRollup,
  [Parameter(Mandatory=$true)][string]$Liquidity,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [string]$Root = (Get-Location).Path
)

$ErrorActionPreference = "Stop"

Write-Host "[start] freeze Market Radar LIQUIDITY CURRENT..."
Write-Host "  mls_rollup: $MlsRollup"
Write-Host "  liquidity:  $Liquidity"
Write-Host "  as_of:      $AsOf"
Write-Host "  root:       $Root"

$py = Join-Path $Root "scripts\market_radar\freeze_market_radar_liquidity_currents_v0_1.py"

$cmd = @(
  "python", $py,
  "--mls_rollup", $MlsRollup,
  "--liquidity",  $Liquidity,
  "--as_of",      $AsOf,
  "--root",       $Root
)

Write-Host ("[run] " + ($cmd -join " "))

& python $py --mls_rollup $MlsRollup --liquidity $Liquidity --as_of $AsOf --root $Root
if ($LASTEXITCODE -ne 0) { throw "[error] freeze liquidity CURRENT failed with exit code $LASTEXITCODE" }

Write-Host "[done] freeze liquidity CURRENT complete."
