param(
  [Parameter(Mandatory=$true)][string]$Stock,
  [Parameter(Mandatory=$true)][string]$AsOf,
  [string]$Root = (Get-Location).Path
)

Write-Host "[start] freeze Market Radar STOCK CURRENT..."
Write-Host ("  stock:  {0}" -f $Stock)
Write-Host ("  as_of:  {0}" -f $AsOf)
Write-Host ("  root:   {0}" -f $Root)

$py = Join-Path $Root "scripts\market_radar\freeze_market_radar_stock_currents_v0_1.py"

if (!(Test-Path $py)) {
  throw "[error] missing python script: $py"
}

$cmd = @(
  "python", $py,
  "--stock", $Stock,
  "--as_of", $AsOf,
  "--root", $Root
)

Write-Host ("[run] {0}" -f ($cmd -join " "))
& $cmd[0] $cmd[1..($cmd.Count-1)]

if ($LASTEXITCODE -ne 0) {
  throw "[error] freeze failed with exit code $LASTEXITCODE"
}

Write-Host "[done] freeze stock CURRENT complete."
