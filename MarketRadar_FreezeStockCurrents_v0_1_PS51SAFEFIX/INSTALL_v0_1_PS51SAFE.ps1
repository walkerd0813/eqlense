param(
  [string]$Root = (Get-Location).Path
)

Write-Host "[start] INSTALL MarketRadar FreezeStockCurrents v0_1..."

$srcPy = Join-Path $PSScriptRoot "scripts\market_radar\freeze_market_radar_stock_currents_v0_1.py"
$srcPs = Join-Path $PSScriptRoot "scripts\market_radar\Run-Freeze-MarketRadar-StockCURRENT-v0_1_PS51SAFE.ps1"

$dstDir = Join-Path $Root "scripts\market_radar"
if (!(Test-Path $dstDir)) { New-Item -ItemType Directory -Path $dstDir -Force | Out-Null }

Copy-Item $srcPy -Destination (Join-Path $dstDir "freeze_market_radar_stock_currents_v0_1.py") -Force
Copy-Item $srcPs -Destination (Join-Path $dstDir "Run-Freeze-MarketRadar-StockCURRENT-v0_1_PS51SAFE.ps1") -Force

Write-Host "[ok] installed python + runner into scripts\market_radar"
Write-Host "[done] INSTALL complete."
