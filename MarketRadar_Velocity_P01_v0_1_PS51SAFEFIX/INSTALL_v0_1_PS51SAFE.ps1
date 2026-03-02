param(
  [Parameter(Mandatory=$false)][string]$Root = "."
)
$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL Market Radar Velocity P01 v0_1..." -ForegroundColor Cyan
$srcPy  = Join-Path $PSScriptRoot "scripts\market_radar\build_velocity_p01_v0_1.py"
$srcRun = Join-Path $PSScriptRoot "scripts\market_radar\Run-Velocity-P01-v0_1_PS51SAFE.ps1"

$dstDir = Join-Path $Root "scripts\market_radar"
New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

Copy-Item $srcPy  (Join-Path $dstDir "build_velocity_p01_v0_1.py") -Force
Copy-Item $srcRun (Join-Path $dstDir "Run-Velocity-P01-v0_1_PS51SAFE.ps1") -Force

Write-Host "[ok] installed build_velocity_p01_v0_1.py" -ForegroundColor Green
Write-Host "[ok] installed Run-Velocity-P01-v0_1_PS51SAFE.ps1" -ForegroundColor Green
Write-Host "[done] INSTALL complete." -ForegroundColor Green
