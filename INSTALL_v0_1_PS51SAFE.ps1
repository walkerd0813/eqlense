param()
$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL Market Radar Absorption P01 v0_1 (copies into .\scripts\market_radar)"

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$srcPy  = Join-Path $here "scripts\market_radar\build_absorption_p01_v0_1.py"
$srcRun = Join-Path $here "scripts\market_radar\Run-Absorption-P01-v0_1_PS51SAFE.ps1"

$dstDir = Join-Path (Get-Location) "scripts\market_radar"
if (!(Test-Path $dstDir)) { New-Item -ItemType Directory -Path $dstDir | Out-Null }

Copy-Item $srcPy  (Join-Path $dstDir "build_absorption_p01_v0_1.py") -Force
Copy-Item $srcRun (Join-Path $dstDir "Run-Absorption-P01-v0_1_PS51SAFE.ps1") -Force

Write-Host "[ok] installed build_absorption_p01_v0_1.py + runner into .\scripts\market_radar"
Write-Host "[done] INSTALL complete"
