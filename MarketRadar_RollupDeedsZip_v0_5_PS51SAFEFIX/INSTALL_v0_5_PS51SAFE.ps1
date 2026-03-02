param(
  [string]$BackendRoot = ""
)

$ErrorActionPreference = "Stop"

if (-not $BackendRoot -or $BackendRoot.Trim().Length -eq 0) {
  $BackendRoot = (Get-Location).Path
}

Write-Host "[start] INSTALL Market Radar deeds ZIP rollup v0_5 (copies into .\scripts\market_radar)"

$srcPy   = Join-Path $PSScriptRoot "scripts\market_radar\rollup_deeds_zip_v0_5.py"
$srcRun  = Join-Path $PSScriptRoot "scripts\market_radar\Run-Rollup-Deeds-Zip-v0_5_PS51SAFE.ps1"

$dstDir  = Join-Path $BackendRoot "scripts\market_radar"
if (-not (Test-Path $dstDir)) { New-Item -ItemType Directory -Path $dstDir | Out-Null }

Copy-Item $srcPy  (Join-Path $dstDir "rollup_deeds_zip_v0_5.py") -Force
Copy-Item $srcRun (Join-Path $dstDir "Run-Rollup-Deeds-Zip-v0_5_PS51SAFE.ps1") -Force

Write-Host "[ok] installed rollup_deeds_zip_v0_5 + runner into .\scripts\market_radar"
Write-Host "[done] INSTALL complete"
