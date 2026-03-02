$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL v1_38_0 (copies into .\scripts\phase5)" -ForegroundColor Cyan

$srcPy  = Join-Path $PSScriptRoot "scripts\phase5\axis2_rescue_promote_unique_v1.py"
$srcRun = Join-Path $PSScriptRoot "scripts\phase5\Run-Axis2-PromoteUnique-v1_38_0_PS51SAFE.ps1"
$srcRead= Join-Path $PSScriptRoot "scripts\phase5\README_v1_38_0.txt"

$dstDir = Join-Path (Get-Location) "scripts\phase5"
if (!(Test-Path $dstDir)) { New-Item -ItemType Directory -Path $dstDir | Out-Null }

Copy-Item $srcPy   (Join-Path $dstDir "axis2_rescue_promote_unique_v1.py") -Force
Copy-Item $srcRun  (Join-Path $dstDir "Run-Axis2-PromoteUnique-v1_38_0_PS51SAFE.ps1") -Force
Copy-Item $srcRead (Join-Path $dstDir "README_v1_38_0.txt") -Force

Write-Host "[ok] installed scripts/phase5/axis2_rescue_promote_unique_v1.py"
Write-Host "[ok] installed scripts/phase5/Run-Axis2-PromoteUnique-v1_38_0_PS51SAFE.ps1"
Write-Host "[ok] installed scripts/phase5/README_v1_38_0.txt"
Write-Host "[done] INSTALL v1_38_0 complete" -ForegroundColor Green
