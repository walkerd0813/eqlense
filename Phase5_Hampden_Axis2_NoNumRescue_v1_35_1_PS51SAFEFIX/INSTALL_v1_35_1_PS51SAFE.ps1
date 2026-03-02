$ErrorActionPreference = "Stop"
Write-Host "[start] INSTALL v1_35_1 (copies into .\scripts\phase5)"

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$srcPy  = Join-Path $here "scripts\phase5\hampden_axis2_nonum_rescue_v1_35_1.py"
$srcRun = Join-Path $here "scripts\phase5\Run-Hampden-Axis2-NoNumRescue-v1_35_1_PS51SAFE.ps1"
$srcRead= Join-Path $here "scripts\phase5\README_v1_35_1.txt"

$dstDir = "scripts\phase5"
if (!(Test-Path $dstDir)) { New-Item -ItemType Directory -Force -Path $dstDir | Out-Null }

Copy-Item $srcPy   (Join-Path $dstDir "hampden_axis2_nonum_rescue_v1_35_1.py") -Force
Copy-Item $srcRun  (Join-Path $dstDir "Run-Hampden-Axis2-NoNumRescue-v1_35_1_PS51SAFE.ps1") -Force
Copy-Item $srcRead (Join-Path $dstDir "README_v1_35_1.txt") -Force

Write-Host "[ok] installed scripts/phase5/hampden_axis2_nonum_rescue_v1_35_1.py"
Write-Host "[ok] installed scripts/phase5/Run-Hampden-Axis2-NoNumRescue-v1_35_1_PS51SAFE.ps1"
Write-Host "[ok] installed scripts/phase5/README_v1_35_1.txt"
Write-Host "[done] INSTALL v1_35_1 complete"
