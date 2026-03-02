$ErrorActionPreference = "Stop"
Write-Host "[start] INSTALL v1_34_0 (copies into .\scripts\phase5)"
$srcRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$src = Join-Path $srcRoot "scripts\phase5"
$dst = Join-Path (Get-Location) "scripts\phase5"
if (!(Test-Path $dst)) { New-Item -ItemType Directory -Path $dst -Force | Out-Null }

Copy-Item (Join-Path $src "Run-Hampden-Axis2-PostMatch-v1_34_0_PS51SAFE.ps1") $dst -Force
Copy-Item (Join-Path $src "hampden_axis2_rescue_fuzzy_range_v1_34_0.py") $dst -Force
Copy-Item (Join-Path $src "README_v1_34_0.txt") $dst -Force

Write-Host "[ok] installed scripts/phase5/Run-Hampden-Axis2-PostMatch-v1_34_0_PS51SAFE.ps1"
Write-Host "[ok] installed scripts/phase5/hampden_axis2_rescue_fuzzy_range_v1_34_0.py"
Write-Host "[ok] installed scripts/phase5/README_v1_34_0.txt"
Write-Host "[done] INSTALL v1_34_0 complete"
