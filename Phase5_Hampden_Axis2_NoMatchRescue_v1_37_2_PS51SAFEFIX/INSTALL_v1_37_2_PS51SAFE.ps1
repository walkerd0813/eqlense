\
$ErrorActionPreference = "Stop"
Write-Host "[start] INSTALL v1_37_2 (copies into .\scripts\phase5)"

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$src = Join-Path $here "scripts\phase5"
$dst = Join-Path (Get-Location) "scripts\phase5"

if (!(Test-Path $dst)) { New-Item -ItemType Directory -Path $dst -Force | Out-Null }

Copy-Item -Path (Join-Path $src "Run-Hampden-Axis2-NoMatchRescue-v1_37_2_PS51SAFE.ps1") -Destination $dst -Force
Copy-Item -Path (Join-Path $src "hampden_axis2_nomatch_rescue_v1_37_2.py") -Destination $dst -Force
Copy-Item -Path (Join-Path $src "README_v1_37_2.txt") -Destination $dst -Force

Write-Host "[ok] installed scripts/phase5/Run-Hampden-Axis2-NoMatchRescue-v1_37_2_PS51SAFE.ps1"
Write-Host "[ok] installed scripts/phase5/hampden_axis2_nomatch_rescue_v1_37_2.py"
Write-Host "[ok] installed scripts/phase5/README_v1_37_2.txt"
Write-Host "[done] INSTALL v1_37_2 complete"
