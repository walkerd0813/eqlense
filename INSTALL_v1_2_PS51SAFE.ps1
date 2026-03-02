# INSTALL_v1_2_PS51SAFE.ps1
# Copies scripts into ./scripts/phase5
$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL arms-length classifier v1_2"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$srcDir = Join-Path $here "payload\scripts\phase5"
$dstDir = Join-Path (Get-Location) "scripts\phase5"

New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

Copy-Item -Path (Join-Path $srcDir "arms_length_classify_v1_2.py") -Destination (Join-Path $dstDir "arms_length_classify_v1_2.py") -Force
Copy-Item -Path (Join-Path $srcDir "Run-ArmsLength-Classify-v1_2_PS51SAFE.ps1") -Destination (Join-Path $dstDir "Run-ArmsLength-Classify-v1_2_PS51SAFE.ps1") -Force

Write-Host "[ok] installed scripts/phase5/arms_length_classify_v1_2.py"
Write-Host "[ok] installed scripts/phase5/Run-ArmsLength-Classify-v1_2_PS51SAFE.ps1"
Write-Host "[done] INSTALL complete"
