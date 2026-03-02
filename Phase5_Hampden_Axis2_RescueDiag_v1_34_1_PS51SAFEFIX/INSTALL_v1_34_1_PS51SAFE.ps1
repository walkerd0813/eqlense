$ErrorActionPreference = "Stop"

Write-Host "[start] INSTALL v1_34_1 (copies into .\scripts\phase5)"
$srcDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$srcPhase5 = Join-Path $srcDir "scripts\phase5"
$dstPhase5 = Join-Path (Get-Location) "scripts\phase5"

if (!(Test-Path $dstPhase5)) { New-Item -ItemType Directory -Path $dstPhase5 | Out-Null }

Copy-Item -Path (Join-Path $srcPhase5 "hampden_axis2_rescue_diagnostics_v1_34_1.py") -Destination (Join-Path $dstPhase5 "hampden_axis2_rescue_diagnostics_v1_34_1.py") -Force
Write-Host "[ok] installed scripts/phase5/hampden_axis2_rescue_diagnostics_v1_34_1.py"

Copy-Item -Path (Join-Path $srcPhase5 "Run-Hampden-Axis2-RescueDiagnostics-v1_34_1_PS51SAFE.ps1") -Destination (Join-Path $dstPhase5 "Run-Hampden-Axis2-RescueDiagnostics-v1_34_1_PS51SAFE.ps1") -Force
Write-Host "[ok] installed scripts/phase5/Run-Hampden-Axis2-RescueDiagnostics-v1_34_1_PS51SAFE.ps1"

Write-Host "[done] INSTALL v1_34_1 complete"
