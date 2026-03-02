# PS51SAFE installer: copies Phase5 Hampden Axis2 PostMatch v1_33_0 into .\scripts\phase5
param(
  [string]$TargetRoot = (Get-Location).Path
)

$ErrorActionPreference = 'Stop'

function Copy-One($srcRel, $dstRel) {
  $src = Join-Path $PSScriptRoot $srcRel
  $dst = Join-Path $TargetRoot $dstRel
  $dstDir = Split-Path -Parent $dst
  if (!(Test-Path $src)) { throw "Missing source file: $src" }
  if (!(Test-Path $dstDir)) { New-Item -ItemType Directory -Force -Path $dstDir | Out-Null }

  $bak = $null
  if (Test-Path $dst) {
    $bak = ($dst + '.bak_' + (Get-Date -Format 'yyyyMMdd_HHmmss'))
    Copy-Item -Path $dst -Destination $bak -Force
    Write-Host ("[backup] {0}" -f $bak)
  }

  Copy-Item -Path $src -Destination $dst -Force
  Write-Host ("[ok] installed {0}" -f $dstRel)
}

Write-Host "[start] INSTALL v1_33_0 (copies into .\\scripts\\phase5)"
Copy-One 'scripts/phase5/Run-Hampden-Axis2-PostMatch-v1_33_0_PS51SAFE.ps1' 'scripts/phase5/Run-Hampden-Axis2-PostMatch-v1_33_0_PS51SAFE.ps1'
Copy-One 'scripts/phase5/hampden_axis2_postmatch_fuzzy_range_v1_33_0.py' 'scripts/phase5/hampden_axis2_postmatch_fuzzy_range_v1_33_0.py'
Copy-One 'scripts/phase5/README_v1_33_0.txt' 'scripts/phase5/README_v1_33_0.txt'
Write-Host "[done] INSTALL v1_33_0 complete"
