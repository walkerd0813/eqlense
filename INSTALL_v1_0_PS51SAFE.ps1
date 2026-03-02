param(
  [string]$DestRoot = "."
)

$ErrorActionPreference = 'Stop'

Write-Host "[start] INSTALL consideration extractor v1_0 (copies into .\\scripts\\phase5)"

$dest = Join-Path $DestRoot "scripts\phase5"
New-Item -ItemType Directory -Force -Path $dest | Out-Null

$here = Split-Path -Parent $MyInvocation.MyCommand.Path

Copy-Item (Join-Path $here "consideration_extract_v1_0.py") $dest -Force
Copy-Item (Join-Path $here "Run-Consideration-Extract-v1_0_PS51SAFE.ps1") $dest -Force
Copy-Item (Join-Path $here "README_v1_0.txt") $dest -Force

Write-Host "[ok] installed scripts/phase5/consideration_extract_v1_0.py"
Write-Host "[ok] installed scripts/phase5/Run-Consideration-Extract-v1_0_PS51SAFE.ps1"
Write-Host "[ok] installed scripts/phase5/README_v1_0.txt"
Write-Host "[done] INSTALL complete"
