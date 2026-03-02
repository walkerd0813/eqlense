Param(
  [Parameter(Mandatory=$false)][string]$BackendRoot = (Get-Location).Path
)

$ErrorActionPreference = 'Stop'

Write-Host "[start] INSTALL arms-length classifier v1_0" -ForegroundColor Cyan

$src = Join-Path $PSScriptRoot "scripts\phase5"
$dst = Join-Path $BackendRoot "scripts\phase5"

New-Item -ItemType Directory -Force -Path $dst | Out-Null

Copy-Item (Join-Path $src "arms_length_classify_v1_0.py") $dst -Force
Copy-Item (Join-Path $src "Run-ArmsLength-Classify-v1_0_PS51SAFE.ps1") $dst -Force

Write-Host "[ok] installed scripts/phase5/arms_length_classify_v1_0.py" -ForegroundColor Green
Write-Host "[ok] installed scripts/phase5/Run-ArmsLength-Classify-v1_0_PS51SAFE.ps1" -ForegroundColor Green
Write-Host "[done] INSTALL complete" -ForegroundColor Green
