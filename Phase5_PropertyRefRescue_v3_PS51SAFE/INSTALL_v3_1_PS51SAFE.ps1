
param(
  [string]$Root = (Get-Location).Path
)

$ErrorActionPreference = 'Stop'

Write-Host '[start] INSTALL property_ref rescue v3.1 (copies into .\scripts\_registry\postfix)' -ForegroundColor Cyan

$srcPy = Join-Path $PSScriptRoot 'scripts\_registry\postfix\unknown_propertyref_to_attachedA_v3.py'
$srcRunner = Join-Path $PSScriptRoot 'scripts\_registry\postfix\Run-Unknown-PropertyRef-To-AttachedA_v3_PS51SAFE.ps1'

$dstDir = Join-Path $Root 'scripts\_registry\postfix'
New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

Copy-Item -Force $srcPy (Join-Path $dstDir 'unknown_propertyref_to_attachedA_v3.py')
Copy-Item -Force $srcRunner (Join-Path $dstDir 'Run-Unknown-PropertyRef-To-AttachedA_v3_PS51SAFE.ps1')

Write-Host '[ok] installed scripts/_registry/postfix/unknown_propertyref_to_attachedA_v3.py' -ForegroundColor Green
Write-Host '[ok] installed scripts/_registry/postfix/Run-Unknown-PropertyRef-To-AttachedA_v3_PS51SAFE.ps1' -ForegroundColor Green

Write-Host '[done] INSTALL complete' -ForegroundColor Green
