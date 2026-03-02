param()

$ErrorActionPreference='Stop'
$root = 'C:\seller-app\backend'
$src = Split-Path -Parent $MyInvocation.MyCommand.Path

$dstPostfix = Join-Path $root 'scripts\_registry\postfix'
New-Item -ItemType Directory -Force -Path $dstPostfix | Out-Null

Copy-Item (Join-Path $src 'unknown_propertyref_to_attachedA_v2.py') (Join-Path $dstPostfix 'unknown_propertyref_to_attachedA_v2.py') -Force
Copy-Item (Join-Path $src 'Run-Unknown-PropertyRef-To-AttachedA_v2_PS51SAFE.ps1') (Join-Path $dstPostfix 'Run-Unknown-PropertyRef-To-AttachedA_v2_PS51SAFE.ps1') -Force

Write-Host '[ok] installed unknown_propertyref_to_attachedA_v2.py' -ForegroundColor Green
Write-Host '[ok] installed Run-Unknown-PropertyRef-To-AttachedA_v2_PS51SAFE.ps1' -ForegroundColor Green
Write-Host '[done] INSTALL complete' -ForegroundColor Green
