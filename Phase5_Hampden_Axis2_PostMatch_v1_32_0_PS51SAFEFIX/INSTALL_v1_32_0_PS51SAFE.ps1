$ErrorActionPreference = "Stop"

$src = Join-Path $PSScriptRoot "scripts\phase5"
$dst = Join-Path (Get-Location) "scripts\phase5"

if (!(Test-Path $src)) { throw "Missing source folder: $src" }
if (!(Test-Path $dst)) { New-Item -ItemType Directory -Path $dst -Force | Out-Null }

Copy-Item -Path (Join-Path $src '*') -Destination $dst -Force

Write-Host "[ok] installed into $dst"
Write-Host "Next: run"
Write-Host "  .\scripts\phase5\Run-Hampden-Axis2-PostMatch-v1_32_0_PS51SAFE.ps1 -In \"<in.ndjson>\" -Spine \"<spine.ndjson>\" -Out \"<out.ndjson>\""
