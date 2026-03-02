param(
  [string]$Root = "C:\seller-app\backend"
)

$ErrorActionPreference = "Stop"

Write-Host "[start] Installing dedupe v3 patch into $Root" -ForegroundColor Cyan

$src = Join-Path $PSScriptRoot "scripts\_registry\attach\events_dedupe_by_eventid_v3.py"
$dstDir = Join-Path $Root "scripts\_registry\attach"
if (!(Test-Path $dstDir)) { New-Item -ItemType Directory -Force -Path $dstDir | Out-Null }
Copy-Item -Force $src (Join-Path $dstDir "events_dedupe_by_eventid_v3.py")
Write-Host "[ok] installed scripts/_registry/attach/events_dedupe_by_eventid_v3.py" -ForegroundColor Green

powershell -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "PATCH_PACK_SPEC_DEDUPE_v1_1_PS51SAFE.ps1") -Root $Root

Write-Host "[done] Patch install complete" -ForegroundColor Green
