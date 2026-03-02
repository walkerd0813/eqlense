param(
  [string]$Root = ""
)

$ErrorActionPreference = "Stop"

function Find-BackendRoot {
  param([string]$Start)
  if ($Start -and (Test-Path $Start)) { return (Resolve-Path $Start).Path }
  $c = Get-Location
  if (Test-Path (Join-Path $c "scripts") -and Test-Path (Join-Path $c "publicData")) {
    return (Resolve-Path $c).Path
  }
  throw "Could not auto-detect backend root. Re-run with -Root C:\seller-app\backend"
}

$rootPath = Find-BackendRoot -Start $Root
Write-Host "[ok] backend root: $rootPath"

$srcPack = Join-Path $PSScriptRoot "pack"
$srcEng = Join-Path $PSScriptRoot "engine_registry_additions"

$dstPackDir = Join-Path $rootPath "scripts\watchdog\packs\REGISTRY_SUFFOLK_MIM_V1_CANON_ATTACH_PACK_v1"
$dstEngDir  = Join-Path $rootPath "scripts\governance\engine_registry_patches"

New-Item -ItemType Directory -Force -Path $dstPackDir | Out-Null
New-Item -ItemType Directory -Force -Path $dstEngDir  | Out-Null

Copy-Item -Force -Recurse (Join-Path $srcPack "*") $dstPackDir
Copy-Item -Force -Recurse (Join-Path $srcEng "*")  $dstEngDir

Write-Host "[done] Installed pack into: $dstPackDir"
Write-Host "[done] Engine registry additions placed into: $dstEngDir"
Write-Host ""
Write-Host "NEXT: append engine_registry_additions.ndjson into your canonical engine registry file."
Write-Host "If you already have an Engine Registry loader that reads scripts/governance/engine_registry_patches/*.ndjson, you are done."
