param(
  [Parameter(Mandatory=$true)][string]$Root,
  [switch]$NoAttach
)

$ErrorActionPreference = "Stop"

Write-Host "===================================================="
Write-Host "  PHASE 3 — UTILITIES FREEZE + ATTACH (v1)"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] NoAttach: {0}" -f ([bool]$NoAttach))

$scriptPath = Join-Path $Root "scripts\packs\phase3_utilities_pack_v1\phase3_utilities_freeze_attach_v1.mjs"

if (-not (Test-Path $scriptPath)) {
  throw "Missing Node script: $scriptPath"
}

$args = @("--root", $Root)
if ($NoAttach) { $args += @("--noAttach","true") }

Write-Host ("[run] node {0} {1}" -f $scriptPath, ($args -join " "))
& node $scriptPath @args
if ($LASTEXITCODE -ne 0) { throw ("Node script failed with exit code {0}" -f $LASTEXITCODE) }

Write-Host "[done] Phase 3 utilities pack completed."
