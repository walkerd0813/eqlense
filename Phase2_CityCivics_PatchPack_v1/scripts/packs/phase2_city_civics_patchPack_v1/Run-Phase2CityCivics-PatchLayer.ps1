param(
  [Parameter(Mandatory=$false)]
  [string]$Root = "C:\seller-app\backend",

  [Parameter(Mandatory=$true)]
  [string]$LayerKey,

  [Parameter(Mandatory=$true)]
  [string]$Source,

  [Parameter(Mandatory=$false)]
  [switch]$UpdatePointers
)

$ErrorActionPreference = "Stop"

Write-Host "===================================================="
Write-Host "  PHASE 2 — CITY CIVICS PATCH ONE LAYER (v1)"
Write-Host "===================================================="

$scriptPath = Join-Path $PSScriptRoot "patch_phase2_cityCivics_layer_v1.mjs"
if (!(Test-Path $scriptPath)) { throw ("Missing script: {0}" -f $scriptPath) }

$update = "false"
if ($UpdatePointers) { $update = "true" }

Write-Host ("[info] Root: {0}" -f $Root)
Write-Host ("[info] LayerKey: {0}" -f $LayerKey)
Write-Host ("[info] Source: {0}" -f $Source)
Write-Host ("[info] UpdatePointers: {0}" -f $update)

$cmd = "node `"$scriptPath`" --root `"$Root`" --layerKey `"$LayerKey`" --source `"$Source`" --updatePointers $update"
Write-Host ("[run] {0}" -f $cmd)

Invoke-Expression $cmd

if ($LASTEXITCODE -ne 0) {
  throw ("Node patch failed with exit code {0}" -f $LASTEXITCODE)
}

Write-Host "[done] Phase 2 patch layer completed."
