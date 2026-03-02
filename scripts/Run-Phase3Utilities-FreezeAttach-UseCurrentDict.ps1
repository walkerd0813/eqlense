param(
  [Parameter(Mandatory=$true)][string]$Root
)

$ErrorActionPreference = "Stop"
Write-Host "===================================================="
Write-Host "PHASE 3 — RUN freeze_attach USING CURRENT CURATED DICT"
Write-Host "===================================================="
Write-Host ("[info] Root: {0}" -f $Root)

$ptr = Join-Path $Root "publicData\overlays\_frozen\_dict\CURRENT_PHASE3_UTILITIES_DICT.json"
if (-not (Test-Path $ptr)) { throw ("[fatal] Missing pointer file: {0}" -f $ptr) }

$cur = (Get-Content $ptr -Raw | ConvertFrom-Json).current
if (-not $cur -or -not (Test-Path $cur)) { throw ("[fatal] Pointer current path missing: {0}" -f $cur) }

$scriptPath = Join-Path $Root "scripts\packs\phase3_utilities_pack_v1\phase3_utilities_freeze_attach_v1.mjs"
if (-not (Test-Path $scriptPath)) { throw ("[fatal] Missing Node script: {0}" -f $scriptPath) }

Write-Host ("[info] CURRENT dict: {0}" -f $cur)
Write-Host ("[run] node {0} --root ""{1}"" --dictIn ""{2}""" -f $scriptPath, $Root, $cur)

& node $scriptPath --root $Root --dictIn $cur
if ($LASTEXITCODE -ne 0) { throw ("Node script failed with exit code {0}" -f $LASTEXITCODE) }

Write-Host "[done] freeze_attach run complete."
