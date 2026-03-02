# Phase 4 - assessor_best tax_fy provenance upgrade (v3)
# PowerShell 5.1 safe runner

$ErrorActionPreference = 'Stop'

$ROOT = (Get-Location).Path
$CONFIG = Join-Path $ROOT 'phase4_assessor_best_provenance_upgrade_config_v4.json'
$NODE = 'node'
$NODE_SCRIPT = Join-Path $ROOT 'scripts\phase4_assessor\phase4_assessor_best_provenance_upgrade_v4.mjs'

Write-Host "[start] Phase4 assessor_best tax_fy provenance upgrade (v4 runner)"
Write-Host ("[info] root: {0}" -f $ROOT)
Write-Host ("[info] config: {0}" -f $CONFIG)
Write-Host ("[info] node script: {0}" -f $NODE_SCRIPT)

if (-not (Test-Path $CONFIG)) { throw "[err] config not found: $CONFIG  (Did you Expand-Archive the pack into backend root?)" }
if (-not (Test-Path $NODE_SCRIPT)) { throw "[err] node script not found: $NODE_SCRIPT" }

& $NODE $NODE_SCRIPT --config $CONFIG
if ($LASTEXITCODE -ne 0) { throw ("[err] node script failed with exit code " + $LASTEXITCODE) }

Write-Host "[done] Phase4 assessor_best tax_fy provenance upgrade v3 complete."
