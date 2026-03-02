# Phase4 AssessorBest tax_fy provenance fill (v6) - PS51SAFE runner
$ErrorActionPreference = "Stop"

$ROOT = (Get-Location).Path
$CONFIG = Join-Path $ROOT "phase4_assessor_best_taxfy_provenance_fill_config_v6.json"
$NODE = "node"
$NODE_SCRIPT = Join-Path $ROOT "scripts\phase4_assessor\phase4_assessor_best_taxfy_provenance_fill_v6.mjs"

Write-Host "[start] Phase4 assessor_best tax_fy provenance fill (v6 runner)"
Write-Host ("[info] root: {0}" -f $ROOT)
Write-Host ("[info] config: {0}" -f $CONFIG)
Write-Host ("[info] node script: {0}" -f $NODE_SCRIPT)

if (!(Test-Path $CONFIG)) { throw ("[err] config not found: " + $CONFIG + " (Did you Expand-Archive into C:\seller-app\backend ?)") }
if (!(Test-Path $NODE_SCRIPT)) { throw ("[err] node script not found: " + $NODE_SCRIPT) }

& $NODE $NODE_SCRIPT --config $CONFIG
if ($LASTEXITCODE -ne 0) { throw ("[err] node script failed with exit code " + $LASTEXITCODE) }

Write-Host "[done] Phase4 assessor_best tax_fy provenance fill v6 runner complete."
