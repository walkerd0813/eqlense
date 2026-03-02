$ErrorActionPreference = "Stop"

Write-Host "[start] Phase4 assessor_best provenance upgrade (v1 runner)"

$ROOT = (Get-Location).Path
$CONFIG = Join-Path $ROOT "phase4_assessor_best_provenance_upgrade_config_v1.json"
$nodeScript = Join-Path $ROOT "scripts\phase4_assessor\phase4_assessor_best_provenance_upgrade_v1.mjs"

Write-Host ("[info] root: {0}" -f $ROOT)
Write-Host ("[info] config: {0}" -f $CONFIG)
Write-Host ("[info] node script: {0}" -f $nodeScript)

if (!(Test-Path $CONFIG)) {
  throw ("[err] config not found: " + $CONFIG + " (Did you Expand-Archive the pack into backend root?)")
}
if (!(Test-Path $nodeScript)) {
  throw ("[err] node script not found: " + $nodeScript)
}

node $nodeScript --root "$ROOT" --config "$CONFIG"
if ($LASTEXITCODE -ne 0) {
  throw ("[err] node script failed with exit code " + $LASTEXITCODE)
}

Write-Host "[done] Phase4 assessor_best provenance upgrade complete."
