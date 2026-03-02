$ErrorActionPreference = "Stop"
$ROOT = (Get-Location).Path
$CONFIG = Join-Path $ROOT "phase4_global_master_merge_attach_config_v7.json"
$nodeScript = Join-Path $ROOT "scripts\phase4_assessor\phase4_global_master_merge_and_attach_v7.mjs"

Write-Host "[start] Phase4 GLOBAL merge+attach v7 runner"
Write-Host ("[info] root: {0}" -f $ROOT)
Write-Host ("[info] config: {0}" -f $CONFIG)
Write-Host ("[info] node script: {0}" -f $nodeScript)

if (!(Test-Path $CONFIG)) { throw ("[err] config not found: " + $CONFIG) }
if (!(Test-Path $nodeScript)) { throw ("[err] node script not found: " + $nodeScript) }

node $nodeScript --config $CONFIG
if ($LASTEXITCODE -ne 0) { throw ("[err] node script failed with exit code " + $LASTEXITCODE) }

Write-Host "[done] Phase4 GLOBAL merge+attach v7 runner complete."
