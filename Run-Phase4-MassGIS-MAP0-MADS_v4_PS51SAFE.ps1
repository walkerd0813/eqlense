$ErrorActionPreference = 'Stop'
$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
$NODE = 'node'
$CONFIG = Join-Path $ROOT 'phase4_massgis_map0_mads_config_v4.json'
$SCRIPT = Join-Path $ROOT 'scripts\phase4_assessor\phase4_massgis_map0_mads_build_master_v4.mjs'

Write-Host '[start] Phase 4 — MassGIS MAP0 MADS master build (v4)'
Write-Host ('[info] root: ' + $ROOT)
Write-Host ('[info] config: ' + $CONFIG)
Write-Host ('[info] node: ' + $NODE)

& $NODE $SCRIPT --root $ROOT --config $CONFIG
if ($LASTEXITCODE -ne 0) { throw ('[err] node script failed with exit code ' + $LASTEXITCODE) }

Write-Host '[done] MassGIS MAP0 MADS master build (v4) complete.'
