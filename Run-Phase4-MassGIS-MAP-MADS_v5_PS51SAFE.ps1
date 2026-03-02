$ErrorActionPreference = 'Stop'
$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
$NODE = 'node'
$CONFIG = Join-Path $ROOT 'phase4_massgis_map_mads_config_v5.json'
$SCRIPT = Join-Path $ROOT 'scripts\phase4_assessor\phase4_massgis_map_mads_build_master_v5.mjs'

Write-Host '[start] Phase 4 — MassGIS MAP MADS master build (v5: try layer 0 + 4, MapServer->FeatureServer fallback)'
Write-Host ('[info] root: ' + $ROOT)
Write-Host ('[info] config: ' + $CONFIG)
Write-Host ('[info] node: ' + $NODE)

& $NODE $SCRIPT --root $ROOT --config $CONFIG
if ($LASTEXITCODE -ne 0) { throw ('[err] node script failed with exit code ' + $LASTEXITCODE) }

Write-Host '[done] MassGIS MAP MADS master build (v5) complete.'
